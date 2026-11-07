package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DefaultExtendedSessionTimeout is the default idle and transfer timeout of an
// extended session.
const DefaultExtendedSessionTimeout = 30 * time.Second

// defaultSessionCleanupTimeout bounds all release or abandon attempts for one
// session work item.
const defaultSessionCleanupTimeout = 30 * time.Second

// errSessionCleanupFailed ends an intake generation after a session work item
// could not be released or abandoned. Closing intake makes the service abandon
// every work item of that generation.
var errSessionCleanupFailed = errors.New("extended-session cleanup failed; reconnecting the work item stream")

// ExtendedSessionOptions configures extended sessions. In an extended session
// the worker keeps an orchestration or entity loaded in memory after a turn,
// and the service sends only the new messages for the next turn. This saves
// the replay of the full history.
//
// A waiting session keeps its concurrency slot, a service credit, and an
// open stream, the same as in Durable Functions. With a slow backend or many
// short orchestrations, waiting sessions can leave fewer slots for new work,
// so raise [WithMaxConcurrentOrchestrationWorkItems]. Leave sessions off for
// hosts that scale to zero and behind proxies that close idle streams quickly.
type ExtendedSessionOptions struct {
	// IdleTimeout is the longest time a session waits for new messages after
	// the service accepts a turn. Zero means [DefaultExtendedSessionTimeout].
	IdleTimeout time.Duration
	// TransferTimeout is the longest time from the first part of a batch to
	// the end of the batch. Zero means [DefaultExtendedSessionTimeout].
	TransferTimeout time.Duration
	// MaxOrchestrationSessions limits waiting orchestration sessions. Zero
	// means half of the orchestration concurrency limit, rounded down. The
	// value must be lower than that limit, so new work always has a slot.
	MaxOrchestrationSessions int
	// MaxEntitySessions limits waiting entity sessions in the same way.
	MaxEntitySessions int
}

// WithExtendedSessions turns on extended sessions. Sessions are off unless
// this option is set. When the service does not support sessions, the worker
// completes work items without them.
func WithExtendedSessions(options ExtendedSessionOptions) TaskHubGrpcWorkerOption {
	return func(target *taskHubGrpcWorkerOptions) error {
		if options.IdleTimeout < 0 || options.TransferTimeout < 0 {
			return fmt.Errorf("extended session timeouts cannot be negative")
		}
		if options.MaxOrchestrationSessions < 0 || options.MaxEntitySessions < 0 {
			return fmt.Errorf("extended session limits cannot be negative")
		}
		// Each worker resolves its own defaults, so it needs its own copy.
		copied := options
		if copied.IdleTimeout == 0 {
			copied.IdleTimeout = DefaultExtendedSessionTimeout
		}
		if copied.TransferTimeout == 0 {
			copied.TransferTimeout = DefaultExtendedSessionTimeout
		}
		target.extendedSessions = &copied
		return nil
	}
}

// resolveExtendedSessionLimits applies the default session limits after all
// concurrency options are known.
func (options *taskHubGrpcWorkerOptions) resolveExtendedSessionLimits() error {
	sessions := options.extendedSessions
	if sessions == nil {
		return nil
	}
	resolve := func(kind string, limit *int, concurrency int) error {
		if *limit == 0 {
			*limit = concurrency / 2
			return nil
		}
		if *limit >= concurrency {
			return fmt.Errorf("maximum %s sessions must be lower than the %s concurrency limit %d", kind, kind, concurrency)
		}
		return nil
	}
	if err := resolve("orchestration", &sessions.MaxOrchestrationSessions, options.maxConcurrentOrchestrations); err != nil {
		return err
	}
	return resolve("entity", &sessions.MaxEntitySessions, options.maxConcurrentEntities)
}

type sessionKind int

const (
	orchestrationSessionKind sessionKind = iota
	entitySessionKind
)

// sessionManager enforces the session limits of one worker run.
type sessionManager struct {
	options *ExtendedSessionOptions

	mu             sync.Mutex
	orchestrations int
	entities       int
}

// sessionLease is one session's place within the limits.
type sessionLease struct {
	manager *sessionManager
	kind    sessionKind
	once    sync.Once
}

func newSessionManager(options *ExtendedSessionOptions) *sessionManager {
	return &sessionManager{options: options}
}

// acquire returns nil when the limit for kind is reached.
func (m *sessionManager) acquire(kind sessionKind) *sessionLease {
	m.mu.Lock()
	defer m.mu.Unlock()
	count, limit := m.counter(kind)
	if *count >= limit {
		return nil
	}
	*count++
	return &sessionLease{manager: m, kind: kind}
}

func (m *sessionManager) counter(kind sessionKind) (*int, int) {
	if kind == entitySessionKind {
		return &m.entities, m.options.MaxEntitySessions
	}
	return &m.orchestrations, m.options.MaxOrchestrationSessions
}

// release returns the lease. It is safe to call on a nil lease.
func (l *sessionLease) release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		l.manager.mu.Lock()
		defer l.manager.mu.Unlock()
		count, _ := l.manager.counter(l.kind)
		*count--
	})
}

// workerSessionScope is the intake generation that dispatched a work item.
// Sessions belong to it: when its stream ends, the service abandons the work
// items, so the sessions close without a release.
type workerSessionScope struct {
	run        *grpcWorkerRun
	connection *grpcWorkerConnection
}

// sessionScopeAfterAck returns scope once the service has accepted at least
// one session turn, so the work item is in a session. Otherwise it returns
// nil, and the work item is handled like any other.
func sessionScopeAfterAck(scope *workerSessionScope, turnCount int32) *workerSessionScope {
	if turnCount == 0 {
		return nil
	}
	return scope
}

// available reports whether a new session turn may use the streaming RPC.
func (s *workerSessionScope) available() bool {
	return s != nil && s.run.sessions != nil &&
		!s.connection.sessionsUnsupported.Load() &&
		s.run.intakeCtx.Err() == nil &&
		s.connection.streamCtx.Err() == nil
}

type sessionOutcome int

const (
	// sessionBatchReady means the turn was accepted and a full batch arrived.
	sessionBatchReady sessionOutcome = iota
	// sessionUseUnary means nothing was applied; complete the same turn with
	// the unary RPC and the same count.
	sessionUseUnary
	// sessionEnded means the session is over and the work item needs no more
	// calls from this worker.
	sessionEnded
)

type sessionTimerKind int

const (
	sessionAckTimer sessionTimerKind = iota
	sessionIdleTimer
	sessionTransferTimer
)

// sessionCall describes one turn of a session completion.
type sessionCall struct {
	instanceID string
	open       func(context.Context) (grpc.ServerStreamingClient[protos.CompleteTaskWithExtendedSessionResponse], error)
	// onAck runs when the service accepts the turn.
	onAck   func(*protos.ExtendedSessionAck)
	release func(context.Context) error
	abandon func(context.Context) error
}

type sessionMessage struct {
	response *protos.CompleteTaskWithExtendedSessionResponse
	err      error
}

// awaitSessionBatch completes one turn with the streaming RPC and waits for
// the next batch. Transient errors before the acknowledgement retry the same
// request. It releases or abandons the work item itself when the session
// cannot continue.
func (w *TaskHubGrpcWorker) awaitSessionBatch(
	ctx context.Context,
	scope *workerSessionScope,
	call sessionCall,
) (sessionOutcome, []*protos.HistoryEvent) {
	for attempt := 1; ; attempt++ {
		outcome, batch, preAckErr := w.runSessionStream(ctx, scope, call)
		if preAckErr == nil {
			return outcome, batch
		}
		switch {
		case errors.Is(preAckErr, errSessionStreamBudgetFull):
			return sessionUseUnary, nil
		case status.Code(preAckErr) == codes.Unimplemented:
			w.logger.Infof("%s: the service does not support extended sessions; completing without them until the next reconnect", call.instanceID)
			scope.connection.sessionsUnsupported.Store(true)
			return sessionUseUnary, nil
		case ctx.Err() != nil || scope.connection.streamCtx.Err() != nil:
			return sessionEnded, nil
		case isTransientWorkerRPCError(preAckErr) && attempt < w.options.transientRetryMaxAttempts:
			if w.wait(ctx, w.retryDelay(attempt)) != nil {
				return sessionEnded, nil
			}
		default:
			w.logger.Warnf("%s: extended-session completion failed before the service accepted it; abandoning the work item: %v", call.instanceID, preAckErr)
			w.cleanupSessionWorkItem(ctx, scope, call.instanceID, "abandon", call.abandon)
			return sessionEnded, nil
		}
	}
}

// runSessionStream runs one streaming completion attempt. This goroutine owns
// the stream and all of its timers, so a timeout and a message cannot both
// win. A non-nil error means that the attempt failed before the service
// accepted the turn.
func (w *TaskHubGrpcWorker) runSessionStream(
	ctx context.Context,
	scope *workerSessionScope,
	call sessionCall,
) (sessionOutcome, []*protos.HistoryEvent, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := call.open(streamCtx)
	if err != nil {
		return 0, nil, err
	}
	messages := make(chan sessionMessage)
	received := make(chan struct{})
	go func() {
		defer close(received)
		for {
			response, recvErr := stream.Recv()
			select {
			case messages <- sessionMessage{response: response, err: recvErr}:
			case <-streamCtx.Done():
				return
			}
			if recvErr != nil {
				return
			}
		}
	}()
	defer func() {
		cancel()
		<-received
	}()

	timer, stopTimer := w.sessionTimer(sessionAckTimer, w.options.rpcTimeout)
	defer func() { stopTimer() }()
	restartTimer := func(kind sessionTimerKind, timeout time.Duration) {
		stopTimer()
		timer, stopTimer = w.sessionTimer(kind, timeout)
	}
	end := func(reason string, release bool) (sessionOutcome, []*protos.HistoryEvent, error) {
		cancel()
		w.logger.Debugf("%s: extended session ended: %s", call.instanceID, reason)
		if release {
			w.cleanupSessionWorkItem(ctx, scope, call.instanceID, "release", call.release)
		}
		return sessionEnded, nil, nil
	}

	acked, transferring := false, false
	var batch []*protos.HistoryEvent
	for {
		// Shutdown ends a session only while it waits.
		var stopping <-chan struct{}
		if acked && !transferring {
			stopping = scope.run.intakeCtx.Done()
		}
		select {
		case <-ctx.Done():
			return end("the worker stopped", false)
		case <-scope.connection.streamCtx.Done():
			return end("the work item stream ended", false)
		case <-stopping:
			return end("the worker is shutting down", true)
		case <-timer:
			switch {
			case !acked:
				return 0, nil, status.Error(codes.DeadlineExceeded, "the service did not acknowledge the completion in time")
			case transferring:
				return end("the batch transfer timed out", true)
			default:
				return end("no new messages arrived before the idle timeout", true)
			}
		case message := <-messages:
			if message.err != nil {
				switch {
				case !acked && errors.Is(message.err, io.EOF):
					return 0, nil, status.Error(codes.Unknown, "the session stream ended before the acknowledgement")
				case !acked:
					return 0, nil, message.err
				case transferring && errors.Is(message.err, io.EOF):
					return sessionBatchReady, batch, nil
				case errors.Is(message.err, io.EOF):
					return end("the service ended the session", false)
				default:
					return end(fmt.Sprintf("the session stream failed: %v", message.err), true)
				}
			}
			switch response := message.response.GetResponse().(type) {
			case *protos.CompleteTaskWithExtendedSessionResponse_Ack:
				if acked {
					return end("the service sent a second acknowledgement", true)
				}
				acked = true
				call.onAck(response.Ack)
				restartTimer(sessionIdleTimer, w.options.extendedSessions.IdleTimeout)
			case *protos.CompleteTaskWithExtendedSessionResponse_HealthPing:
				// A ping shows only that the connection is alive.
			case *protos.CompleteTaskWithExtendedSessionResponse_NewMessages:
				if !acked {
					return 0, nil, status.Error(codes.Internal, "the service sent messages before the acknowledgement")
				}
				if !transferring {
					transferring = true
					restartTimer(sessionTransferTimer, w.options.extendedSessions.TransferTimeout)
				}
				batch = append(batch, response.NewMessages.GetEvents()...)
			default:
				if !acked {
					return 0, nil, status.Error(codes.Internal, "the service sent an unknown session message")
				}
				return end("the service sent an unknown session message", true)
			}
		}
	}
}

func (w *TaskHubGrpcWorker) sessionTimer(kind sessionTimerKind, timeout time.Duration) (<-chan time.Time, func()) {
	if w.options.sessionTimerFn != nil {
		return w.options.sessionTimerFn(kind, timeout), func() {}
	}
	timer := time.NewTimer(timeout)
	return timer.C, func() { timer.Stop() }
}

// cleanupSessionWorkItem releases or abandons a session work item. Attempts
// stop at one absolute deadline. If that passes, the worker ends the intake
// stream, so the service abandons all work items of this generation.
func (w *TaskHubGrpcWorker) cleanupSessionWorkItem(
	ctx context.Context,
	scope *workerSessionScope,
	instanceID string,
	operation string,
	action func(context.Context) error,
) {
	cleanupCtx, cancel := context.WithTimeout(ctx, w.options.sessionCleanupTimeout)
	defer cancel()
	for attempt := 1; ; attempt++ {
		callCtx, cancelCall := context.WithTimeout(cleanupCtx, w.options.rpcTimeout)
		err := action(callCtx)
		cancelCall()
		if err == nil || isWorkItemGone(err) || ctx.Err() != nil || scope.connection.streamCtx.Err() != nil {
			return
		}
		if isTransientWorkerRPCError(err) && cleanupCtx.Err() == nil &&
			w.wait(cleanupCtx, w.retryDelay(attempt)) == nil && cleanupCtx.Err() == nil {
			continue
		}
		w.logger.Errorf("%s: failed to %s the session work item; reconnecting the work item stream: %v", instanceID, operation, err)
		scope.connection.abortForSessionCleanup()
		return
	}
}

func releaseOrchestrationOnce(client protos.TaskHubSidecarServiceClient, token string) func(context.Context) error {
	return func(ctx context.Context) error {
		_, err := client.ReleaseTaskOrchestratorWorkItem(ctx, &protos.ReleaseTaskWorkItemRequest{CompletionToken: token})
		return err
	}
}

func releaseEntityOnce(client protos.TaskHubSidecarServiceClient, token string) func(context.Context) error {
	return func(ctx context.Context) error {
		_, err := client.ReleaseTaskEntityWorkItem(ctx, &protos.ReleaseTaskWorkItemRequest{CompletionToken: token})
		return err
	}
}

func abandonOrchestrationOnce(client protos.TaskHubSidecarServiceClient, token string) func(context.Context) error {
	return func(ctx context.Context) error {
		_, err := client.AbandonTaskOrchestratorWorkItem(ctx, &protos.AbandonOrchestrationTaskRequest{CompletionToken: token})
		return err
	}
}

func abandonEntityOnce(client protos.TaskHubSidecarServiceClient, token string) func(context.Context) error {
	return func(ctx context.Context) error {
		_, err := client.AbandonTaskEntityWorkItem(ctx, &protos.AbandonEntityTaskRequest{CompletionToken: token})
		return err
	}
}
