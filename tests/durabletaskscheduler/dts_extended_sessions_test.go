package durabletaskscheduler_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// sessionTracker observes extended-session streams and can inject faults.
// The fault switches are one-shot.
type sessionTracker struct {
	acks                atomic.Int32
	entityAcks          atomic.Int32
	open                atomic.Int32
	maxOpen             atomic.Int32
	unaryOrchestrations atomic.Int32
	releases            atomic.Int32

	dropAck       atomic.Bool
	cutBatch      atomic.Bool
	breakWhenIdle atomic.Bool

	mu    sync.Mutex
	codes map[codes.Code]int
}

func newSessionTracker() *sessionTracker {
	return &sessionTracker{codes: make(map[codes.Code]int)}
}

func (tr *sessionTracker) recordCode(err error) {
	if err == nil || errors.Is(err, io.EOF) {
		return
	}
	tr.mu.Lock()
	tr.codes[status.Code(err)]++
	tr.mu.Unlock()
}

func (tr *sessionTracker) code(code codes.Code) int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.codes[code]
}

func (tr *sessionTracker) streamInterceptor() grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		stream, err := streamer(ctx, desc, cc, method, opts...)
		entity := method == protos.TaskHubSidecarService_CompleteEntityTaskWithExtendedSession_FullMethodName
		if err != nil || (!entity && method != protos.TaskHubSidecarService_CompleteOrchestratorTaskWithExtendedSession_FullMethodName) {
			tr.recordCode(err)
			return stream, err
		}
		open := tr.open.Add(1)
		for {
			current := tr.maxOpen.Load()
			if open <= current || tr.maxOpen.CompareAndSwap(current, open) {
				break
			}
		}
		tracked := &trackedSessionStream{ClientStream: stream, tracker: tr, entity: entity}
		context.AfterFunc(ctx, tracked.finish)
		return tracked, nil
	}
}

func (tr *sessionTracker) unaryInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		switch method {
		case protos.TaskHubSidecarService_CompleteOrchestratorTask_FullMethodName:
			tr.unaryOrchestrations.Add(1)
		case protos.TaskHubSidecarService_ReleaseTaskOrchestratorWorkItem_FullMethodName,
			protos.TaskHubSidecarService_ReleaseTaskEntityWorkItem_FullMethodName:
			tr.releases.Add(1)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

type trackedSessionStream struct {
	grpc.ClientStream
	tracker *sessionTracker
	entity  bool
	once    sync.Once
	broken  bool
}

func (s *trackedSessionStream) finish() {
	s.once.Do(func() { s.tracker.open.Add(-1) })
}

func (s *trackedSessionStream) fail(message string) error {
	s.finish()
	err := status.Error(codes.Unavailable, message)
	s.tracker.recordCode(err)
	return err
}

func (s *trackedSessionStream) RecvMsg(m any) error {
	if s.broken {
		return s.fail("injected: session stream broke")
	}
	err := s.ClientStream.RecvMsg(m)
	if err != nil {
		s.finish()
		s.tracker.recordCode(err)
		return err
	}
	response := m.(*protos.CompleteTaskWithExtendedSessionResponse)
	switch {
	case response.GetAck() != nil:
		if s.tracker.dropAck.CompareAndSwap(true, false) {
			return s.fail("injected: acknowledgement lost")
		}
		if s.entity {
			s.tracker.entityAcks.Add(1)
		} else {
			s.tracker.acks.Add(1)
		}
		s.broken = s.tracker.breakWhenIdle.CompareAndSwap(true, false)
	case response.GetNewMessages() != nil:
		// Deliver the first chunk, then break the stream before the batch ends.
		if s.tracker.cutBatch.CompareAndSwap(true, false) {
			s.broken = true
		}
	}
	return nil
}

func startSessionEmulatorWorker(
	t *testing.T,
	registry *task.TaskRegistry,
	tracker *sessionTracker,
	sessions *durabletaskclient.ExtendedSessionOptions,
	extra ...durabletaskclient.TaskHubGrpcWorkerOption,
) *durabletaskscheduler.Client {
	t.Helper()
	options := emulatorOptions(t)
	options.StreamInterceptors = append(slices.Clone(options.StreamInterceptors), tracker.streamInterceptor())
	options.UnaryInterceptors = append(slices.Clone(options.UnaryInterceptors), tracker.unaryInterceptor())
	workerOptions := []durabletaskclient.TaskHubGrpcWorkerOption{
		durabletaskclient.WithAutoWorkItemFilters(),
		durabletaskclient.WithMaxConcurrentOrchestrationWorkItems(8),
		durabletaskclient.WithMaxConcurrentActivityWorkItems(8),
		durabletaskclient.WithMaxConcurrentEntityWorkItems(8),
		durabletaskclient.WithWorkerSilentDisconnectTimeout(15 * time.Second),
	}
	if sessions != nil {
		workerOptions = append(workerOptions, durabletaskclient.WithExtendedSessions(*sessions))
	}
	client, _ := startEmulatorWithOptions(t, options, registry, append(workerOptions, extra...)...)
	return client
}

var (
	sessionProbeOnce      sync.Once
	sessionProbeSupported bool
	sessionProbeErr       error
)

// requireExtendedSessions skips the test when the endpoint does not support
// extended sessions, for example the public emulator before the feature.
func requireExtendedSessions(t *testing.T) {
	t.Helper()
	emulatorOptions(t)
	sessionProbeOnce.Do(func() {
		registry := task.NewTaskRegistry()
		if err := registry.AddActivityN("SessionProbeActivity", func(task.ActivityContext) (any, error) { return nil, nil }); err != nil {
			sessionProbeErr = err
			return
		}
		if err := registry.AddOrchestratorN("SessionProbe", func(ctx *task.OrchestrationContext) (any, error) {
			return nil, ctx.CallActivity("SessionProbeActivity").Await(nil)
		}); err != nil {
			sessionProbeErr = err
			return
		}
		tracker := newSessionTracker()
		client := startSessionEmulatorWorker(t, registry, tracker, &durabletaskclient.ExtendedSessionOptions{})
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		id, err := client.ScheduleNewOrchestration(ctx, "SessionProbe")
		if err != nil {
			sessionProbeErr = err
			return
		}
		if _, err := client.WaitForOrchestrationCompletion(ctx, id); err != nil {
			sessionProbeErr = err
			return
		}
		sessionProbeSupported = tracker.acks.Load() > 0
		if !sessionProbeSupported && tracker.code(codes.Unimplemented) == 0 {
			sessionProbeErr = fmt.Errorf("the session probe saw neither an acknowledgement nor Unimplemented")
		}
	})
	require.NoError(t, sessionProbeErr)
	if !sessionProbeSupported {
		t.Skip("the DTS endpoint returns Unimplemented for extended sessions")
	}
}

func sessionActivityChain(name string, starts *atomic.Int32, steps int) (*task.TaskRegistry, error) {
	registry := task.NewTaskRegistry()
	if err := registry.AddActivityN(name+"Step", func(ctx task.ActivityContext) (any, error) {
		var input int
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return input + 1, nil
	}); err != nil {
		return nil, err
	}
	err := registry.AddOrchestratorN(name, func(ctx *task.OrchestrationContext) (any, error) {
		starts.Add(1)
		value := 0
		for range steps {
			if err := ctx.CallActivity(name+"Step", task.WithActivityInput(value)).Await(&value); err != nil {
				return nil, err
			}
		}
		return value, nil
	})
	return registry, err
}

func runSessionOrchestration(
	t *testing.T,
	client *durabletaskscheduler.Client,
	name string,
	opts ...api.NewOrchestrationOptions,
) *api.OrchestrationMetadata {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	id, err := client.ScheduleNewOrchestration(ctx, name, opts...)
	require.NoError(t, err)
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	return metadata
}

func requireCompletedValue(t *testing.T, metadata *api.OrchestrationMetadata, want int) {
	t.Helper()
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus,
		"failure: %v", metadata.FailureDetails)
	var got int
	require.NoError(t, metadata.ReadOutput(&got))
	require.Equal(t, want, got)
}

// Scenario 1: a multi-turn session, then a terminal unary completion.
func TestDTSExtendedSessionMultiTurn(t *testing.T) {
	requireExtendedSessions(t)
	starts := new(atomic.Int32)
	registry, err := sessionActivityChain("SessionMultiTurn", starts, 4)
	require.NoError(t, err)
	tracker := newSessionTracker()
	client := startSessionEmulatorWorker(t, registry, tracker, &durabletaskclient.ExtendedSessionOptions{})

	requireCompletedValue(t, runSessionOrchestration(t, client, "SessionMultiTurn"), 4)
	require.EqualValues(t, 1, starts.Load(), "the session path must run every turn without a replay")
	require.GreaterOrEqual(t, tracker.acks.Load(), int32(4))
	require.GreaterOrEqual(t, tracker.unaryOrchestrations.Load(), int32(1))
}

// Scenario 2: a session, continue-as-new, and a terminal completion.
func TestDTSExtendedSessionContinueAsNew(t *testing.T) {
	requireExtendedSessions(t)
	starts := new(atomic.Int32)
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddActivityN("SessionCANStep", func(task.ActivityContext) (any, error) { return nil, nil }))
	require.NoError(t, registry.AddOrchestratorN("SessionCAN", func(ctx *task.OrchestrationContext) (any, error) {
		starts.Add(1)
		var generation int
		if err := ctx.GetInput(&generation); err != nil {
			return nil, err
		}
		for range 2 {
			if err := ctx.CallActivity("SessionCANStep").Await(nil); err != nil {
				return nil, err
			}
		}
		if generation < 2 {
			ctx.ContinueAsNew(generation + 1)
			return nil, nil
		}
		return generation, nil
	}))
	tracker := newSessionTracker()
	client := startSessionEmulatorWorker(t, registry, tracker, &durabletaskclient.ExtendedSessionOptions{})

	requireCompletedValue(t, runSessionOrchestration(t, client, "SessionCAN", api.WithInput(0)), 2)
	require.EqualValues(t, 3, starts.Load(), "each execution must start once and then continue in its session")
	require.GreaterOrEqual(t, tracker.acks.Load(), int32(6))
}

// Scenarios 3, 4 and 5: lost acknowledgement, a batch cut off after its
// first chunk, and a session stream that breaks while idle. The worker
// recovers with a full replay and the result is the same.
func TestDTSExtendedSessionRecovery(t *testing.T) {
	requireExtendedSessions(t)
	tests := []struct {
		name   string
		inject func(*sessionTracker)
		check  func(*testing.T, *sessionTracker)
	}{
		{
			name:   "lost acknowledgement",
			inject: func(tr *sessionTracker) { tr.dropAck.Store(true) },
			check: func(t *testing.T, tr *sessionTracker) {
				require.False(t, tr.dropAck.Load(), "the fault was not injected")
				require.GreaterOrEqual(t, tr.code(codes.NotFound), 1, "a retry with the old count must get NotFound")
			},
		},
		{
			name:   "batch cut off after the first chunk",
			inject: func(tr *sessionTracker) { tr.cutBatch.Store(true) },
			check: func(t *testing.T, tr *sessionTracker) {
				require.False(t, tr.cutBatch.Load(), "the fault was not injected")
				require.GreaterOrEqual(t, tr.releases.Load(), int32(1))
			},
		},
		{
			name:   "stream breaks while idle",
			inject: func(tr *sessionTracker) { tr.breakWhenIdle.Store(true) },
			check: func(t *testing.T, tr *sessionTracker) {
				require.False(t, tr.breakWhenIdle.Load(), "the fault was not injected")
				require.GreaterOrEqual(t, tr.releases.Load(), int32(1))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name := "SessionRecovery" + strings.ReplaceAll(strings.ToUpper(tt.name[:1])+tt.name[1:], " ", "")
			starts := new(atomic.Int32)
			registry, err := sessionActivityChain(name, starts, 3)
			require.NoError(t, err)
			tracker := newSessionTracker()
			tt.inject(tracker)
			client := startSessionEmulatorWorker(t, registry, tracker, &durabletaskclient.ExtendedSessionOptions{})
			requireCompletedValue(t, runSessionOrchestration(t, client, name), 3)
			tt.check(t, tracker)
			require.Greater(t, starts.Load(), int32(1), "recovery must use a full replay")
		})
	}
}

// Scenario 7: the worker releases idle sessions while new messages arrive.
func TestDTSExtendedSessionReleaseRace(t *testing.T) {
	requireExtendedSessions(t)
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("SessionReleaseRace", func(ctx *task.OrchestrationContext) (any, error) {
		sum := 0
		for range 8 {
			var value int
			if err := ctx.WaitForSingleEvent("value", -1).Await(&value); err != nil {
				return nil, err
			}
			sum += value
		}
		return sum, nil
	}))
	tracker := newSessionTracker()
	client := startSessionEmulatorWorker(t, registry, tracker,
		&durabletaskclient.ExtendedSessionOptions{IdleTimeout: 100 * time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	id, err := client.ScheduleNewOrchestration(ctx, "SessionReleaseRace")
	require.NoError(t, err)
	_, err = client.WaitForOrchestrationStart(ctx, id)
	require.NoError(t, err)
	for i := 1; i <= 8; i++ {
		time.Sleep(time.Duration(40*(i%4)) * time.Millisecond)
		require.NoError(t, client.RaiseEvent(ctx, id, "value", api.WithEventPayload(i)))
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	requireCompletedValue(t, metadata, 36)
	require.GreaterOrEqual(t, tracker.releases.Load(), int32(1), "the idle timeout must have released a session")
}

// Scenario 9: entity operations across turns, a failed batch, and deletion.
func TestDTSExtendedSessionEntities(t *testing.T) {
	requireExtendedSessions(t)
	var failBatches atomic.Bool
	var failedBatches atomic.Int32
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddEntityFactoryN("SessionCounter", func(task.EntityFactoryContext) (task.EntityBatch, error) {
		if failBatches.Load() {
			failedBatches.Add(1)
			return task.EntityBatch{}, errors.New("injected entity batch failure")
		}
		return task.EntityBatch{Entity: func(ctx *task.EntityContext) (any, error) {
			if ctx.Operation == "delete" {
				ctx.DeleteState()
				return nil, nil
			}
			var value int
			if ctx.HasState() {
				if err := ctx.GetState(&value); err != nil {
					return nil, err
				}
			}
			return nil, ctx.SetState(value + 1)
		}}, nil
	}))
	tracker := newSessionTracker()
	client := startSessionEmulatorWorker(t, registry, tracker, &durabletaskclient.ExtendedSessionOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	entityID := api.NewEntityID("SessionCounter", uuid.NewString())
	requireState := func(want string) {
		t.Helper()
		require.Eventually(t, func() bool {
			metadata, err := client.GetEntity(ctx, entityID)
			if err != nil {
				return false
			}
			if want == "" {
				return metadata == nil || !metadata.HasState
			}
			return metadata != nil && metadata.SerializedState == want
		}, time.Minute, 50*time.Millisecond, "entity state did not become %q", want)
	}

	for range 3 {
		require.NoError(t, client.SignalEntity(ctx, entityID, "add"))
		time.Sleep(300 * time.Millisecond)
	}
	requireState("3")
	require.GreaterOrEqual(t, tracker.entityAcks.Load(), int32(1), "entity batches must use sessions")

	failBatches.Store(true)
	require.NoError(t, client.SignalEntity(ctx, entityID, "add"))
	require.Eventually(t, func() bool { return failedBatches.Load() > 0 }, time.Minute, 10*time.Millisecond)
	failBatches.Store(false)
	requireState("4")

	require.NoError(t, client.SignalEntity(ctx, entityID, "delete"))
	requireState("")
}

// Scenario 10: a session crosses the history limit, and the result equals a
// worker without sessions.
func TestDTSExtendedSessionHistoryLimitMatchesReplay(t *testing.T) {
	requireExtendedSessions(t)
	run := func(sessions *durabletaskclient.ExtendedSessionOptions) *api.OrchestrationMetadata {
		registry := task.NewTaskRegistry()
		require.NoError(t, registry.AddActivityN("SessionLimitStep", func(task.ActivityContext) (any, error) { return nil, nil }))
		require.NoError(t, registry.AddOrchestratorN("SessionHistoryLimit", func(ctx *task.OrchestrationContext) (any, error) {
			for {
				if err := ctx.CallActivity("SessionLimitStep").Await(nil); err != nil {
					return nil, err
				}
			}
		}))
		tracker := newSessionTracker()
		options := emulatorOptions(t)
		options.StreamInterceptors = append(slices.Clone(options.StreamInterceptors), tracker.streamInterceptor())
		workerOptions := []durabletaskclient.TaskHubGrpcWorkerOption{
			durabletaskclient.WithAutoWorkItemFilters(),
			durabletaskclient.WithMaxConcurrentOrchestrationWorkItems(4),
			durabletaskclient.WithTaskExecutorOptions(task.WithOrchestrationOptions(task.OrchestrationOptions{MaxHistoryEvents: 30})),
		}
		if sessions != nil {
			workerOptions = append(workerOptions, durabletaskclient.WithExtendedSessions(*sessions))
		}
		client, worker := startEmulatorWithOptions(t, options, registry, workerOptions...)
		metadata := runSessionOrchestration(t, client, "SessionHistoryLimit")
		if sessions != nil {
			require.Greater(t, tracker.acks.Load(), int32(1), "the first run must use sessions")
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, worker.Shutdown(shutdownCtx))
		return metadata
	}
	withSessions := run(&durabletaskclient.ExtendedSessionOptions{})
	withoutSessions := run(nil)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, withSessions.RuntimeStatus)
	require.Equal(t, withoutSessions.RuntimeStatus, withSessions.RuntimeStatus)
	require.NotNil(t, withSessions.FailureDetails)
	require.Equal(t, withoutSessions.FailureDetails.ErrorType, withSessions.FailureDetails.ErrorType)
	normalize := func(metadata *api.OrchestrationMetadata) string {
		return strings.ReplaceAll(metadata.FailureDetails.ErrorMessage, string(metadata.InstanceID), "<id>")
	}
	require.Equal(t, normalize(withoutSessions), normalize(withSessions))
}

// Scenario 11: when the stream budget is full, turns use the unary RPC, and
// sessions start again once streams are free.
func TestDTSExtendedSessionStreamBudget(t *testing.T) {
	requireExtendedSessions(t)
	const instances = 56
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("SessionBudget", func(ctx *task.OrchestrationContext) (any, error) {
		var value int
		if err := ctx.WaitForSingleEvent("go", -1).Await(&value); err != nil {
			return nil, err
		}
		return value, nil
	}))
	tracker := newSessionTracker()
	client := startSessionEmulatorWorker(t, registry, tracker, &durabletaskclient.ExtendedSessionOptions{},
		durabletaskclient.WithMaxConcurrentOrchestrationWorkItems(2*instances),
		durabletaskclient.WithWorkerCompletionConnections(1),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ids := make([]api.InstanceID, 0, instances)
	for range instances {
		id, err := client.ScheduleNewOrchestration(ctx, "SessionBudget")
		require.NoError(t, err)
		ids = append(ids, id)
	}
	require.Eventually(t, func() bool {
		return tracker.acks.Load()+tracker.unaryOrchestrations.Load() >= instances
	}, 2*time.Minute, 50*time.Millisecond)
	require.LessOrEqual(t, tracker.maxOpen.Load(), int32(50), "one connection must not exceed its session stream budget")
	require.Greater(t, tracker.unaryOrchestrations.Load(), int32(0), "turns over the budget must use the unary RPC")
	for i, id := range ids {
		require.NoError(t, client.RaiseEvent(ctx, id, "go", api.WithEventPayload(i)))
	}
	for i, id := range ids {
		metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
		require.NoError(t, err)
		requireCompletedValue(t, metadata, i)
	}

	// With free streams, a new instance uses a session again.
	acksBefore := tracker.acks.Load()
	id, err := client.ScheduleNewOrchestration(ctx, "SessionBudget")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return tracker.acks.Load() > acksBefore }, time.Minute, 20*time.Millisecond)
	require.NoError(t, client.RaiseEvent(ctx, id, "go", api.WithEventPayload(7)))
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	requireCompletedValue(t, metadata, 7)
}
