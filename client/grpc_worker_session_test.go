package client

import (
	"context"
	"errors"
	"io"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/payload"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// fakeSessionStream is a session completion stream that the test drives.
type fakeSessionStream struct {
	grpc.ServerStreamingClient[protos.CompleteTaskWithExtendedSessionResponse]
	ctx      context.Context
	messages chan sessionMessage
}

func (s *fakeSessionStream) Recv() (*protos.CompleteTaskWithExtendedSessionResponse, error) {
	select {
	case <-s.ctx.Done():
		return nil, status.FromContextError(s.ctx.Err()).Err()
	case message := <-s.messages:
		return message.response, message.err
	}
}

func (s *fakeSessionStream) ack(count int32) {
	s.messages <- sessionMessage{response: &protos.CompleteTaskWithExtendedSessionResponse{
		Response: &protos.CompleteTaskWithExtendedSessionResponse_Ack{Ack: &protos.ExtendedSessionAck{HistoryEventCount: count}},
	}}
}

func (s *fakeSessionStream) ping() {
	s.messages <- sessionMessage{response: &protos.CompleteTaskWithExtendedSessionResponse{
		Response: &protos.CompleteTaskWithExtendedSessionResponse_HealthPing{HealthPing: &protos.HealthPing{}},
	}}
}

func (s *fakeSessionStream) chunk(events ...*protos.HistoryEvent) {
	s.messages <- sessionMessage{response: &protos.CompleteTaskWithExtendedSessionResponse{
		Response: &protos.CompleteTaskWithExtendedSessionResponse_NewMessages{NewMessages: &protos.HistoryChunk{Events: events}},
	}}
}

func (s *fakeSessionStream) end(err error) {
	if err == nil {
		err = io.EOF
	}
	s.messages <- sessionMessage{err: err}
}

// batch sends a complete batch and ends the stream with OK.
func (s *fakeSessionStream) batch(events ...*protos.HistoryEvent) {
	s.chunk(events...)
	s.end(nil)
}

type fakeSessionCall struct {
	orchestration *protos.OrchestratorResponse
	entity        *protos.EntityBatchResult
	stream        *fakeSessionStream
}

func (c *fakeSessionCall) count() int32 {
	if c.entity != nil {
		return c.entity.CompletionCount
	}
	return c.orchestration.CompletionCount
}

// sessionSchedulerClient adds the extended-session RPCs to the fake scheduler.
type sessionSchedulerClient struct {
	*fakeSchedulerClient
	calls   chan *fakeSessionCall
	openErr func() error
	// openWait runs before the stream opens and can block like a send.
	openWait   func(context.Context) error
	unaryErr   func() error
	releaseErr func() error
	abandonErr func() error

	releases           atomic.Int32
	orchestrationAbort atomic.Int32
	connects           atomic.Int32
}

func newSessionSchedulerClient() *sessionSchedulerClient {
	return &sessionSchedulerClient{
		fakeSchedulerClient: &fakeSchedulerClient{stream: newFakeWorkItemStream(16)},
		calls:               make(chan *fakeSessionCall, 16),
	}
}

func (c *sessionSchedulerClient) GetWorkItems(
	ctx context.Context,
	request *protos.GetWorkItemsRequest,
	opts ...grpc.CallOption,
) (protos.TaskHubSidecarService_GetWorkItemsClient, error) {
	c.connects.Add(1)
	return c.fakeSchedulerClient.GetWorkItems(ctx, request, opts...)
}

func (c *sessionSchedulerClient) CompleteOrchestratorTask(
	ctx context.Context,
	response *protos.OrchestratorResponse,
	opts ...grpc.CallOption,
) (*protos.CompleteTaskResponse, error) {
	if c.unaryErr != nil {
		if err := c.unaryErr(); err != nil {
			return nil, err
		}
	}
	return c.fakeSchedulerClient.CompleteOrchestratorTask(ctx, response, opts...)
}

func (c *sessionSchedulerClient) open(ctx context.Context, call *fakeSessionCall) (grpc.ServerStreamingClient[protos.CompleteTaskWithExtendedSessionResponse], error) {
	if c.openWait != nil {
		if err := c.openWait(ctx); err != nil {
			return nil, err
		}
	}
	if c.openErr != nil {
		if err := c.openErr(); err != nil {
			return nil, err
		}
	}
	call.stream = &fakeSessionStream{ctx: ctx, messages: make(chan sessionMessage, 16)}
	c.calls <- call
	return call.stream, nil
}

func (c *sessionSchedulerClient) CompleteOrchestratorTaskWithExtendedSession(
	ctx context.Context,
	response *protos.OrchestratorResponse,
	_ ...grpc.CallOption,
) (grpc.ServerStreamingClient[protos.CompleteTaskWithExtendedSessionResponse], error) {
	return c.open(ctx, &fakeSessionCall{orchestration: proto.Clone(response).(*protos.OrchestratorResponse)})
}

func (c *sessionSchedulerClient) CompleteEntityTaskWithExtendedSession(
	ctx context.Context,
	result *protos.EntityBatchResult,
	_ ...grpc.CallOption,
) (grpc.ServerStreamingClient[protos.CompleteTaskWithExtendedSessionResponse], error) {
	return c.open(ctx, &fakeSessionCall{entity: proto.Clone(result).(*protos.EntityBatchResult)})
}

func (c *sessionSchedulerClient) release() (*protos.ReleaseTaskWorkItemResponse, error) {
	c.releases.Add(1)
	if c.releaseErr != nil {
		if err := c.releaseErr(); err != nil {
			return nil, err
		}
	}
	return &protos.ReleaseTaskWorkItemResponse{}, nil
}

func (c *sessionSchedulerClient) ReleaseTaskOrchestratorWorkItem(
	context.Context,
	*protos.ReleaseTaskWorkItemRequest,
	...grpc.CallOption,
) (*protos.ReleaseTaskWorkItemResponse, error) {
	return c.release()
}

func (c *sessionSchedulerClient) ReleaseTaskEntityWorkItem(
	context.Context,
	*protos.ReleaseTaskWorkItemRequest,
	...grpc.CallOption,
) (*protos.ReleaseTaskWorkItemResponse, error) {
	return c.release()
}

func (c *sessionSchedulerClient) AbandonTaskOrchestratorWorkItem(
	ctx context.Context,
	request *protos.AbandonOrchestrationTaskRequest,
	opts ...grpc.CallOption,
) (*protos.AbandonOrchestrationTaskResponse, error) {
	c.orchestrationAbort.Add(1)
	if c.abandonErr != nil {
		if err := c.abandonErr(); err != nil {
			return nil, err
		}
	}
	return c.fakeSchedulerClient.AbandonTaskOrchestratorWorkItem(ctx, request, opts...)
}

func (c *sessionSchedulerClient) nextCall(t *testing.T) *fakeSessionCall {
	t.Helper()
	select {
	case call := <-c.calls:
		return call
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a session completion")
		return nil
	}
}

func (c *sessionSchedulerClient) noCall(t *testing.T) {
	t.Helper()
	select {
	case call := <-c.calls:
		t.Fatalf("unexpected session completion with count %d", call.count())
	default:
	}
}

func (c *sessionSchedulerClient) unaryOrchestrations() []*protos.OrchestratorResponse {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*protos.OrchestratorResponse(nil), c.orchestrationCompletions...)
}

func (c *sessionSchedulerClient) unaryEntities() []*protos.EntityBatchResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*protos.EntityBatchResult(nil), c.entityCompletions...)
}

func (c *sessionSchedulerClient) abandons() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.orchestrationAbandons
}

// fakeSessionTimers lets a test fire session timers by hand.
type fakeSessionTimers struct {
	mu      sync.Mutex
	created []sessionTimerKind
	timers  chan fakeSessionTimer
}

type fakeSessionTimer struct {
	kind sessionTimerKind
	fire chan time.Time
}

func newFakeSessionTimers() *fakeSessionTimers {
	return &fakeSessionTimers{timers: make(chan fakeSessionTimer, 64)}
}

func (f *fakeSessionTimers) new(kind sessionTimerKind, _ time.Duration) <-chan time.Time {
	timer := fakeSessionTimer{kind: kind, fire: make(chan time.Time, 1)}
	f.mu.Lock()
	f.created = append(f.created, kind)
	f.mu.Unlock()
	f.timers <- timer
	return timer.fire
}

// next returns the next created timer of the given kind.
func (f *fakeSessionTimers) next(t *testing.T, kind sessionTimerKind) fakeSessionTimer {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case timer := <-f.timers:
			if timer.kind == kind {
				return timer
			}
		case <-deadline:
			t.Fatalf("timed out waiting for session timer %d", kind)
		}
	}
}

func (f *fakeSessionTimers) count(kind sessionTimerKind) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, created := range f.created {
		if created == kind {
			n++
		}
	}
	return n
}

type sessionWorkerHarness struct {
	worker *TaskHubGrpcWorker
	client *sessionSchedulerClient
	timers *fakeSessionTimers
	starts *atomic.Int32
	cancel context.CancelFunc
}

func sessionOrchestrator(starts *atomic.Int32, activities int) task.Orchestrator {
	return func(ctx *task.OrchestrationContext) (any, error) {
		starts.Add(1)
		results := make([]string, 0, activities)
		for range activities {
			var result string
			if err := ctx.CallActivity("work").Await(&result); err != nil {
				return nil, err
			}
			results = append(results, result)
		}
		return strings.Join(results, ","), nil
	}
}

func startSessionWorker(
	t *testing.T,
	registry *task.TaskRegistry,
	client *sessionSchedulerClient,
	opts ...TaskHubGrpcWorkerOption,
) *sessionWorkerHarness {
	t.Helper()
	options := []TaskHubGrpcWorkerOption{
		WithWorkerHelloTimeout(time.Second),
		WithWorkerSilentDisconnectTimeout(10 * time.Second),
		WithWorkerRPCTimeout(time.Second),
		WithWorkerReconnectBackoff(time.Millisecond, 5*time.Millisecond),
		WithWorkerTransientRetryPolicy(3, time.Millisecond, 5*time.Millisecond),
		WithMaxConcurrentOrchestrationWorkItems(4),
		WithMaxConcurrentEntityWorkItems(4),
		WithExtendedSessions(ExtendedSessionOptions{}),
	}
	worker, err := newTaskHubGrpcWorker(
		func(context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error) {
			return client, nil, nil
		},
		registry,
		api.DefaultLogger(),
		append(options, opts...)...,
	)
	require.NoError(t, err)
	timers := newFakeSessionTimers()
	worker.options.sessionTimerFn = timers.new
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, worker.Start(ctx))
	h := &sessionWorkerHarness{worker: worker, client: client, timers: timers, cancel: cancel}
	t.Cleanup(func() {
		cancel()
		// A turn that a test never acknowledges waits for its acknowledgement
		// timer, so end the worker quickly.
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancelShutdown()
		_ = worker.Shutdown(shutdownCtx)
	})
	return h
}

func newSessionHarness(t *testing.T, activities int, opts ...TaskHubGrpcWorkerOption) *sessionWorkerHarness {
	t.Helper()
	starts := new(atomic.Int32)
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("session", sessionOrchestrator(starts, activities)))
	h := startSessionWorker(t, registry, newSessionSchedulerClient(), opts...)
	h.starts = starts
	return h
}

func (h *sessionWorkerHarness) dispatch(instanceID string, newEvents ...*protos.HistoryEvent) {
	if len(newEvents) == 0 {
		newEvents = []*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("session", instanceID, nil, nil, nil, nil),
		}
	}
	h.client.stream.results <- fakeWorkItemResult{item: &protos.WorkItem{
		Request: &protos.WorkItem_OrchestratorRequest{OrchestratorRequest: &protos.OrchestratorRequest{
			InstanceId: instanceID,
			NewEvents:  newEvents,
		}},
		CompletionToken: "token-" + instanceID,
	}}
}

func taskCompleted(id int32, result string) []*protos.HistoryEvent {
	return []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewTaskCompletedEvent(id, wrapperspb.String(`"`+result+`"`)),
	}
}

func (h *sessionWorkerHarness) waitUnary(t *testing.T, n int) []*protos.OrchestratorResponse {
	t.Helper()
	require.Eventually(t, func() bool { return len(h.client.unaryOrchestrations()) >= n }, 5*time.Second, time.Millisecond)
	return h.client.unaryOrchestrations()
}

func completedOutput(t *testing.T, response *protos.OrchestratorResponse) string {
	t.Helper()
	require.Len(t, response.Actions, 1)
	completed := response.Actions[0].GetCompleteOrchestration()
	require.NotNil(t, completed)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, completed.OrchestrationStatus,
		"failure: %v", completed.GetFailureDetails())
	return completed.GetResult().GetValue()
}

func TestWorkerSessionRunsTurnsWithoutReplay(t *testing.T) {
	h := newSessionHarness(t, 2)
	h.dispatch("multi")

	first := h.client.nextCall(t)
	require.EqualValues(t, 0, first.count())
	require.Equal(t, "token-multi", first.orchestration.CompletionToken)
	require.Equal(t, "work", first.orchestration.Actions[0].GetScheduleTask().GetName())
	first.stream.ack(4)
	first.stream.ping()
	first.stream.batch(taskCompleted(0, "a")...)

	second := h.client.nextCall(t)
	require.EqualValues(t, 1, second.count())
	require.Len(t, second.orchestration.Actions, 1)
	require.EqualValues(t, 1, second.orchestration.Actions[0].Id)
	second.stream.ack(8)
	second.stream.batch(taskCompleted(1, "b")...)

	final := h.waitUnary(t, 1)[0]
	require.EqualValues(t, 2, final.CompletionCount, "the terminal unary completion must carry the session count")
	require.Equal(t, `"a,b"`, completedOutput(t, final))
	require.EqualValues(t, 1, h.starts.Load(), "the orchestrator must not replay between session turns")
	require.Zero(t, h.client.releases.Load())
	require.Zero(t, h.client.abandons())
}

func TestWorkerSessionRetriesTransientErrorWithSameCount(t *testing.T) {
	h := newSessionHarness(t, 2)
	h.dispatch("retry")

	first := h.client.nextCall(t)
	first.stream.ack(4)
	first.stream.batch(taskCompleted(0, "a")...)

	failed := h.client.nextCall(t)
	require.EqualValues(t, 1, failed.count())
	failed.stream.end(status.Error(codes.Unavailable, "lost"))
	retried := h.client.nextCall(t)
	require.EqualValues(t, 1, retried.count(), "a retry before the acknowledgement must reuse the turn count")
	require.True(t, proto.Equal(failed.orchestration, retried.orchestration))
	retried.stream.ack(8)
	retried.stream.batch(taskCompleted(1, "b")...)

	require.EqualValues(t, 2, h.waitUnary(t, 1)[0].CompletionCount)
}

func TestWorkerSessionAbandonsOnPreAckFailure(t *testing.T) {
	for _, err := range []error{
		status.Error(codes.NotFound, "count mismatch"),
		status.Error(codes.InvalidArgument, "bad request"),
	} {
		t.Run(status.Code(err).String(), func(t *testing.T) {
			h := newSessionHarness(t, 2)
			h.dispatch("abandon")
			first := h.client.nextCall(t)
			first.stream.end(err)
			require.Eventually(t, func() bool { return h.client.abandons() == 1 }, 5*time.Second, time.Millisecond)
			h.client.noCall(t)
			require.Empty(t, h.client.unaryOrchestrations())
			require.Zero(t, h.client.releases.Load())
		})
	}
}

func TestWorkerSessionAckTimeoutRetries(t *testing.T) {
	h := newSessionHarness(t, 1)
	h.dispatch("ack-timeout")
	first := h.client.nextCall(t)
	h.timers.next(t, sessionAckTimer).fire <- time.Now()
	retried := h.client.nextCall(t)
	require.EqualValues(t, 0, retried.count())
	require.True(t, proto.Equal(first.orchestration, retried.orchestration))
}

func TestWorkerSessionReleasesAfterAckFailures(t *testing.T) {
	tests := []struct {
		name string
		fail func(t *testing.T, h *sessionWorkerHarness, stream *fakeSessionStream)
	}{
		{name: "stream breaks while idle", fail: func(_ *testing.T, _ *sessionWorkerHarness, stream *fakeSessionStream) {
			stream.end(status.Error(codes.Unavailable, "broken"))
		}},
		{name: "batch cut off after first chunk", fail: func(_ *testing.T, _ *sessionWorkerHarness, stream *fakeSessionStream) {
			stream.chunk(taskCompleted(0, "partial")...)
			stream.end(status.Error(codes.Unavailable, "cut"))
		}},
		{name: "idle timeout", fail: func(t *testing.T, h *sessionWorkerHarness, stream *fakeSessionStream) {
			stream.ping()
			stream.ping()
			h.timers.next(t, sessionIdleTimer).fire <- time.Now()
		}},
		{name: "transfer timeout", fail: func(t *testing.T, h *sessionWorkerHarness, stream *fakeSessionStream) {
			stream.chunk(taskCompleted(0, "partial")...)
			h.timers.next(t, sessionTransferTimer).fire <- time.Now()
		}},
		{name: "second acknowledgement", fail: func(_ *testing.T, _ *sessionWorkerHarness, stream *fakeSessionStream) {
			stream.ack(1)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newSessionHarness(t, 2)
			h.dispatch("release")
			first := h.client.nextCall(t)
			first.stream.ack(4)
			tt.fail(t, h, first.stream)
			require.Eventually(t, func() bool { return h.client.releases.Load() == 1 }, 5*time.Second, time.Millisecond)
			// The partial batch is discarded: no further turn runs.
			h.client.noCall(t)
			require.Empty(t, h.client.unaryOrchestrations())
			require.Zero(t, h.client.abandons())
			require.EqualValues(t, 1, h.starts.Load())
		})
	}
}

func TestWorkerSessionPingsDoNotResetIdleTimer(t *testing.T) {
	h := newSessionHarness(t, 2)
	h.dispatch("pings")
	first := h.client.nextCall(t)
	first.stream.ack(4)
	for range 5 {
		first.stream.ping()
	}
	first.stream.chunk(taskCompleted(0, "a")...)
	// The transfer timer starts only after every earlier message was handled.
	h.timers.next(t, sessionTransferTimer)
	require.Equal(t, 1, h.timers.count(sessionIdleTimer), "pings must not restart the idle timer")
	first.stream.end(nil)
	require.EqualValues(t, 1, h.client.nextCall(t).count())
}

func TestWorkerSessionTimeoutAndFirstChunkHaveOneWinner(t *testing.T) {
	h := newSessionHarness(t, 2)
	h.dispatch("race")
	first := h.client.nextCall(t)
	first.stream.ack(4)
	idle := h.timers.next(t, sessionIdleTimer)
	first.stream.chunk(taskCompleted(0, "a")...)
	transfer := h.timers.next(t, sessionTransferTimer)
	// The old idle timer fires after the first chunk switched to transfer.
	idle.fire <- time.Now()
	first.stream.end(nil)
	second := h.client.nextCall(t)
	require.EqualValues(t, 1, second.count())
	select {
	case transfer.fire <- time.Now():
	default:
	}
	require.Zero(t, h.client.releases.Load())
}

func TestWorkerSessionEndsWhenServiceEndsStream(t *testing.T) {
	h := newSessionHarness(t, 2)
	h.dispatch("service-end")
	first := h.client.nextCall(t)
	first.stream.ack(4)
	first.stream.end(nil)
	require.Never(t, func() bool {
		return h.client.releases.Load() != 0 || h.client.abandons() != 0 || len(h.client.unaryOrchestrations()) != 0
	}, 50*time.Millisecond, 5*time.Millisecond)
	h.client.noCall(t)
}

func TestWorkerSessionUnimplementedFallsBackToUnary(t *testing.T) {
	h := newSessionHarness(t, 1)
	h.dispatch("first")
	first := h.client.nextCall(t)
	first.stream.end(status.Error(codes.Unimplemented, "no sessions"))
	unary := h.waitUnary(t, 1)[0]
	require.EqualValues(t, 0, unary.CompletionCount)
	require.True(t, proto.Equal(first.orchestration, unary), "the fallback must send the same result")

	// Sessions stay off on this intake generation.
	h.dispatch("second")
	require.Len(t, h.waitUnary(t, 2), 2)
	h.client.noCall(t)
}

func TestWorkerSessionUnimplementedAfterFirstTurnUsesTurnCount(t *testing.T) {
	h := newSessionHarness(t, 2)
	h.dispatch("later")
	first := h.client.nextCall(t)
	first.stream.ack(4)
	first.stream.batch(taskCompleted(0, "a")...)
	second := h.client.nextCall(t)
	second.stream.end(status.Error(codes.Unimplemented, "switched off"))
	unary := h.waitUnary(t, 1)[0]
	require.EqualValues(t, 1, unary.CompletionCount)
	require.Equal(t, "work", unary.Actions[0].GetScheduleTask().GetName())
}

func TestWorkerSessionStreamBudgetFullUsesUnary(t *testing.T) {
	h := newSessionHarness(t, 1)
	h.client.openErr = func() error { return errSessionStreamBudgetFull }
	h.dispatch("budget")
	unary := h.waitUnary(t, 1)[0]
	require.EqualValues(t, 0, unary.CompletionCount)
	require.Len(t, unary.Actions, 1)
}

func TestWorkerSessionLimitUsesUnary(t *testing.T) {
	h := newSessionHarness(t, 1, WithExtendedSessions(ExtendedSessionOptions{MaxOrchestrationSessions: 1}))
	h.dispatch("one")
	first := h.client.nextCall(t)
	first.stream.ack(3)
	h.dispatch("two")
	unary := h.waitUnary(t, 1)[0]
	require.Equal(t, "two", unary.InstanceId)
}

func TestWorkerSessionPartialTurnUsesUnary(t *testing.T) {
	starts := new(atomic.Int32)
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("session", sessionOrchestrator(starts, 1)))
	h := startSessionWorker(t, registry, newSessionSchedulerClient())
	h.worker.executor = task.NewTaskExecutor(registry,
		task.WithOrchestrationOptions(task.OrchestrationOptions{MaxEventsPerTurn: 1}))
	h.dispatch("partial",
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent("session", "partial", nil, nil, nil, nil),
		helpers.NewEventRaisedEvent("x", nil),
	)
	unary := h.waitUnary(t, 1)[0]
	require.EqualValues(t, 1, unary.GetNumEventsProcessed().GetValue())
	h.client.noCall(t)
}

func TestWorkerSessionRebuiltResponsesCarryCount(t *testing.T) {
	t.Run("oversized response", func(t *testing.T) {
		registry := task.NewTaskRegistry()
		require.NoError(t, registry.AddOrchestratorN("session", func(ctx *task.OrchestrationContext) (any, error) {
			if err := ctx.CallActivity("work").Await(nil); err != nil {
				return nil, err
			}
			return nil, ctx.CallActivity("big", task.WithRawActivityInput(strings.Repeat("x", 100*1024))).Await(nil)
		}))
		h := startSessionWorker(t, registry, newSessionSchedulerClient(), WithMaxOrchestratorCompletionBytes(64*1024))
		h.dispatch("oversized")
		first := h.client.nextCall(t)
		first.stream.ack(4)
		first.stream.batch(taskCompleted(0, "a")...)
		unary := h.waitUnary(t, 1)[0]
		require.EqualValues(t, 1, unary.CompletionCount)
		require.Equal(t, "token-oversized", unary.CompletionToken)
		require.Equal(t, string(api.ErrorTypeOrchestratorResponseTooLarge),
			unary.Actions[0].GetCompleteOrchestration().GetFailureDetails().GetErrorType())
	})
	t.Run("orchestrator panic", func(t *testing.T) {
		registry := task.NewTaskRegistry()
		require.NoError(t, registry.AddOrchestratorN("session", func(ctx *task.OrchestrationContext) (any, error) {
			if err := ctx.CallActivity("work").Await(nil); err != nil {
				return nil, err
			}
			panic("boom")
		}))
		h := startSessionWorker(t, registry, newSessionSchedulerClient())
		h.dispatch("panic")
		first := h.client.nextCall(t)
		first.stream.ack(4)
		first.stream.batch(taskCompleted(0, "a")...)
		unary := h.waitUnary(t, 1)[0]
		require.EqualValues(t, 1, unary.CompletionCount)
		require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
			unary.Actions[0].GetCompleteOrchestration().GetOrchestrationStatus())
	})
}

func TestWorkerSessionInvalidBatchReleases(t *testing.T) {
	h := newSessionHarness(t, 2)
	h.dispatch("invalid")
	first := h.client.nextCall(t)
	first.stream.ack(4)
	// A batch must start with OrchestratorStarted.
	first.stream.batch(helpers.NewTaskCompletedEvent(0, nil))
	require.Eventually(t, func() bool { return h.client.releases.Load() == 1 }, 5*time.Second, time.Millisecond)
	require.Empty(t, h.client.unaryOrchestrations())
}

func TestWorkerSessionCleanupFailureReconnectsIntake(t *testing.T) {
	h := newSessionHarness(t, 2)
	h.worker.options.sessionCleanupTimeout = 20 * time.Millisecond
	h.client.releaseErr = func() error { return status.Error(codes.Unavailable, "down") }
	h.dispatch("cleanup")
	first := h.client.nextCall(t)
	first.stream.ack(4)
	first.stream.end(status.Error(codes.Unavailable, "broken"))
	require.Eventually(t, func() bool { return h.client.connects.Load() >= 2 }, 5*time.Second, time.Millisecond,
		"a failed cleanup must reconnect the work item stream")
	require.Greater(t, h.client.releases.Load(), int32(1), "release must be retried before giving up")
	require.True(t, h.worker.Running())
}

func TestWorkerSessionAbandonFailureReconnectsIntake(t *testing.T) {
	h := newSessionHarness(t, 1)
	h.worker.options.sessionCleanupTimeout = 20 * time.Millisecond
	h.client.abandonErr = func() error { return status.Error(codes.Unavailable, "down") }
	h.dispatch("abandon-fails")
	first := h.client.nextCall(t)
	first.stream.end(status.Error(codes.InvalidArgument, "rejected"))
	require.Eventually(t, func() bool { return h.client.connects.Load() >= 2 }, 5*time.Second, time.Millisecond)
	require.Greater(t, h.client.orchestrationAbort.Load(), int32(1))
}

func TestWorkerSessionUnaryFailureAfterAckReconnectsIntake(t *testing.T) {
	h := newSessionHarness(t, 1)
	h.worker.options.sessionCleanupTimeout = 20 * time.Millisecond
	h.client.unaryErr = func() error { return status.Error(codes.InvalidArgument, "rejected") }
	h.client.abandonErr = func() error { return status.Error(codes.Unavailable, "down") }
	h.dispatch("unary-fails")
	first := h.client.nextCall(t)
	first.stream.ack(4)
	first.stream.batch(taskCompleted(0, "a")...)
	require.Eventually(t, func() bool { return h.client.connects.Load() >= 2 }, 5*time.Second, time.Millisecond,
		"a failed abandon of a session work item must reconnect intake")
	require.Greater(t, h.client.orchestrationAbort.Load(), int32(1))
}

func TestWorkerEntitySessionUnaryFailureAfterAckReconnectsIntake(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddEntityN("counter", func(*task.EntityContext) (any, error) { return nil, nil }))
	client := newSessionSchedulerClient()
	client.entityCompletionErr = status.Error(codes.InvalidArgument, "rejected")
	client.entityAbandonFailures = math.MaxInt32
	h := startSessionWorker(t, registry, client)
	h.worker.options.sessionCleanupTimeout = 20 * time.Millisecond
	// A batch that fails ends the session with the unary RPC.
	h.worker.executor = &recordingExecutor{executeEntity: func(_ context.Context, request *protos.EntityBatchRequest) (*protos.EntityBatchResult, error) {
		if request.EntityState != nil {
			return nil, errors.New("framework failure")
		}
		return &protos.EntityBatchResult{
			Results:     []*protos.OperationResult{{ResultType: &protos.OperationResult_Success{Success: &protos.OperationResultSuccess{}}}},
			EntityState: wrapperspb.String("1"),
		}, nil
	}}
	client.stream.results <- fakeWorkItemResult{item: &protos.WorkItem{
		Request:         &protos.WorkItem_EntityRequestV2{EntityRequestV2: entitySessionRequest("@counter@c", entitySignal("add"))},
		CompletionToken: "entity-token",
	}}
	first := client.nextCall(t)
	first.stream.ack(0)
	first.stream.batch(entitySignal("add"))
	require.Eventually(t, func() bool { return client.connects.Load() >= 2 }, 5*time.Second, time.Millisecond,
		"a failed abandon of a session work item must reconnect intake")
	client.mu.Lock()
	defer client.mu.Unlock()
	require.Greater(t, client.entityAbandonAttempts, 1)
}

func TestWorkerSessionClosesWithRetiredGeneration(t *testing.T) {
	h := newSessionHarness(t, 2)
	h.dispatch("retired")
	first := h.client.nextCall(t)
	first.stream.ack(4)
	h.timers.next(t, sessionIdleTimer)
	// The intake stream fails, so the service abandons the generation's work.
	h.client.stream.results <- fakeWorkItemResult{err: status.Error(codes.Unavailable, "intake lost")}
	require.Eventually(t, func() bool { return h.client.connects.Load() >= 2 }, 5*time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		select {
		case <-first.stream.ctx.Done():
			return true
		default:
			return false
		}
	}, 5*time.Second, time.Millisecond, "the session stream must close with its generation")
	require.Zero(t, h.client.releases.Load(), "a retired generation's sessions close without release")
	require.Zero(t, h.client.abandons())
}

func TestWorkerSessionShutdownReleasesIdleSessions(t *testing.T) {
	h := newSessionHarness(t, 2)
	h.dispatch("shutdown")
	first := h.client.nextCall(t)
	first.stream.ack(4)
	h.timers.next(t, sessionIdleTimer)
	h.cancel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, h.worker.Shutdown(ctx))
	require.EqualValues(t, 1, h.client.releases.Load())
}

func entitySessionRequest(instanceID string, operations ...*protos.HistoryEvent) *protos.EntityRequest {
	return &protos.EntityRequest{InstanceId: instanceID, OperationRequests: operations}
}

func entitySignal(operation string) *protos.HistoryEvent {
	return newFakeEntitySignalRequest(operation, nil).OperationRequests[0]
}

func TestWorkerEntitySessionUsesCachedState(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddEntityN("counter", func(ctx *task.EntityContext) (any, error) {
		var value int
		if ctx.HasState() {
			if err := ctx.GetState(&value); err != nil {
				return nil, err
			}
		}
		return nil, ctx.SetState(value + 1)
	}))
	client := newSessionSchedulerClient()
	h := startSessionWorker(t, registry, client)
	var requests []*protos.EntityBatchRequest
	var mu sync.Mutex
	base := h.worker.executor.(task.EntityExecutor)
	h.worker.executor = &recordingExecutor{executeEntity: func(ctx context.Context, request *protos.EntityBatchRequest) (*protos.EntityBatchResult, error) {
		mu.Lock()
		requests = append(requests, proto.Clone(request).(*protos.EntityBatchRequest))
		mu.Unlock()
		return base.ExecuteEntity(ctx, request)
	}}
	client.stream.results <- fakeWorkItemResult{item: &protos.WorkItem{
		Request:         &protos.WorkItem_EntityRequestV2{EntityRequestV2: entitySessionRequest("@counter@a", entitySignal("add"))},
		CompletionToken: "entity-token",
	}}

	first := client.nextCall(t)
	require.EqualValues(t, 0, first.count())
	require.Equal(t, "1", first.entity.EntityState.GetValue())
	first.stream.ack(0)
	first.stream.batch(helpers.NewOrchestratorStartedEvent(), entitySignal("add"), entitySignal("add"))

	second := client.nextCall(t)
	require.EqualValues(t, 1, second.count())
	require.Equal(t, "3", second.entity.EntityState.GetValue())
	require.Len(t, second.entity.Results, 2)
	second.stream.ack(0)
	// A batch whose executor fails ends the session with the unary RPC.
	h.worker.executor = &recordingExecutor{executeEntity: func(context.Context, *protos.EntityBatchRequest) (*protos.EntityBatchResult, error) {
		return nil, errors.New("framework failure")
	}}
	second.stream.batch(entitySignal("add"))

	require.Eventually(t, func() bool { return len(client.unaryEntities()) == 1 }, 5*time.Second, time.Millisecond)
	failed := client.unaryEntities()[0]
	require.EqualValues(t, 2, failed.CompletionCount)
	require.Equal(t, "entity-token", failed.CompletionToken)
	require.NotNil(t, failed.FailureDetails)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, requests, 2)
	require.Equal(t, "1", requests[1].EntityState.GetValue(), "a resumed batch must use the cached state")
	require.Len(t, requests[1].Operations, 2, "control events are not entity operations")
}

func TestWorkerEntitySessionAbandonsInvalidBatch(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddEntityN("counter", func(*task.EntityContext) (any, error) { return nil, nil }))
	client := newSessionSchedulerClient()
	startSessionWorker(t, registry, client)
	client.stream.results <- fakeWorkItemResult{item: &protos.WorkItem{
		Request:         &protos.WorkItem_EntityRequestV2{EntityRequestV2: entitySessionRequest("@counter@b", entitySignal("noop"))},
		CompletionToken: "entity-token",
	}}
	first := client.nextCall(t)
	first.stream.ack(0)
	first.stream.batch(helpers.NewTaskCompletedEvent(1, nil))
	require.Eventually(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return client.entityAbandonAttempts == 1
	}, 5*time.Second, time.Millisecond)
	require.Empty(t, client.unaryEntities())
	require.Zero(t, client.releases.Load())
}

func TestWithExtendedSessionsValidation(t *testing.T) {
	newWorker := func(opts ...TaskHubGrpcWorkerOption) error {
		_, err := newTaskHubGrpcWorker(
			func(context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error) { return nil, nil, nil },
			task.NewTaskRegistry(),
			api.DefaultLogger(),
			opts...,
		)
		return err
	}
	require.Error(t, newWorker(WithExtendedSessions(ExtendedSessionOptions{IdleTimeout: -1})))
	require.Error(t, newWorker(WithExtendedSessions(ExtendedSessionOptions{MaxEntitySessions: -1})))
	require.ErrorContains(t, newWorker(
		WithMaxConcurrentOrchestrationWorkItems(2),
		WithExtendedSessions(ExtendedSessionOptions{MaxOrchestrationSessions: 2}),
	), "must be lower")

	shared := WithExtendedSessions(ExtendedSessionOptions{})
	small, err := newTaskHubGrpcWorker(
		func(context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error) { return nil, nil, nil },
		task.NewTaskRegistry(), api.DefaultLogger(), WithMaxConcurrentOrchestrationWorkItems(4), shared)
	require.NoError(t, err)
	large, err := newTaskHubGrpcWorker(
		func(context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error) { return nil, nil, nil },
		task.NewTaskRegistry(), api.DefaultLogger(), WithMaxConcurrentOrchestrationWorkItems(40), shared)
	require.NoError(t, err)
	require.Equal(t, 2, small.options.extendedSessions.MaxOrchestrationSessions,
		"workers built from one option value must not share their session options")
	require.Equal(t, 20, large.options.extendedSessions.MaxOrchestrationSessions)

	options := defaultTaskHubGrpcWorkerOptions()
	require.NoError(t, WithExtendedSessions(ExtendedSessionOptions{})(&options))
	options.maxConcurrentOrchestrations = 1
	options.maxConcurrentEntities = 9
	require.NoError(t, options.resolveExtendedSessionLimits())
	require.Zero(t, options.extendedSessions.MaxOrchestrationSessions, "one slot leaves no room for sessions")
	require.Equal(t, 4, options.extendedSessions.MaxEntitySessions)
	require.Equal(t, DefaultExtendedSessionTimeout, options.extendedSessions.IdleTimeout)
	require.Equal(t, DefaultExtendedSessionTimeout, options.extendedSessions.TransferTimeout)
}

func TestWorkerSessionsOffByDefault(t *testing.T) {
	starts := new(atomic.Int32)
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("session", sessionOrchestrator(starts, 1)))
	client := newSessionSchedulerClient()
	worker := newFakeWorker(t, client.fakeSchedulerClient)
	worker.clientFactory = func(context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error) {
		return client, nil, nil
	}
	worker.executor = task.NewTaskExecutor(registry)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, worker.Start(ctx))
	h := &sessionWorkerHarness{worker: worker, client: client}
	h.dispatch("off")
	h.waitUnary(t, 1)
	client.noCall(t)
	cancel()
	require.NoError(t, worker.Shutdown(context.Background()))
}

// sessionStreamConn records session streams opened on a completion connection.
type sessionStreamConn struct {
	fakeClientConn
	opened atomic.Int32
}

func (c *sessionStreamConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	c.opened.Add(1)
	return c.fakeClientConn.NewStream(ctx, desc, method, opts...)
}

func TestWorkerCompletionTransportRoutesSessionStreams(t *testing.T) {
	intake := &sessionStreamConn{}
	completions := []*sessionStreamConn{{}, {}}
	transport := &workerCompletionTransport{
		intake:      intake,
		completions: []grpc.ClientConnInterface{completions[0], completions[1]},
	}
	desc := &protos.TaskHubSidecarService_ServiceDesc.Streams[1]
	method := protos.TaskHubSidecarService_CompleteOrchestratorTaskWithExtendedSession_FullMethodName

	var cancels []context.CancelFunc
	for range 2 * workerSessionStreamsPerConnection {
		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)
		_, err := transport.NewStream(ctx, desc, method)
		require.NoError(t, err)
	}
	require.EqualValues(t, workerSessionStreamsPerConnection, completions[0].opened.Load())
	require.EqualValues(t, workerSessionStreamsPerConnection, completions[1].opened.Load())
	require.Zero(t, intake.opened.Load())

	_, err := transport.NewStream(context.Background(), desc, protos.TaskHubSidecarService_CompleteEntityTaskWithExtendedSession_FullMethodName)
	require.ErrorIs(t, err, errSessionStreamBudgetFull)

	// An ended stream returns its budget.
	cancels[0]()
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)
		_, err := transport.NewStream(ctx, desc, method)
		return err == nil
	}, time.Second, time.Millisecond)

	// Other streams stay on intake.
	_, err = transport.NewStream(context.Background(), desc, protos.TaskHubSidecarService_GetWorkItems_FullMethodName)
	require.NoError(t, err)
	require.EqualValues(t, 1, intake.opened.Load())
	for _, cancel := range cancels {
		cancel()
	}

	require.True(t, isWorkerCompletionMethod(protos.TaskHubSidecarService_ReleaseTaskOrchestratorWorkItem_FullMethodName))
	require.True(t, isWorkerCompletionMethod(protos.TaskHubSidecarService_ReleaseTaskEntityWorkItem_FullMethodName))
}

func TestBudgetedStreamReturnsBudgetOnRecvError(t *testing.T) {
	transport := &workerCompletionTransport{
		intake:      &fakeClientConn{},
		completions: []grpc.ClientConnInterface{&erroringStreamConn{}},
	}
	stream, err := transport.NewStream(context.Background(), &grpc.StreamDesc{ServerStreams: true},
		protos.TaskHubSidecarService_CompleteOrchestratorTaskWithExtendedSession_FullMethodName)
	require.NoError(t, err)
	require.Equal(t, []int{1}, transport.sessionStreams)
	require.ErrorIs(t, stream.RecvMsg(nil), io.EOF)
	require.Equal(t, []int{0}, transport.sessionStreams)
}

type erroringStreamConn struct{ fakeClientConn }

func (*erroringStreamConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return erroringClientStream{}, nil
}

type erroringClientStream struct{ grpc.ClientStream }

func (erroringClientStream) RecvMsg(any) error { return io.EOF }

func TestWorkerSessionBudgetFullAfterUncertainAttemptAbandons(t *testing.T) {
	h := newSessionHarness(t, 2)
	var opens atomic.Int32
	h.client.openErr = func() error {
		if opens.Add(1) == 2 {
			return errSessionStreamBudgetFull
		}
		return nil
	}
	h.dispatch("uncertain")
	// The service may have applied this attempt before the stream broke.
	h.client.nextCall(t).stream.end(status.Error(codes.Unavailable, "lost"))
	require.Eventually(t, func() bool { return h.client.abandons() == 1 }, 5*time.Second, time.Millisecond)
	require.EqualValues(t, 2, opens.Load())
	h.client.noCall(t)
	require.Empty(t, h.client.unaryOrchestrations(), "a unary completion after an uncertain attempt can leave the session open")
}

func TestWorkerSessionOpenTimeoutRetries(t *testing.T) {
	h := newSessionHarness(t, 1, WithWorkerRPCTimeout(200*time.Millisecond))
	var opens atomic.Int32
	h.client.openWait = func(ctx context.Context) error {
		if opens.Add(1) == 1 {
			<-ctx.Done()
			return status.FromContextError(ctx.Err()).Err()
		}
		return nil
	}
	h.dispatch("slow-open")
	retried := h.client.nextCall(t)
	require.EqualValues(t, 2, opens.Load(), "a stuck open must time out and retry")
	require.EqualValues(t, 0, retried.count())
	require.Zero(t, h.client.abandons())
}

func TestWorkerSessionOpenStopsWhenIntakeEnds(t *testing.T) {
	h := newSessionHarness(t, 1, WithWorkerRPCTimeout(time.Minute))
	opening := make(chan struct{})
	unblocked := make(chan struct{})
	var opens atomic.Int32
	h.client.openWait = func(ctx context.Context) error {
		if opens.Add(1) > 1 {
			return nil
		}
		close(opening)
		<-ctx.Done()
		close(unblocked)
		return status.FromContextError(ctx.Err()).Err()
	}
	h.dispatch("stuck")
	<-opening
	h.client.stream.results <- fakeWorkItemResult{err: status.Error(codes.Unavailable, "intake ended")}
	select {
	case <-unblocked:
	case <-time.After(5 * time.Second):
		t.Fatal("the stuck open did not stop when its intake generation ended")
	}
	require.Never(t, func() bool {
		return h.client.abandons() != 0 || len(h.client.unaryOrchestrations()) != 0
	}, 50*time.Millisecond, 5*time.Millisecond)
}

func TestWorkerSessionBatchLimitsRelease(t *testing.T) {
	t.Run("events", func(t *testing.T) {
		h := newSessionHarness(t, 2, WithMaxStreamedHistoryEvents(2))
		h.dispatch("events")
		first := h.client.nextCall(t)
		first.stream.ack(4)
		first.stream.chunk(helpers.NewOrchestratorStartedEvent(), helpers.NewEventRaisedEvent("x", nil))
		first.stream.chunk(helpers.NewTaskCompletedEvent(0, nil))
		require.Eventually(t, func() bool { return h.client.releases.Load() == 1 }, 5*time.Second, time.Millisecond)
		h.client.noCall(t)
		require.Empty(t, h.client.unaryOrchestrations())
	})
	t.Run("bytes", func(t *testing.T) {
		h := newSessionHarness(t, 2, WithMaxStreamedHistoryBytes(256))
		h.dispatch("bytes")
		first := h.client.nextCall(t)
		first.stream.ack(4)
		first.stream.chunk(helpers.NewOrchestratorStartedEvent(),
			helpers.NewEventRaisedEvent("x", wrapperspb.String(strings.Repeat("x", 512))))
		require.Eventually(t, func() bool { return h.client.releases.Load() == 1 }, 5*time.Second, time.Millisecond)
		h.client.noCall(t)
		require.Empty(t, h.client.unaryOrchestrations())
	})
}

func TestWorkerSessionReservesCompletionCountBytes(t *testing.T) {
	for _, message := range []proto.Message{&protos.OrchestratorResponse{InstanceId: "x"}, &protos.EntityBatchResult{}} {
		before := proto.Size(message)
		switch m := message.(type) {
		case *protos.OrchestratorResponse:
			m.CompletionCount = math.MaxInt32
		case *protos.EntityBatchResult:
			m.CompletionCount = math.MaxInt32
		}
		require.LessOrEqual(t, proto.Size(message)-before, sessionCompletionCountReserve)
	}
	off := newSessionHarness(t, 1, WithMaxOrchestratorCompletionBytes(64*1024)).worker
	off.options.extendedSessions = nil
	require.Equal(t, 64*1024, off.orchestratorCompletionLimit())
	on := newSessionHarness(t, 1, WithMaxOrchestratorCompletionBytes(64*1024)).worker
	require.Equal(t, 64*1024-sessionCompletionCountReserve, on.orchestratorCompletionLimit())
}

type countingPayloadStore struct {
	*payload.MemoryStore
	resolves atomic.Int32
}

func (s *countingPayloadStore) Resolve(ctx context.Context, location string) ([]byte, error) {
	s.resolves.Add(1)
	return s.MemoryStore.Resolve(ctx, location)
}

func TestWorkerEntitySessionCachesStateBeforeExternalization(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddEntityN("text", func(ctx *task.EntityContext) (any, error) {
		var value string
		if ctx.HasState() {
			if err := ctx.GetState(&value); err != nil {
				return nil, err
			}
		}
		return nil, ctx.SetState(value + "x")
	}))
	store := &countingPayloadStore{MemoryStore: payload.NewMemoryStore()}
	client := newSessionSchedulerClient()
	h := startSessionWorker(t, registry, client, WithWorkerLargePayloads(&api.LargePayloadOptions{
		Store: store, Resolver: store, ThresholdBytes: 1, MaxPayloadBytes: 1024,
	}))
	var requests []*protos.EntityBatchRequest
	var mu sync.Mutex
	base := h.worker.executor.(task.EntityExecutor)
	h.worker.executor = &recordingExecutor{executeEntity: func(ctx context.Context, request *protos.EntityBatchRequest) (*protos.EntityBatchResult, error) {
		mu.Lock()
		requests = append(requests, proto.Clone(request).(*protos.EntityBatchRequest))
		mu.Unlock()
		return base.ExecuteEntity(ctx, request)
	}}
	client.stream.results <- fakeWorkItemResult{item: &protos.WorkItem{
		Request:         &protos.WorkItem_EntityRequestV2{EntityRequestV2: entitySessionRequest("@text@a", entitySignal("add"))},
		CompletionToken: "entity-token",
	}}

	first := client.nextCall(t)
	require.True(t, api.IsLargePayloadReference(first.entity.EntityState.GetValue()), "the service must get the externalized state")
	first.stream.ack(0)
	first.stream.batch(helpers.NewOrchestratorStartedEvent(), entitySignal("add"))

	second := client.nextCall(t)
	require.True(t, api.IsLargePayloadReference(second.entity.EntityState.GetValue()))
	require.Zero(t, store.resolves.Load(), "a resumed batch must not download the cached state")
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, requests, 2)
	require.Equal(t, `"x"`, requests[1].EntityState.GetValue())
}

func TestWorkerSessionUnimplementedAfterUncertainAttemptAbandons(t *testing.T) {
	h := newSessionHarness(t, 2)
	var opens atomic.Int32
	h.client.openErr = func() error {
		if opens.Add(1) == 2 {
			return status.Error(codes.Unimplemented, "switched off")
		}
		return nil
	}
	h.dispatch("uncertain")
	h.client.nextCall(t).stream.end(status.Error(codes.Unavailable, "lost"))
	require.Eventually(t, func() bool { return h.client.abandons() == 1 }, 5*time.Second, time.Millisecond)
	require.Empty(t, h.client.unaryOrchestrations(), "a unary completion after an uncertain attempt can leave the session open")

	// Sessions stay off on this intake generation.
	h.dispatch("next")
	require.Len(t, h.waitUnary(t, 1), 1)
	h.client.noCall(t)
}

// detachedSessionScope returns a session scope whose intake stream the test
// ends by calling the returned function.
func detachedSessionScope() (*workerSessionScope, context.CancelFunc) {
	streamCtx, endStream := context.WithCancel(context.Background())
	connection := &grpcWorkerConnection{streamCtx: streamCtx, cancelStream: endStream}
	run := &grpcWorkerRun{intakeCtx: context.Background()}
	return &workerSessionScope{run: run, connection: connection}, endStream
}

func TestWorkerSessionRetryWaitStopsWhenIntakeEnds(t *testing.T) {
	worker := newSessionHarness(t, 1, WithWorkerTransientRetryPolicy(3, time.Hour, time.Hour)).worker
	scope, endStream := detachedSessionScope()
	opened := make(chan struct{}, 3)
	outcome := make(chan sessionOutcome, 1)
	go func() {
		result, _ := worker.awaitSessionBatch(context.Background(), scope, sessionCall{
			instanceID: "retry-wait",
			open: func(context.Context) (grpc.ServerStreamingClient[protos.CompleteTaskWithExtendedSessionResponse], error) {
				opened <- struct{}{}
				return nil, status.Error(codes.Unavailable, "down")
			},
			abandon: func(context.Context) error { return nil },
		})
		outcome <- result
	}()
	<-opened
	endStream()
	select {
	case result := <-outcome:
		require.Equal(t, sessionEnded, result)
	case <-time.After(5 * time.Second):
		t.Fatal("the retry wait did not stop when the intake stream ended")
	}
	require.Len(t, opened, 0, "no attempt may run after the intake stream ended")
}

func TestWorkerSessionCleanupStopsWhenIntakeEnds(t *testing.T) {
	worker := newSessionHarness(t, 1, WithWorkerTransientRetryPolicy(3, time.Hour, time.Hour)).worker
	worker.options.sessionCleanupTimeout = time.Hour
	scope, endStream := detachedSessionScope()
	attempts := make(chan struct{}, 3)
	done := make(chan struct{})
	go func() {
		defer close(done)
		worker.cleanupSessionWorkItem(context.Background(), scope, "cleanup", "release", func(context.Context) error {
			attempts <- struct{}{}
			return status.Error(codes.Unavailable, "down")
		})
	}()
	<-attempts
	endStream()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup did not stop when the intake stream ended")
	}
	require.Len(t, attempts, 0)
	require.False(t, scope.connection.sessionCleanupFailed.Load(), "an ended generation needs no extra reconnect")
}
