package client

import (
	"context"
	"errors"
	"io"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// recordedWaits captures the deterministic retry schedule without sleeping.
type recordedWaits struct {
	mu     sync.Mutex
	delays []time.Duration
}

func (r *recordedWaits) wait(ctx context.Context, delay time.Duration) error {
	r.mu.Lock()
	r.delays = append(r.delays, delay)
	r.mu.Unlock()
	return ctx.Err()
}

func (r *recordedWaits) snapshot() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Duration(nil), r.delays...)
}

func (r *recordedWaits) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.delays)
}

// withRecordedWaits installs the wait seam so reconnect and RPC retry delays are
// observable without sleeping.
func withRecordedWaits(waits *recordedWaits) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		options.waitFn = waits.wait
		options.reconnectRandom = nil
		return nil
	}
}

func TestWorkerBackoffScheduleStaysWithinConfiguredBounds(t *testing.T) {
	schedule := newWorkerBackoff(200*time.Millisecond, time.Second, nil)
	delays := make([]time.Duration, 0, 6)
	for i := 0; i < 6; i++ {
		delays = append(delays, schedule.Next())
	}
	require.Equal(t, []time.Duration{
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		time.Second,
		time.Second,
		time.Second,
	}, delays)

	schedule.Reset()
	require.Equal(t, 200*time.Millisecond, schedule.Next())

	repeat := newWorkerBackoff(200*time.Millisecond, time.Second, nil)
	for _, expected := range delays {
		require.Equal(t, expected, repeat.Next())
	}
}

func TestWorkerBackoffSaturatesInsteadOfOverflowing(t *testing.T) {
	for _, random := range []randomInt64N{
		nil,
		func(n int64) int64 { return n - 1 },
	} {
		schedule := newWorkerBackoff(time.Hour, time.Duration(math.MaxInt64), random)
		previous := time.Duration(0)
		for i := range 200 {
			delay := schedule.Next()
			require.Positive(t, delay, "delay %d overflowed to a non-positive duration", i)
			require.GreaterOrEqual(t, delay, time.Hour)
			require.LessOrEqual(t, delay, time.Duration(math.MaxInt64))
			if random == nil {
				require.GreaterOrEqual(t, delay, previous)
			}
			previous = delay
		}
		if random == nil {
			require.Equal(t, time.Duration(math.MaxInt64), previous)
		}
	}

	require.Equal(t, time.Second, doubleDurationBounded(time.Second, time.Second))
	require.Equal(t, 2*time.Second, doubleDurationBounded(time.Second, time.Minute))
	require.Equal(
		t,
		time.Duration(math.MaxInt64),
		doubleDurationBounded(time.Duration(math.MaxInt64)-1, time.Duration(math.MaxInt64)),
	)
}

func TestWorkerBackoffJitterUsesTheFullBoundedRange(t *testing.T) {
	tests := []struct {
		name     string
		random   randomInt64N
		expected []time.Duration
	}{
		{
			name:     "lower bound",
			random:   func(int64) int64 { return 0 },
			expected: []time.Duration{200 * time.Millisecond, 300 * time.Millisecond, 600 * time.Millisecond, 750 * time.Millisecond},
		},
		{
			name:     "upper bound",
			random:   func(n int64) int64 { return n - 1 },
			expected: []time.Duration{250 * time.Millisecond, 500 * time.Millisecond, time.Second, time.Second},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schedule := newWorkerBackoff(200*time.Millisecond, time.Second, test.random)
			for _, expected := range test.expected {
				require.Equal(t, expected, schedule.Next())
			}
		})
	}
}

func TestWorkerTransientRetryDelayIsDeterministicAndBounded(t *testing.T) {
	worker := &TaskHubGrpcWorker{options: taskHubGrpcWorkerOptions{
		transientRetryBaseDelay: 100 * time.Millisecond,
		transientRetryMaxDelay:  500 * time.Millisecond,
	}}
	delays := make([]time.Duration, 0, 6)
	for attempt := 1; attempt <= 6; attempt++ {
		delays = append(delays, worker.retryDelay(attempt))
	}
	require.Equal(t, []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		500 * time.Millisecond,
		500 * time.Millisecond,
		500 * time.Millisecond,
	}, delays)

	overflowing := &TaskHubGrpcWorker{options: taskHubGrpcWorkerOptions{
		transientRetryBaseDelay: time.Hour,
		transientRetryMaxDelay:  time.Duration(math.MaxInt64),
	}}
	for attempt := 1; attempt <= 200; attempt++ {
		require.GreaterOrEqual(t, overflowing.retryDelay(attempt), time.Hour)
	}
}

func TestWorkerTransientRPCRetryFollowsTheDeterministicSchedule(t *testing.T) {
	waits := &recordedWaits{}
	worker := newFakeWorker(
		t,
		&fakeSchedulerClient{stream: newFakeWorkItemStream(0)},
		WithWorkerTransientRetryPolicy(4, 10*time.Millisecond, 40*time.Millisecond),
		withRecordedWaits(waits),
	)

	var attempts atomic.Int32
	err := worker.executeRPCWithRetry(context.Background(), "test rpc", func(context.Context) error {
		attempts.Add(1)
		return status.Error(codes.Unavailable, "transient")
	})
	require.ErrorContains(t, err, "test rpc failed after 4 attempts")
	require.EqualValues(t, 4, attempts.Load())
	require.Equal(t, []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		40 * time.Millisecond,
	}, waits.snapshot())
}

func TestWorkerDoesNotRetryExpiredWorkItemLease(t *testing.T) {
	waits := &recordedWaits{}
	worker := newFakeWorker(
		t,
		&fakeSchedulerClient{stream: newFakeWorkItemStream(0)},
		WithWorkerTransientRetryPolicy(4, 10*time.Millisecond, 40*time.Millisecond),
		withRecordedWaits(waits),
	)

	var attempts atomic.Int32
	err := worker.executeRPCWithRetry(context.Background(), "complete task", func(context.Context) error {
		attempts.Add(1)
		return status.Error(codes.NotFound, "work item not found")
	})
	require.ErrorContains(t, err, "non-retryable")
	require.EqualValues(t, 1, attempts.Load())
	require.Empty(t, waits.snapshot())
	require.True(t, isWorkItemGone(err))
}

// replayStream delivers a fixed number of health pings and then reports the
// silent-disconnect sentinel, standing in for a stream that ends without ever
// producing a server-side error.
type replayStream struct {
	protos.TaskHubSidecarService_GetWorkItemsClient
	remaining int
}

func (s *replayStream) Recv() (*protos.WorkItem, error) {
	if s.remaining > 0 {
		s.remaining--
		return &protos.WorkItem{Request: &protos.WorkItem_HealthPing{HealthPing: &protos.HealthPing{}}}, nil
	}
	return nil, errSilentDisconnect
}

// replaySchedulerClient hands out a fresh replayStream per stream generation.
type replaySchedulerClient struct {
	*fakeSchedulerClient
	messages int
}

func (c *replaySchedulerClient) GetWorkItems(
	context.Context,
	*protos.GetWorkItemsRequest,
	...grpc.CallOption,
) (protos.TaskHubSidecarService_GetWorkItemsClient, error) {
	return &replayStream{remaining: c.messages}, nil
}

func newRunLoopTestRun() *grpcWorkerRun {
	run := &grpcWorkerRun{
		orchestrationSlots: make(chan struct{}, 1),
		activitySlots:      make(chan struct{}, 1),
		entitySlots:        make(chan struct{}, 1),
	}
	run.intakeCtx, run.cancelIntake = context.WithCancel(context.Background())
	run.processingCtx, run.cancelProcessing = context.WithCancel(context.Background())
	return run
}

// runLoopUntilWaits drives runLoop until the wait seam has observed the wanted
// number of delays, then cancels intake so the loop returns deterministically.
func runLoopUntilWaits(
	t *testing.T,
	worker *TaskHubGrpcWorker,
	waits *recordedWaits,
	wanted int,
	stream workItemsStream,
) {
	t.Helper()
	run := newRunLoopTestRun()
	defer run.cancelIntake()
	defer run.cancelProcessing()

	watcher := make(chan struct{})
	go func() {
		defer close(watcher)
		for waits.count() < wanted {
			time.Sleep(time.Millisecond)
		}
		run.cancelIntake()
	}()

	_, cancelStream := context.WithCancel(context.Background())
	defer cancelStream()
	require.NoError(t, worker.runLoop(run, &grpcWorkerConnection{
		stream:       stream,
		cancelStream: cancelStream,
	}))
	<-watcher
	run.pending.Wait()
	run.cancelProcessing()
	run.retired.Wait()
}

func TestPoisonedSilenceEscalatesWhileDrainAfterFirstMessageResets(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		messages int
		expected []time.Duration
	}{
		{
			name:     "silence before the first message keeps escalating",
			messages: 0,
			expected: []time.Duration{
				10 * time.Millisecond,
				20 * time.Millisecond,
				40 * time.Millisecond,
				80 * time.Millisecond,
				80 * time.Millisecond,
			},
		},
		{
			name:     "a drain after the first message restarts at the base delay",
			messages: 1,
			expected: []time.Duration{
				10 * time.Millisecond,
				10 * time.Millisecond,
				10 * time.Millisecond,
				10 * time.Millisecond,
				10 * time.Millisecond,
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			waits := &recordedWaits{}
			worker := newFakeWorker(
				t,
				&fakeSchedulerClient{stream: newFakeWorkItemStream(0)},
				WithWorkerReconnectBackoff(10*time.Millisecond, 80*time.Millisecond),
				withRecordedWaits(waits),
			)
			worker.clientFactory = func(context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error) {
				return &replaySchedulerClient{
					fakeSchedulerClient: &fakeSchedulerClient{stream: newFakeWorkItemStream(0)},
					messages:            testCase.messages,
				}, nil, nil
			}

			runLoopUntilWaits(
				t,
				worker,
				waits,
				len(testCase.expected),
				&replayStream{remaining: testCase.messages},
			)

			delays := waits.snapshot()
			require.GreaterOrEqual(t, len(delays), len(testCase.expected))
			require.Equal(t, testCase.expected, delays[:len(testCase.expected)])
		})
	}
}

func TestTransientConnectFailuresEscalateWithinTheReconnectLoop(t *testing.T) {
	waits := &recordedWaits{}
	worker := newFakeWorker(
		t,
		&fakeSchedulerClient{stream: newFakeWorkItemStream(0)},
		WithWorkerReconnectBackoff(10*time.Millisecond, 40*time.Millisecond),
		withRecordedWaits(waits),
	)
	worker.clientFactory = func(context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error) {
		return nil, nil, status.Error(codes.Unavailable, "cannot dial")
	}

	runLoopUntilWaits(t, worker, waits, 4, &replayStream{})
	require.Equal(t, []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		40 * time.Millisecond,
		40 * time.Millisecond,
	}, waits.snapshot()[:4])
}

// racingStream stays silent until the worker cancels the stream and then
// delivers a work item, reproducing a message that races the silence timeout.
type racingStream struct {
	protos.TaskHubSidecarService_GetWorkItemsClient
	ctx       context.Context
	item      *protos.WorkItem
	delivered atomic.Bool
}

func (s *racingStream) Recv() (*protos.WorkItem, error) {
	<-s.ctx.Done()
	if s.delivered.Swap(true) {
		return nil, status.FromContextError(s.ctx.Err()).Err()
	}
	return s.item, nil
}

type racingSchedulerClient struct {
	*fakeSchedulerClient
	racing *racingStream
}

func (c *racingSchedulerClient) GetWorkItems(
	ctx context.Context,
	_ *protos.GetWorkItemsRequest,
	_ ...grpc.CallOption,
) (protos.TaskHubSidecarService_GetWorkItemsClient, error) {
	c.racing.ctx = ctx
	return c.racing, nil
}

func TestSilenceTimeoutDoesNotDropAConcurrentlyDeliveredWorkItem(t *testing.T) {
	inner := &fakeSchedulerClient{stream: newFakeWorkItemStream(0)}
	client := &racingSchedulerClient{
		fakeSchedulerClient: inner,
		racing: &racingStream{item: &protos.WorkItem{
			Request: &protos.WorkItem_ActivityRequest{ActivityRequest: &protos.ActivityRequest{
				Name:                  "activity",
				TaskId:                1,
				OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: "instance"},
			}},
			CompletionToken: "raced-token",
		}},
	}
	worker := newFakeWorker(t, inner, WithWorkerSilentDisconnectTimeout(10*time.Millisecond))
	worker.clientFactory = func(context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error) {
		return client, nil, nil
	}
	worker.executor = &recordingExecutor{
		executeActivity: func(context.Context, api.InstanceID, *protos.HistoryEvent) (*protos.HistoryEvent, error) {
			return &protos.HistoryEvent{
				EventType: &protos.HistoryEvent_TaskCompleted{TaskCompleted: &protos.TaskCompletedEvent{}},
			}, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, worker.Start(ctx))
	require.Eventually(t, func() bool {
		inner.mu.Lock()
		defer inner.mu.Unlock()
		return len(inner.activityCompletions) == 1
	}, 5*time.Second, time.Millisecond, "the work item delivered with the silence timeout was dropped")

	cancel()
	require.NoError(t, worker.Shutdown(context.Background()))
	inner.mu.Lock()
	defer inner.mu.Unlock()
	require.Equal(t, "raced-token", inner.activityCompletions[0].CompletionToken)
	require.Zero(t, inner.activityAbandons)
}

// silencedRecv blocks until the silence timer cancels the stream and then
// reports the supplied terminal error, which makes the timer race deterministic.
func silencedRecv(ctx context.Context, err error) func() (*protos.WorkItem, error) {
	return func() (*protos.WorkItem, error) {
		<-ctx.Done()
		return nil, err
	}
}

func TestSilenceTimeoutOnlyRewritesItsOwnCancellation(t *testing.T) {
	for _, testCase := range []struct {
		name string
		err  error
		want error
	}{
		{
			name: "stream cancellation becomes the silent disconnect sentinel",
			err:  status.Error(codes.Canceled, "context canceled"),
			want: errSilentDisconnect,
		},
		{
			name: "bare context cancellation becomes the silent disconnect sentinel",
			err:  context.Canceled,
			want: errSilentDisconnect,
		},
		{
			name: "unauthenticated is propagated",
			err:  status.Error(codes.Unauthenticated, "token expired"),
			want: status.Error(codes.Unauthenticated, "token expired"),
		},
		{
			name: "permission denied is propagated",
			err:  status.Error(codes.PermissionDenied, "task hub forbidden"),
			want: status.Error(codes.PermissionDenied, "task hub forbidden"),
		},
		{
			name: "unavailable is propagated",
			err:  status.Error(codes.Unavailable, "endpoint restarting"),
			want: status.Error(codes.Unavailable, "endpoint restarting"),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := recvBeforeSilenceTimeout(silencedRecv(ctx, testCase.err), cancel, time.Millisecond)
			require.Equal(t, testCase.want, err)
		})
	}
}

// TestSilenceTimeoutRacingNonRetryableStatusStopsTheWorker asserts a genuine
// non-retryable status that races the silence timer ends the run loop instead
// of being masked as a retryable silent disconnect and reconnected forever.
func TestSilenceTimeoutRacingNonRetryableStatusStopsTheWorker(t *testing.T) {
	streamCtx, cancelStream := context.WithCancel(context.Background())
	defer cancelStream()
	authFailure := status.Error(codes.InvalidArgument, "invalid worker request")

	worker := newFakeWorker(
		t,
		&fakeSchedulerClient{stream: newFakeWorkItemStream(0)},
		WithWorkerSilentDisconnectTimeout(time.Millisecond),
		withRecordedWaits(&recordedWaits{}),
	)

	run := newRunLoopTestRun()
	defer run.cancelIntake()
	defer run.cancelProcessing()

	err := worker.runLoop(run, &grpcWorkerConnection{
		stream:       &silencedStream{ctx: streamCtx, err: authFailure},
		cancelStream: cancelStream,
	})
	require.ErrorContains(t, err, "non-retryable error")
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.NotErrorIs(t, err, errSilentDisconnect)
	run.retired.Wait()
}

// silencedStream never delivers a message and reports a terminal error only
// once the silence timer has canceled the stream.
type silencedStream struct {
	protos.TaskHubSidecarService_GetWorkItemsClient
	ctx context.Context
	err error
}

func (s *silencedStream) Recv() (*protos.WorkItem, error) {
	<-s.ctx.Done()
	return nil, s.err
}

func TestStreamHistoryHandlesManySingleEventChunksWithoutQuadraticCopying(t *testing.T) {
	const chunks = 20000
	history := make([]*protos.HistoryChunk, 0, chunks)
	for i := 0; i < chunks; i++ {
		history = append(history, &protos.HistoryChunk{
			Events: []*protos.HistoryEvent{{EventId: int32(i)}},
		})
	}
	client := &fakeSchedulerClient{stream: newFakeWorkItemStream(0), history: history}
	worker := newFakeWorker(t, client, WithWorkerSilentDisconnectTimeout(time.Minute))

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	events, err := worker.streamHistory(
		context.Background(),
		client,
		&protos.OrchestratorRequest{InstanceId: "instance"},
	)
	runtime.ReadMemStats(&after)

	require.NoError(t, err)
	require.Len(t, events, chunks)
	for i, event := range events {
		require.EqualValues(t, i, event.EventId, "history chunks must be accumulated in service order")
	}

	// Amortized appends allocate a small constant per event. Reallocating an
	// exactly sized slice per chunk is quadratic and allocates on the order of
	// chunks/2 pointers per event, which is several orders of magnitude more.
	bytesPerEvent := (after.TotalAlloc - before.TotalAlloc) / chunks
	require.Less(
		t,
		bytesPerEvent,
		uint64(8192),
		"history accumulation allocated %d bytes per event, which suggests quadratic copying",
		bytesPerEvent,
	)
}

func TestStreamHistoryEnforcesEventAndByteLimitsBeforeRetainingChunk(t *testing.T) {
	tests := []struct {
		name    string
		history []*protos.HistoryChunk
		option  TaskHubGrpcWorkerOption
		message string
	}{
		{
			name: "events",
			history: []*protos.HistoryChunk{
				{Events: []*protos.HistoryEvent{{EventId: 1}, {EventId: 2}}},
				{Events: []*protos.HistoryEvent{{EventId: 3}}},
			},
			option:  WithMaxStreamedHistoryEvents(2),
			message: "2 events",
		},
		{
			name: "bytes",
			history: []*protos.HistoryChunk{
				{Events: []*protos.HistoryEvent{{EventId: 1}}},
			},
			option:  WithMaxStreamedHistoryBytes(1),
			message: "1 bytes",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeSchedulerClient{
				stream:  newFakeWorkItemStream(0),
				history: test.history,
			}
			worker := newFakeWorker(t, client, test.option)

			events, err := worker.streamHistory(
				context.Background(),
				client,
				&protos.OrchestratorRequest{InstanceId: "instance"},
			)
			require.Nil(t, events)
			require.ErrorIs(t, err, ErrStreamedHistoryLimitExceeded)
			require.Equal(t, codes.ResourceExhausted, status.Code(err))
			require.ErrorContains(t, err, test.message)
		})
	}
}

// unrepresentableConcurrency returns the smallest concurrency that cannot be
// advertised on the 32-bit GetWorkItems fields. It is computed at runtime
// because the constant expression math.MaxInt32+1 does not fit in an int on
// 32-bit targets, where no int can exceed the limit and the case cannot exist.
func unrepresentableConcurrency() (int, bool) {
	limit := int64(math.MaxInt32) + 1
	if limit > int64(maxInt) {
		return 0, false
	}
	return int(limit), true
}

// maxInt is the largest value of the platform int type.
const maxInt = int(^uint(0) >> 1)

func TestWorkerLimitOptionValidation(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		option TaskHubGrpcWorkerOption
	}{
		{"zero orchestrations", WithMaxConcurrentOrchestrationWorkItems(0)},
		{"negative orchestrations", WithMaxConcurrentOrchestrationWorkItems(-1)},
		{"zero activities", WithMaxConcurrentActivityWorkItems(0)},
		{"negative activities", WithMaxConcurrentActivityWorkItems(-1)},
		{"zero entities", WithMaxConcurrentEntityWorkItems(0)},
		{"negative entities", WithMaxConcurrentEntityWorkItems(-1)},
		{"zero streamed history events", WithMaxStreamedHistoryEvents(0)},
		{"excessive streamed history events", WithMaxStreamedHistoryEvents(MaxStreamedHistoryEvents + 1)},
		{"zero streamed history bytes", WithMaxStreamedHistoryBytes(0)},
		{"excessive streamed history bytes", WithMaxStreamedHistoryBytes(MaxStreamedHistoryBytes + 1)},
		{"small orchestrator completion bytes", WithMaxOrchestratorCompletionBytes(minOrchestratorCompletionBytes - 1)},
		{"excessive orchestrator completion bytes", WithMaxOrchestratorCompletionBytes(DefaultMaxOrchestratorCompletionBytes + 1)},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			options := defaultTaskHubGrpcWorkerOptions()
			require.Error(t, testCase.option(&options))
		})
	}

	if unrepresentable, ok := unrepresentableConcurrency(); ok {
		for _, testCase := range []struct {
			name   string
			option TaskHubGrpcWorkerOption
		}{
			{"unrepresentable orchestrations", WithMaxConcurrentOrchestrationWorkItems(unrepresentable)},
			{"unrepresentable activities", WithMaxConcurrentActivityWorkItems(unrepresentable)},
			{"unrepresentable entities", WithMaxConcurrentEntityWorkItems(unrepresentable)},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				options := defaultTaskHubGrpcWorkerOptions()
				require.Error(t, testCase.option(&options))
			})
		}
	}

	options := defaultTaskHubGrpcWorkerOptions()
	require.NoError(t, WithMaxConcurrentOrchestrationWorkItems(math.MaxInt32)(&options))
	require.EqualValues(t, math.MaxInt32, int32(options.maxConcurrentOrchestrations))
	require.Equal(t, DefaultMaxStreamedHistoryEvents, options.maxStreamedHistoryEvents)
	require.Equal(t, DefaultMaxStreamedHistoryBytes, options.maxStreamedHistoryBytes)
	require.Equal(t, DefaultMaxOrchestratorCompletionBytes, options.maxOrchestratorCompletionBytes)
	require.NoError(t, WithMaxOrchestratorCompletionBytes(64*1024)(&options))
	require.NoError(t, WithMaxOrchestratorCompletionBytes(DefaultMaxOrchestratorCompletionBytes)(&options))
	require.Equal(t, 64*1024, options.maxOrchestratorCompletionBytes)
}

func TestWorkerOptionValidationRejectsInvalidConfiguration(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		option TaskHubGrpcWorkerOption
	}{
		{"non-positive hello timeout", WithWorkerHelloTimeout(0)},
		{"non-positive silent disconnect timeout", WithWorkerSilentDisconnectTimeout(-time.Second)},
		{"non-positive rpc timeout", WithWorkerRPCTimeout(0)},
		{"non-positive reconnect base delay", WithWorkerReconnectBackoff(0, time.Second)},
		{"inverted reconnect delays", WithWorkerReconnectBackoff(time.Second, time.Millisecond)},
		{"non-positive retry attempts", WithWorkerTransientRetryPolicy(0, time.Second, time.Second)},
		{"non-positive retry base delay", WithWorkerTransientRetryPolicy(3, 0, time.Second)},
		{"inverted retry delays", WithWorkerTransientRetryPolicy(3, time.Second, time.Millisecond)},
		{"negative maximum timer interval", WithMaximumTimerInterval(-time.Second)},
		{"nil task executor option", WithTaskExecutorOptions(nil)},
		{"unsupported capability", WithWorkerCapabilities(WorkerCapability(9999))},
		{"unspecified capability", WithWorkerCapabilities(protos.WorkerCapability_WORKER_CAPABILITY_UNSPECIFIED)},
		{"blank unversioned orchestrator name", WithUnversionedOrchestratorNames("  ")},
		{"empty orchestration filter name", WithWorkItemFilters(&WorkItemFilters{
			Orchestrations: []WorkItemFilter{{Name: ""}},
		})},
		{"empty activity filter name", WithWorkItemFilters(&WorkItemFilters{
			Activities: []WorkItemFilter{{Name: ""}},
		})},
		{"blank entity filter name", WithWorkItemFilters(&WorkItemFilters{Entities: []string{" "}})},
		{"contradictory orchestration rejection", WithWorkItemFilters(&WorkItemFilters{
			Orchestrations:          []WorkItemFilter{{Name: "orchestration"}},
			RejectAllOrchestrations: true,
		})},
		{"contradictory activity rejection", WithWorkItemFilters(&WorkItemFilters{
			Activities:          []WorkItemFilter{{Name: "activity"}},
			RejectAllActivities: true,
		})},
		{"contradictory entity rejection", WithWorkItemFilters(&WorkItemFilters{
			Entities:          []string{"counter"},
			RejectAllEntities: true,
		})},
		{"duplicate orchestration filter", WithWorkItemFilters(&WorkItemFilters{
			Orchestrations: []WorkItemFilter{{Name: "Orchestration"}, {Name: "orchestration"}},
		})},
		{"duplicate activity filter", WithWorkItemFilters(&WorkItemFilters{
			Activities: []WorkItemFilter{{Name: "Activity"}, {Name: "activity"}},
		})},
		{"duplicate entity filter", WithWorkItemFilters(&WorkItemFilters{
			Entities: []string{"Counter", "counter"},
		})},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			options := defaultTaskHubGrpcWorkerOptions()
			require.Error(t, testCase.option(&options))
		})
	}
}

func TestWorkerConstructorPropagatesOptionValidationErrors(t *testing.T) {
	factory := func(context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error) {
		return nil, nil, errors.New("unused")
	}
	_, err := newTaskHubGrpcWorker(
		factory,
		task.NewTaskRegistry(),
		api.DefaultLogger(),
		WithTaskExecutorOptions(nil),
	)
	require.ErrorContains(t, err, "task executor option cannot be nil")

	_, err = newTaskHubGrpcWorker(
		factory,
		task.NewTaskRegistry(),
		api.DefaultLogger(),
		WithWorkerCapabilities(WorkerCapabilityLargePayloads),
	)
	require.ErrorContains(t, err, "large-payload capability requires worker large-payload options")

	_, err = NewTaskHubGrpcWorker(nil, task.NewTaskRegistry(), api.DefaultLogger())
	require.ErrorContains(t, err, "gRPC connection is required")

	_, err = NewTaskHubGrpcWorkerWithConnectionFactory(nil, task.NewTaskRegistry(), api.DefaultLogger())
	require.ErrorContains(t, err, "connection factory is required")

	_, err = newTaskHubGrpcWorker(factory, nil, api.DefaultLogger())
	require.ErrorContains(t, err, "task registry is required")
}

// countingCloser records how many times a retired connection was closed.
type countingCloser struct {
	closes atomic.Int32
}

func (c *countingCloser) Close() error {
	c.closes.Add(1)
	return nil
}

func TestConnectionFactoryRejectsNilConnections(t *testing.T) {
	closer := &countingCloser{}
	worker, err := NewTaskHubGrpcWorkerWithConnectionFactory(
		func(context.Context) (grpc.ClientConnInterface, io.Closer, error) {
			return nil, closer, nil
		},
		task.NewTaskRegistry(),
		api.DefaultLogger(),
	)
	require.NoError(t, err)
	err = worker.Start(context.Background())
	require.ErrorContains(t, err, "returned a nil connection")
	require.False(t, worker.Running())
	require.EqualValues(t, 1, closer.closes.Load(), "a rejected connection must still be closed")
}

func TestOwnedConnectionFactoryRecreatesAndClosesRetiredConnections(t *testing.T) {
	firstStream := newFakeWorkItemStream(1)
	firstStream.results <- fakeWorkItemResult{err: status.Error(codes.Unavailable, "disconnect")}
	secondStream := newFakeWorkItemStream(1)
	secondStream.results <- fakeWorkItemResult{item: &protos.WorkItem{
		Request: &protos.WorkItem_HealthPing{HealthPing: &protos.HealthPing{}},
	}}
	clients := []*fakeSchedulerClient{{stream: firstStream}, {stream: secondStream}}
	closers := []*countingCloser{{}, {}}

	var generations atomic.Int32
	worker, err := newTaskHubGrpcWorker(
		func(context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error) {
			index := int(generations.Add(1)) - 1
			if index >= len(clients) {
				index = len(clients) - 1
			}
			return clients[index], closers[index], nil
		},
		task.NewTaskRegistry(),
		api.DefaultLogger(),
		WithWorkerHelloTimeout(time.Second),
		WithWorkerSilentDisconnectTimeout(time.Second),
		WithWorkerReconnectBackoff(time.Millisecond, 5*time.Millisecond),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, worker.Start(ctx))
	require.Eventually(t, func() bool {
		return closers[0].closes.Load() == 1
	}, 5*time.Second, time.Millisecond, "the retired owned connection was never closed")
	require.Eventually(t, func() bool {
		return generations.Load() >= 2
	}, 5*time.Second, time.Millisecond)

	cancel()
	require.NoError(t, worker.Shutdown(context.Background()))
	require.EqualValues(t, 1, closers[0].closes.Load(), "retired connections must be closed exactly once")
}

// fakeClientConn is a borrowed grpc.ClientConnInterface that records how many
// streams the worker opened on it and whether it was ever closed.
type fakeClientConn struct {
	mu      sync.Mutex
	streams int
	items   []*protos.WorkItem
	closes  atomic.Int32
}

func (c *fakeClientConn) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	return nil
}

func (c *fakeClientConn) NewStream(
	ctx context.Context,
	_ *grpc.StreamDesc,
	_ string,
	_ ...grpc.CallOption,
) (grpc.ClientStream, error) {
	c.mu.Lock()
	c.streams++
	stream := &fakeClientStream{ctx: ctx, items: c.items, generation: c.streams}
	c.mu.Unlock()
	return stream, nil
}

func (c *fakeClientConn) Close() error {
	c.closes.Add(1)
	return nil
}

func (c *fakeClientConn) streamCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.streams
}

type fakeClientStream struct {
	ctx        context.Context
	items      []*protos.WorkItem
	generation int
	index      int
}

func (s *fakeClientStream) Header() (metadata.MD, error) { return nil, nil }
func (s *fakeClientStream) Trailer() metadata.MD         { return nil }
func (s *fakeClientStream) CloseSend() error             { return nil }
func (s *fakeClientStream) Context() context.Context     { return s.ctx }
func (s *fakeClientStream) SendMsg(any) error            { return nil }

// RecvMsg lets the first stream generation deliver its items and then
// disconnect; later generations stay open so the worker settles.
func (s *fakeClientStream) RecvMsg(message any) error {
	if s.generation == 1 {
		if s.index < len(s.items) {
			proto.Merge(message.(*protos.WorkItem), s.items[s.index])
			s.index++
			return nil
		}
		return status.Error(codes.Unavailable, "disconnect")
	}
	<-s.ctx.Done()
	return status.FromContextError(s.ctx.Err()).Err()
}

func TestBorrowedConnectionIsReusedAcrossReconnectsAndNeverClosed(t *testing.T) {
	connection := &fakeClientConn{items: []*protos.WorkItem{{
		Request: &protos.WorkItem_HealthPing{HealthPing: &protos.HealthPing{}},
	}}}
	worker, err := NewTaskHubGrpcWorker(
		connection,
		task.NewTaskRegistry(),
		api.DefaultLogger(),
		WithWorkerHelloTimeout(time.Second),
		WithWorkerSilentDisconnectTimeout(time.Second),
		WithWorkerReconnectBackoff(time.Millisecond, 5*time.Millisecond),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, worker.Start(ctx))
	require.Eventually(t, func() bool {
		return connection.streamCount() >= 2
	}, 5*time.Second, time.Millisecond, "the worker did not reopen a stream after a transient disconnect")

	cancel()
	require.NoError(t, worker.Shutdown(context.Background()))

	// Documented limitation: a borrowed connection is never replaced or closed,
	// so every reconnect reuses the caller's channel however wedged it is. Only
	// the connection-factory worker, which durabletaskscheduler.NewWorker uses,
	// can obtain a fresh channel and close the retired one.
	require.Zero(t, connection.closes.Load(), "the worker must never close a borrowed connection")
	require.GreaterOrEqual(t, connection.streamCount(), 2)
}
