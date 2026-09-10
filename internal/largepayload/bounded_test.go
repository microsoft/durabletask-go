package largepayload

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/payload"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var errOperation = errors.New("operation failed")

func cancelledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func memoryStoreOptions(store *payload.MemoryStore) *api.LargePayloadOptions {
	return &api.LargePayloadOptions{
		Store:           store,
		Resolver:        store,
		ThresholdBytes:  1,
		MaxPayloadBytes: 1024,
	}
}

func externalizedValue(
	t *testing.T,
	options *api.LargePayloadOptions,
	value string,
) *wrapperspb.StringValue {
	t.Helper()
	externalized, err := Externalize(context.Background(), options, wrapperspb.String(value))
	require.NoError(t, err)
	return externalized
}

func TestRunBoundedDispatchAndCancellation(t *testing.T) {
	tests := []struct {
		name        string
		ctx         context.Context
		results     []error
		wantErr     error
		wantStarted int64
	}{
		{
			name: "no operations never reports cancellation",
			ctx:  cancelledContext(),
		},
		{
			name:        "single operation runs",
			ctx:         context.Background(),
			results:     []error{nil},
			wantStarted: 1,
		},
		{
			name:        "single operation is skipped when already cancelled",
			ctx:         cancelledContext(),
			results:     []error{nil},
			wantErr:     context.Canceled,
			wantStarted: 0,
		},
		{
			name:        "many operations are skipped when already cancelled",
			ctx:         cancelledContext(),
			results:     []error{nil, nil, nil, nil},
			wantErr:     context.Canceled,
			wantStarted: 0,
		},
		{
			name:        "many operations all run",
			ctx:         context.Background(),
			results:     []error{nil, nil, nil, nil},
			wantStarted: 4,
		},
		{
			name:        "operation failure is returned",
			ctx:         context.Background(),
			results:     []error{nil, errOperation},
			wantErr:     errOperation,
			wantStarted: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var started atomic.Int64
			operations := make([]func(context.Context) error, 0, len(test.results))
			for _, result := range test.results {
				operations = append(operations, func(context.Context) error {
					started.Add(1)
					return result
				})
			}
			err := runBounded(test.ctx, operations)
			if test.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, test.wantErr)
			}
			require.Equal(t, test.wantStarted, started.Load())
		})
	}
}

// TestRunBoundedReturnsLowestOrdinalFailure pins message order as the tie
// breaker rather than whichever concurrent operation happens to fail first.
func TestRunBoundedReturnsLowestOrdinalFailure(t *testing.T) {
	errEarly := errors.New("early field")
	errLate := errors.New("late field")
	lateFailed := make(chan struct{})

	operations := []func(context.Context) error{
		func(context.Context) error { return nil },
		func(context.Context) error {
			<-lateFailed
			return errEarly
		},
		func(context.Context) error { return nil },
		func(context.Context) error {
			close(lateFailed)
			return errLate
		},
	}

	require.ErrorIs(t, runBounded(context.Background(), operations), errEarly)
}

// TestRunBoundedStopsDispatchAndDrainsClaimedOperations verifies that a failure
// prevents any further operation from starting while every operation that did
// start runs to completion before runBounded returns.
func TestRunBoundedStopsDispatchAndDrainsClaimedOperations(t *testing.T) {
	const operationCount = 4 * maxConcurrentPayloadOperations
	var (
		started  atomic.Int64
		finished atomic.Int64
	)
	release := make(chan struct{})
	failureRecorded := make(chan struct{})

	operations := make([]func(context.Context) error, 0, operationCount)
	// The first slots stay occupied so the dispatch loop can only advance past
	// the concurrency bound once the failing operation frees its slot, which it
	// does strictly after recording the failure.
	for i := 0; i < maxConcurrentPayloadOperations-1; i++ {
		operations = append(operations, func(context.Context) error {
			started.Add(1)
			<-release
			finished.Add(1)
			return nil
		})
	}
	operations = append(operations, func(context.Context) error {
		started.Add(1)
		finished.Add(1)
		return errOperation
	})
	for i := maxConcurrentPayloadOperations; i < operationCount; i++ {
		operations = append(operations, func(context.Context) error {
			started.Add(1)
			finished.Add(1)
			return nil
		})
	}

	result := make(chan error, 1)
	go func() {
		result <- runBoundedWithHook(context.Background(), operations, func() { close(failureRecorded) })
	}()

	<-failureRecorded
	close(release)
	require.ErrorIs(t, <-result, errOperation)
	require.Equal(t, int64(maxConcurrentPayloadOperations), started.Load(),
		"no operation may start after a failure is recorded")
	require.Equal(t, started.Load(), finished.Load(), "claimed operations must drain")
}

// TestRunBoundedFailureWinsOverCancellation pins failure precedence: a real
// error is never masked by cancellation that arrives alongside it.
func TestRunBoundedFailureWinsOverCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	operations := []func(context.Context) error{
		func(context.Context) error {
			cancel()
			return errOperation
		},
		func(context.Context) error { return nil },
		func(context.Context) error { return nil },
	}

	err := runBounded(ctx, operations)
	require.ErrorIs(t, err, errOperation)
	require.NotErrorIs(t, err, context.Canceled)
}

// TestRunBoundedCancellationAfterFullDispatchSucceeds verifies that
// cancellation observed once every operation has started does not turn
// completed work into an error.
func TestRunBoundedCancellationAfterFullDispatchSucceeds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	operations := []func(context.Context) error{
		func(context.Context) error { return nil },
		func(context.Context) error {
			// The final operation only runs once dispatch is complete.
			cancel()
			return nil
		},
	}

	require.NoError(t, runBounded(ctx, operations))
}

// TestTransformPlanUsesFieldSnapshot verifies that a queued field is
// transformed from the value observed when the plan was built.
func TestTransformPlanUsesFieldSnapshot(t *testing.T) {
	store := payload.NewMemoryStore()
	options := memoryStoreOptions(store)

	target := wrapperspb.String("planned")
	plan := newTransformPlan(options, Externalize)
	plan.add(&target)
	target = wrapperspb.String("replaced after planning")

	require.NoError(t, plan.run(context.Background()))
	hydrated, err := Hydrate(context.Background(), options, target)
	require.NoError(t, err)
	require.Equal(t, "planned", hydrated.GetValue())
}

func TestTransformPlanSkipsAbsentFields(t *testing.T) {
	store := payload.NewMemoryStore()
	options := memoryStoreOptions(store)
	state := &protos.OrchestrationState{}

	// A message with no payloads is zero-work, so a cancelled context has
	// nothing to cancel.
	require.NoError(t, TransformOrchestrationState(cancelledContext(), options, state))
	require.NoError(t, TransformOrchestrationState(cancelledContext(), options, nil))
	require.NoError(t, TransformOrchestratorResponse(cancelledContext(), options, &protos.OrchestratorResponse{}))
	require.NoError(t, TransformEntityBatchRequest(cancelledContext(), options, &protos.EntityBatchRequest{}))
}

// TestTransformResolvesFieldsConcurrently deadlocks unless every payload field
// of a message is resolved concurrently under the shared bound.
func TestTransformResolvesFieldsConcurrently(t *testing.T) {
	store := payload.NewMemoryStore()
	options := memoryStoreOptions(store)

	values := []string{"state", "first", "second", "third"}
	request := &protos.EntityBatchRequest{Operations: make([]*protos.OperationRequest, 0, len(values)-1)}
	for i, value := range values {
		externalized := externalizedValue(t, options, value)
		if i == 0 {
			request.EntityState = externalized
			continue
		}
		request.Operations = append(request.Operations, &protos.OperationRequest{Input: externalized})
	}

	var barrier sync.WaitGroup
	barrier.Add(len(values))
	options.Resolver = barrierResolver{store: store, barrier: &barrier}

	require.NoError(t, TransformEntityBatchRequest(context.Background(), options, request))
	require.Equal(t, "state", request.EntityState.GetValue())
	for i, operation := range request.Operations {
		require.Equal(t, values[i+1], operation.Input.GetValue())
	}
}

// TestTransformPlansFieldsInMessageOrder pins the ordinals a message assigns to
// its payload fields. runBounded returns the lowest-ordinal failure, so failing
// a known set of fields makes the plan order observable without timing control.
func TestTransformPlansFieldsInMessageOrder(t *testing.T) {
	store := payload.NewMemoryStore()
	options := memoryStoreOptions(store)

	externalized := make([]*wrapperspb.StringValue, 0, 3)
	locations := make([]string, 0, 3)
	for _, value := range []string{"state", "first", "second"} {
		external := externalizedValue(t, options, value)
		externalized = append(externalized, external)
		locations = append(locations, referenceLocation(t, external))
	}

	errState := errors.New("entity state field")
	errFirst := errors.New("first operation field")
	errSecond := errors.New("second operation field")

	tests := []struct {
		name     string
		failures map[string]error
		want     error
	}{
		{
			name:     "entity state precedes the operations",
			failures: map[string]error{locations[0]: errState, locations[1]: errFirst, locations[2]: errSecond},
			want:     errState,
		},
		{
			name:     "operations keep their slice order",
			failures: map[string]error{locations[1]: errFirst, locations[2]: errSecond},
			want:     errFirst,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			failing := &api.LargePayloadOptions{
				Store:           store,
				Resolver:        failingResolver{store: store, failures: test.failures},
				ThresholdBytes:  1,
				MaxPayloadBytes: 1024,
			}
			err := TransformEntityBatchRequest(context.Background(), failing, &protos.EntityBatchRequest{
				EntityState: externalized[0],
				Operations: []*protos.OperationRequest{
					{Input: externalized[1]},
					{Input: externalized[2]},
				},
			})
			require.ErrorIs(t, err, test.want)
		})
	}
}

// TestTransformPlanQueuesEachFieldOnce verifies that a field a message reaches
// twice is transformed exactly once. Without deduplication the field would be
// externalized twice, leaving an orphaned payload behind in the store.
func TestTransformPlanQueuesEachFieldOnce(t *testing.T) {
	store := payload.NewMemoryStore()
	var stores atomic.Int64
	options := &api.LargePayloadOptions{
		Store:           countingStore{store: store, calls: &stores},
		Resolver:        store,
		ThresholdBytes:  1,
		MaxPayloadBytes: 1024,
	}

	// A rewind action can list the same event object more than once, so both
	// entries resolve to the same input field.
	event := &protos.HistoryEvent{EventType: &protos.HistoryEvent_ExecutionStarted{
		ExecutionStarted: &protos.ExecutionStartedEvent{Input: wrapperspb.String("payload")},
	}}
	response := &protos.OrchestratorResponse{Actions: []*protos.OrchestratorAction{
		{OrchestratorActionType: &protos.OrchestratorAction_RewindOrchestration{
			RewindOrchestration: &protos.RewindOrchestrationAction{
				NewHistory: []*protos.HistoryEvent{event, event},
			},
		}},
	}}

	require.NoError(t, TransformOrchestratorResponse(context.Background(), options, response))
	require.Equal(t, int64(1), stores.Load(), "a field reached twice must be externalized once")

	hydrated, err := Hydrate(context.Background(), options, event.GetExecutionStarted().Input)
	require.NoError(t, err)
	require.Equal(t, "payload", hydrated.GetValue())
}

// TestTransformMessageShapesResolveEveryPayloadField walks the multi-field
// messages that use bounded concurrency and verifies every payload round-trips.
func TestTransformMessageShapesResolveEveryPayloadField(t *testing.T) {
	store := payload.NewMemoryStore()
	options := memoryStoreOptions(store)
	external := func(value string) *wrapperspb.StringValue {
		externalized := externalizedValue(t, options, value)
		require.NotEqual(t, value, externalized.GetValue())
		return externalized
	}
	startedEvent := func(input string) *protos.HistoryEvent {
		return &protos.HistoryEvent{EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{Input: external(input)},
		}}
	}

	orchestratorRequest := &protos.OrchestratorRequest{
		PastEvents: []*protos.HistoryEvent{startedEvent("past-one"), startedEvent("past-two")},
		NewEvents:  []*protos.HistoryEvent{startedEvent("new-one")},
	}
	state := &protos.OrchestrationState{
		Input:        external("state-input"),
		Output:       external("state-output"),
		CustomStatus: external("state-status"),
	}
	entityRequest := &protos.EntityBatchRequest{
		EntityState: external("entity-state"),
		Operations: []*protos.OperationRequest{
			{Input: external("entity-one")},
			{Input: external("entity-two")},
		},
	}

	require.NoError(t, TransformOrchestratorRequest(context.Background(), options, orchestratorRequest))
	require.NoError(t, TransformOrchestrationState(context.Background(), options, state))
	require.NoError(t, TransformEntityBatchRequest(context.Background(), options, entityRequest))

	require.Equal(t, "past-one", orchestratorRequest.PastEvents[0].GetExecutionStarted().Input.GetValue())
	require.Equal(t, "past-two", orchestratorRequest.PastEvents[1].GetExecutionStarted().Input.GetValue())
	require.Equal(t, "new-one", orchestratorRequest.NewEvents[0].GetExecutionStarted().Input.GetValue())
	require.Equal(t, "state-input", state.Input.GetValue())
	require.Equal(t, "state-output", state.Output.GetValue())
	require.Equal(t, "state-status", state.CustomStatus.GetValue())
	require.Equal(t, "entity-state", entityRequest.EntityState.GetValue())
	require.Equal(t, "entity-one", entityRequest.Operations[0].Input.GetValue())
	require.Equal(t, "entity-two", entityRequest.Operations[1].Input.GetValue())
}

// TestTransformPreCancelledContextPerformsNoStoreCalls verifies that a message
// with payload fields reports cancellation without touching the payload store.
func TestTransformPreCancelledContextPerformsNoStoreCalls(t *testing.T) {
	store := payload.NewMemoryStore()
	options := memoryStoreOptions(store)
	request := &protos.EntityBatchRequest{
		EntityState: externalizedValue(t, options, "state"),
		Operations:  []*protos.OperationRequest{{Input: externalizedValue(t, options, "input")}},
	}

	var calls atomic.Int64
	options.Resolver = countingResolver{store: store, calls: &calls}
	require.ErrorIs(t, TransformEntityBatchRequest(cancelledContext(), options, request), context.Canceled)
	require.Zero(t, calls.Load())
}

type countingResolver struct {
	store *payload.MemoryStore
	calls *atomic.Int64
}

func (r countingResolver) Resolve(ctx context.Context, location string) ([]byte, error) {
	r.calls.Add(1)
	return r.store.Resolve(ctx, location)
}

type countingStore struct {
	store *payload.MemoryStore
	calls *atomic.Int64
}

func (s countingStore) Store(ctx context.Context, value []byte) (string, error) {
	s.calls.Add(1)
	return s.store.Store(ctx, value)
}

func referenceLocation(t *testing.T, value *wrapperspb.StringValue) string {
	t.Helper()
	ref, ok, err := parseReference(value.GetValue(), api.DefaultLargePayloadMaxBytes)
	require.NoError(t, err)
	require.True(t, ok)
	return ref.Location
}

type barrierResolver struct {
	store   *payload.MemoryStore
	barrier *sync.WaitGroup
}

func (r barrierResolver) Resolve(ctx context.Context, location string) ([]byte, error) {
	r.barrier.Done()
	r.barrier.Wait()
	return r.store.Resolve(ctx, location)
}

// failingResolver fails the configured locations and resolves everything else,
// which makes the ordinal a message assigned to a field observable.
type failingResolver struct {
	store    *payload.MemoryStore
	failures map[string]error
}

func (r failingResolver) Resolve(ctx context.Context, location string) ([]byte, error) {
	if failure, ok := r.failures[location]; ok {
		return nil, failure
	}
	return r.store.Resolve(ctx, location)
}
