package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/largepayload"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/payload"
	"github.com/microsoft/durabletask-go/task"
	"github.com/microsoft/durabletask-go/tracing"
)

type tracingHarness struct {
	recorder *tracetest.SpanRecorder
	client   *fakeSchedulerClient
	worker   *TaskHubGrpcWorker
}

func newTracingHarness(
	t *testing.T,
	registry *task.TaskRegistry,
	enabled bool,
	executorOptions ...task.TaskExecutorOption,
) *tracingHarness {
	t.Helper()
	return newTracingHarnessWithProviderOptions(t, registry, enabled, nil, executorOptions...)
}

func newTracingHarnessWithProviderOptions(
	t *testing.T,
	registry *task.TaskRegistry,
	enabled bool,
	providerOptions []sdktrace.TracerProviderOption,
	executorOptions ...task.TaskExecutorOption,
) *tracingHarness {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(append([]sdktrace.TracerProviderOption{
		tracing.WithIDGenerator(nil),
		tracing.WithSampler(nil),
		sdktrace.WithSpanProcessor(recorder),
	}, providerOptions...)...)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	client := new(fakeSchedulerClient)
	var options []TaskHubGrpcWorkerOption
	if enabled {
		options = append(options, WithWorkerTracerProvider(provider))
	}
	worker := newFakeWorker(t, client, options...)
	worker.executor = task.NewTaskExecutor(registry, executorOptions...)
	return &tracingHarness{recorder: recorder, client: client, worker: worker}
}

func (h *tracingHarness) spans() []sdktrace.ReadOnlySpan {
	return h.recorder.Ended()
}

// fakeOrchestrationBackend persists history and orchestration trace context
// between dispatches the way a Durable Task backend does.
type fakeOrchestrationBackend struct {
	instanceID       string
	history          []*protos.HistoryEvent
	traceContext     *protos.OrchestrationTraceContext
	scheduled        map[int32]*protos.TraceContext
	pending          []*protos.HistoryEvent
	entityParameters *protos.OrchestratorEntityParameters
}

func newFakeOrchestrationBackend(instanceID string, started *protos.HistoryEvent) *fakeOrchestrationBackend {
	return &fakeOrchestrationBackend{
		instanceID: instanceID,
		scheduled:  make(map[int32]*protos.TraceContext),
		pending:    []*protos.HistoryEvent{started},
	}
}

// dispatch delivers pending events plus newEvents and records the accepted
// response into history. Events left unconsumed under MaxEventsPerTurn are
// redelivered on the next dispatch.
func (b *fakeOrchestrationBackend) dispatch(
	t *testing.T,
	h *tracingHarness,
	newEvents ...*protos.HistoryEvent,
) *protos.OrchestratorResponse {
	t.Helper()
	delivered := append(append([]*protos.HistoryEvent(nil), b.pending...), newEvents...)
	b.pending = nil
	request := &protos.OrchestratorRequest{
		InstanceId:                b.instanceID,
		PastEvents:                append([]*protos.HistoryEvent(nil), b.history...),
		NewEvents:                 append([]*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent()}, delivered...),
		OrchestrationTraceContext: b.traceContext,
		EntityParameters:          b.entityParameters,
	}
	before := len(h.client.orchestrationCompletions)
	h.worker.processOrchestration(context.Background(), h.client, "token", request)
	require.Len(t, h.client.orchestrationCompletions, before+1)
	response := h.client.orchestrationCompletions[before]

	consumed := request.NewEvents
	if processed := response.GetNumEventsProcessed(); processed != nil {
		consumed = request.NewEvents[:1+int(processed.GetValue())]
		b.pending = append(b.pending, request.NewEvents[1+int(processed.GetValue()):]...)
	}
	b.history = append(b.history, consumed...)
	for _, action := range response.Actions {
		switch {
		case action.GetScheduleTask() != nil:
			scheduled := action.GetScheduleTask()
			b.scheduled[action.Id] = scheduled.ParentTraceContext
			b.history = append(b.history, helpers.NewTaskScheduledEvent(
				action.Id, scheduled.Name, scheduled.Version, scheduled.Input, scheduled.ParentTraceContext))
		case action.GetCreateTimer() != nil:
			b.history = append(b.history, helpers.NewTimerCreatedEvent(action.Id, action.GetCreateTimer().FireAt))
		case action.GetCreateSubOrchestration() != nil:
			created := action.GetCreateSubOrchestration()
			b.scheduled[action.Id] = created.ParentTraceContext
			b.history = append(b.history, helpers.NewSubOrchestrationCreatedEvent(
				action.Id, created.Name, created.Version, created.Input, created.InstanceId, created.ParentTraceContext))
		case action.GetSendEvent() != nil:
			b.history = append(b.history, tracingEventSent(action))
		case action.GetSendEntityMessage() != nil:
			b.history = append(b.history, tracingEntitySent(action))
		}
	}
	b.traceContext = response.OrchestrationTraceContext
	return response
}

func tracingEntitySent(action *protos.OrchestratorAction) *protos.HistoryEvent {
	event := &protos.HistoryEvent{EventId: action.Id, Timestamp: timestamppb.Now()}
	message := action.GetSendEntityMessage()
	if called := message.GetEntityOperationCalled(); called != nil {
		event.EventType = &protos.HistoryEvent_EntityOperationCalled{
			EntityOperationCalled: proto.Clone(called).(*protos.EntityOperationCalledEvent),
		}
	} else if signal := message.GetEntityOperationSignaled(); signal != nil {
		event.EventType = &protos.HistoryEvent_EntityOperationSignaled{
			EntityOperationSignaled: proto.Clone(signal).(*protos.EntityOperationSignaledEvent),
		}
	} else if lock := message.GetEntityLockRequested(); lock != nil {
		event.EventType = &protos.HistoryEvent_EntityLockRequested{
			EntityLockRequested: proto.Clone(lock).(*protos.EntityLockRequestedEvent),
		}
	} else if unlock := message.GetEntityUnlockSent(); unlock != nil {
		event.EventType = &protos.HistoryEvent_EntityUnlockSent{
			EntityUnlockSent: proto.Clone(unlock).(*protos.EntityUnlockSentEvent),
		}
	}
	return event
}

func TestWorkerTracingEntityLocksEmitNoSpans(t *testing.T) {
	id := api.NewEntityID("counter", "key")
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("Locker", func(ctx *task.OrchestrationContext) (any, error) {
		unlock, err := ctx.LockEntities(id)
		if err != nil {
			return nil, err
		}
		_ = ctx.CallEntity(id, "get").Await(nil)
		unlock()
		return nil, ctx.WaitForSingleEvent("finish", -1).Await(nil)
	}))
	h := newTracingHarness(t, registry, true)
	backend := newFakeOrchestrationBackend("locker", helpers.NewExecutionStartedEvent(
		"Locker", "locker", nil, nil, nil, nil))
	backend.entityParameters = &protos.OrchestratorEntityParameters{EntityMessageReorderWindow: durationpb.New(0)}

	response := backend.dispatch(t, h)
	require.Len(t, response.Actions, 1)
	lock := response.Actions[0].GetSendEntityMessage().GetEntityLockRequested()
	require.NotNil(t, lock)
	granted := &protos.HistoryEvent{EventId: -1, Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_EntityLockGranted{
			EntityLockGranted: &protos.EntityLockGrantedEvent{CriticalSectionId: lock.CriticalSectionId},
		}}
	response = backend.dispatch(t, h, granted)
	var called *protos.EntityOperationCalledEvent
	for _, action := range response.Actions {
		if c := action.GetSendEntityMessage().GetEntityOperationCalled(); c != nil {
			called = c
		}
	}
	require.NotNil(t, called)
	completed := &protos.HistoryEvent{EventId: -1, Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_EntityOperationCompleted{
			EntityOperationCompleted: &protos.EntityOperationCompletedEvent{RequestId: called.RequestId},
		}}
	response = backend.dispatch(t, h, completed)
	unlocked := false
	for _, action := range response.Actions {
		unlocked = unlocked || action.GetSendEntityMessage().GetEntityUnlockSent() != nil
	}
	require.True(t, unlocked)
	backend.dispatch(t, h)

	// Lock acquisition and release emit no spans; calls in the critical section
	// are traced once.
	spans := h.spans()
	require.Len(t, spans, 1)
	require.Equal(t, "entity:counter:get", spans[0].Name())
	require.Equal(t, trace.SpanKindClient, spans[0].SpanKind())
}

func TestWorkerTracingEntityCallsAndSignalsReplay(t *testing.T) {
	for _, mode := range []string{"enabled", "disabled", "unsampled", "failed-call"} {
		t.Run(mode, func(t *testing.T) {
			id := api.NewEntityID("counter", "key")
			scheduled := time.Now().UTC().Add(time.Hour)
			registry := task.NewTaskRegistry()
			require.NoError(t, registry.AddOrchestratorN("EntityCaller", func(ctx *task.OrchestrationContext) (any, error) {
				if err := ctx.SignalEntity(id, "add", task.WithSignalEntityScheduledTime(scheduled)); err != nil {
					return nil, err
				}
				_ = ctx.CallEntity(id, "get").Await(nil)
				return nil, ctx.WaitForSingleEvent("finish", -1).Await(nil)
			}))
			flags := trace.FlagsSampled
			if mode == "unsampled" {
				flags = 0
			}
			_, parent := newTraceParent(flags)
			backend := newFakeOrchestrationBackend("caller", helpers.NewExecutionStartedEvent(
				"EntityCaller", "caller", nil, nil, parent, nil))
			backend.entityParameters = &protos.OrchestratorEntityParameters{EntityMessageReorderWindow: durationpb.New(0)}
			h := newTracingHarness(t, registry, mode != "disabled")
			response := backend.dispatch(t, h)
			require.Len(t, response.Actions, 2)
			var called *protos.EntityOperationCalledEvent
			for _, action := range response.Actions {
				if c := action.GetSendEntityMessage().GetEntityOperationCalled(); c != nil {
					called = c
				}
			}
			require.NotNil(t, called)
			want := 1
			if mode == "disabled" || mode == "unsampled" {
				want = 0
			}
			require.Len(t, spansNamed(h.spans(), "entity:counter:add"), want)
			backend.dispatch(t, h)
			require.Len(t, spansNamed(h.spans(), "entity:counter:add"), want)
			require.Empty(t, spansNamed(h.spans(), "entity:counter:get"))
			completed := &protos.HistoryEvent{Timestamp: timestamppb.Now(),
				EventType: &protos.HistoryEvent_EntityOperationCompleted{
					EntityOperationCompleted: &protos.EntityOperationCompletedEvent{RequestId: called.RequestId},
				}}
			if mode == "failed-call" {
				completed.EventType = &protos.HistoryEvent_EntityOperationFailed{
					EntityOperationFailed: &protos.EntityOperationFailedEvent{
						RequestId: called.RequestId, FailureDetails: &protos.TaskFailureDetails{ErrorMessage: "entity failed"},
					}}
			}
			backend.dispatch(t, h, completed)
			require.Len(t, spansNamed(h.spans(), "entity:counter:get"), want)
			if want != 0 {
				signal := spansNamed(h.spans(), "entity:counter:add")[0]
				call := spansNamed(h.spans(), "entity:counter:get")[0]
				require.Equal(t, trace.SpanKindProducer, signal.SpanKind())
				require.Equal(t, trace.SpanKindClient, call.SpanKind())
				require.Equal(t, "signal_entity", spanAttribute(signal, "durabletask.task.operation"))
				require.Equal(t, "call_entity", spanAttribute(call, "durabletask.task.operation"))
				require.Equal(t, id.String(), spanAttribute(call, "durabletask.event.target_instance_id"))
				require.Equal(t, scheduled.Format(time.RFC3339Nano), spanAttribute(signal, "durabletask.task.scheduled_time"))
				require.Equal(t, response.OrchestrationTraceContext.GetSpanID().GetValue(), call.Parent().SpanID().String())
				require.Equal(t, helpers.SpanContextFromTraceContext(parent).TraceID(), call.SpanContext().TraceID())
				require.True(t, completed.Timestamp.AsTime().Equal(call.EndTime()))
				if mode == "failed-call" {
					require.Equal(t, codes.Error, call.Status().Code)
					require.Equal(t, "entity failed", call.Status().Description)
				}
			}
			backend.dispatch(t, h)
			require.Len(t, spansNamed(h.spans(), "entity:counter:get"), want)
		})
	}
}

func TestWorkerTracingEntitySignalSameDispatch(t *testing.T) {
	id := api.NewEntityID("counter", "key")
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("Signal", func(ctx *task.OrchestrationContext) (any, error) {
		if err := ctx.SignalEntity(id, "add"); err != nil {
			return nil, err
		}
		return nil, ctx.WaitForSingleEvent("finish", -1).Await(nil)
	}))
	h := newTracingHarness(t, registry, true)
	backend := newFakeOrchestrationBackend("caller", helpers.NewExecutionStartedEvent(
		"Signal", "caller", nil, nil, nil, nil))
	backend.entityParameters = &protos.OrchestratorEntityParameters{EntityMessageReorderWindow: durationpb.New(0)}
	response := backend.dispatch(t, h)
	sent := tracingEntitySent(response.Actions[0])
	// A fresh worker receives the same initial history plus its signal acknowledgement.
	h = newTracingHarness(t, registry, true)
	request := &protos.OrchestratorRequest{
		InstanceId: "caller", EntityParameters: backend.entityParameters,
		NewEvents: append(append([]*protos.HistoryEvent(nil), backend.history[:2]...), sent),
	}
	h.worker.processOrchestration(context.Background(), h.client, "token", request)
	require.Empty(t, h.client.orchestrationCompletions[0].Actions)
	require.Len(t, spansNamed(h.spans(), "entity:counter:add"), 1)
	request.PastEvents, request.NewEvents = request.NewEvents, []*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent()}
	request.OrchestrationTraceContext = h.client.orchestrationCompletions[0].OrchestrationTraceContext
	h.worker.processOrchestration(context.Background(), h.client, "token", request)
	require.Len(t, spansNamed(h.spans(), "entity:counter:add"), 1)
}

func TestWorkerTracingEntitySignalAfterResume(t *testing.T) {
	id := api.NewEntityID("counter", "key")
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("SignalAfterResume", func(ctx *task.OrchestrationContext) (any, error) {
		if err := ctx.WaitForSingleEvent("next", -1).Await(nil); err != nil {
			return nil, err
		}
		if err := ctx.SignalEntity(id, "add"); err != nil {
			return nil, err
		}
		return nil, ctx.WaitForSingleEvent("finish", -1).Await(nil)
	}))
	h := newTracingHarness(t, registry, true)
	backend := newFakeOrchestrationBackend("caller", helpers.NewExecutionStartedEvent(
		"SignalAfterResume", "caller", nil, nil, nil, nil))
	backend.entityParameters = &protos.OrchestratorEntityParameters{EntityMessageReorderWindow: durationpb.New(0)}
	backend.dispatch(t, h)
	backend.dispatch(t, h, helpers.NewSuspendOrchestrationEvent(""))
	backend.dispatch(t, h, helpers.NewEventRaisedEvent("next", nil))
	require.Empty(t, h.spans())

	response := backend.dispatch(t, h, helpers.NewResumeOrchestrationEvent(""))
	require.Len(t, response.Actions, 1)
	require.NotNil(t, response.Actions[0].GetSendEntityMessage().GetEntityOperationSignaled())
	require.Len(t, spansNamed(h.spans(), "entity:counter:add"), 1)
	backend.dispatch(t, h)
	backend.dispatch(t, h, helpers.NewEventRaisedEvent("finish", nil))
	require.Len(t, spansNamed(h.spans(), "entity:counter:add"), 1)
}

func TestWorkerTracingEntitySignalCanonicalNameAndAcknowledgement(t *testing.T) {
	id := api.EntityID{Name: "Counter", Key: "key"}
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("Signal", func(ctx *task.OrchestrationContext) (any, error) {
		if err := ctx.SignalEntity(id, "add"); err != nil {
			return nil, err
		}
		return nil, ctx.WaitForSingleEvent("finish", -1).Await(nil)
	}))
	h := newTracingHarness(t, registry, true)
	backend := newFakeOrchestrationBackend("caller", helpers.NewExecutionStartedEvent(
		"Signal", "caller", nil, nil, nil, nil))
	backend.entityParameters = &protos.OrchestratorEntityParameters{EntityMessageReorderWindow: durationpb.New(0)}
	response := backend.dispatch(t, h)
	require.Len(t, response.Actions, 1)
	spans := spansNamed(h.spans(), "entity:counter:add")
	require.Len(t, spans, 1)
	require.Equal(t, id.String(), spanAttribute(spans[0], "durabletask.event.target_instance_id"))

	// Deliver the acknowledgement as new history after the signal was exported.
	backend.history = backend.history[:len(backend.history)-1]
	backend.dispatch(t, h, tracingEntitySent(response.Actions[0]))
	require.Len(t, spansNamed(h.spans(), "entity:counter:add"), 1)
}

func TestWorkerTracingEntitySignalAcknowledgementBeyondTurnBudget(t *testing.T) {
	id := api.NewEntityID("counter", "key")
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("Signal", func(ctx *task.OrchestrationContext) (any, error) {
		if err := ctx.SignalEntity(id, "add"); err != nil {
			return nil, err
		}
		return nil, ctx.WaitForSingleEvent("finish", -1).Await(nil)
	}))
	h := newTracingHarness(t, registry, true,
		task.WithOrchestrationOptions(task.OrchestrationOptions{MaxEventsPerTurn: 1}))
	backend := newFakeOrchestrationBackend("caller", helpers.NewExecutionStartedEvent(
		"Signal", "caller", nil, nil, nil, nil))
	backend.entityParameters = &protos.OrchestratorEntityParameters{EntityMessageReorderWindow: durationpb.New(0)}
	response := backend.dispatch(t, h)
	require.Len(t, response.Actions, 1)
	require.Len(t, spansNamed(h.spans(), "entity:counter:add"), 1)

	ack := tracingEntitySent(response.Actions[0])
	backend.history = backend.history[:len(backend.history)-1]
	response = backend.dispatch(t, h, helpers.NewEventRaisedEvent("other", nil), ack)
	require.NotNil(t, response.NumEventsProcessed)
	require.Len(t, response.Actions, 1)
	require.Len(t, spansNamed(h.spans(), "entity:counter:add"), 1)

	// The backend deduplicates the replayed action and redelivers its pending acknowledgement.
	backend.history = backend.history[:len(backend.history)-1]
	response = backend.dispatch(t, h)
	require.Empty(t, response.Actions)
	require.Len(t, spansNamed(h.spans(), "entity:counter:add"), 1)
}

func TestWorkerTracingNewEntitySignalWithDeferredAcknowledgement(t *testing.T) {
	id := api.NewEntityID("counter", "key")
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("Signal", func(ctx *task.OrchestrationContext) (any, error) {
		if err := ctx.SignalEntity(id, "add"); err != nil {
			return nil, err
		}
		return nil, ctx.WaitForSingleEvent("finish", -1).Await(nil)
	}))
	backend := newFakeOrchestrationBackend("caller", helpers.NewExecutionStartedEvent(
		"Signal", "caller", nil, nil, nil, nil))
	backend.entityParameters = &protos.OrchestratorEntityParameters{EntityMessageReorderWindow: durationpb.New(0)}
	h := newTracingHarness(t, registry, true)
	response := backend.dispatch(t, h)
	require.Len(t, response.Actions, 1)
	sent := tracingEntitySent(response.Actions[0])

	h = newTracingHarness(t, registry, true,
		task.WithOrchestrationOptions(task.OrchestrationOptions{MaxEventsPerTurn: 1}))
	request := &protos.OrchestratorRequest{
		InstanceId:       "caller",
		EntityParameters: backend.entityParameters,
		NewEvents:        append(append([]*protos.HistoryEvent(nil), backend.history[:2]...), sent),
	}
	h.worker.processOrchestration(context.Background(), h.client, "token", request)
	response = h.client.orchestrationCompletions[0]
	require.NotNil(t, response.NumEventsProcessed)
	require.Len(t, response.Actions, 1)
	require.Len(t, spansNamed(h.spans(), "entity:counter:add"), 1)

	request.PastEvents = request.NewEvents[:2]
	request.NewEvents = []*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent(), sent}
	request.OrchestrationTraceContext = response.OrchestrationTraceContext
	h.worker.processOrchestration(context.Background(), h.client, "token", request)
	require.Empty(t, h.client.orchestrationCompletions[1].Actions)
	require.Len(t, spansNamed(h.spans(), "entity:counter:add"), 1)
}

func TestWorkerTracingEntityOperationResults(t *testing.T) {
	for _, mode := range []string{"enabled", "disabled", "unsampled", "rejected", "batch-failure", "malformed-results"} {
		t.Run(mode, func(t *testing.T) {
			var options []sdktrace.TracerProviderOption
			if mode == "unsampled" {
				options = append(options, tracing.WithSampler(sdktrace.NeverSample()))
			}
			h := newTracingHarnessWithProviderOptions(t, task.NewTaskRegistry(), mode != "disabled", options)
			start, end := time.Now().UTC().Add(-time.Minute), time.Now().UTC().Add(-time.Second)
			h.worker.executor = &recordingExecutor{executeEntity: func(context.Context, *protos.EntityBatchRequest) (*protos.EntityBatchResult, error) {
				if mode == "batch-failure" {
					return nil, errors.New("batch failed")
				}
				if mode == "malformed-results" {
					return &protos.EntityBatchResult{Results: []*protos.OperationResult{nil, {}}}, nil
				}
				return &protos.EntityBatchResult{Results: []*protos.OperationResult{
					{ResultType: &protos.OperationResult_Success{Success: &protos.OperationResultSuccess{
						StartTimeUtc: timestamppb.New(start), EndTimeUtc: timestamppb.New(end),
					}}},
					{ResultType: &protos.OperationResult_Failure{Failure: &protos.OperationResultFailure{
						FailureDetails: &protos.TaskFailureDetails{ErrorMessage: "operation failed"},
						StartTimeUtc:   timestamppb.New(start), EndTimeUtc: timestamppb.New(end),
					}}},
				}}, nil
			}}
			if mode == "rejected" {
				h.client.entityCompletionErr = errors.New("completion failed")
			}
			request := newFakeEntitySignalRequest("add", nil)
			request.OperationRequests = append(request.OperationRequests, &protos.HistoryEvent{
				EventType: &protos.HistoryEvent_EntityOperationCalled{EntityOperationCalled: &protos.EntityOperationCalledEvent{
					RequestId: uuid.NewString(), Operation: "get", ParentInstanceId: wrapperspb.String("caller"),
				}},
			})
			h.worker.processEntityV2(context.Background(), h.client, "token", request)
			if mode == "disabled" || mode == "unsampled" || mode == "rejected" {
				require.Empty(t, h.spans())
				return
			}
			require.Len(t, h.spans(), 2)
			signal, call := h.spans()[0], h.spans()[1]
			require.Equal(t, "entity:counter:add", signal.Name())
			require.Equal(t, "entity:counter:get", call.Name())
			require.Equal(t, trace.SpanKindConsumer, signal.SpanKind())
			require.Equal(t, trace.SpanKindServer, call.SpanKind())
			require.False(t, signal.Parent().IsValid())
			require.False(t, call.Parent().IsValid())
			require.Equal(t, request.InstanceId, spanAttribute(call, "durabletask.task.instance_id"))
			require.Equal(t, "counter", spanAttribute(call, "durabletask.task.name"))
			switch mode {
			case "malformed-results":
				for _, span := range h.spans() {
					require.Equal(t, codes.Error, span.Status().Code)
					require.Contains(t, span.Status().Description, "without success or failure")
				}
			case "batch-failure":
				require.Equal(t, codes.Error, signal.Status().Code)
				require.Contains(t, signal.Status().Description, "batch failed")
			default:
				require.Equal(t, codes.Ok, signal.Status().Code)
				require.True(t, start.Equal(signal.StartTime()))
				require.True(t, end.Equal(call.EndTime()))
				require.Equal(t, codes.Error, call.Status().Code)
				require.Equal(t, "operation failed", spanAttribute(call, "durabletask.entity.error_message"))
			}
		})
	}
}

func TestWorkerTracingEntityExternalizationFailure(t *testing.T) {
	for _, mode := range []string{"accepted", "rejected", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			h := newTracingHarness(t, task.NewTaskRegistry(), true)
			storeErr := errors.New("externalize failed")
			if mode == "canceled" {
				storeErr = context.Canceled
			}
			backend := &failingPayloadBackend{storeErr: storeErr}
			h.worker.options.largePayloads = &api.LargePayloadOptions{
				Store: backend, Resolver: backend, ThresholdBytes: 1, MaxPayloadBytes: 1024,
			}
			h.worker.executor = &recordingExecutor{executeEntity: func(context.Context, *protos.EntityBatchRequest) (*protos.EntityBatchResult, error) {
				return &protos.EntityBatchResult{EntityState: wrapperspb.String("large"), Results: []*protos.OperationResult{
					{ResultType: &protos.OperationResult_Success{Success: &protos.OperationResultSuccess{}}},
				}}, nil
			}}
			if mode == "rejected" {
				h.client.entityCompletionErr = errors.New("completion rejected")
			}
			h.worker.processEntityV2(context.Background(), h.client, "token", newFakeEntitySignalRequest("run", nil))
			if mode != "accepted" {
				require.Empty(t, h.spans())
				require.Equal(t, 1, h.client.entityAbandonAttempts)
				return
			}
			require.Len(t, h.spans(), 1)
			require.Equal(t, codes.Error, h.spans()[0].Status().Code)
			require.Contains(t, h.spans()[0].Status().Description, "externalize failed")
			require.Len(t, h.client.entityCompletions, 1)
			require.NotNil(t, h.client.entityCompletions[0].FailureDetails)
		})
	}
}

func TestWorkerTracingEntityHydrationFailure(t *testing.T) {
	for _, mode := range []string{"accepted", "rejected", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			h := newTracingHarness(t, task.NewTaskRegistry(), true)
			store := payload.NewMemoryStore()
			input, err := largepayload.Externalize(context.Background(),
				&api.LargePayloadOptions{Store: store, Resolver: store, ThresholdBytes: 1, MaxPayloadBytes: 1024},
				wrapperspb.String(`"large"`))
			require.NoError(t, err)
			resolveErr := errors.New("hydrate failed")
			if mode == "canceled" {
				resolveErr = context.Canceled
			}
			h.worker.options.largePayloads = &api.LargePayloadOptions{
				Store: store, Resolver: &failingPayloadBackend{resolveErr: resolveErr}, ThresholdBytes: 1, MaxPayloadBytes: 1024,
			}
			if mode == "rejected" {
				h.client.entityCompletionErr = errors.New("completion rejected")
			}
			h.worker.processEntityV2(context.Background(), h.client, "token", newFakeEntitySignalRequest("run", input))
			if mode != "accepted" {
				require.Empty(t, h.spans())
				return
			}
			require.Len(t, h.spans(), 1)
			span := h.spans()[0]
			require.Equal(t, "entity:counter:run", span.Name())
			require.Equal(t, trace.SpanKindConsumer, span.SpanKind())
			require.Equal(t, codes.Error, span.Status().Code)
			require.Contains(t, span.Status().Description, "hydrate failed")
		})
	}
}

func tracingEventSent(action *protos.OrchestratorAction) *protos.HistoryEvent {
	sent := action.GetSendEvent()
	return &protos.HistoryEvent{
		EventId: action.Id, Timestamp: helpers.NewOrchestratorStartedEvent().Timestamp,
		EventType: &protos.HistoryEvent_EventSent{EventSent: &protos.EventSentEvent{
			InstanceId: sent.GetInstance().GetInstanceId(), Name: sent.GetName(), Input: sent.GetData(),
		}},
	}
}

func TestWorkerTracingEventSendAndReceipt(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("Sender", func(ctx *task.OrchestrationContext) (any, error) {
		if err := ctx.SendEvent("receiver", "approval", "approved"); err != nil {
			return nil, err
		}
		return nil, ctx.WaitForSingleEvent("finish", -1).Await(nil)
	}))
	require.NoError(t, registry.AddOrchestratorN("Receiver", func(ctx *task.OrchestrationContext) (any, error) {
		var value string
		err := ctx.WaitForSingleEvent("approval", -1).Await(&value)
		return value, err
	}))
	parent, parentTrace := newTraceParent(trace.FlagsSampled)
	started := helpers.NewExecutionStartedEvent("Sender", "sender", nil, nil, parentTrace, nil)
	backend := newFakeOrchestrationBackend("sender", started)
	h := newTracingHarness(t, registry, true)
	response := backend.dispatch(t, h)
	events := spansNamed(h.spans(), "orchestration_event:approval")
	require.Len(t, events, 1)
	span := events[0]
	require.Equal(t, trace.SpanKindProducer, span.SpanKind())
	require.Equal(t, parent.TraceID(), span.SpanContext().TraceID())
	require.Equal(t, response.OrchestrationTraceContext.SpanID.GetValue(), span.Parent().SpanID().String())
	require.Equal(t, "vendor=value", span.SpanContext().TraceState().String())
	require.Equal(t, "durabletask", span.InstrumentationScope().Name)
	require.Equal(t, "event", spanAttribute(span, "durabletask.type"))
	require.Equal(t, "approval", spanAttribute(span, "durabletask.task.name"))
	require.Equal(t, "sender", spanAttribute(span, "durabletask.task.instance_id"))
	require.Equal(t, started.GetExecutionStarted().GetOrchestrationInstance().GetExecutionId().GetValue(),
		spanAttribute(span, "durabletask.task.execution_id"))
	require.Equal(t, "receiver", spanAttribute(span, "durabletask.event.target_instance_id"))
	require.Empty(t, spanAttribute(span, "durabletask.task.task_id"))
	require.Empty(t, spanAttribute(span, "durabletask.task.version"))
	require.True(t, span.StartTime().Equal(span.EndTime()))
	sent := response.Actions[0]
	require.Equal(t, `"approved"`, sent.GetSendEvent().GetData().GetValue())

	// An acknowledgement delivered as new history must not export the send a
	// second time, even when replay regenerates the corresponding action.
	backend.history = backend.history[:len(backend.history)-1]
	backend.dispatch(t, h, tracingEventSent(sent))
	backend.dispatch(t, h, helpers.NewEventRaisedEvent("finish", nil))
	require.Len(t, spansNamed(h.spans(), "orchestration_event:approval"), 1)

	// Receipt stays in the receiver's independently scheduled trace. The
	// protocol has no sender trace context to propagate or link here.
	receiverParent := parent.WithTraceID(trace.TraceID{9}).WithSpanID(trace.SpanID{9})
	receiver := newFakeOrchestrationBackend("receiver", helpers.NewExecutionStartedEvent(
		"Receiver", "receiver", nil, nil, helpers.TraceContextFromSpanContext(receiverParent), nil))
	receiver.dispatch(t, h, helpers.NewEventRaisedEvent("approval", sent.GetSendEvent().GetData()))
	received := spansNamed(h.spans(), "orchestration:Receiver")
	require.Len(t, received, 1)
	require.Equal(t, receiverParent, received[0].Parent())
	require.Empty(t, received[0].Links())
	require.Len(t, spansNamed(h.spans(), "orchestration_event:approval"), 1)
	for _, span := range h.spans() {
		require.NotEqual(t, trace.SpanKindConsumer, span.SpanKind())
	}
}

func TestWorkerTracingEventSendAckDeferredByTurnBudget(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("Sender", func(ctx *task.OrchestrationContext) (any, error) {
		if err := ctx.SendEvent("receiver", "approval", nil); err != nil {
			return nil, err
		}
		return nil, ctx.WaitForSingleEvent("finish", -1).Await(nil)
	}))
	_, parentTrace := newTraceParent(trace.FlagsSampled)
	backend := newFakeOrchestrationBackend("sender", helpers.NewExecutionStartedEvent(
		"Sender", "sender", nil, nil, parentTrace, nil))
	h := newTracingHarness(t, registry, true,
		task.WithOrchestrationOptions(task.OrchestrationOptions{MaxEventsPerTurn: 1}))
	response := backend.dispatch(t, h)
	require.Len(t, spansNamed(h.spans(), "orchestration_event:approval"), 1)

	// Deliver the accepted send's acknowledgement behind an unrelated event.
	ack := backend.history[len(backend.history)-1]
	require.NotNil(t, ack.GetEventSent())
	backend.history = backend.history[:len(backend.history)-1]
	deferred := backend.dispatch(t, h, helpers.NewEventRaisedEvent("unrelated", nil), ack)
	require.Equal(t, int32(1), deferred.GetNumEventsProcessed().GetValue())
	require.Len(t, deferred.Actions, 1)
	require.Equal(t, response.Actions[0].Id, deferred.Actions[0].Id)
	require.NotNil(t, deferred.Actions[0].GetSendEvent())
	require.Len(t, spansNamed(h.spans(), "orchestration_event:approval"), 1,
		"a replayed send must not export again while its acknowledgement is deferred")

	// The real backend deduplicates the replayed send; retain only the original
	// acknowledgement, still pending delivery, rather than the fake's new one.
	backend.history = backend.history[:len(backend.history)-1]
	backend.dispatch(t, h)
	backend.dispatch(t, h, helpers.NewEventRaisedEvent("finish", nil))
	require.Len(t, spansNamed(h.spans(), "orchestration_event:approval"), 1)
}

func TestWorkerTracingEventSendSameBatch(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("BatchSend", func(ctx *task.OrchestrationContext) (any, error) {
		if err := ctx.SendEvent("receiver", "approval", nil); err != nil {
			return nil, err
		}
		return nil, ctx.WaitForSingleEvent("finish", -1).Await(nil)
	}))
	_, parentTrace := newTraceParent(trace.FlagsSampled)
	started := helpers.NewExecutionStartedEvent("BatchSend", "sender", nil, nil, parentTrace, nil)
	sent := tracingEventSent(helpers.NewSendEventAction("receiver", "approval", wrapperspb.String("")))
	sent.EventId = 0
	h := newTracingHarness(t, registry, true)
	request := &protos.OrchestratorRequest{
		InstanceId: "sender",
		NewEvents:  []*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent(), started, sent},
	}
	h.worker.processOrchestration(context.Background(), h.client, "token", request)
	require.Empty(t, h.client.orchestrationCompletions[0].Actions)
	require.Len(t, spansNamed(h.spans(), "orchestration_event:approval"), 1)
	request.PastEvents = request.NewEvents
	request.NewEvents = []*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent()}
	request.OrchestrationTraceContext = h.client.orchestrationCompletions[0].OrchestrationTraceContext
	h.worker.processOrchestration(context.Background(), h.client, "token", request)
	require.Empty(t, h.client.orchestrationCompletions[1].Actions)
	require.Len(t, spansNamed(h.spans(), "orchestration_event:approval"), 1)
}

func TestWorkerTracingEventsBufferingAndTurnBudget(t *testing.T) {
	for _, mode := range []string{"event-budget", "suspended", "buffered"} {
		t.Run(mode, func(t *testing.T) {
			registry := task.NewTaskRegistry()
			require.NoError(t, registry.AddOrchestratorN("Relay", func(ctx *task.OrchestrationContext) (any, error) {
				if mode == "buffered" {
					if err := ctx.WaitForSingleEvent("ready", -1).Await(nil); err != nil {
						return nil, err
					}
				}
				for range 2 {
					if err := ctx.WaitForSingleEvent("next", -1).Await(nil); err != nil {
						return nil, err
					}
					if err := ctx.SendEvent("receiver", "forwarded", nil); err != nil {
						return nil, err
					}
				}
				return nil, ctx.WaitForSingleEvent("finish", -1).Await(nil)
			}))
			_, parentTrace := newTraceParent(trace.FlagsSampled)
			backend := newFakeOrchestrationBackend("relay", helpers.NewExecutionStartedEvent(
				"Relay", "relay", nil, nil, parentTrace, nil))
			h := newTracingHarness(t, registry, true,
				task.WithOrchestrationOptions(task.OrchestrationOptions{MaxEventsPerTurn: 1}))
			backend.dispatch(t, h)
			if mode == "suspended" {
				backend.dispatch(t, h, helpers.NewSuspendOrchestrationEvent(""))
			}
			backend.dispatch(t, h, helpers.NewEventRaisedEvent("next", nil), helpers.NewEventRaisedEvent("next", nil))
			want := 1
			if mode != "event-budget" {
				want = 0
			}
			require.Len(t, spansNamed(h.spans(), "orchestration_event:forwarded"), want)
			backend.dispatch(t, h)
			switch mode {
			case "suspended":
				require.Empty(t, h.spans(), "buffered receipts must not manufacture send spans")
				backend.dispatch(t, h, helpers.NewResumeOrchestrationEvent(""))
			case "buffered":
				require.Empty(t, h.spans(), "receipts without a waiter must not manufacture send spans")
				backend.dispatch(t, h, helpers.NewEventRaisedEvent("ready", nil))
			}
			require.Len(t, spansNamed(h.spans(), "orchestration_event:forwarded"), 2)
			backend.dispatch(t, h, helpers.NewEventRaisedEvent("finish", nil))
			require.Len(t, spansNamed(h.spans(), "orchestration_event:forwarded"), 2)
		})
	}
}

func TestWorkerTracingEventsDisabledOrMissingContext(t *testing.T) {
	for _, mode := range []string{"disabled", "missing-context", "unsampled"} {
		t.Run(mode, func(t *testing.T) {
			registry := task.NewTaskRegistry()
			require.NoError(t, registry.AddOrchestratorN("Send", func(ctx *task.OrchestrationContext) (any, error) {
				return nil, ctx.SendEvent("receiver", "approval", 42)
			}))
			_, parentTrace := newTraceParent(trace.FlagsSampled)
			switch mode {
			case "missing-context":
				parentTrace = nil
			case "unsampled":
				_, parentTrace = newTraceParent(0)
			}
			h := newTracingHarness(t, registry, mode != "disabled")
			response := newFakeOrchestrationBackend("sender", helpers.NewExecutionStartedEvent(
				"Send", "sender", nil, nil, parentTrace, nil)).dispatch(t, h)
			if mode == "missing-context" {
				// The orchestration roots a new trace and the send nests under it.
				events := spansNamed(h.spans(), "orchestration_event:approval")
				require.Len(t, events, 1)
				require.Equal(t, response.OrchestrationTraceContext.SpanID.GetValue(), events[0].Parent().SpanID().String())
				require.Len(t, spansNamed(h.spans(), "orchestration:Send"), 1)
				require.False(t, spansNamed(h.spans(), "orchestration:Send")[0].Parent().IsValid())
			} else {
				require.Empty(t, h.spans())
			}
			require.Len(t, response.Actions, 2)
			require.Equal(t, "approval", response.Actions[0].GetSendEvent().GetName())
			require.Equal(t, "42", response.Actions[0].GetSendEvent().GetData().GetValue())
		})
	}
}

func TestWorkerTracingDiscardedSendDoesNotExport(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("LargeSend", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.SendEvent("receiver", "approval", string(make([]byte, 4096)))
	}))
	_, parentTrace := newTraceParent(trace.FlagsSampled)
	h := newTracingHarness(t, registry, true)
	h.worker.options.maxOrchestratorCompletionBytes = 1024
	response := newFakeOrchestrationBackend("sender", helpers.NewExecutionStartedEvent(
		"LargeSend", "sender", nil, nil, parentTrace, nil)).dispatch(t, h)
	require.Len(t, response.Actions, 1)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
		response.Actions[0].GetCompleteOrchestration().GetOrchestrationStatus())
	require.Empty(t, spansNamed(h.spans(), "orchestration_event:approval"))
}

func newTraceParent(flags trace.TraceFlags) (trace.SpanContext, *protos.TraceContext) {
	state, _ := trace.ParseTraceState("vendor=value")
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:     trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		TraceFlags: flags,
		TraceState: state,
		Remote:     true,
	})
	return spanContext, helpers.TraceContextFromSpanContext(spanContext)
}

func spansNamed(spans []sdktrace.ReadOnlySpan, name string) []sdktrace.ReadOnlySpan {
	var matches []sdktrace.ReadOnlySpan
	for _, span := range spans {
		if span.Name() == name {
			matches = append(matches, span)
		}
	}
	return matches
}

func spanAttribute(span sdktrace.ReadOnlySpan, key string) string {
	for _, attr := range span.Attributes() {
		if string(attr.Key) == key {
			return attr.Value.String()
		}
	}
	return ""
}

type fixedIDGenerator struct{}

func (fixedIDGenerator) NewIDs(context.Context) (trace.TraceID, trace.SpanID) {
	return trace.TraceID{1}, trace.SpanID{1}
}

func (fixedIDGenerator) NewSpanID(context.Context, trace.TraceID) trace.SpanID {
	return trace.SpanID{1}
}

func TestWorkerTracerProviderRejectsUnsupportedProviders(t *testing.T) {
	for name, provider := range map[string]trace.TracerProvider{
		"nil":         nil,
		"noop":        noop.NewTracerProvider(),
		"default-sdk": sdktrace.NewTracerProvider(),
		"replaced-idgen": sdktrace.NewTracerProvider(
			tracing.WithIDGenerator(nil),
			tracing.WithSampler(nil),
			sdktrace.WithIDGenerator(fixedIDGenerator{}),
		),
		"missing-sampler": sdktrace.NewTracerProvider(tracing.WithIDGenerator(nil)),
		"replaced-sampler": sdktrace.NewTracerProvider(
			tracing.WithIDGenerator(nil),
			tracing.WithSampler(nil),
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
		),
	} {
		t.Run(name, func(t *testing.T) {
			var options taskHubGrpcWorkerOptions
			err := WithWorkerTracerProvider(provider)(&options)
			require.Error(t, err)
			if provider != nil {
				require.ErrorIs(t, err, tracing.ErrUnsupportedTracerProvider)
			}
			require.Nil(t, options.tracer)
		})
	}
}

func TestWorkerTracingReplayAwareLifecycle(t *testing.T) {
	registry := task.NewTaskRegistry()
	var orchestrationSpanContexts []trace.SpanContext
	require.NoError(t, registry.AddOrchestratorN("Parent", func(ctx *task.OrchestrationContext) (any, error) {
		orchestrationSpanContexts = append(orchestrationSpanContexts, trace.SpanContextFromContext(ctx.Context()))
		var greeting string
		if err := ctx.CallActivity("Hello", task.WithActivityInput("world")).Await(&greeting); err != nil {
			return nil, err
		}
		if err := ctx.CreateTimer(time.Millisecond).Await(nil); err != nil {
			return nil, err
		}
		if err := ctx.CallSubOrchestrator("Child", task.WithSubOrchestrationInstanceID("child")).Await(nil); err != nil {
			return nil, err
		}
		return greeting, nil
	}))
	// Application spans must not inherit the override used to reconstruct
	// Durable Task spans.
	appRecorder := tracetest.NewSpanRecorder()
	appProvider := sdktrace.NewTracerProvider(tracing.WithIDGenerator(nil), sdktrace.WithSpanProcessor(appRecorder))
	require.NoError(t, registry.AddActivityN("Hello", func(ctx task.ActivityContext) (any, error) {
		_, span := appProvider.Tracer("app").Start(ctx.Context(), "app-work")
		span.End()
		return "hello", nil
	}))

	parent, parentTrace := newTraceParent(trace.FlagsSampled)
	backend := newFakeOrchestrationBackend("parent", helpers.NewExecutionStartedEvent(
		"Parent", "parent", nil, nil, parentTrace, nil))

	// Each dispatch uses a fresh worker, so identity can only come from the
	// persisted orchestration trace context.
	first := newTracingHarness(t, registry, true)
	response := backend.dispatch(t, first, nil...)
	require.Empty(t, first.spans())
	require.NotNil(t, response.OrchestrationTraceContext)
	persisted := response.OrchestrationTraceContext
	scheduleTrace := response.Actions[0].GetScheduleTask().ParentTraceContext
	require.NotNil(t, scheduleTrace)

	activity := newTracingHarness(t, registry, true)
	activity.worker.processActivity(context.Background(), activity.client, "token", &protos.ActivityRequest{
		Name:                  "Hello",
		TaskId:                response.Actions[0].Id,
		OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: "parent"},
		Input:                 wrapperspb.String(`"world"`),
		ParentTraceContext:    scheduleTrace,
	})
	activitySpans := activity.spans()
	require.Len(t, activitySpans, 1)
	scheduleSpan := helpers.SpanContextFromTraceContext(scheduleTrace)
	require.Equal(t, "activity:Hello", activitySpans[0].Name())
	require.Equal(t, trace.SpanKindServer, activitySpans[0].SpanKind())
	require.Equal(t, scheduleSpan.SpanID(), activitySpans[0].Parent().SpanID())
	appSpans := appRecorder.Ended()
	require.Len(t, appSpans, 1)
	require.Equal(t, activitySpans[0].SpanContext(), appSpans[0].Parent())
	require.NotEqual(t, scheduleSpan.SpanID(), appSpans[0].SpanContext().SpanID())

	second := newTracingHarness(t, registry, true)
	response = backend.dispatch(t, second, helpers.NewTaskCompletedEvent(response.Actions[0].Id, wrapperspb.String(`"hello"`)))
	require.Equal(t, persisted.SpanID.GetValue(), response.OrchestrationTraceContext.SpanID.GetValue())
	require.True(t, persisted.SpanStartTime.AsTime().Equal(response.OrchestrationTraceContext.SpanStartTime.AsTime()))
	clientSpans := second.spans()
	require.Len(t, clientSpans, 1)
	require.Equal(t, "activity:Hello", clientSpans[0].Name())
	require.Equal(t, trace.SpanKindClient, clientSpans[0].SpanKind())
	require.Equal(t, scheduleSpan.SpanID(), clientSpans[0].SpanContext().SpanID())
	require.Equal(t, persisted.SpanID.GetValue(), clientSpans[0].Parent().SpanID().String())
	require.Equal(t, "Hello", spanAttribute(clientSpans[0], "durabletask.task.name"))
	timer := response.Actions[0].GetCreateTimer()
	require.NotNil(t, timer)

	third := newTracingHarness(t, registry, true)
	response = backend.dispatch(t, third, helpers.NewTimerFiredEvent(response.Actions[0].Id, timer.FireAt, nil))
	timerSpans := third.spans()
	require.Len(t, timerSpans, 1)
	require.Equal(t, "orchestration:Parent:timer", timerSpans[0].Name())
	require.Equal(t, trace.SpanKindInternal, timerSpans[0].SpanKind())
	require.Equal(t, persisted.SpanID.GetValue(), timerSpans[0].Parent().SpanID().String())
	require.NotEmpty(t, spanAttribute(timerSpans[0], "durabletask.fire_at"))
	subOrchestration := response.Actions[0].GetCreateSubOrchestration()
	require.NotNil(t, subOrchestration)
	require.NotNil(t, subOrchestration.ParentTraceContext)

	fourth := newTracingHarness(t, registry, true)
	completed := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: helpers.NewOrchestratorStartedEvent().Timestamp,
		EventType: &protos.HistoryEvent_SubOrchestrationInstanceCompleted{
			SubOrchestrationInstanceCompleted: &protos.SubOrchestrationInstanceCompletedEvent{
				TaskScheduledId: response.Actions[0].Id,
			},
		},
	}
	backend.dispatch(t, fourth, completed)
	finalSpans := fourth.spans()
	require.Len(t, finalSpans, 2)
	subSpan := spansNamed(finalSpans, "orchestration:Child")
	require.Len(t, subSpan, 1)
	require.Equal(t, trace.SpanKindClient, subSpan[0].SpanKind())
	require.Equal(t, helpers.SpanContextFromTraceContext(subOrchestration.ParentTraceContext).SpanID(),
		subSpan[0].SpanContext().SpanID())
	require.Equal(t, "child", spanAttribute(subSpan[0], "durabletask.task.instance_id"))
	orchestration := spansNamed(finalSpans, "orchestration:Parent")
	require.Len(t, orchestration, 1)
	require.Equal(t, trace.SpanKindServer, orchestration[0].SpanKind())
	require.Equal(t, persisted.SpanID.GetValue(), orchestration[0].SpanContext().SpanID().String())
	require.True(t, persisted.SpanStartTime.AsTime().Equal(orchestration[0].StartTime()))
	require.Equal(t, parent.SpanID(), orchestration[0].Parent().SpanID())
	require.Equal(t, parent.TraceID(), orchestration[0].SpanContext().TraceID())
	require.Equal(t, parent.TraceState().String(), orchestration[0].SpanContext().TraceState().String())
	require.Equal(t, "Completed", spanAttribute(orchestration[0], "durabletask.task.status"))
	require.Equal(t, codes.Unset, orchestration[0].Status().Code)

	// The logical span context is visible to orchestrator code on every replay.
	require.NotEmpty(t, orchestrationSpanContexts)
	for _, spanContext := range orchestrationSpanContexts {
		require.Equal(t, persisted.SpanID.GetValue(), spanContext.SpanID().String())
		require.True(t, spanContext.IsRemote())
	}
}

func TestWorkerTracingRootTraceWithoutScheduledContext(t *testing.T) {
	registry := task.NewTaskRegistry()
	var logical []trace.SpanContext
	require.NoError(t, registry.AddOrchestratorN("Root", func(ctx *task.OrchestrationContext) (any, error) {
		logical = append(logical, trace.SpanContextFromContext(ctx.Context()))
		if err := ctx.CallActivity("Work").Await(nil); err != nil {
			return nil, err
		}
		return nil, ctx.CreateTimer(time.Millisecond).Await(nil)
	}))
	backend := newFakeOrchestrationBackend("root", helpers.NewExecutionStartedEvent(
		"Root", "root", nil, nil, nil, nil))

	// Fresh workers per dispatch: the trace can only be recovered from the
	// persisted orchestration identity.
	first := newTracingHarness(t, registry, true)
	response := backend.dispatch(t, first)
	require.Empty(t, first.spans())
	persisted := response.OrchestrationTraceContext
	require.NotNil(t, persisted)
	scheduled := helpers.SpanContextFromTraceContext(response.Actions[0].GetScheduleTask().ParentTraceContext)
	require.True(t, scheduled.IsValid())
	require.True(t, scheduled.IsSampled())

	second := newTracingHarness(t, registry, true)
	response = backend.dispatch(t, second, helpers.NewTaskCompletedEvent(response.Actions[0].Id, nil))
	clientSpans := second.spans()
	require.Len(t, clientSpans, 1)
	require.Equal(t, scheduled.SpanID(), clientSpans[0].SpanContext().SpanID())
	require.Equal(t, scheduled.TraceID(), clientSpans[0].SpanContext().TraceID())
	require.Equal(t, persisted.SpanID.GetValue(), clientSpans[0].Parent().SpanID().String())
	timer := response.Actions[0].GetCreateTimer()
	require.NotNil(t, timer)

	third := newTracingHarness(t, registry, true)
	backend.dispatch(t, third, helpers.NewTimerFiredEvent(response.Actions[0].Id, timer.FireAt, nil))
	timerSpans := spansNamed(third.spans(), "orchestration:Root:timer")
	require.Len(t, timerSpans, 1)
	require.Equal(t, scheduled.TraceID(), timerSpans[0].SpanContext().TraceID())
	orchestration := spansNamed(third.spans(), "orchestration:Root")
	require.Len(t, orchestration, 1)
	require.False(t, orchestration[0].Parent().IsValid())
	require.Equal(t, scheduled.TraceID(), orchestration[0].SpanContext().TraceID())
	require.Equal(t, persisted.SpanID.GetValue(), orchestration[0].SpanContext().SpanID().String())
	require.True(t, persisted.SpanStartTime.AsTime().Equal(orchestration[0].StartTime()))
	require.Equal(t, "Completed", spanAttribute(orchestration[0], "durabletask.task.status"))

	require.NotEmpty(t, logical)
	for _, spanContext := range logical {
		require.Equal(t, scheduled.TraceID(), spanContext.TraceID())
		require.Equal(t, persisted.SpanID.GetValue(), spanContext.SpanID().String())
	}
	// Root sampling queries never reach span processors.
	for _, h := range []*tracingHarness{first, second, third} {
		require.Len(t, h.recorder.Started(), len(h.recorder.Ended()))
	}
}

func TestWorkerTracingSameBatchCompletionAndRetries(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("Retry", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.CallActivity("Flaky", task.WithActivityRetryPolicy(&task.RetryPolicy{
			MaxAttempts:          2,
			InitialRetryInterval: time.Millisecond,
		})).Await(nil)
	}))
	_, parentTrace := newTraceParent(trace.FlagsSampled)
	backend := newFakeOrchestrationBackend("retry", helpers.NewExecutionStartedEvent(
		"Retry", "retry", nil, nil, parentTrace, nil))

	h := newTracingHarness(t, registry, true)
	response := backend.dispatch(t, h)
	firstAttempt := response.Actions[0].Id
	response = backend.dispatch(t, h, helpers.NewTaskFailedEvent(firstAttempt, &protos.TaskFailureDetails{
		ErrorType: "Boom", ErrorMessage: "first attempt failed",
	}))
	require.Len(t, h.spans(), 1)
	require.Equal(t, codes.Error, h.spans()[0].Status().Code)

	// Deliver the retry timer and a replayed schedule plus completion of the
	// second attempt together.
	timer := response.Actions[0]
	require.NotNil(t, timer.GetCreateTimer())
	response = backend.dispatch(t, h, helpers.NewTimerFiredEvent(timer.Id, timer.GetCreateTimer().FireAt, nil))
	secondAttempt := response.Actions[0]
	require.NotNil(t, secondAttempt.GetScheduleTask())
	require.NotEqual(t,
		helpers.SpanContextFromTraceContext(backend.scheduled[firstAttempt]).SpanID(),
		helpers.SpanContextFromTraceContext(secondAttempt.GetScheduleTask().ParentTraceContext).SpanID())
	backend.dispatch(t, h, helpers.NewTaskCompletedEvent(secondAttempt.Id, nil))

	activities := spansNamed(h.spans(), "activity:Flaky")
	require.Len(t, activities, 2)
	require.Len(t, spansNamed(h.spans(), "orchestration:Retry"), 1)
	require.Len(t, spansNamed(h.spans(), "orchestration:Retry:timer"), 1)
}

func TestWorkerTracingSameDispatchScheduleAndCompletion(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("Batch", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.CallActivity("Work").Await(nil)
	}))
	_, parentTrace := newTraceParent(trace.FlagsSampled)
	h := newTracingHarness(t, registry, true)
	scheduled := helpers.SpanContextFromTraceContext(parentTrace).WithSpanID(trace.SpanID{9, 9, 9, 9, 9, 9, 9, 9})
	request := &protos.OrchestratorRequest{
		InstanceId: "batch",
		NewEvents: []*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("Batch", "batch", nil, nil, parentTrace, nil),
			helpers.NewTaskScheduledEvent(0, "Work", nil, nil, helpers.TraceContextFromSpanContext(scheduled)),
			helpers.NewTaskCompletedEvent(0, nil),
		},
	}
	h.worker.processOrchestration(context.Background(), h.client, "token", request)
	activities := spansNamed(h.spans(), "activity:Work")
	require.Len(t, activities, 1)
	require.Equal(t, scheduled.SpanID(), activities[0].SpanContext().SpanID())
	require.Len(t, spansNamed(h.spans(), "orchestration:Batch"), 1)
}

func TestWorkerTracingPartialConsumption(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("FanOut", func(ctx *task.OrchestrationContext) (any, error) {
		first, second := ctx.CallActivity("Work"), ctx.CallActivity("Work")
		if err := first.Await(nil); err != nil {
			return nil, err
		}
		return nil, second.Await(nil)
	}))
	_, parentTrace := newTraceParent(trace.FlagsSampled)
	backend := newFakeOrchestrationBackend("fan-out", helpers.NewExecutionStartedEvent(
		"FanOut", "fan-out", nil, nil, parentTrace, nil))
	h := newTracingHarness(t, registry, true,
		task.WithOrchestrationOptions(task.OrchestrationOptions{MaxEventsPerTurn: 1}))

	backend.dispatch(t, h)
	require.Len(t, backend.scheduled, 2)
	response := backend.dispatch(t, h,
		helpers.NewTaskCompletedEvent(0, nil),
		helpers.NewTaskCompletedEvent(1, nil))
	require.NotNil(t, response.NumEventsProcessed)
	require.Len(t, spansNamed(h.spans(), "activity:Work"), 1)

	backend.dispatch(t, h)
	require.Len(t, spansNamed(h.spans(), "activity:Work"), 2)
	require.Len(t, spansNamed(h.spans(), "orchestration:FanOut"), 1)
}

func TestWorkerTracingSuspendedCompletionExportsOnce(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("Suspend", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.CallActivity("Work").Await(nil)
	}))
	_, parentTrace := newTraceParent(trace.FlagsSampled)
	backend := newFakeOrchestrationBackend("suspend", helpers.NewExecutionStartedEvent(
		"Suspend", "suspend", nil, nil, parentTrace, nil))
	h := newTracingHarness(t, registry, true)

	backend.dispatch(t, h)
	backend.dispatch(t, h, helpers.NewSuspendOrchestrationEvent(""), helpers.NewTaskCompletedEvent(0, nil))
	require.Len(t, spansNamed(h.spans(), "activity:Work"), 1)
	backend.dispatch(t, h, helpers.NewResumeOrchestrationEvent(""))
	require.Len(t, spansNamed(h.spans(), "activity:Work"), 1)
	require.Len(t, spansNamed(h.spans(), "orchestration:Suspend"), 1)
}

func TestWorkerTracingFailureOutcomes(t *testing.T) {
	_, parentTrace := newTraceParent(trace.FlagsSampled)
	t.Run("orchestrator-error", func(t *testing.T) {
		registry := task.NewTaskRegistry()
		require.NoError(t, registry.AddOrchestratorN("Fails", func(*task.OrchestrationContext) (any, error) {
			return nil, errors.New("boom")
		}))
		h := newTracingHarness(t, registry, true)
		newFakeOrchestrationBackend("fails", helpers.NewExecutionStartedEvent(
			"Fails", "fails", nil, nil, parentTrace, nil)).dispatch(t, h)
		spans := spansNamed(h.spans(), "orchestration:Fails")
		require.Len(t, spans, 1)
		require.Equal(t, codes.Error, spans[0].Status().Code)
		require.Equal(t, "Failed", spanAttribute(spans[0], "durabletask.task.status"))
	})
	t.Run("oversized-response", func(t *testing.T) {
		registry := task.NewTaskRegistry()
		require.NoError(t, registry.AddOrchestratorN("Large", func(*task.OrchestrationContext) (any, error) {
			return string(make([]byte, 4096)), nil
		}))
		h := newTracingHarness(t, registry, true)
		h.worker.options.maxOrchestratorCompletionBytes = 1024
		newFakeOrchestrationBackend("large", helpers.NewExecutionStartedEvent(
			"Large", "large", nil, nil, parentTrace, nil)).dispatch(t, h)
		spans := spansNamed(h.spans(), "orchestration:Large")
		require.Len(t, spans, 1)
		require.Equal(t, "Failed", spanAttribute(spans[0], "durabletask.task.status"))
		require.Equal(t, codes.Error, spans[0].Status().Code)
	})
	t.Run("abandoned", func(t *testing.T) {
		registry := task.NewTaskRegistry()
		require.NoError(t, registry.AddOrchestratorN("Abandon", func(*task.OrchestrationContext) (any, error) {
			return nil, nil
		}))
		h := newTracingHarness(t, registry, true)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		h.worker.executor = &recordingExecutor{executeOrchestrator: func(
			context.Context, api.InstanceID, []*protos.HistoryEvent, []*protos.HistoryEvent,
		) (*task.ExecutionResults, error) {
			return nil, context.Canceled
		}}
		h.worker.processOrchestration(ctx, h.client, "token", &protos.OrchestratorRequest{
			InstanceId: "abandon",
			NewEvents: []*protos.HistoryEvent{
				helpers.NewOrchestratorStartedEvent(),
				helpers.NewExecutionStartedEvent("Abandon", "abandon", nil, nil, parentTrace, nil),
			},
		})
		require.Equal(t, 1, h.client.orchestrationAbandons)
		require.Empty(t, h.spans())
	})
}

func TestWorkerTracingActivityFailures(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddActivityN("Fails", func(task.ActivityContext) (any, error) {
		return nil, errors.New("activity failed")
	}))
	require.NoError(t, registry.AddActivityN("Panics", func(task.ActivityContext) (any, error) {
		panic("activity panicked")
	}))
	_, parentTrace := newTraceParent(trace.FlagsSampled)
	for _, name := range []string{"Fails", "Panics", "Missing"} {
		t.Run(name, func(t *testing.T) {
			h := newTracingHarness(t, registry, true)
			h.worker.processActivity(context.Background(), h.client, "token", &protos.ActivityRequest{
				Name:                  name,
				OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: "instance"},
				ParentTraceContext:    parentTrace,
			})
			require.Len(t, h.client.activityCompletions, 1)
			require.NotNil(t, h.client.activityCompletions[0].FailureDetails)
			spans := h.spans()
			require.Len(t, spans, 1)
			require.Equal(t, codes.Error, spans[0].Status().Code)
			require.Equal(t, "Failed", spanAttribute(spans[0], "durabletask.task.status"))
		})
	}
}

func TestWorkerTracingContinueAsNewAndRewindUseFreshIdentity(t *testing.T) {
	_, parentTrace := newTraceParent(trace.FlagsSampled)
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("Loop", func(ctx *task.OrchestrationContext) (any, error) {
		ctx.ContinueAsNew(nil)
		return nil, nil
	}))
	h := newTracingHarness(t, registry, true)
	backend := newFakeOrchestrationBackend("loop", helpers.NewExecutionStartedEvent(
		"Loop", "loop", nil, nil, parentTrace, nil))
	response := backend.dispatch(t, h)
	spans := spansNamed(h.spans(), "orchestration:Loop")
	require.Len(t, spans, 1)
	require.Equal(t, "ContinuedAsNew", spanAttribute(spans[0], "durabletask.task.status"))
	require.NotEqual(t, spans[0].SpanContext().SpanID().String(), response.OrchestrationTraceContext.SpanID.GetValue())

	failed := &protos.OrchestrationTraceContext{
		SpanID:        wrapperspb.String(trace.SpanID{7, 7, 7, 7, 7, 7, 7, 7}.String()),
		SpanStartTime: response.OrchestrationTraceContext.SpanStartTime,
	}
	h.worker.processOrchestration(context.Background(), h.client, "token", &protos.OrchestratorRequest{
		InstanceId:                "rewind",
		OrchestrationTraceContext: failed,
		PastEvents: []*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("Loop", "rewind", nil, nil, parentTrace, nil),
			{EventId: -1, EventType: &protos.HistoryEvent_ExecutionCompleted{
				ExecutionCompleted: &protos.ExecutionCompletedEvent{
					OrchestrationStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
				},
			}},
		},
		NewEvents: []*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent(), {
			EventId:   -1,
			Timestamp: helpers.NewOrchestratorStartedEvent().Timestamp,
			EventType: &protos.HistoryEvent_ExecutionRewound{ExecutionRewound: &protos.ExecutionRewoundEvent{}},
		}},
	})
	rewound := h.client.orchestrationCompletions[len(h.client.orchestrationCompletions)-1]
	require.NotNil(t, rewound.Actions[0].GetRewindOrchestration())
	require.NotEqual(t, failed.SpanID.GetValue(), rewound.OrchestrationTraceContext.SpanID.GetValue())
}

func TestWorkerTracingSamplingAndDisabledMode(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("Work", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.CallActivity("Work").Await(nil)
	}))
	t.Run("unsampled", func(t *testing.T) {
		_, parentTrace := newTraceParent(0)
		h := newTracingHarness(t, registry, true)
		backend := newFakeOrchestrationBackend("unsampled", helpers.NewExecutionStartedEvent(
			"Work", "unsampled", nil, nil, parentTrace, nil))
		response := backend.dispatch(t, h)
		scheduled := helpers.SpanContextFromTraceContext(response.Actions[0].GetScheduleTask().ParentTraceContext)
		require.False(t, scheduled.IsSampled())
		require.Equal(t, "vendor=value", scheduled.TraceState().String())
		backend.dispatch(t, h, helpers.NewTaskCompletedEvent(0, nil))
		require.Empty(t, h.spans())
	})
	t.Run("root unsampled", func(t *testing.T) {
		h := newTracingHarnessWithProviderOptions(t, registry, true,
			[]sdktrace.TracerProviderOption{tracing.WithSampler(sdktrace.ParentBased(sdktrace.NeverSample()))})
		backend := newFakeOrchestrationBackend("root-unsampled", helpers.NewExecutionStartedEvent(
			"Work", "root-unsampled", nil, nil, nil, nil))
		response := backend.dispatch(t, h)
		scheduled := helpers.SpanContextFromTraceContext(response.Actions[0].GetScheduleTask().ParentTraceContext)
		require.True(t, scheduled.IsValid())
		require.False(t, scheduled.IsSampled())
		backend.dispatch(t, h, helpers.NewTaskCompletedEvent(0, nil))
		require.Empty(t, h.spans())
	})
	t.Run("disabled", func(t *testing.T) {
		_, parentTrace := newTraceParent(trace.FlagsSampled)
		h := newTracingHarness(t, registry, false)
		passthrough := &protos.OrchestrationTraceContext{SpanID: wrapperspb.String("0102030405060708")}
		backend := newFakeOrchestrationBackend("disabled", helpers.NewExecutionStartedEvent(
			"Work", "disabled", nil, nil, parentTrace, nil))
		backend.traceContext = passthrough
		response := backend.dispatch(t, h)
		require.Same(t, passthrough, response.OrchestrationTraceContext)
		scheduled := helpers.SpanContextFromTraceContext(response.Actions[0].GetScheduleTask().ParentTraceContext)
		require.Equal(t, helpers.SpanContextFromTraceContext(parentTrace).TraceID(), scheduled.TraceID())
		backend.dispatch(t, h, helpers.NewTaskCompletedEvent(0, nil))
		require.Empty(t, h.spans())
	})
}
