package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
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
	instanceID   string
	history      []*protos.HistoryEvent
	traceContext *protos.OrchestrationTraceContext
	scheduled    map[int32]*protos.TraceContext
	pending      []*protos.HistoryEvent
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
		}
	}
	b.traceContext = response.OrchestrationTraceContext
	return response
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
		"nil":          nil,
		"noop":         noop.NewTracerProvider(),
		"default-sdk":  sdktrace.NewTracerProvider(),
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
