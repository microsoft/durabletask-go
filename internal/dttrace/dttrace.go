package dttrace

import (
	"context"
	"encoding/hex"
	"errors"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
)

// ScopeName is the instrumentation scope shared with the other Durable Task SDKs.
const ScopeName = "durabletask"

// Attribute keys shared with the other Durable Task SDKs.
const (
	AttrType        = attribute.Key("durabletask.type")
	AttrName        = attribute.Key("durabletask.task.name")
	AttrVersion     = attribute.Key("durabletask.task.version")
	AttrInstanceID  = attribute.Key("durabletask.task.instance_id")
	AttrExecutionID = attribute.Key("durabletask.task.execution_id")
	AttrStatus      = attribute.Key("durabletask.task.status")
	AttrTaskID      = attribute.Key("durabletask.task.task_id")
	AttrFireAt      = attribute.Key("durabletask.fire_at")
)

const (
	typeOrchestration = "orchestration"
	typeActivity      = "activity"
	typeTimer         = "timer"
)

// ErrUnsupportedTracerProvider reports a provider that cannot reconstruct spans
// with persisted IDs, which would otherwise export a disconnected span tree.
var ErrUnsupportedTracerProvider = errors.New(
	"durabletask tracing requires an OpenTelemetry SDK tracer provider configured with tracing.WithIDGenerator")

// Tracer emits Durable Task spans through a validated tracer provider.
type Tracer struct {
	tracer trace.Tracer
}

// NewTracer validates that provider honors persisted span IDs.
func NewTracer(provider trace.TracerProvider) (*Tracer, error) {
	if provider == nil {
		return nil, errors.New("tracer provider cannot be nil")
	}
	tracer := provider.Tracer(ScopeName)
	if !honorsSpanIDOverride(tracer) {
		return nil, ErrUnsupportedTracerProvider
	}
	return &Tracer{tracer: tracer}, nil
}

// honorsSpanIDOverride starts, but never ends, an unsampled probe span. Unended
// spans are not exported.
func honorsSpanIDOverride(tracer trace.Tracer) bool {
	want := RandomSpanID()
	parent := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: RandomTraceID(),
		SpanID:  RandomSpanID(),
		Remote:  true,
	})
	ctx := trace.ContextWithRemoteSpanContext(withSpanIDOverride(context.Background(), want), parent)
	_, span := tracer.Start(ctx, "durabletask.probe")
	return span.SpanContext().SpanID() == want
}

// emit reconstructs and ends one span with a persisted identity and timestamps.
func (t *Tracer) emit(
	name string,
	kind trace.SpanKind,
	parent trace.SpanContext,
	spanID trace.SpanID,
	start, end time.Time,
	attrs []attribute.KeyValue,
	failure *protos.TaskFailureDetails,
) {
	if end.Before(start) {
		end = start
	}
	ctx := trace.ContextWithRemoteSpanContext(withSpanIDOverride(context.Background(), spanID), parent)
	_, span := t.tracer.Start(ctx, name,
		trace.WithSpanKind(kind),
		trace.WithTimestamp(start),
		trace.WithAttributes(attrs...),
	)
	if failure != nil {
		span.SetStatus(codes.Error, failure.GetErrorMessage())
	}
	span.End(trace.WithTimestamp(end))
}

// SpanName formats task span names like the other Durable Task SDKs.
func SpanName(kind, name, version string) string {
	if version == "" {
		return kind + ":" + name
	}
	return kind + ":" + name + "@(" + version + ")"
}

func taskAttributes(kind, name, version, instanceID string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		AttrType.String(kind),
		AttrName.String(name),
		AttrInstanceID.String(instanceID),
	}
	if version != "" {
		attrs = append(attrs, AttrVersion.String(version))
	}
	return attrs
}

type activitySpanKey struct{}

// StartActivity starts the live SERVER span for one activity execution, parented
// by the persisted scheduling span. It returns nil when parent is unusable.
func (t *Tracer) StartActivity(
	ctx context.Context,
	name, version, instanceID string,
	taskID int32,
	parent *protos.TraceContext,
) (context.Context, trace.Span) {
	parentContext := helpers.SpanContextFromTraceContext(parent)
	if !parentContext.IsValid() {
		return ctx, nil
	}
	attrs := append(taskAttributes(typeActivity, name, version, instanceID),
		AttrTaskID.String(strconv.Itoa(int(taskID))))
	_, span := t.tracer.Start(
		trace.ContextWithRemoteSpanContext(context.Background(), parentContext),
		SpanName(typeActivity, name, version),
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(attrs...),
	)
	return context.WithValue(ctx, activitySpanKey{}, span), span
}

// ActivitySpanFromContext returns the activity span started by StartActivity.
func ActivitySpanFromContext(ctx context.Context) trace.Span {
	span, _ := ctx.Value(activitySpanKey{}).(trace.Span)
	return span
}

// EndActivity ends an activity span. Durable failures are reported as errors
// even though the executor returns them as failure events without a Go error.
func EndActivity(span trace.Span, failure *protos.TaskFailureDetails, abandoned bool) {
	if span == nil {
		return
	}
	switch {
	case abandoned:
		span.SetStatus(codes.Error, "activity work item abandoned")
	case failure != nil:
		span.SetAttributes(AttrStatus.String("Failed"))
		span.SetStatus(codes.Error, failure.GetErrorMessage())
	default:
		span.SetAttributes(AttrStatus.String("Completed"))
	}
	span.End()
}

type orchestrationKey struct{}

// WithOrchestration attaches an orchestration trace to an executor call.
func WithOrchestration(ctx context.Context, orchestration *Orchestration) context.Context {
	return context.WithValue(ctx, orchestrationKey{}, orchestration)
}

// OrchestrationFromContext returns the orchestration trace for an executor call.
func OrchestrationFromContext(ctx context.Context) *Orchestration {
	orchestration, _ := ctx.Value(orchestrationKey{}).(*Orchestration)
	return orchestration
}

type scheduledTask struct {
	kind       string
	name       string
	version    string
	instanceID string
	spanID     trace.SpanID
	start      time.Time
}

type createdTimer struct {
	start  time.Time
	fireAt *timestamppb.Timestamp
}

type pendingSpan struct {
	name    string
	kind    trace.SpanKind
	spanID  trace.SpanID
	start   time.Time
	end     time.Time
	attrs   []attribute.KeyValue
	failure *protos.TaskFailureDetails
}

// Orchestration holds one dispatch's replay-aware trace state. The logical
// orchestration span is never live: its persisted identity is restored on each
// dispatch and the span is reconstructed only when the execution completes.
type Orchestration struct {
	tracer      *Tracer
	instanceID  string
	name        string
	version     string
	executionID string
	parent      trace.SpanContext
	spanID      trace.SpanID
	start       time.Time
	passthrough *protos.OrchestrationTraceContext
	scheduled   map[int32]scheduledTask
	timers      map[int32]createdTimer
	pending     []pendingSpan
}

// StartOrchestration restores the persisted orchestration span identity, or
// reserves one on the first dispatch of an execution.
func (t *Tracer) StartOrchestration(
	instanceID string,
	persisted *protos.OrchestrationTraceContext,
	eventLists ...[]*protos.HistoryEvent,
) *Orchestration {
	o := &Orchestration{
		tracer:      t,
		instanceID:  instanceID,
		passthrough: persisted,
		scheduled:   make(map[int32]scheduledTask),
		timers:      make(map[int32]createdTimer),
	}
	for _, events := range eventLists {
		for _, event := range events {
			if started := event.GetExecutionStarted(); started != nil {
				o.name = started.GetName()
				o.version = started.GetVersion().GetValue()
				o.executionID = started.GetOrchestrationInstance().GetExecutionId().GetValue()
				o.parent = helpers.SpanContextFromTraceContext(started.GetParentTraceContext())
			}
		}
	}
	if spanID, ok := parseSpanID(persisted.GetSpanID().GetValue()); ok && persisted.GetSpanStartTime() != nil {
		o.spanID = spanID
		o.start = persisted.GetSpanStartTime().AsTime()
	} else {
		o.spanID = RandomSpanID()
		o.start = time.Now()
	}
	return o
}

func parseSpanID(value string) (trace.SpanID, bool) {
	var spanID trace.SpanID
	if len(value) != hex.EncodedLen(len(spanID)) {
		return spanID, false
	}
	if _, err := hex.Decode(spanID[:], []byte(value)); err != nil {
		return trace.SpanID{}, false
	}
	return spanID, spanID.IsValid()
}

// Enabled reports whether this execution has a valid parent trace to join.
// Without one the worker keeps propagation-only behavior.
func (o *Orchestration) Enabled() bool {
	return o != nil && o.parent.IsValid()
}

// SpanContext returns the non-recording logical orchestration span context.
func (o *Orchestration) SpanContext() trace.SpanContext {
	if !o.Enabled() {
		return trace.SpanContext{}
	}
	return o.parent.WithSpanID(o.spanID).WithRemote(true)
}

// Observe records one consumed history event. Completions consumed as new
// queue a reconstructed span; replayed completions only drop correlation.
func (o *Orchestration) Observe(event *protos.HistoryEvent, replaying bool) {
	if !o.Enabled() || event == nil {
		return
	}
	timestamp := event.GetTimestamp().AsTime()
	switch {
	case event.GetTaskScheduled() != nil:
		scheduled := event.GetTaskScheduled()
		o.schedule(event.EventId, scheduledTask{
			kind:       typeActivity,
			name:       scheduled.GetName(),
			version:    scheduled.GetVersion().GetValue(),
			instanceID: o.instanceID,
			start:      timestamp,
		}, scheduled.GetParentTraceContext())
	case event.GetSubOrchestrationInstanceCreated() != nil:
		created := event.GetSubOrchestrationInstanceCreated()
		o.schedule(event.EventId, scheduledTask{
			kind:       typeOrchestration,
			name:       created.GetName(),
			version:    created.GetVersion().GetValue(),
			instanceID: created.GetInstanceId(),
			start:      timestamp,
		}, created.GetParentTraceContext())
	case event.GetTaskCompleted() != nil:
		o.complete(event.GetTaskCompleted().TaskScheduledId, timestamp, replaying, nil)
	case event.GetTaskFailed() != nil:
		failed := event.GetTaskFailed()
		o.complete(failed.TaskScheduledId, timestamp, replaying, failureOrDefault(failed.FailureDetails))
	case event.GetSubOrchestrationInstanceCompleted() != nil:
		o.complete(event.GetSubOrchestrationInstanceCompleted().TaskScheduledId, timestamp, replaying, nil)
	case event.GetSubOrchestrationInstanceFailed() != nil:
		failed := event.GetSubOrchestrationInstanceFailed()
		o.complete(failed.TaskScheduledId, timestamp, replaying, failureOrDefault(failed.FailureDetails))
	case event.GetTimerCreated() != nil:
		o.timers[event.EventId] = createdTimer{start: timestamp, fireAt: event.GetTimerCreated().GetFireAt()}
	case event.GetTimerFired() != nil:
		o.fireTimer(event.GetTimerFired(), timestamp, replaying)
	}
}

func failureOrDefault(details *protos.TaskFailureDetails) *protos.TaskFailureDetails {
	if details == nil {
		return &protos.TaskFailureDetails{}
	}
	return details
}

func (o *Orchestration) schedule(eventID int32, task scheduledTask, traceContext *protos.TraceContext) {
	spanContext := helpers.SpanContextFromTraceContext(traceContext)
	if !spanContext.IsValid() {
		return
	}
	task.spanID = spanContext.SpanID()
	o.scheduled[eventID] = task
}

func (o *Orchestration) complete(taskID int32, end time.Time, replaying bool, failure *protos.TaskFailureDetails) {
	task, ok := o.scheduled[taskID]
	if !ok {
		return
	}
	delete(o.scheduled, taskID)
	if replaying {
		return
	}
	attrs := append(taskAttributes(task.kind, task.name, task.version, task.instanceID),
		AttrTaskID.String(strconv.Itoa(int(taskID))))
	o.pending = append(o.pending, pendingSpan{
		name:    SpanName(task.kind, task.name, task.version),
		kind:    trace.SpanKindClient,
		spanID:  task.spanID,
		start:   task.start,
		end:     end,
		attrs:   attrs,
		failure: failure,
	})
}

func (o *Orchestration) fireTimer(fired *protos.TimerFiredEvent, end time.Time, replaying bool) {
	timer, ok := o.timers[fired.TimerId]
	if !ok {
		return
	}
	delete(o.timers, fired.TimerId)
	if replaying {
		return
	}
	attrs := []attribute.KeyValue{
		AttrType.String(typeTimer),
		AttrName.String(o.name),
		AttrInstanceID.String(o.instanceID),
		AttrTaskID.String(strconv.Itoa(int(fired.TimerId))),
	}
	fireAt := timer.fireAt
	if fireAt == nil {
		fireAt = fired.GetFireAt()
	}
	if fireAt != nil {
		attrs = append(attrs, AttrFireAt.String(fireAt.AsTime().UTC().Format(time.RFC3339Nano)))
	}
	o.pending = append(o.pending, pendingSpan{
		name:   SpanName(typeOrchestration, o.name, "") + ":timer",
		kind:   trace.SpanKindInternal,
		spanID: RandomSpanID(),
		start:  timer.start,
		end:    end,
		attrs:  attrs,
	})
}

// PrepareResponse persists the orchestration span identity on response and
// assigns scheduling span IDs to new activity and sub-orchestration actions.
// Continue-as-new and rewind responses carry a fresh identity so the next
// execution never reuses this execution's span.
func (o *Orchestration) PrepareResponse(response *protos.OrchestratorResponse) {
	if response == nil {
		return
	}
	if !o.Enabled() {
		if response.OrchestrationTraceContext == nil {
			response.OrchestrationTraceContext = o.passthrough
		}
		return
	}
	spanID, start := o.spanID, o.start
	if startsNewExecution(response) {
		spanID, start = RandomSpanID(), time.Now()
	}
	response.OrchestrationTraceContext = &protos.OrchestrationTraceContext{
		SpanID:        wrapperspb.String(spanID.String()),
		SpanStartTime: timestamppb.New(start),
	}
	logical := o.SpanContext().WithRemote(false)
	for _, action := range response.Actions {
		switch {
		case action.GetScheduleTask() != nil && action.GetScheduleTask().ParentTraceContext == nil:
			action.GetScheduleTask().ParentTraceContext =
				helpers.TraceContextFromSpanContext(logical.WithSpanID(RandomSpanID()))
		case action.GetCreateSubOrchestration() != nil && action.GetCreateSubOrchestration().ParentTraceContext == nil:
			action.GetCreateSubOrchestration().ParentTraceContext =
				helpers.TraceContextFromSpanContext(logical.WithSpanID(RandomSpanID()))
		}
	}
}

func startsNewExecution(response *protos.OrchestratorResponse) bool {
	for _, action := range response.Actions {
		if action.GetRewindOrchestration() != nil {
			return true
		}
		if complete := action.GetCompleteOrchestration(); complete != nil &&
			complete.OrchestrationStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW {
			return true
		}
	}
	return false
}

// Emit exports spans for a response the backend accepted. Call it only after
// the completion succeeds so abandoned work never exports telemetry.
func (o *Orchestration) Emit(response *protos.OrchestratorResponse) {
	if !o.Enabled() {
		return
	}
	logical := o.SpanContext()
	for _, span := range o.pending {
		o.tracer.emit(span.name, span.kind, logical, span.spanID, span.start, span.end, span.attrs, span.failure)
	}
	o.pending = nil

	var complete *protos.CompleteOrchestrationAction
	for _, action := range response.GetActions() {
		if action.GetCompleteOrchestration() != nil {
			complete = action.GetCompleteOrchestration()
		}
	}
	if complete == nil {
		return
	}
	attrs := taskAttributes(typeOrchestration, o.name, o.version, o.instanceID)
	if o.executionID != "" {
		attrs = append(attrs, AttrExecutionID.String(o.executionID))
	}
	attrs = append(attrs, AttrStatus.String(statusName(complete.OrchestrationStatus)))
	var failure *protos.TaskFailureDetails
	if complete.OrchestrationStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED {
		failure = failureOrDefault(complete.FailureDetails)
	}
	o.tracer.emit(SpanName(typeOrchestration, o.name, o.version), trace.SpanKindServer,
		o.parent, o.spanID, o.start, time.Now(), attrs, failure)
}

func statusName(status protos.OrchestrationStatus) string {
	switch status {
	case protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED:
		return "Completed"
	case protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED:
		return "Failed"
	case protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED:
		return "Terminated"
	case protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW:
		return "ContinuedAsNew"
	default:
		return status.String()
	}
}
