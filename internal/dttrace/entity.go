package dttrace

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
)

// Entity attribute keys shared with the other Durable Task SDKs.
const (
	// AttrOperation is DurableTask.Core's entity request kind: "call_entity"
	// or "signal_entity".
	AttrOperation = attribute.Key("durabletask.task.operation")
	// AttrEntityOperation is the entity operation name, as reported by the
	// Python and JavaScript SDKs.
	AttrEntityOperation = attribute.Key("durabletask.entity.operation")
	// AttrScheduledTime is the RFC 3339 delivery time of a scheduled entity
	// request.
	AttrScheduledTime = attribute.Key("durabletask.task.scheduled_time")
	// AttrEntityError is the failure message of a failed entity operation.
	AttrEntityError = attribute.Key("durabletask.entity.error_message")
)

func entitySpanName(id api.EntityID, operation string) string {
	return typeEntity + ":" + helpers.ToLowerInvariant(id.Name) + ":" + operation
}

func entityAttributes(signal bool, operation string) []attribute.KeyValue {
	kind := "call_entity"
	if signal {
		kind = "signal_entity"
	}
	return []attribute.KeyValue{
		AttrType.String(typeEntity),
		AttrOperation.String(kind),
		AttrEntityOperation.String(operation),
	}
}

// EntityBatch holds the span identities reserved for one V2 entity batch.
// V2 entity messages carry no trace context, so every operation span is an
// independent root. Its identity and sampling decision are reserved before
// execution so handlers can see it, and the span is reconstructed only after
// the backend accepts the batch result.
type EntityBatch struct {
	tracer     *Tracer
	operations []entityOperation
}

type entityOperation struct {
	requestID   string
	name        string
	kind        trace.SpanKind
	attrs       []attribute.KeyValue
	spanContext trace.SpanContext
}

// NewEntityBatch reserves a root span context for each operation in request.
func (t *Tracer) NewEntityBatch(id api.EntityID, request *protos.EntityBatchRequest) *EntityBatch {
	batch := &EntityBatch{tracer: t, operations: make([]entityOperation, len(request.GetOperations()))}
	for i, operation := range request.GetOperations() {
		if operation == nil {
			continue
		}
		signal := request.GetProperties()[helpers.EntitySignalProperty(operation.RequestId)].GetBoolValue()
		kind := trace.SpanKindServer
		if signal {
			kind = trace.SpanKindConsumer
		}
		name := entitySpanName(id, operation.Operation)
		attrs := append(entityAttributes(signal, operation.Operation),
			AttrInstanceID.String(id.String()), AttrName.String(id.Name))
		batch.operations[i] = entityOperation{
			requestID:   operation.RequestId,
			name:        name,
			kind:        kind,
			attrs:       attrs,
			spanContext: t.rootSpanContext(trace.TraceID{}, trace.SpanID{}, name, kind, attrs),
		}
	}
	return batch
}

type entityBatchKey struct{}

// WithEntityBatch attaches an entity batch trace to an executor call.
func WithEntityBatch(ctx context.Context, batch *EntityBatch) context.Context {
	if batch == nil {
		return ctx
	}
	return context.WithValue(ctx, entityBatchKey{}, batch)
}

// EntityOperationSpanContext returns the reserved, non-recording span context
// of the operation at index in the batch attached to ctx. It returns an invalid
// span context when tracing is disabled or requestID does not match.
func EntityOperationSpanContext(ctx context.Context, index int, requestID string) trace.SpanContext {
	batch, _ := ctx.Value(entityBatchKey{}).(*EntityBatch)
	if batch == nil || index < 0 || index >= len(batch.operations) ||
		batch.operations[index].requestID != requestID {
		return trace.SpanContext{}
	}
	return batch.operations[index].spanContext
}

// Emit exports the reserved operation spans. Call it only after the backend
// accepts result, so rejected or abandoned batches export nothing. start and
// end bound operations whose result has no timestamps. Unsampled operations
// export nothing.
func (b *EntityBatch) Emit(result *protos.EntityBatchResult, start, end time.Time) {
	if b == nil {
		return
	}
	for i, operation := range b.operations {
		if !operation.spanContext.IsSampled() {
			continue
		}
		opStart, opEnd, failure := operationOutcome(result, i, start, end)
		attrs := operation.attrs
		if failure != nil {
			attrs = append(attrs[:len(attrs):len(attrs)], AttrEntityError.String(failure.GetErrorMessage()))
		}
		b.tracer.emit(operation.name, operation.kind, trace.SpanContext{},
			operation.spanContext.TraceID(), operation.spanContext.SpanID(), opStart, opEnd, attrs, failure)
	}
}

// operationOutcome returns the timestamps and failure of the operation at
// index. A batch failure fails every operation.
func operationOutcome(
	result *protos.EntityBatchResult,
	index int,
	start, end time.Time,
) (time.Time, time.Time, *protos.TaskFailureDetails) {
	if result.GetFailureDetails() != nil || index >= len(result.GetResults()) {
		return start, end, failureOrDefault(result.GetFailureDetails())
	}
	var failure *protos.TaskFailureDetails
	var startTime, endTime *timestamppb.Timestamp
	operationResult := result.GetResults()[index]
	if failed := operationResult.GetFailure(); failed != nil {
		failure = failureOrDefault(failed.FailureDetails)
		startTime, endTime = failed.StartTimeUtc, failed.EndTimeUtc
	} else if success := operationResult.GetSuccess(); success != nil {
		startTime, endTime = success.StartTimeUtc, success.EndTimeUtc
	} else {
		failure = &protos.TaskFailureDetails{
			ErrorType:    "InvalidEntityOperationResult",
			ErrorMessage: "entity executor returned an operation result without success or failure",
		}
	}
	if startTime != nil {
		start = startTime.AsTime()
	}
	if endTime != nil {
		end = endTime.AsTime()
	}
	return start, end, failure
}

// entitySpan builds an orchestration's CLIENT span for an entity call or
// PRODUCER span for an entity signal.
func (o *Orchestration) entitySpan(
	id api.EntityID,
	operation string,
	signal bool,
	taskID int32,
	start, end time.Time,
	scheduled *timestamppb.Timestamp,
	failure *protos.TaskFailureDetails,
) pendingSpan {
	attrs := append(entityAttributes(signal, operation),
		AttrEventTargetInstanceID.String(id.String()),
		AttrInstanceID.String(o.instanceID),
		AttrTaskID.Int(int(taskID)))
	if scheduled != nil {
		attrs = append(attrs, AttrScheduledTime.String(scheduled.AsTime().UTC().Format(time.RFC3339Nano)))
	}
	kind := trace.SpanKindClient
	if signal {
		kind = trace.SpanKindProducer
	}
	return pendingSpan{
		name:    entitySpanName(id, operation),
		kind:    kind,
		spanID:  RandomSpanID(),
		start:   start,
		end:     end,
		attrs:   attrs,
		failure: failure,
	}
}

// entityCall is the minimal state of an entity call awaiting its completion.
// Its span is built only when a completion is consumed as new.
type entityCall struct {
	target    string
	operation string
	taskID    int32
	start     time.Time
	scheduled *timestamppb.Timestamp
}

func (o *Orchestration) callEntity(eventID int32, called *protos.EntityOperationCalledEvent, at time.Time) {
	if o.entityCalls == nil {
		o.entityCalls = make(map[string]entityCall)
	}
	o.entityCalls[called.RequestId] = entityCall{
		target:    called.GetTargetInstanceId().GetValue(),
		operation: called.Operation,
		taskID:    eventID,
		start:     at,
		scheduled: called.ScheduledTime,
	}
}

// completeEntity queues the CLIENT span of an entity call whose completion is
// consumed as new. A call whose recorded target is not a valid entity ID emits
// no span.
func (o *Orchestration) completeEntity(
	requestID string,
	end time.Time,
	replaying bool,
	failure *protos.TaskFailureDetails,
) {
	call, ok := o.entityCalls[requestID]
	if !ok {
		return
	}
	delete(o.entityCalls, requestID)
	if replaying {
		return
	}
	id, err := api.EntityIDFromString(call.target)
	if err != nil {
		return
	}
	start, end := spanTimes(call.start, end)
	o.pending = append(o.pending,
		o.entitySpan(id, call.operation, false, call.taskID, start, end, call.scheduled, failure))
}

// entitySignal is the minimal state of a signal action. Its span is built
// only when the signal is queued for export.
type entitySignal struct {
	id        api.EntityID
	signal    *protos.EntityOperationSignaledEvent
	at        time.Time
	replaying bool
}

type entitySignalKey struct {
	eventID   int32
	requestID string
}

// RecordEntitySignal records a signal action. Each signal exports one PRODUCER
// span with the accepted response that first carries or consumes it, following
// the same rules as event sends (see eventSpan):
//
//   - A new signal still in the response exports with that response.
//   - A new signal whose acknowledgement is consumed in the same dispatch is
//     queued by ConfirmEntitySignal and exports with the response. DTS
//     normally delivers the acknowledgement as a past event of a later
//     dispatch, so this is defensive and best effort: the acknowledgement
//     proves an earlier response carrying the signal was accepted, and the
//     span exports twice if the worker that sent it also exported it.
//   - A signal issued while replaying exports only if it is still in the
//     response and no acknowledgement with its ID and request ID appears in
//     the request. A buffered event can create a new action while the replay
//     engine still reports replaying, and a turn budget can defer an
//     acknowledgement, so the request rather than the replay flag decides.
func (o *Orchestration) RecordEntitySignal(action *protos.OrchestratorAction, id api.EntityID, replaying bool) {
	if !o.Enabled() {
		return
	}
	signal := action.GetSendEntityMessage().GetEntityOperationSignaled()
	key := entitySignalKey{eventID: action.Id, requestID: signal.GetRequestId()}
	if _, acknowledged := o.entityAcks[key]; acknowledged && replaying {
		return
	}
	if o.entitySignals == nil {
		o.entitySignals = make(map[int32]entitySignal)
	}
	recorded := entitySignal{id: id, signal: signal, replaying: replaying}
	if !replaying {
		recorded.at = time.Now()
	}
	o.entitySignals[action.Id] = recorded
}

// ConfirmEntitySignal queues a signal consumed in the same dispatch only after
// the replay engine validates its action/history match.
func (o *Orchestration) ConfirmEntitySignal(eventID int32, replaying bool) {
	if !o.Enabled() {
		return
	}
	if recorded, ok := o.entitySignals[eventID]; ok && !recorded.replaying && !replaying {
		o.pending = append(o.pending, o.signalSpanFor(eventID, recorded))
	}
	delete(o.entitySignals, eventID)
}

// signalSpan returns the span for a signal action left in an accepted
// response, or false when it was not recorded or was already exported.
func (o *Orchestration) signalSpan(action *protos.OrchestratorAction) (pendingSpan, bool) {
	recorded, ok := o.entitySignals[action.Id]
	if !ok {
		return pendingSpan{}, false
	}
	return o.signalSpanFor(action.Id, recorded), true
}

func (o *Orchestration) signalSpanFor(eventID int32, recorded entitySignal) pendingSpan {
	at := recorded.at
	if at.IsZero() {
		at = o.responseTime
	}
	if at.IsZero() {
		at = time.Now()
	}
	return o.entitySpan(recorded.id, recorded.signal.GetOperation(), true, eventID,
		at, at, recorded.signal.GetScheduledTime(), nil)
}
