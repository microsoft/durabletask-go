package dttrace

import (
	"context"
	"strconv"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	AttrOperation     = attribute.Key("durabletask.task.operation")
	AttrScheduledTime = attribute.Key("durabletask.task.scheduled_time")
	AttrEntityError   = attribute.Key("durabletask.entity.error_message")
	typeEntity        = "entity"
)

func entitySpanName(id api.EntityID, operation string) string {
	return typeEntity + ":" + helpers.ToLowerInvariant(id.Name) + ":" + operation
}

func entityAttributes(signal bool) []attribute.KeyValue {
	operation := "call_entity"
	if signal {
		operation = "signal_entity"
	}
	return []attribute.KeyValue{AttrType.String(typeEntity), AttrOperation.String(operation)}
}

// EmitEntityBatch exports operation execution spans after an accepted V2 batch.
// V2 operation messages have no trace context, so these are independent roots.
func (t *Tracer) EmitEntityBatch(id api.EntityID, request *protos.EntityBatchRequest, result *protos.EntityBatchResult, start, end time.Time) {
	for i, operation := range request.Operations {
		opStart, opEnd := start, end
		failure := result.FailureDetails
		if failure == nil && i < len(result.Results) {
			opResult := result.Results[i]
			var startTime, endTime *timestamppb.Timestamp
			if failed := opResult.GetFailure(); failed != nil {
				failure = failureOrDefault(failed.FailureDetails)
				startTime, endTime = failed.StartTimeUtc, failed.EndTimeUtc
			} else if success := opResult.GetSuccess(); success != nil {
				startTime, endTime = success.StartTimeUtc, success.EndTimeUtc
			} else {
				failure = &protos.TaskFailureDetails{
					ErrorType:    "InvalidEntityOperationResult",
					ErrorMessage: "entity executor returned an operation result without success or failure",
				}
			}
			if startTime != nil {
				opStart = startTime.AsTime()
			}
			if endTime != nil {
				opEnd = endTime.AsTime()
			}
		}
		if opEnd.Before(opStart) {
			opEnd = opStart
		}
		signal := request.Properties[helpers.EntitySignalProperty(operation.RequestId)].GetBoolValue()
		kind := trace.SpanKindServer
		if signal {
			kind = trace.SpanKindConsumer
		}
		_, span := t.tracer.Start(context.Background(), entitySpanName(id, operation.Operation),
			trace.WithNewRoot(), trace.WithSpanKind(kind), trace.WithTimestamp(opStart),
			trace.WithAttributes(append(entityAttributes(signal), AttrInstanceID.String(id.String()), AttrName.String(id.Name))...))
		if failure != nil {
			span.SetAttributes(AttrEntityError.String(failure.GetErrorMessage()))
			span.SetStatus(codes.Error, failure.GetErrorMessage())
		} else {
			span.SetStatus(codes.Ok, "Completed")
		}
		span.End(trace.WithTimestamp(opEnd))
	}
}

func (o *Orchestration) entitySpan(id api.EntityID, operation string, signal bool, taskID int32, start time.Time, scheduled *timestamppb.Timestamp) pendingSpan {
	attrs := append(entityAttributes(signal), AttrEventTargetInstanceID.String(id.String()),
		AttrInstanceID.String(o.instanceID), AttrTaskID.String(strconv.Itoa(int(taskID))))
	if scheduled != nil {
		attrs = append(attrs, AttrScheduledTime.String(scheduled.AsTime().UTC().Format(time.RFC3339Nano)))
	}
	kind := trace.SpanKindClient
	if signal {
		kind = trace.SpanKindProducer
	}
	return pendingSpan{
		name: entitySpanName(id, operation), kind: kind,
		spanID: RandomSpanID(), start: start, attrs: attrs,
	}
}

func (o *Orchestration) completeEntity(requestID string, end time.Time, replaying bool, failure *protos.TaskFailureDetails) {
	span, ok := o.entities[requestID]
	if !ok {
		return
	}
	delete(o.entities, requestID)
	if replaying {
		return
	}
	span.end, span.failure = end, failure
	o.pending = append(o.pending, span)
}

type entitySignalSpan struct {
	pendingSpan
	createdDuringReplay bool
}

// RecordEntitySignal also reserves replayed actions: a buffered event may
// create a new action while the replay engine still considers it replaying.
func (o *Orchestration) RecordEntitySignal(action *protos.OrchestratorAction, id api.EntityID, replaying bool) {
	if !o.Enabled() {
		return
	}
	signal := action.GetSendEntityMessage().GetEntityOperationSignaled()
	span := o.entitySpan(id, signal.Operation, true, action.Id, time.Now(), signal.ScheduledTime)
	span.end = span.start
	o.entitySignals[action.Id] = entitySignalSpan{pendingSpan: span, createdDuringReplay: replaying}
}

// ConfirmEntitySignal queues a signal consumed in the same dispatch only after
// the replay engine validates its action/history match.
func (o *Orchestration) ConfirmEntitySignal(eventID int32, replaying bool) {
	if !o.Enabled() {
		return
	}
	if span, ok := o.entitySignals[eventID]; ok && !span.createdDuringReplay && !replaying {
		o.pending = append(o.pending, span.pendingSpan)
	}
	delete(o.entitySignals, eventID)
}
