package dttrace

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// AttrEventTargetInstanceID identifies the orchestration that receives an event.
const AttrEventTargetInstanceID = attribute.Key("durabletask.event.target_instance_id")

const (
	typeEvent               = "event"
	typeCreateOrchestration = "create_orchestration"
	typeOrchestrationEvent  = "orchestration_event"
)

// ClientTracer emits live client PRODUCER spans. Unlike [Tracer] it never
// reconstructs spans, so any provider is accepted.
type ClientTracer struct {
	tracer trace.Tracer
}

// NewClientTracer returns nil for a nil provider, which disables client spans.
func NewClientTracer(provider trace.TracerProvider) *ClientTracer {
	if provider == nil {
		return nil
	}
	return &ClientTracer{tracer: provider.Tracer(ScopeName)}
}

// StartCreateOrchestration starts the PRODUCER span for scheduling an
// orchestration as a child of the span in ctx. The returned context carries
// the new span so it becomes the orchestration's parent trace context. An
// empty executionID, the usual case, omits the execution ID attribute.
func (t *ClientTracer) StartCreateOrchestration(
	ctx context.Context,
	name, version, instanceID, executionID string,
) (context.Context, trace.Span) {
	attrs := taskAttributes(typeOrchestration, name, version, instanceID)
	if executionID != "" {
		attrs = append(attrs, AttrExecutionID.String(executionID))
	}
	return t.tracer.Start(ctx, SpanName(typeCreateOrchestration, name, version),
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attrs...),
	)
}

// StartRaiseEvent starts the PRODUCER span for raising an event to an
// orchestration as a child of the span in ctx.
func (t *ClientTracer) StartRaiseEvent(ctx context.Context, eventName, instanceID string) (context.Context, trace.Span) {
	return t.tracer.Start(ctx, SpanName(typeOrchestrationEvent, eventName, ""),
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			AttrType.String(typeEvent),
			AttrName.String(eventName),
			AttrEventTargetInstanceID.String(instanceID),
		),
	)
}

// EndClientSpan records err, if any, and ends span.
func EndClientSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
