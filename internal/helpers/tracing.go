package helpers

import (
	"context"
	"crypto/rand"
	"fmt"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/internal/protos"
)

// TraceContextFromSpan converts a sampled OpenTelemetry span into the W3C trace
// context sent to Durable Task Scheduler. Unsampled or invalid spans return nil.
func TraceContextFromSpan(span trace.Span) *protos.TraceContext {
	if span == nil || !span.SpanContext().IsSampled() {
		return nil
	}
	spanContext := span.SpanContext()
	if !spanContext.IsValid() {
		return nil
	}
	return &protos.TraceContext{
		TraceParent: traceParent(spanContext.TraceID(), spanContext.SpanID(), spanContext.TraceFlags()),
		TraceState:  traceStateValue(spanContext.TraceState()),
	}
}

// traceParent formats the W3C traceparent header value for the given
// version-00 components.
func traceParent(traceID trace.TraceID, spanID trace.SpanID, flags trace.TraceFlags) string {
	return "00-" + traceID.String() + "-" + spanID.String() + "-" + flags.String()
}

// traceStateValue wraps a non-empty W3C tracestate for the wire, or returns
// nil if the tracestate is absent.
func traceStateValue(state trace.TraceState) *wrapperspb.StringValue {
	if value := state.String(); value != "" {
		return wrapperspb.String(value)
	}
	return nil
}

// OrchestratorActionTraceContext creates the trace context for a service-owned
// activity or sub-orchestration scheduling span.
func OrchestratorActionTraceContext(parent *protos.TraceContext) (*protos.TraceContext, error) {
	if parent == nil {
		return nil, nil
	}
	carrier := propagation.MapCarrier{"traceparent": parent.GetTraceParent()}
	if traceState := parent.GetTraceState().GetValue(); traceState != "" {
		carrier.Set("tracestate", traceState)
	}
	parentContext := trace.SpanContextFromContext(
		propagation.TraceContext{}.Extract(context.Background(), carrier),
	)
	if !parentContext.IsValid() {
		return nil, fmt.Errorf("invalid parent trace context")
	}

	var spanID trace.SpanID
	for !spanID.IsValid() {
		if _, err := rand.Read(spanID[:]); err != nil {
			return nil, fmt.Errorf("generate action span ID: %w", err)
		}
	}

	return &protos.TraceContext{
		TraceParent: traceParent(parentContext.TraceID(), spanID, parentContext.TraceFlags()),
		TraceState:  traceStateValue(parentContext.TraceState()),
	}, nil
}

// CloneTraceContext returns an independent copy of the active W3C trace fields.
func CloneTraceContext(value *protos.TraceContext) *protos.TraceContext {
	if value == nil {
		return nil
	}
	result := &protos.TraceContext{TraceParent: value.GetTraceParent()}
	if value.TraceState != nil {
		result.TraceState = wrapperspb.String(value.GetTraceState().GetValue())
	}
	return result
}
