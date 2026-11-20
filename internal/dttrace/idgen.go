// Package dttrace implements replay-aware Durable Task OpenTelemetry spans.
package dttrace

import (
	"context"
	"encoding/binary"
	"math/rand/v2"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// spanIDOverrideKey is only placed on short-lived contexts used to reconstruct
// a span whose identity was persisted by an earlier dispatch. Those contexts
// are never returned to application code.
type spanIDOverrideKey struct{}

func withSpanIDOverride(ctx context.Context, spanID trace.SpanID) context.Context {
	return context.WithValue(ctx, spanIDOverrideKey{}, spanID)
}

// IDGenerator wraps an application ID generator so the SDK can reconstruct
// spans with persisted IDs. Ordinary spans always use the base generator.
type IDGenerator struct {
	Base sdktrace.IDGenerator
}

// NewIDs implements sdktrace.IDGenerator. Reconstructed spans always have a
// remote parent, so root spans never use an override.
func (g IDGenerator) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	if g.Base != nil {
		return g.Base.NewIDs(ctx)
	}
	return RandomTraceID(), RandomSpanID()
}

// NewSpanID implements sdktrace.IDGenerator.
func (g IDGenerator) NewSpanID(ctx context.Context, traceID trace.TraceID) trace.SpanID {
	if spanID, ok := ctx.Value(spanIDOverrideKey{}).(trace.SpanID); ok && spanID.IsValid() {
		return spanID
	}
	if g.Base != nil {
		return g.Base.NewSpanID(ctx, traceID)
	}
	return RandomSpanID()
}

// RandomSpanID returns a non-zero random span ID.
func RandomSpanID() trace.SpanID {
	var spanID trace.SpanID
	for !spanID.IsValid() {
		binary.BigEndian.PutUint64(spanID[:], rand.Uint64())
	}
	return spanID
}

// RandomTraceID returns a non-zero random trace ID.
func RandomTraceID() trace.TraceID {
	var traceID trace.TraceID
	for !traceID.IsValid() {
		binary.BigEndian.PutUint64(traceID[:8], rand.Uint64())
		binary.BigEndian.PutUint64(traceID[8:], rand.Uint64())
	}
	return traceID
}
