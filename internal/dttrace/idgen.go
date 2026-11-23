// Package dttrace implements replay-aware Durable Task OpenTelemetry spans.
package dttrace

import (
	"context"
	"encoding/binary"
	"math/rand/v2"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// idOverrideKey is only placed on short-lived contexts used to reconstruct a
// span whose identity was persisted by an earlier dispatch. Those contexts are
// never returned to application code.
type idOverrideKey struct{}

type idOverride struct {
	traceID trace.TraceID
	spanID  trace.SpanID
}

// withIDOverride requests spanID for the next span. traceID is used only for
// root spans; child spans always inherit their parent's trace ID.
func withIDOverride(ctx context.Context, traceID trace.TraceID, spanID trace.SpanID) context.Context {
	return context.WithValue(ctx, idOverrideKey{}, idOverride{traceID: traceID, spanID: spanID})
}

// IDGenerator wraps an application ID generator so the SDK can reconstruct
// spans with persisted IDs. Ordinary spans always use the base generator.
type IDGenerator struct {
	Base sdktrace.IDGenerator
}

// NewIDs implements sdktrace.IDGenerator.
func (g IDGenerator) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	if override, ok := ctx.Value(idOverrideKey{}).(idOverride); ok &&
		override.traceID.IsValid() && override.spanID.IsValid() {
		return override.traceID, override.spanID
	}
	if g.Base != nil {
		return g.Base.NewIDs(ctx)
	}
	return RandomTraceID(), RandomSpanID()
}

// NewSpanID implements sdktrace.IDGenerator.
func (g IDGenerator) NewSpanID(ctx context.Context, traceID trace.TraceID) trace.SpanID {
	if override, ok := ctx.Value(idOverrideKey{}).(idOverride); ok && override.spanID.IsValid() {
		return override.spanID
	}
	if g.Base != nil {
		return g.Base.NewSpanID(ctx, traceID)
	}
	return RandomSpanID()
}

// samplingQueryKey marks a span start that only asks the sampler for its
// decision. Like the ID override, it is only placed on private contexts.
type samplingQueryKey struct{}

type samplingQuery struct {
	answered bool
	result   sdktrace.SamplingResult
}

// Sampler wraps an application sampler so the SDK can learn a reconstructed
// root span's sampling decision without starting a recording span.
type Sampler struct {
	Base sdktrace.Sampler
}

// ShouldSample implements sdktrace.Sampler. For sampling queries it records the
// base decision and drops the query span, so processors never observe it.
func (s Sampler) ShouldSample(parameters sdktrace.SamplingParameters) sdktrace.SamplingResult {
	result := s.base().ShouldSample(parameters)
	if parameters.ParentContext != nil {
		if query, ok := parameters.ParentContext.Value(samplingQueryKey{}).(*samplingQuery); ok {
			query.answered = true
			query.result = result
			return sdktrace.SamplingResult{Decision: sdktrace.Drop, Tracestate: result.Tracestate}
		}
	}
	return result
}

// Description implements sdktrace.Sampler.
func (s Sampler) Description() string {
	return "DurableTaskSampler{" + s.base().Description() + "}"
}

func (s Sampler) base() sdktrace.Sampler {
	if s.Base == nil {
		return sdktrace.ParentBased(sdktrace.AlwaysSample())
	}
	return s.Base
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
