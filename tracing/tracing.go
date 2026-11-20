// Package tracing configures opt-in OpenTelemetry spans for Durable Task
// clients and workers.
//
// By default clients and workers only propagate W3C trace context and emit no
// spans. Enable worker spans with client.WithWorkerTracerProvider and a
// provider built with [WithIDGenerator] and [WithSampler]:
//
//	provider := sdktrace.NewTracerProvider(
//		tracing.WithIDGenerator(nil),
//		tracing.WithSampler(nil),
//		sdktrace.WithBatcher(exporter),
//	)
//	worker, err := durabletaskscheduler.NewWorker(..., client.WithWorkerTracerProvider(provider))
//
// Enable client spans with client.WithTracerProvider, which accepts any
// provider:
//
//	c, err := durabletaskscheduler.NewClient(ctx, options, logger, client.WithTracerProvider(provider))
//
// The client emits a PRODUCER span named "create_orchestration:<name>" (with
// an "@(<version>)" suffix when versioned) for each ScheduleNewOrchestration
// call and parents the new orchestration to it, and a PRODUCER span named
// "orchestration_event:<event>" for each RaiseEvent call. Both are children of
// the span in the call's context, and failed calls record an error status.
//
// The application owns the provider's resource, sampling policy, processors,
// exporter, and shutdown. Spans use the "durabletask" instrumentation scope.
//
// Orchestrations replay from history, so the worker never keeps a live
// orchestration span between dispatches. It persists the span's ID and start
// time with the backend and reconstructs completed spans with their original
// identity. Reconstruction needs the provider to honor requested span IDs and
// to report sampling decisions without starting spans, which is why
// [WithIDGenerator] and [WithSampler] are required; the worker rejects other
// providers instead of exporting a disconnected span tree. Consequently:
//
//   - The orchestration span is exported once, when the execution completes,
//     fails, terminates, or continues as new.
//   - Activity and sub-orchestration scheduling (CLIENT) spans and timer spans
//     are exported when the dispatch that consumes their completion is
//     accepted by the backend. Replayed history exports nothing.
//   - Activity executions are live SERVER spans that are active in the
//     activity's context, so application spans nest under them.
//   - Orchestrator SendEvent actions emit "orchestration_event:<event>"
//     PRODUCER spans under the sending orchestration when accepted. Replayed
//     sends and later history acknowledgements emit nothing. A new send
//     matched to history in the same dispatch also emits once.
//   - Event receipt does not emit a consumer span. The event protocol carries
//     no trace context, so sends cannot parent or link the receiving
//     orchestration to the sender's event span.
//   - Abandoned dispatches export no orchestration telemetry. Replay
//     suppression is best effort and is not exactly-once delivery: a dispatch
//     whose completion succeeds but is retried by the backend may export again.
//   - Orchestration spans join the trace context recorded when the
//     orchestration was scheduled. An orchestration scheduled without trace
//     context becomes the root of a new trace. Its trace ID is derived from
//     the persisted span identity, and its sampling decision comes from the
//     sampler passed to [WithSampler], which is consulted again on later
//     dispatches. Use a sampler whose root decision depends only on the trace
//     ID, such as the OpenTelemetry SDK's built-in samplers, so every dispatch
//     agrees.
//   - Span identity persistence depends on the backend returning the
//     orchestration trace context on later dispatches. Continue-as-new and
//     rewind start a new orchestration span identity.
//
// Sampling follows the caller's trace flags: unsampled orchestrations stay
// unsampled, and tracestate is preserved.
package tracing

import (
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/microsoft/durabletask-go/internal/dttrace"
)

// ErrUnsupportedTracerProvider is returned when worker tracing is enabled with
// a provider that does not use both [WithIDGenerator] and [WithSampler].
var ErrUnsupportedTracerProvider = dttrace.ErrUnsupportedTracerProvider

// NewIDGenerator wraps base so Durable Task can reconstruct spans with their
// persisted IDs. Application spans always receive IDs from base. A nil base
// uses random IDs, like the OpenTelemetry SDK default.
func NewIDGenerator(base sdktrace.IDGenerator) sdktrace.IDGenerator {
	return dttrace.IDGenerator{Base: base}
}

// WithIDGenerator returns the tracer provider option required by Durable Task
// tracing. Pass any custom application ID generator as base instead of also
// using sdktrace.WithIDGenerator, which would replace this one.
func WithIDGenerator(base sdktrace.IDGenerator) sdktrace.TracerProviderOption {
	return sdktrace.WithIDGenerator(NewIDGenerator(base))
}

// NewSampler wraps base so Durable Task can learn sampling decisions for
// reconstructed root orchestration spans without starting spans. All other
// spans are sampled exactly as base decides. A nil base uses
// sdktrace.ParentBased(sdktrace.AlwaysSample()), the OpenTelemetry SDK default.
func NewSampler(base sdktrace.Sampler) sdktrace.Sampler {
	return dttrace.Sampler{Base: base}
}

// WithSampler returns the tracer provider option required by Durable Task
// tracing. Pass the application's sampler as base instead of also using
// sdktrace.WithSampler, which would replace this one.
func WithSampler(base sdktrace.Sampler) sdktrace.TracerProviderOption {
	return sdktrace.WithSampler(NewSampler(base))
}
