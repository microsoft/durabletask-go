// Package tracing configures opt-in OpenTelemetry spans for Durable Task
// workers.
//
// By default workers only propagate W3C trace context and emit no spans.
// Enable spans with client.WithWorkerTracerProvider and a provider built with
// [WithIDGenerator]:
//
//	provider := sdktrace.NewTracerProvider(
//		tracing.WithIDGenerator(nil),
//		sdktrace.WithBatcher(exporter),
//	)
//	worker, err := durabletaskscheduler.NewWorker(..., client.WithWorkerTracerProvider(provider))
//
// The application owns the provider's resource, sampler, processors, exporter,
// and shutdown. Spans use the "durabletask" instrumentation scope.
//
// Orchestrations replay from history, so the worker never keeps a live
// orchestration span between dispatches. It persists the span's ID and start
// time with the backend and reconstructs completed spans with their original
// identity. Reconstruction needs the provider to honor requested span IDs,
// which is why [WithIDGenerator] is required; the worker rejects other
// providers instead of exporting a disconnected span tree. Consequently:
//
//   - The orchestration span is exported once, when the execution completes,
//     fails, terminates, or continues as new.
//   - Activity and sub-orchestration scheduling (CLIENT) spans and timer spans
//     are exported when the dispatch that consumes their completion is
//     accepted by the backend. Replayed history exports nothing.
//   - Activity executions are live SERVER spans that are active in the
//     activity's context, so application spans nest under them.
//   - Abandoned dispatches export no orchestration telemetry. Replay
//     suppression is best effort and is not exactly-once delivery: a dispatch
//     whose completion succeeds but is retried by the backend may export again.
//   - Orchestration spans join the trace context recorded when the
//     orchestration was scheduled. Orchestrations scheduled without trace
//     context produce no orchestration, scheduling, or timer spans.
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
// a provider that does not use [WithIDGenerator].
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
