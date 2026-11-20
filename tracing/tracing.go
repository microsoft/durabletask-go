// Package tracing configures opt-in OpenTelemetry spans for Durable Task
// clients and workers.
//
// The tracing README contains the span catalog, OTLP sample, and a parity
// comparison with durabletask-python and durabletask-dotnet's gRPC SDK.
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
// Enable client spans with client.WithTracerProvider. Client spans are
// started live and never reconstructed, so any provider works, including
// otel.GetTracerProvider(), the global delegating provider. The client and
// worker can also share one provider configured with [WithIDGenerator] and
// [WithSampler]:
//
//	c, err := durabletaskscheduler.NewClient(ctx, options, logger, client.WithTracerProvider(provider))
//
// The client emits a PRODUCER span named "create_orchestration:<name>" (with
// an "@(<version>)" suffix when versioned) for each ScheduleNewOrchestration
// call and parents the new orchestration to it, and a PRODUCER span named
// "orchestration_event:<event>" for each RaiseEvent call. Both are children of
// the span in the call's context, and failed calls, including payload
// externalization failures, record an error status.
//
// The application owns the provider's resource, sampling policy, processors,
// exporter, and shutdown. Spans use the "Microsoft.DurableTask"
// instrumentation scope, like the Durable Task .NET, JavaScript, and Java SDKs.
//
// Orchestrations replay from history, so the worker never keeps a live
// orchestration span between dispatches. It persists the span's ID and start
// time with the backend and reconstructs completed spans with their original
// identity. Reconstruction needs the provider to honor requested span IDs and
// to report sampling decisions without starting spans, which is why
// [WithIDGenerator] and [WithSampler] are required; the worker rejects other
// providers instead of exporting a disconnected span tree. The wrappers must be
// the provider's top-level ID generator and sampler: to customize them, pass
// the application's generator or sampler as base rather than wrapping the
// Durable Task wrapper, for example tracing.WithSampler(sdktrace.ParentBased(...))
// rather than sdktrace.WithSampler(sdktrace.ParentBased(tracing.NewSampler(...))).
// Consequently:
//
//   - The orchestration span is exported once, when the execution completes,
//     fails, terminates, or continues as new.
//   - Activity and sub-orchestration scheduling (CLIENT) spans and timer spans
//     are exported when the dispatch that consumes their completion is
//     accepted by the backend. Replayed history exports nothing.
//   - Activity executions are live SERVER spans that are active in the
//     activity's context, so application spans nest under them. The span
//     reports the activity's own outcome, whether or not that outcome reaches
//     the backend. Work the worker rejects before running activity code, such
//     as a version mismatch with task.VersionFailureReject, exports no span.
//   - Orchestrator SendEvent actions emit "orchestration_event:<event>"
//     PRODUCER spans under the sending orchestration when accepted. Replayed
//     sends and later history acknowledgements emit nothing. A new send whose
//     acknowledgement arrives in the same dispatch is exported with that
//     dispatch.
//   - Event receipt does not emit a consumer span. The event protocol carries
//     no trace context, so sends cannot parent or link the receiving
//     orchestration to the sender's event span.
//   - Entity spans are named "entity:<name>:<operation>" and carry the
//     DurableTask.Core "durabletask.task.operation" attribute ("call_entity"
//     or "signal_entity") and the "durabletask.entity.operation" attribute
//     (the operation name) used by the Python and JavaScript SDKs.
//     Orchestrator entity signals emit PRODUCER spans under the orchestration
//     when accepted, following the same replay rules as SendEvent. Entity
//     calls emit CLIENT spans when the dispatch that consumes their
//     completion is accepted. Replayed history emits neither.
//   - Entity operation executions emit SERVER spans for calls and CONSUMER
//     spans for signals. Each operation's span identity and sampling decision
//     are reserved before the batch runs, and the operation's handler context
//     carries that identity as a non-recording span context, so application
//     spans started from EntityContext.Context() nest under it. Signals and
//     orchestrations started by the operation carry it as their parent trace
//     context. The span itself is reconstructed with the operation's result
//     timestamps and exported only after the backend accepts the batch
//     result; rejected, abandoned, or lost batches export nothing, and spans
//     the application started under it may then reference a parent that is
//     never exported. Successful operations leave the status unset; failed
//     operations and failed batches record an error status and the
//     "durabletask.entity.error_message" attribute. Durable Task Scheduler
//     does not currently forward the parent trace context of entity-started
//     signals and orchestrations.
//   - V2 entity messages carry no trace context, so entity execution spans are
//     independent trace roots, sampled by the provider's root policy, rather
//     than children of the calling orchestration or client span. Their IDs
//     come from the base generator passed to [WithIDGenerator]. No synthetic
//     parent or span link is added.
//   - Client SignalEntity emits no span. It propagates the caller's trace
//     context on the request only when that context is sampled, like the
//     Durable Task .NET gRPC client. The Python SDK also emits no client span
//     for signals; the JavaScript SDK emits a "signal_entity" PRODUCER span.
//   - Entity lock acquisition and release emit no spans, matching
//     DurableTask.Core; entity calls made inside a critical section are traced.
//   - Abandoned orchestration dispatches export no orchestration telemetry.
//   - Telemetry is best effort, not exactly once, in both directions. A
//     completion the backend retries may export its spans again. A completion
//     the backend accepted but whose reply was lost, so that the retry
//     reports the work item gone, may export none.
//   - Orchestration spans join the trace context recorded when the
//     orchestration was scheduled, or the parent trace context recorded when a
//     sub-orchestration was rewound. An orchestration scheduled without trace
//     context becomes the root of a new trace. Its trace ID is derived from the
//     orchestration's instance, execution, and span identity, so it does not
//     come from the base generator passed to [WithIDGenerator]; for example, an
//     AWS X-Ray format generator does not apply to it. Its sampling decision
//     comes from the sampler passed to [WithSampler] and is re-evaluated on
//     every dispatch, so it is stable only for samplers that decide
//     deterministically from the trace ID, such as the OpenTelemetry SDK's
//     built-in samplers.
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
// a provider that does not use both [WithIDGenerator] and [WithSampler]. The
// returned error wraps it and names the unmet requirement; match it with
// errors.Is.
var ErrUnsupportedTracerProvider = dttrace.ErrUnsupportedTracerProvider

// NewIDGenerator wraps base so Durable Task can reconstruct spans with their
// persisted IDs. Application spans always receive IDs from base. A nil base
// uses random IDs, like the OpenTelemetry SDK default. The result must be the
// provider's top-level ID generator; do not wrap it in another generator.
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
// reconstructed root orchestration and entity operation spans without starting
// spans. All other spans are sampled exactly as base decides. A nil base uses
// sdktrace.ParentBased(sdktrace.AlwaysSample()), the OpenTelemetry SDK default.
// The result must be the provider's top-level sampler: pass a composite such
// as sdktrace.ParentBased as base instead of nesting this sampler inside it.
func NewSampler(base sdktrace.Sampler) sdktrace.Sampler {
	return dttrace.Sampler{Base: base}
}

// WithSampler returns the tracer provider option required by Durable Task
// tracing. Pass the application's sampler as base instead of also using
// sdktrace.WithSampler, which would replace this one.
func WithSampler(base sdktrace.Sampler) sdktrace.TracerProviderOption {
	return sdktrace.WithSampler(NewSampler(base))
}
