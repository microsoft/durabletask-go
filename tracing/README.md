# Replay-aware OpenTelemetry tracing

By default, Durable Task clients and workers **only propagate W3C trace
context**. They emit no spans. When you opt in, the SDK exports durable
orchestration, activity, sub-orchestration, timer, event, and entity spans
through your application's OpenTelemetry provider, next to your own application
spans. Your provider sends them to any OpenTelemetry-compatible backend. You
don't need a scheduler-specific telemetry exporter.

## Enable tracing

Configure the worker's OpenTelemetry SDK provider with **both** Durable Task
wrappers. Then pass the provider to the worker and, optionally, to the client:

```go
provider := sdktrace.NewTracerProvider(
	tracing.WithIDGenerator(nil),
	tracing.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	sdktrace.WithBatcher(exporter),
	sdktrace.WithResource(res),
)

worker, err := durabletaskscheduler.NewWorker(options, registry, logger,
	client.WithWorkerTracerProvider(provider))
if err != nil {
	// errors.Is(err, tracing.ErrUnsupportedTracerProvider) reports a provider
	// that is missing a wrapper.
	return err
}
managementClient, err := durabletaskscheduler.NewClient(ctx, options, logger,
	client.WithTracerProvider(provider))
if err != nil {
	return err
}
```

`client.WithTracerProvider` accepts any tracer provider, including
`otel.GetTracerProvider()`. `client.WithWorkerTracerProvider` rejects a provider
that doesn't use both wrappers. The error wraps
`tracing.ErrUnsupportedTracerProvider` and names the failed check. The wrappers
must be the provider's top-level ID generator and sampler:

- The ID generator applies persisted span identities when the worker
  reconstructs spans. Your application spans still get IDs from its base
  generator.
- The sampler answers sampling queries without starting live spans.

To customize either one, pass your generator or sampler to the wrapper as its
base. Don't replace the wrapper with `sdktrace.WithIDGenerator` or
`sdktrace.WithSampler`, and don't nest the wrapper inside another sampler.

Your application owns the provider's resource, processors, exporter, flushing,
and shutdown. Shut down the worker, close the client, and then shut down the
provider. `otel.SetTracerProvider(provider)` is optional for Durable Task, but
it helps instrumentation libraries that use the global provider. For a complete
setup, see the [distributed tracing sample](../samples/distributedtracing). It
exports to an OTLP/HTTP collector, which can forward the spans to Jaeger, Azure
Monitor, or another OTLP backend.

## Service-side telemetry and duplicates

Durable Task Scheduler (DTS) persists and forwards trace context, but it doesn't
emit durable orchestration, activity, timer, sub-orchestration, or entity spans.
It also doesn't export spans to your telemetry backend. To see durable spans in
your backend, enable SDK tracing.

SDK spans use the `Microsoft.DurableTask` instrumentation scope and your
provider's resource. If another component, such as a host or a future
scheduler-side export, emits durable spans for the same work into the same
backend, enable only one source. To keep the SDK silent, omit
`client.WithWorkerTracerProvider` and `client.WithTracerProvider`.

## Logical spans, not execution-turn spans

An orchestrator replays from history on each dispatch. The SDK doesn't emit a
span for each dispatch or replay. Instead, it emits one logical
`orchestration:<name>` SERVER span for each execution. Each continue-as-new
generation is a separate execution. The span starts at the execution's first
traced dispatch and ends when the execution completes, fails, terminates, or
continues as new. The worker persists the span's ID and start time with the
backend, and reconstructs the span with that identity when the execution
finishes.

As a result:

- A running, suspended, or never-completing orchestration has no exported
  orchestration span yet. Its exported children can appear under a parent that
  your backend doesn't show until the orchestration completes. If the
  orchestration is purged or abandoned first, the parent never appears.
- If tracing is enabled while an orchestration is already running, its span
  starts at the first traced dispatch.
- The JavaScript SDK works differently. It emits one SERVER span per dispatch.

## Spans and lifecycle

Task span names use `<type>:<name>@(<version>)`. The `@(<version>)` suffix is
omitted when the task is unversioned. Entity names are lowercase, as in
`EntityID.String`.

| Operation | Span name | Kind and parent |
| --- | --- | --- |
| Client schedules orchestration | `create_orchestration:<name>` | PRODUCER under caller |
| Orchestration execution | `orchestration:<name>` | SERVER under create span, or under the propagated caller context when client tracing is off; trace root without caller context |
| Activity scheduling | `activity:<name>` | CLIENT under orchestration |
| Activity execution | `activity:<name>` | SERVER under scheduling span; active in `ActivityContext.Context()` |
| Sub-orchestration scheduling | `orchestration:<child>` | CLIENT under parent orchestration |
| Sub-orchestration execution | `orchestration:<child>` | SERVER under scheduling span |
| Durable timer | `orchestration:<name>:timer` | INTERNAL under orchestration |
| Client raises event | `orchestration_event:<event>` | PRODUCER under caller |
| Orchestrator sends event | `orchestration_event:<event>` | PRODUCER under sending orchestration |
| Orchestrator signals entity | `entity:<name>:<operation>` | PRODUCER under orchestration |
| Orchestrator calls entity | `entity:<name>:<operation>` | CLIENT under orchestration |
| Entity signal execution | `entity:<name>:<operation>` | CONSUMER, independent root |
| Entity call execution | `entity:<name>:<operation>` | SERVER, independent root |

When each kind of span is exported:

- **Orchestration spans** are exported after the backend accepts the completion
  of the execution.
- **Activity execution spans** are live. HTTP, database, and other application
  spans that you start from `ActivityContext.Context()` nest under them. The
  span starts after the worker's version check, so work that the worker rejects
  for a version mismatch exports no span. The span ends right after the
  activity runs and reports the activity's own outcome, even if that outcome
  doesn't reach the backend.
- **Scheduling, entity-call, and timer spans** are reconstructed from history.
  They're exported when the backend accepts the dispatch that consumes their
  completion.
- **Event-send and entity-signal spans** are exported after the backend accepts
  their sending dispatch. If a new send's acknowledgement arrives in the same
  dispatch, the span is exported with that dispatch.
- **Entity execution spans** are exported after the backend accepts the batch.
  They use the operation result's timestamps.
  `EntityContext.Context()` carries the operation's span context as a
  non-recording span, so application spans that you start from it nest under
  the operation span.

Replayed history emits no new spans. Orchestration spans aren't live inside the
orchestrator. An application span that you start from orchestrator code isn't
replay-safe.

Attributes include `durabletask.type`, `durabletask.task.name`,
`durabletask.task.version`, and `durabletask.task.instance_id`. Some spans also
carry `durabletask.task.execution_id`, the integer `durabletask.task.task_id`,
`durabletask.task.status`, or `durabletask.fire_at`. On activity, timer,
sub-orchestration scheduling, and entity caller spans,
`durabletask.task.instance_id` is the calling orchestration's instance ID. On
entity execution spans, it's the entity ID. Entity spans follow
[DurableTask.Core](https://github.com/Azure/durabletask/tree/main/src/DurableTask.Core/Tracing)
names and kinds:

- `durabletask.task.operation` is `call_entity` or `signal_entity`.
- `durabletask.entity.operation` is the operation name, as in the Python and
  JavaScript SDKs.

Failures get an error status. Successful spans leave the status unset.

Without caller context, an orchestration becomes the root of a new trace. Its
trace ID is derived from the orchestration's persisted identity, not from the
base ID generator. Sampling follows the propagated trace flags and keeps
tracestate, so unsampled orchestration trees stay unsampled. The worker
evaluates root sampling again on each dispatch. Use a sampler whose decision
depends only on the trace ID, such as the SDK's built-in parent-based and
trace-ID-ratio samplers. Continue-as-new and rewind start a new orchestration
span identity. A rewind that carries a parent trace context reparents the
orchestration under that context.

## SDK parity

This table compares tracing in the application process of each Durable Task
SDK:
[durabletask-dotnet](https://github.com/microsoft/durabletask-dotnet/blob/main/src/Shared/Grpc/Tracing/TraceHelper.cs)
(gRPC),
[durabletask-python](https://github.com/microsoft/durabletask-python/blob/main/durabletask/internal/tracing.py),
and [durabletask-js](https://github.com/microsoft/durabletask-js)
(`@microsoft/durabletask-js`). It doesn't cover Azure Functions host telemetry.

| Feature | Go | .NET gRPC | Python | JavaScript |
| --- | --- | --- | --- | --- |
| Enabled by default | No. Opt in with an injected provider | Yes. `ActivitySource` is always on; spans are exported when the app adds the source | Yes, with the global provider. Opt out with `emit_trace_spans=False` | Yes, with the global `@opentelemetry/api` provider |
| Instrumentation scope | `Microsoft.DurableTask` | `Microsoft.DurableTask` | `durabletask` | `Microsoft.DurableTask` |
| Client create orchestration and raise event | PRODUCER | PRODUCER | PRODUCER | PRODUCER |
| Orchestration span | One logical SERVER span per execution, with persisted ID and start time | One logical SERVER span per execution, with persisted ID and start time | One logical SERVER span per execution, with persisted ID and start time | One SERVER span per dispatch that starts at the dispatch, with `durabletask.task.replay_span_id` and `replay_start_time` attributes |
| Orchestration without caller context | New root trace | No orchestration span | New root trace | Uses the active context, or a new root trace if there isn't one |
| Export timing | After the backend accepts the completion | Before the completion is accepted | Before the completion is accepted | Before the completion is accepted |
| Activity and sub-orchestration scheduling and execution | CLIENT and SERVER | CLIENT and SERVER | CLIENT and SERVER | CLIENT and SERVER. The CLIENT span doesn't reuse the propagated span ID, so the SERVER span's parent is never exported |
| Durable timers | `orchestration:<name>:timer` INTERNAL | `orchestration:<name>:timer` INTERNAL | `orchestration:<name>:timer` INTERNAL | `orchestration:<name>:timer` INTERNAL |
| Orchestrator sends event | PRODUCER | PRODUCER | PRODUCER | PRODUCER |
| Event receipt | No CONSUMER span | No CONSUMER span | No CONSUMER span | No CONSUMER span |
| Orchestrator calls or signals entity | `entity:<name>:<op>` CLIENT or PRODUCER | No spans | No spans | `call_entity:<op>` CLIENT, or `signal_entity:<op>` PRODUCER with zero duration |
| Entity operation execution | `entity:<name>:<op>` SERVER for calls, CONSUMER for signals | No spans | `entity:<name>:<op>` SERVER for calls and signals | No spans |
| Client signals entity | No span. Sampled caller context is propagated | No span. Caller context is propagated | No span | `signal_entity:<op>` PRODUCER |
| Entity locks | No spans | No spans | No spans | `entity:lock_request` INTERNAL |
| Success status | Unset | Unset | Unset | OK |
| Span reconstruction | Public ID-generator and sampler wrappers | `Activity` reconstruction helpers | Deferred spans through OpenTelemetry SDK internals | Not applicable, spans are per dispatch |

Go's entity span names, kinds, and `durabletask.task.operation` values follow
DurableTask.Core, which also emits the full caller and execution set.
Go also adds the `durabletask.entity.operation` attribute that the Python and
JavaScript SDKs use.

## Limitations

- **Telemetry is best effort in both directions, not exactly once.**
  - Retried or redelivered work can export the same spans again.
  - If the backend accepts a completion but the reply is lost, and the retry
    reports the work item gone, that completion's spans are lost.
  - Abandoned orchestration dispatches export no orchestration telemetry.
  - Live activity spans describe attempts, even when the result isn't accepted.
- **Events carry no trace context.** Receipt emits no CONSUMER span, and a send
  doesn't reparent or link the receiving orchestration.
- **V2 entity operations carry no parent trace context.** Entity execution spans
  are independent roots that the provider's root sampler samples. They don't
  form a joined orchestration and entity tree, and the SDK adds no synthetic
  parents or links.
- **Entity-started work doesn't join the trace on DTS.** Signals and
  orchestration starts from an entity carry the operation's trace context on
  the action, but DTS doesn't currently forward it.
- **No PRODUCER spans for entity-started work.** The SDK doesn't emit
  DurableTask.Core's entity-to-entity signal or orchestration-start PRODUCER
  spans (`StartActivityForEntityStartingAnOrchestration`).
- **Span reconstruction depends on the backend.** The backend must return the
  persisted orchestration trace context on later dispatches.
