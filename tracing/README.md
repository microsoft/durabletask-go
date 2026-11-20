# Replay-aware OpenTelemetry tracing

Durable Task clients and workers **only propagate W3C context by default**.
Opt-in tracing exports durable operation spans through the application's
OpenTelemetry provider, alongside its ordinary application spans. It does not
require a scheduler-specific telemetry exporter.

## Enable tracing

Configure the worker's OpenTelemetry SDK provider with **both** wrappers:

```go
provider := sdktrace.NewTracerProvider(
    tracing.WithIDGenerator(nil),
    tracing.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
    sdktrace.WithBatcher(exporter),
    sdktrace.WithResource(applicationResource),
)
worker, err := durabletaskscheduler.NewWorker(options, registry, logger,
    client.WithWorkerTracerProvider(provider))
managementClient, err := durabletaskscheduler.NewClient(ctx, options, logger,
    client.WithTracerProvider(provider))
```

Check both constructor errors. `client.WithTracerProvider` can use any tracer
provider, but `client.WithWorkerTracerProvider` rejects a provider missing either
wrapper with `tracing.ErrUnsupportedTracerProvider`. The ID generator honors
persisted span identities during reconstruction; ordinary application spans
still use its base generator. The sampler supports sampling queries without
creating live spans. Pass a custom generator or sampler into these wrappers
instead of replacing them with `sdktrace.WithIDGenerator` or
`sdktrace.WithSampler`.

The application owns resources, processors, exporter, flushing, and provider
shutdown. Stop the worker before flushing/shutting down the provider.
`otel.SetTracerProvider(provider)` is optional for Durable Task, but useful for
application instrumentation that uses the global provider. See the
[distributed tracing sample](../samples/distributedtracing) for a complete
OTLP/HTTP collector setup and collector-receipt verification.

## Spans and lifecycle

All SDK spans use the `durabletask` instrumentation scope. Task names use
`<type>:<name>@(<version>)`, omitting the version suffix when unversioned.
Entity names use the lowercase form from `EntityID.String`.

| Operation | Span name | Kind and parent |
| --- | --- | --- |
| Client schedules orchestration | `create_orchestration:<name>` | PRODUCER under caller |
| Orchestration execution | `orchestration:<name>` | SERVER under create span, or propagated caller when client tracing is disabled |
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

Orchestration spans are reconstructed with persisted identity and start time
when an execution completes, fails, terminates, or continues as new. Activities
are live spans, so HTTP/database/application spans can nest under them.
Scheduling and timer spans are reconstructed from history when a dispatch
consuming the completion is accepted. Orchestrator event/entity signals export
after their sending dispatch is accepted. Entity executions export after an
accepted batch with operation-result timestamps and error status.

Replayed history emits no new spans. Orchestration and entity execution spans
are not live inside their handlers; starting an application span from an
orchestrator does not automatically make it replay-safe.

Attributes include `durabletask.type`, task name/version, instance ID, and
operation-specific status, execution/task IDs, or timer fire time. Entity spans
follow **DurableTask.Core** names and kinds, including
`durabletask.task.operation=signal_entity` or `call_entity`, rather than
introducing a new Go-only convention. Failures are marked with error status.

Without caller context, an orchestration starts a trace derived from its
persisted identity. Sampling follows propagated trace flags, preserves
tracestate, and keeps unsampled orchestration trees unsampled. Root sampling is
queried again on each dispatch: use a sampler with a stable decision based on
the trace ID, such as the built-in parent-based/trace-ID-ratio samplers.
Continue-as-new and rewind start a new orchestration span identity.

## SDK parity

This comparison concerns application-process tracing in
[durabletask-python](https://github.com/microsoft/durabletask-python/blob/main/durabletask/internal/tracing.py)
and the
[durabletask-dotnet gRPC SDK](https://github.com/microsoft/durabletask-dotnet/blob/main/src/Shared/Grpc/Tracing/TraceHelper.cs),
not Azure Functions host or scheduler-service telemetry.

| Feature | Go (enabled) | Python | .NET gRPC |
| --- | --- | --- | --- |
| Application provider/exporter integration | OpenTelemetry SDK provider; OTLP or another exporter | OpenTelemetry provider | `ActivitySource` via application OpenTelemetry |
| Client create orchestration / raise event | PRODUCER | PRODUCER | PRODUCER |
| Replay-aware orchestration execution | Reconstructed SERVER | Deferred SERVER | Reconstructed SERVER |
| Activity and sub-orchestration scheduling/execution | CLIENT / SERVER | CLIENT / SERVER | CLIENT / SERVER |
| Durable timers | INTERNAL | INTERNAL | INTERNAL |
| Orchestrator event send | PRODUCER | PRODUCER | PRODUCER |
| Event receipt consumer / sender-to-receiver trace join | Not available | Not available | Not available |
| Entity signal/call and operation execution | DurableTask.Core PRODUCER/CLIENT and CONSUMER/SERVER | Not implemented | Not implemented |
| Client entity signal | No span; sampled caller context propagated | None | No span; caller context propagated |
| Entity lock acquisition/release | No spans | No spans | No spans |
| Persisted span reconstruction integration | Public SDK ID-generator/sampler wrappers | Deferred spans via OTel SDK internals | `Activity` reconstruction helpers |

Entity tracing matches the
[DurableTask.Core reference](https://github.com/Azure/durabletask/tree/main/src/DurableTask.Core/Tracing);
it is beyond the current Python and .NET gRPC tracing surfaces, not a claim that
those SDKs emit entity spans.

## Limitations

- Event messages have no trace context. Receipt emits no CONSUMER span, and
  sending an event does not reparent or link the receiving orchestration.
- V2 entity operations have no propagated parent trace context. Execution spans
  are independent roots sampled by the provider's root policy, not a joined
  orchestration/entity tree. There are no synthetic request-ID links.
- Telemetry is **not exactly-once**. Abandoned dispatches emit no reconstructed
  orchestration telemetry, but a successfully accepted completion retried by the
  backend can export again. Live activity spans describe attempts, even when a
  result is not accepted.
- Reconstruction requires the backend to return persisted orchestration trace
  identity on subsequent dispatches. Service-side telemetry remains separate
  and may overlap in operation names with enabled application-process spans.

## End-to-end coverage

`tests/durabletaskscheduler/dts_tracing_enabled_test.go` uses an explicit provider
and an in-memory exporter to assert client/orchestration/activity/sub-orchestration/
timer/event parentage and independent entity roots, including entity failures
and a client signal that emits no client-side span.
The existing disabled-mode tests continue to assert propagation without SDK span
emission.

Run these against a DTS emulator with `DTS_EMULATOR_ENDPOINT` or a configured
`DTS_CONNECTION_STRING`. They skip when neither is set. PR validation supplies
the pinned DTS emulator endpoint.
