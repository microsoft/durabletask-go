# Distributed tracing sample

Demonstrates opt-in, replay-aware Durable Task Scheduler tracing using the
application's OpenTelemetry provider, OTLP/HTTP, and a real collector.

## Features

- Starts a caller span and schedules an orchestration under that trace.
- Uses a local loopback HTTP target instead of an external dependency.
- Enables worker spans with `client.WithWorkerTracerProvider` and client
  PRODUCER spans with `client.WithTracerProvider`.
- Exports orchestration, activity scheduling/execution, timer, and application
  spans over OTLP/HTTP to a collector.
- Verifies that the collector file exporter received these spans, with their
  expected span kinds, under the caller's trace ID.
- Reads DTS history and verifies persisted trace context shares the caller trace.

Tracing remains disabled unless explicitly enabled. The sample configures its
provider with **both** `tracing.WithIDGenerator(nil)` and
`tracing.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample()))`. These wrappers
let the worker reconstruct completed spans with persisted identities and make
consistent sampling decisions across replay; do not replace them with the raw
SDK options. The same provider is also installed globally for HTTP instrumentation.

The exported tree is caller -> create-orchestration PRODUCER -> orchestration
SERVER -> activity CLIENT -> activity SERVER -> application HTTP spans, with a
timer INTERNAL span under the orchestration. The worker exports the orchestration
when it finishes and durable scheduling/timer spans after accepted completions,
not on every replay. The application flushes the provider after worker shutdown
and owns its final shutdown.

DTS persists and forwards trace context but doesn't emit durable spans, so these
SDK spans are how durable work reaches your telemetry backend. They use the
`Microsoft.DurableTask` instrumentation scope and the provider's resource. See
the [tracing guide](../../tracing/README.md) for span semantics, SDK parity,
and delivery and propagation limitations.

The collector in this sample writes spans to a file so the sample can verify
them. To view the spans, add an exporter to the collector's `traces` pipeline
for Jaeger, Azure Monitor, or any other OTLP-compatible backend. You can also
point `OTEL_EXPORTER_OTLP_ENDPOINT` straight at a backend that accepts
OTLP/HTTP, but then the sample's receipt check fails because it reads the
collector's file.

## Prerequisites

- `DTS_CONNECTION_STRING` for DTS.
- A real OpenTelemetry Collector reachable through `OTEL_EXPORTER_OTLP_ENDPOINT`
  or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`.
- `OTEL_CAPTURE_FILE`, mounted/readable by both the collector and this process.

Collector config for local/CI runs is provided in
`otel-collector-config.yaml`. Use
`otel/opentelemetry-collector-contrib:0.160.0`, which includes the `file`
exporter. The trace file may live outside the repository, but the same host file
must be mounted into the collector and exposed to the sample through
`OTEL_CAPTURE_FILE`.

Collector contract: inside the collector container, set
`OTEL_CAPTURE_FILE=/output/traces.json`. In the sample process, set
`OTEL_CAPTURE_FILE` to the corresponding host path. The sample reads that host
file back and requires each expected span name to belong to the current trace ID,
so stale data from earlier runs is ignored.

## Run

Start a collector in one shell:

```bash
cd samples/distributedtracing
export HOST_OTEL_CAPTURE_FILE="${HOST_OTEL_CAPTURE_FILE:-$PWD/otel-traces.json}"
docker run --rm \
  -p 4317:4317 -p 4318:4318 \
  -v "$PWD/otel-collector-config.yaml:/etc/otelcol/config.yaml:ro" \
  -v "$(dirname "$HOST_OTEL_CAPTURE_FILE"):/output" \
  -e OTEL_CAPTURE_FILE="/output/$(basename "$HOST_OTEL_CAPTURE_FILE")" \
  otel/opentelemetry-collector-contrib:0.160.0 \
  --config=/etc/otelcol/config.yaml
```

Run the sample from another shell:

```bash
cd samples/distributedtracing
export OTEL_CAPTURE_FILE="${HOST_OTEL_CAPTURE_FILE:-$PWD/otel-traces.json}"
export OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4318"
go run .
```

## Expected outcome

The process exits 0 and prints:

```text
SAMPLE_OK distributedtracing
```

## Cleanup

The orchestration is recursively terminated/purged before worker shutdown. The
sample does not create cloud telemetry resources.

## Required validation variants

Integration validation should start the real OTLP collector with this config and
execute `go run .`. Do not clear an open collector file; unique trace IDs
disambiguate repeated runs.
