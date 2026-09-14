# Distributed tracing sample

Demonstrates application trace propagation into Durable Task Scheduler using
OTLP/HTTP and a real OpenTelemetry collector.

## Features

- Starts a caller span and schedules an orchestration under that trace.
- Uses a local loopback HTTP target instead of an external dependency.
- Exports application spans over OTLP/HTTP to a collector.
- Verifies the collector file exporter received the caller and local HTTP spans
  under the same trace ID.
- Reads DTS history and verifies persisted trace context shares the caller trace.

DTS orchestration/activity/timer spans are service-owned. This sample validates
the trace context recorded in history and does not claim that the local worker
duplicates service spans.

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
