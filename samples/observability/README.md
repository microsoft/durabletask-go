# Observability hooks

This sample uses the SDK's standard-library observability surfaces: tags,
immutable context fields, `slog` logging, and retry/history metric hooks. It
does not add a telemetry framework or an OpenTelemetry collector; DTS-owned
durable spans are covered elsewhere.

## Run

```bash
export DTS_CONNECTION_STRING='Endpoint=http://127.0.0.1:8080;TaskHub=default;Authentication=None'
go run ./samples/observability
```

## What it proves

- Root tags are visible in metadata and history.
- Immutable root context fields are persisted and visible in orchestration
  context/history. Activity contexts receive fields carried by the activity work
  item plus worker-local fields; current DTS emulator activity work items expose
  the worker-local field used here.
- `ctx.Logger()` suppresses replayed orchestrator output while activity logging
  keeps task identity fields.
- Retry and history metric hooks receive bounded, thread-safe signals without
  depending on exact batch-turn counts.

`SAMPLE_OK observability` is printed only after field values, log records,
metrics, orchestration output, and exact-ID cleanup are verified.
