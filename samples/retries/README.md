# Retries and durable failures

This sample demonstrates retry behavior with deterministic, activity-owned
failure injection. It does not use randomness, wall-clock time, or atomics inside
orchestrator or retry-handler code.

## Run

```bash
export DTS_CONNECTION_STRING='Endpoint=http://127.0.0.1:8080;TaskHub=default;Authentication=None'
go run ./samples/retries
```

## What it proves

- A transient activity failure is retried and recovers after the known first two
  failures.
- The retry handler uses `task.RetryContext` and durable failure details to stop
  retrying a typed permanent failure.
- A custom non-retriable error bypasses retry and preserves custom properties in
  the failure chain.

Successful paths use `api.WithFetchPayloads(true)` and `dtssample.RequireCompleted`.
Intentional failure paths assert `FAILED` status, concrete durable error types,
properties, and attempt counts. `SAMPLE_OK retries` appears only after all
assertions and exact-ID cleanup succeed.
