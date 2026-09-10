# Durable entities

This sample exercises entity state and routing against a real Durable Task
Scheduler task hub. It reuses one instructional flow rather than generating
separate toy programs for every API option.

## Run

```bash
export DTS_CONNECTION_STRING='Endpoint=http://127.0.0.1:8080;TaskHub=default;Authentication=None'
go run ./samples/entity
```

## What it proves

- Raw counter entity state, signals, scheduled signals, and entity queries.
- Entity-to-entity signals and orchestrator-to-entity calls/signals.
- Reflected entity registration with optional inputs.
- Factory registration with captured dependencies, initialized state, and
  thread-safe batch cleanup.
- Persistent bank-account objects with a locked transfer and balance invariant.
- Entity-started orchestration with an explicit, sample-owned instance ID.

Every scenario asserts the resulting state or orchestration output. The sample
prints `SAMPLE_OK entity` only after those assertions and cleanup succeed.

## Cleanup

All orchestration IDs are declared before scheduling and then terminated/purged
by exact ID. Entity state is deleted by signaling `delete` to the sample-owned
entity keys and waiting until their state is gone. The sample never calls
hub-wide entity maintenance or deletes a task hub.
