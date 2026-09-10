# Management sample

This sample demonstrates safe, ID-scoped Durable Task Scheduler management operations:

- start and wait for completion
- typed custom status progress
- `QueryInstances` with tags, ID prefix, payload fetching, and continuation pages
- `ListInstanceIDs` for completed orchestration IDs
- suspend and resume
- terminate with a supplied output
- restart and validate the restarted payload
- status-based instance-ID reuse
- single and batch ID-scoped purge

The sample intentionally does **not** use filtered purge or clean-hub operations.

## Prerequisites

Set `DTS_CONNECTION_STRING` to a task hub you can use for samples:

```bash
export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
```

`ListInstanceIDs` and `RestartInstance` must be implemented by the target service. If a target reports success but omits owned IDs or drops restart payloads, the sample exits with a precise error instead of treating the limitation as a pass.

## Run

```bash
go run ./samples/management
```

## Expected result

The program validates each management scenario and prints:

```text
SAMPLE_OK management
```

only after every scenario and cleanup succeeds.

## Cleanup

Every orchestration uses a `sample-management-*` unique owned ID that is recorded before scheduling. Cleanup uses `dtssample.Cleanup` before worker shutdown, and the purge scenario uses only explicit single/batch ID-scoped purge requests.
