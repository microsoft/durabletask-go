# Versioning sample

This sample demonstrates Durable Task version-aware dispatch:

- two worker registries for versions `1.0` and `2.0`
- client default version routing
- explicit orchestration name/version routing
- activity version inheritance from orchestrators
- controlled explicit unversioned fallback
- strict version mismatch as a bounded negative case
- `ContinueAsNew` migration from version `1.0` to `2.0`
- `CurrentOrOlder` routing where a version `2.0` worker accepts version `1.0` work

Worker handling is verified from activity outputs and recorded orchestration/activity versions, not from orchestrator-side global state or I/O.

## Prerequisites

Set `DTS_CONNECTION_STRING` to a task hub you can use for samples:

```bash
export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
```

## Run

```bash
go run ./samples/versioning
```

## Expected result

The program starts real versioned orchestrations, validates outputs and metadata for every routing scenario, asserts the strict mismatch failure, cleans up owned instances, and prints:

```text
SAMPLE_OK versioning
```

only after validation and cleanup both succeed.

## Cleanup

Every scenario creates a unique owned instance ID with `dtssample.NewInstanceID`, records it before scheduling, stops all workers gracefully, and purges owned orchestration instances before shutting down the main app.
