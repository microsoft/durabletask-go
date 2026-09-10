# Hub maintenance

> **Warning:** This sample performs hub-wide cleanup and can delete data outside
> its own instances. Use a test hub, not production or valuable application data.

## Run

Use a DTS service that actually implements the operations below:

```bash
export DTS_CONNECTION_STRING='Endpoint=https://<scheduler-host>;TaskHub=<hub>;Authentication=DefaultAzure'
go run ./samples/serviceoperations
```

No special hub name or additional opt-in variable is required. The sample does
not provision Azure resources or change permissions.

## What it proves

- **Filtered purge:** three completed subjects must become unreadable.
- **Entity maintenance:** empty state must be absent afterward and a nonempty
  entity must remain unchanged. If a transient record exists before cleanup,
  the sample also requires a reported removal and verifies that record disappears.

Checks are bounded and failures are reported by operation. Cleanup of the
sample's instances uses their generated IDs; the demonstrated filter and entity
maintenance APIs act across the selected hub.

`SAMPLE_OK serviceoperations` is printed only if every operation and cleanup
succeeds. Unsupported APIs and success responses with no observable state change
produce a nonzero exit, not coverage. The current DTS emulator has known limitations;
see the [feature matrix](../../durabletaskscheduler/README.md#feature-matrix).
Some services eagerly remove empty entity records. The output explicitly reports
that case: it validates idempotency and live-state preservation, not a removal
that did not happen. It never claims a nonzero removal count for an empty set.

Rewind, skip-graceful termination, and SDK task-hub create/delete are intentionally
not supported. Provision and remove task hubs through Azure's control plane or
CLI, not SDK lifecycle RPCs.
