# Isolated hub maintenance

**This sample performs hub-wide purge and entity maintenance. Never point it at
a shared, production, or otherwise valuable hub.** Provision a new, disposable
hub whose name starts with `sample-`, and remove that resource through Azure's
control plane after the experiment.
The sample does not provision Azure resources or change permissions.

## Run

Use a DTS service that actually implements the operations below:

```bash
export DTS_CONNECTION_STRING='Endpoint=https://<scheduler-host>;TaskHub=sample-<unique-disposable-hub>;Authentication=DefaultAzure'
export DTS_SAMPLE_ALLOW_HUB_MAINTENANCE=1
go run ./samples/serviceoperations
```

Both the explicit acknowledgement and the disposable-name prefix are required.
Filters and entity maintenance act on the whole hub, so unique instance IDs alone
are not adequate protection.

## What it proves

- **Filtered purge:** three completed subjects must become unreadable.
- **Entity maintenance:** empty state must be absent afterward and a nonempty
  entity must remain unchanged. If a transient record exists before cleanup,
  the sample also requires a reported removal and verifies that record disappears.

Checks are bounded and failures are reported by operation. Ordinary instance
cleanup uses only the generated IDs; hub-wide operations are intentionally
restricted to the disposable hub.

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
