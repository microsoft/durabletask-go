# Sub-orchestrations sample

This sample demonstrates parent/child orchestration patterns in Durable Task Scheduler:

- a parent orchestration scheduling multiple child orchestrations
- explicit child instance IDs derived from the owned parent ID
- combined child results returned to the parent
- user tags and explicit context fields propagated to child orchestration history
- child failure propagation to the parent
- recursive cleanup of the owned parent/child instance tree

## Prerequisites

Set `DTS_CONNECTION_STRING` to a task hub you can use for samples, for example a local emulator hub:

```bash
export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
```

## Run

```bash
go run ./samples/suborchestrations
```

## Expected result

The program starts one successful parent and one controlled-failure parent. It validates child outputs, parent and child histories, propagated context fields, and the expected failed-child history event. It prints:

```text
SAMPLE_OK suborchestrations
```

only after validation and cleanup both succeed.

The child reads its durable context fields from its orchestration context.
Values needed by an activity are passed explicitly as activity input; this sample
does not assume the activity's worker-local Go context inherits those fields.

## Cleanup

The sample tracks the parent and planned child instance IDs before scheduling. Before shutting down its worker, it calls `dtssample.Cleanup` with the owned IDs, using recursive purge for child orchestration state. It does not create schedules, task hubs, or other shared resources.
