# Scheduled tasks sample

This sample demonstrates the Go scheduled-task APIs for Durable Task Scheduler:

- registering system scheduled-task orchestrators/entities with `RegisterScheduledTasks`
- starting a worker with `WithScheduledTasks`
- creating a unique schedule ID
- observing real target orchestration executions
- `Get`, `List`, `Update`, `Pause`, `Resume`, and `Delete`
- propagated tags and context fields
- bounded retry configuration using deterministic activity failure injection

## Prerequisites

Set `DTS_CONNECTION_STRING` to a task hub you can use for samples:

```bash
export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
```

The target must support the Go scheduled-task hub protocol. The sample does not use Azure CLI, resource creation, fixed target instance IDs, or retry plus fixed instance ID combinations.

## Run

```bash
go run ./samples/scheduledtasks
```

## Expected result

The program creates a schedule, waits for an initial target execution that succeeds after one scheduled retry, pauses and updates the schedule, verifies that the updated target does not run while paused, resumes it, observes an updated target execution, deletes the schedule, cleans up captured target instance IDs, and prints:

```text
SAMPLE_OK scheduledtasks
```

only after validation and cleanup both succeed.

Schedule context fields are read from the target orchestration's durable context,
not assumed to be present in the activity's worker-local Go context.

## Cleanup

The schedule ID is unique to the sample run. The sample deletes the schedule before worker shutdown, captures actual target orchestration IDs started by the scheduler, and purges only those owned target IDs with `dtssample.Cleanup`.
