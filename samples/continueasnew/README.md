# Continue-as-new sample

This sample demonstrates three bounded ContinueAsNew scenarios:

- `checkpoint`: explicit checkpointed generations with external-event carryover
- `history-limit`: `MaxHistoryEvents` and `OnHistoryLimitExceeded` compacting a running orchestration without dropping carried business events
- `event-budget`: `MaxEventsPerTurn` consuming a queued batch across bounded service turns without losing or duplicating work

## Prerequisites

Set `DTS_CONNECTION_STRING` to a task hub you can use for samples:

```bash
export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
```

## Run

Run every scenario:

```bash
go run ./samples/continueasnew
```

Run one or more named scenarios:

```bash
go run ./samples/continueasnew checkpoint
go run ./samples/continueasnew history-limit event-budget
```

## Expected result

Each scenario starts real orchestrations, raises real external events, validates fetched outputs/history, and cleans up its own instance IDs. The program prints:

```text
SAMPLE_OK continueasnew
```

only after selected scenarios validate and cleanup succeeds.

The event-budget scenario queues its input before starting the worker and checks
the public history metrics: every turn processes at most one work-item event,
and all queued business items must still be returned.

## Cleanup

Every scenario creates a unique owned instance ID with `dtssample.NewInstanceID`, records it before scheduling, calls `dtssample.Cleanup` before worker shutdown, and does not purge by broad filters.
