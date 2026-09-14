# Worker lifecycle

This sample focuses on worker lifecycle behavior using the public DTS client and
worker APIs. It does not modify emulator/cloud networking or fake service
faults.

## Run

```bash
export DTS_CONNECTION_STRING='Endpoint=http://127.0.0.1:8080;TaskHub=default;Authentication=None'
go run ./samples/worker
```

## What it proves

- `Start` runs a worker in the background.
- Activity concurrency stays within the configured bound.
- `Shutdown` is started while an accepted activity is blocked; the sample proves
  shutdown does not finish until that in-flight activity is released, then waits
  for bounded graceful drain.
- A new owned worker can restart processing after a prior worker stops, and
  the orchestration continuation after the drained activity completes after
  restart.
- `Run` blocks until its context is canceled and exits cleanly.
- The compatibility listener on a management client demonstrates the borrowed
  connection contract; new applications should prefer owned workers.

Activities are at-least-once, so the drain scenario uses an idempotent output
and does not assert an exact delivery count. The recovery scenario is limited to
bounded worker process restart ordering. Controlled service restarts or
proxy fault injection are needed to validate network-fault recovery. `SAMPLE_OK
worker` appears only after all output assertions, shutdown checks, deadlines,
and exact-ID cleanup succeed.
