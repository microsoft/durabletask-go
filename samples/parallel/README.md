# Parallel sample

## Features

- Uses durable timers with `WhenAny` so the winning branch is deterministic.
- Fans out a controlled set of activity calls.
- Uses `WhenAll` to wait for all activity results.
- Aggregates successful updates and one expected business failure without
  random outcomes or host I/O in the orchestrator.
- Cleans up only the orchestration instance created by this run.

## Prerequisites

Start a Durable Task Scheduler endpoint or emulator and set:

```sh
export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
```

## Run

```sh
go run ./samples/parallel
```

## Expected result

The `fast-timer` branch wins the durable race. Four device update activities run
in parallel; three are updated and `door-lock-02` returns the expected
`blocked-by-policy` business result. On success it prints:

```text
SAMPLE_OK parallel
```

## Cleanup

The sample terminates if needed and recursively purges only its generated
`sample-parallel-*` orchestration instance before shutting down.

## Noninteractive command

```sh
DTS_CONNECTION_STRING="$DTS_CONNECTION_STRING" go run ./samples/parallel
```
