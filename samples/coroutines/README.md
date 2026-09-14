# Coroutines sample

## Features

- Starts bounded orchestration coroutines with `ctx.Go`.
- Coordinates coroutine completion with `ctx.NewWaitGroup`.
- Uses `Select` on a typed event channel and a durable timer from a child
  cancellation scope.
- Cancels the losing timer when the approval event wins.
- Validates the returned doubled values and selected event outcome.

## Prerequisites

Start a Durable Task Scheduler endpoint or emulator and set:

```sh
export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
```

## Run

```sh
go run ./samples/coroutines
```

## Expected result

The host raises the `Approval` event after the orchestration starts. The event
wins over the durable timer, the timer scope is canceled, three activity-backed
coroutines return `4`, `8`, and `12`, and the sum is verified as `24`. On success
it prints:

```text
SAMPLE_OK coroutines
```

## Cleanup

The sample terminates if needed and recursively purges only its generated
`sample-coroutines-*` orchestration instance before shutting down.

## Noninteractive command

```sh
DTS_CONNECTION_STRING="$DTS_CONNECTION_STRING" go run ./samples/coroutines
```
