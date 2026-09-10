# External events sample

## Features

- Reads the `Name` event payload from standard input asynchronously.
- Preserves the five-minute host timeout and the 30-second durable event timeout
  on the single-event orchestration.
- Demonstrates repeated typed `EventChannel` receives and verifies event order.
- Demonstrates cross-instance durable `SendEvent`.
- Runs a controlled expected timeout path that completes successfully only when
  the durable event wait times out.
- Cleans up only orchestration instances created by this run.

## Prerequisites

Start a Durable Task Scheduler endpoint or emulator and set:

```sh
export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
```

## Run

```sh
printf 'Taylor\n' | go run ./samples/externalevents
```

## Expected result

The sample verifies `Hello, Taylor!`, receives three typed checkpoint events in
order, sends a typed signal from one orchestration to another, and confirms the
expected timeout scenario. On success it prints:

```text
SAMPLE_OK externalevents
```

## Cleanup

The sample terminates if needed and recursively purges only generated
`sample-externalevents-*` orchestration instances before shutting down.

## Noninteractive command

```sh
printf 'Taylor\n' | DTS_CONNECTION_STRING="$DTS_CONNECTION_STRING" go run ./samples/externalevents
```
