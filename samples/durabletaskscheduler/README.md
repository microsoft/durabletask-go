# Durable Task Scheduler sample

## Features

- Connects a Durable Task Scheduler client and worker using `DTS_CONNECTION_STRING`.
- Schedules an orchestration with an explicit sample-owned instance ID.
- Passes typed orchestration input and validates typed activity output.
- Cleans up only the orchestration instance created by this run.

## Prerequisites

Start a Durable Task Scheduler endpoint or emulator and set:

```sh
export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
```

## Run

```sh
go run ./samples/durabletaskscheduler
```

## Expected result

The orchestration calls the `SayHello` activity for Tokyo, London, and Seattle,
decodes the typed output, and verifies the exact greetings. On success it prints:

```text
SAMPLE_OK durabletaskscheduler
```

## Cleanup

The sample terminates if needed and recursively purges only its generated
`sample-durabletaskscheduler-*` orchestration instance before shutting down.

## Noninteractive command

```sh
DTS_CONNECTION_STRING="$DTS_CONNECTION_STRING" go run ./samples/durabletaskscheduler
```
