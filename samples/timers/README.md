# Timers sample

## Features

- Schedules an orchestration to start in the future.
- Uses one logical durable timer with a five-second deadline.
- Configures a two-second maximum physical timer interval so the long timer is
  split into multiple history records.
- Demonstrates replay-stable `CurrentTimeUtc` and `NewGuid` values by recording
  them through an activity and validating the final payload.
- Reads orchestration history to verify the physical timer split.

## Prerequisites

Start a Durable Task Scheduler endpoint or emulator and set:

```sh
export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
```

## Run

```sh
go run ./samples/timers
```

## Expected result

The orchestration starts at the scheduled time, waits for one five-second
logical timer, and history contains three created/fired physical timer chunks
ending at the single logical deadline. On success it prints:

```text
SAMPLE_OK timers
```

## Cleanup

The sample terminates if needed and recursively purges only its generated
`sample-timers-*` orchestration instance before shutting down.

## Noninteractive command

```sh
DTS_CONNECTION_STRING="$DTS_CONNECTION_STRING" go run ./samples/timers
```
