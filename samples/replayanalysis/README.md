# Replay analysis sample

Shows replay-safe orchestration patterns and how to verify them with
`cmd/orchestratorvet`.

## Features

- Good runnable orchestration uses deterministic counterparts:
  `ctx.CurrentTimeUtc`, `ctx.NewGuid`, `ctx.CreateTimer`, and activities for
  side effects.
- Intentional bad fixtures live under `testdata/bad`, outside normal
  `go test ./...` package discovery.
- Integration validation can build the existing vet tool and run positive/negative checks
  without adding a module or root dependency.

## Prerequisites

- `DTS_CONNECTION_STRING` for DTS.
- Existing `cmd/orchestratorvet` sources.

## Run

```bash
# from the repository root
go run ./samples/replayanalysis
```

Build and run the analyzer:

```bash
# from the repository root
(cd cmd/orchestratorvet && go build -o ../../samples/replayanalysis/orchestratorvet-local .)
go vet -vettool="$PWD/samples/replayanalysis/orchestratorvet-local" ./samples/replayanalysis
! go vet -vettool="$PWD/samples/replayanalysis/orchestratorvet-local" ./samples/replayanalysis/testdata/bad
rm -f samples/replayanalysis/orchestratorvet-local
```

The negative command must report hazards such as wall-clock time, HTTP I/O,
goroutines/channels/select, random UUIDs, and stdout logging.

## Suggested fixes

To preview suggested fixes, copy the bad fixture to a scratch file inside the
workspace and run `go vet -fix` against that copy. Do not modify repository
fixtures during integration validation.

## Expected outcome

The runnable sample exits 0 and prints:

```text
SAMPLE_OK replayanalysis
```

The good vet command exits 0. The bad fixture vet command exits nonzero with the
expected diagnostics.

## Cleanup

The sample recursively terminates/purges its orchestration before worker
shutdown. It creates no external resources.

## Required validation variants

Integration validation should run the `go run`, positive vet, and negative vet
commands above.
