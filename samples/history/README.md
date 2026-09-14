# Orchestration history

This sample runs a bounded workflow and reads the service's actual orchestration
history through the public buffered and streaming APIs. It does not construct
internal protocol events or use hand-built history as a substitute.

## Run

```bash
export DTS_CONNECTION_STRING='Endpoint=http://127.0.0.1:8080;TaskHub=default;Authentication=None'
go run ./samples/history
```

## What it proves

- The completed execution ID is captured from metadata and used to pin history
  reads to one execution.
- Buffered history preserves meaningful event order and typed payload readers
  decode started input, activity input/result, and completed output.
- Streaming history returns the same event identity and order as the buffered
  snapshot.
- Event and byte caps fail with `api.ErrHistoryLimitExceeded`.

`SAMPLE_OK history` is printed only after the real history checks and exact-ID
cleanup succeed. Some emulator builds may not support every history surface; an
unsupported or inconsistent response is reported as a failed run, not fake
coverage.
