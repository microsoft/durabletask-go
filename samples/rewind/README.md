# Rewind failed work

This sample repairs a failed activity and rewinds its orchestration after the
dependency becomes available.

## Run

Use a DTS service that supports rewind:

```bash
export DTS_CONNECTION_STRING='Endpoint=http://127.0.0.1:8080;TaskHub=default;Authentication=None'
go run ./samples/rewind
```

## What it proves

- The original execution reaches `FAILED` before rewind is requested.
- `RewindInstance` enqueues recovery rather than waiting for it to finish.
- Recovery creates a new execution ID and produces the expected
  `"kept:recovered"` output.
- The successful activity runs once, while only the failed activity runs again.
- Cleanup purges only the sample-owned instance.

`WaitForOrchestrationCompletion` can initially return the previous failed
execution, so the sample polls metadata until it observes the replacement
execution complete. `SAMPLE_OK rewind` is printed only after the recovery
assertions, exact-ID cleanup, and worker shutdown succeed.

Rewind follows the pinned Python SDK algorithm. Activity retry-policy histories
that contain retry timers are not supported because they can replay
nondeterministically. Rewind also cannot repair application code or a dependency
that is still failing.
