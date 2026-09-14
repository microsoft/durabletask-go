# Export history sample

Runs real source orchestrations, creates Durable Task Scheduler export-history
jobs, downloads the emitted Azure Blob objects, and verifies that each object
contains the expected source execution and durable events.

## Features

- Batch and continuous export jobs.
- Gzip-compressed JSONL and plain JSON history formats.
- `CreateJob`, `GetJob`, `ListJobs`, `Describe`, `Delete`, and batch recreate.
- Continuous scenarios complete an additional source orchestration after the
  initial export and verify the same active job exports it on the next service
  poll.
- Bounded wait for `ListInstanceIDs`; an index omission is a hard failure.
- Prefix-scoped blob cleanup and retained-generation cleanup.

## Prerequisites

- `DTS_CONNECTION_STRING` for an isolated hub.
- `DTS_SAMPLE_ISOLATED_TASKHUB=1`, explicitly acknowledging that this hub is
  isolated. A name prefix alone is not proof that a hub is safe to export.
- `AZURE_STORAGE_CONNECTION_STRING`; `EXPORT_STORAGE_CONNECTION_STRING` remains
  supported for compatibility.
- `DTS_SAMPLE_ALLOW_INSECURE_STORAGE=1` for loopback Azurite HTTP endpoints.

The export window cannot filter by instance ID prefix, so do not run this
sample against a shared cloud hub.

## Run

```bash
# from the repository root
go run ./samples/exporthistory -scenario=batch-jsonl
go run ./samples/exporthistory -scenario=batch-json
go run ./samples/exporthistory -scenario=continuous-jsonl
go run ./samples/exporthistory -scenario=continuous-json
```

`-scenario=all` runs all advertised variants and may take several minutes. The
two continuous variants intentionally wait for the service's default
approximately one-minute continuous-export poll after a late source completion,
so use a 6-8 minute budget for the all-variants command.

## Expected outcome

Each command exits 0 and the last line is:

```text
SAMPLE_OK exporthistory
```

## Cleanup

The sample deletes each export job, purges retained batch export generations,
recursively purges source orchestrations before worker shutdown, and deletes its
owned container. If `EXPORT_CONTAINER` is provided, only
run-owned `samples/exporthistory/<job-id>/` prefixes are deleted and the
container remains.

## Required validation variants

Integration validation should run the four individual commands above after DTS
and Azurite are ready.
