# History export (preview)

`exporthistory` exports terminal orchestration histories out of a task hub and
into Azure Blob Storage. It is the Go counterpart of the .NET
`Microsoft.DurableTask.ExportHistory` preview package, but its export-job
lifecycle protocol is Go-specific. Mixed .NET or older Go export-history workers
are not supported in the same task hub.

> **Maturity: preview.** The exported API, the serialized shape of the
> `ExportJob` entity state, and the names of the system entity, orchestrators,
> and activities may change in a future release without a major version bump.
> Drain and delete export jobs before upgrading. Legacy export-job state is not
> supported or migrated.

## Model

An export job is a durable entity keyed by job ID.

| Component | Name | Role |
| --- | --- | --- |
| Entity | `ExportJob` | Owns lifecycle, configuration, checkpoint, and progress |
| Orchestrator | `ExecuteExportJobOperationOrchestrator` | Runs one entity operation on behalf of a client |
| Orchestrator | `ExportJobOrchestrator` | Lists, exports, and checkpoints; one per run generation |
| Activity | `ListTerminalInstancesActivity` | Returns one page of terminal instance IDs |
| Activity | `ExportInstanceHistoryActivity` | Exports one instance's history |

Creating a job moves the entity to `Active`, mints a run token, and reserves the
generation-specific instance ID `ExportJob-<jobId>-<runToken>` before signaling
`Run`. `Run` starts `ExportJobOrchestrator` using that stored ID. Read
`description.OrchestratorInstanceID` to identify the current generation; a job
ID alone no longer identifies an orchestration. The orchestration repeatedly
lists terminal instances matching the job's filter,
exports each instance's history in bounded parallel windows, and commits a
checkpoint back to the entity. A batch job completes when the task hub reports
no more pages; a continuous job idles for a minute and lists again.

### Progress counters

`ScannedInstances` and `ExportedInstances` are cumulative processing totals, not
counts of distinct instances or executions. This retains the Python SDK's
counter semantics. A continuous job preserves the last opaque cursor when a
terminal page has no next token. If the source returns that page again, its
entries and successful exports are counted again, even when no new instance has
completed. Retries of blob writes remain idempotent, but that does not make these
counters unique. Do not use them as exact unique-export or billing totals.

### Lifecycle

| Operation | From | To |
| --- | --- | --- |
| `Create` | `Pending`, `Failed`, `Completed` | `Active` |
| `MarkAsCompleted` | `Active` | `Completed` |
| `MarkAsFailed` | `Active` | `Failed` |

Every other transition raises an `InvalidTransitionError`, including recreating
a job that is already `Active`. Recreating a `Failed` or `Completed` job resets
its progress counters and checkpoint but preserves its original creation time.
Each successful `Create` assigns a fresh orchestration ID, so concurrent creates
are resolved entirely by the entity's lifecycle transition: only one generation
can be `Active` at a time, and creation performs no destructive cleanup. Previous generations'
orchestration histories are retained, not purged on recreation.

A page whose exports keep failing commits without a checkpoint and with the
collected failures, which implicitly moves the job to `Failed` and leaves the
cursor on the failing page so a fix can resume from it. That implicit failure
goes through the same lifecycle transition as an explicit `MarkAsFailed`.

The entity's `Delete` atomically captures its current orchestration ID, clears
the state, and returns that ID as a string (an empty string if the job is absent).
The client then terminates and purges only that captured generation. State
deletion and orchestration cleanup are not atomic, but a `Create` between them
is safe: cleanup never looks up or targets the replacement. A result decoding
failure is reported without attempting cleanup.

Deleting an absent job is a no-op. A separate `Delete` that executes after
recreation legitimately removes the new generation. If cleanup fails, the
removed entity state stays deleted.

### Run fencing

Each `Create` mints a random run token that is stored on the job and carried by
the run it starts. Every orchestration-originated mutation — `CommitCheckpoint`,
`MarkAsCompleted`, `MarkAsFailed`, and the `Run` signal — carries that token, and
the entity requires a matching **nonempty** token. A run left over from a
job that was deleted and recreated, or from a prior in-place recreate, therefore
cannot checkpoint, complete, or fail the new job. The run also stops itself as
soon as it reads a job whose token no longer matches.

Missing tokens never match, even when both the request and stored token are
empty. Runs do not adopt another generation's token. A delayed `Run` signal is
dropped after deletion or recreation, and an already-emitted delayed Start stops
when it reads the missing or replacement job instead of exporting its data.

There is one **current state-owning generation**, not a guarantee of zero
overlap. Previous orchestrations and already-scheduled activities can briefly
remain in flight, but cannot mutate the replacement's job state. Recreation
does not purge their histories; `Delete` cleans only the generation it removes.

## Output layout

Each exported instance becomes one object named
`<prefix><sha256(completedAt|instanceId)>.<extension>`:

| Format | Extension | Content type | Content-Encoding |
| --- | --- | --- | --- |
| `ExportFormatJSONL` (default) | `jsonl.gz` | `application/gzip` | not set |
| `ExportFormatJSON` | `json` | `application/json` | not set |

A JSONL object is gzip-compressed and stored as an opaque gzip file: the name and
content type agree and no content coding is declared, so every reader downloads
exactly the gzip stream the name promises. Declaring the compression as
`Content-Encoding: gzip` instead would let some clients transparently decompress
the download while others would not, leaving a reader unable to tell what it
received.

JSONL objects carry one `api.HistoryEvent` per line; JSON objects contain an
array of the same events. Azure objects carry `schemaVersion`,
`instanceIdBase64`, and `executionIdBase64` metadata. The identifier values are
always UTF-8 bytes encoded as **unpadded RFC 4648 base64url**
(`base64.RawURLEncoding` in Go), even for ASCII identifiers. Decode those
suffixed keys to recover the original IDs. Raw identifiers are never sent as
Azure metadata headers; the history body and object-name hash still use the
original IDs. Metadata keys may be returned with different casing by Azure.
The name is derived deterministically, so re-exporting an instance overwrites
its object instead of duplicating it.

When no destination is supplied, a job writes to the client's configured
container under the prefix `<mode>-<jobId>/`.

## Worker setup

```go
store, err := exporthistory.NewAzureBlobHistoryStore(exporthistory.AzureBlobHistoryStoreOptions{
    ConnectionString: storageConnectionString,
    ContainerName:    "history-exports",
})
err = exporthistory.Register(registry, exporthistory.WorkerOptions{
    Source: taskHubClient,
    Store:  store,
})
worker, err := durabletaskscheduler.NewWorker(options, registry, logger,
    durabletaskclient.WithAutoWorkItemFilters(),
    exporthistory.WithExportHistory(),
)
```

`Source` supplies the three management reads the export performs:
`ListInstanceIDs`, `FetchOrchestrationMetadata`, and `StreamOrchestrationHistory`.
`*client.TaskHubGrpcClient` and the Durable Task Scheduler client satisfy it.

The exporter pins the read to metadata's execution ID and incrementally checks
`HistoryQuery.MaxEvents`, the aggregate approximate byte limit, and the observed
`ExecutionStarted` identity. It does not retain an event list or a whole
serialized/compressed history. Per export, memory consists of the current source
chunk/event, its encoded JSON, gzip state when needed, a single 1 MiB upload
block, and Azure's bounded block-ID list (at most 50,000 entries). Parallel
exports each own that budget; large individual events/source chunks still
require memory.

`Store` is a narrow interface with a single `Write` method. `AzureBlobHistoryStore` is
the production implementation; supply your own to export elsewhere. It is
deliberately separate from `payload.AzureBlobStore`, whose large-payload
contract assigns random object names inside a single container. Its endpoint
validation is at least as strict as that store's: an `AccountURL` carrying
userinfo, a query string, or a fragment is rejected outright, and plaintext HTTP
is confined to loopback endpoints behind `AllowInsecureHTTP`.

`ExportObject.Content` is now a single-use `io.Reader`, not `[]byte`. Custom
stores must consume it synchronously, honor cancellation, and publish only
after a clean EOF: a read error must leave an existing object unchanged. The
Azure implementation stages blocks sequentially and commits the block list
only after successful source validation and gzip finalization. On source or
upload failure, the opposite side is canceled and joined. SDK transport retries
rewind only the current block; an activity retry opens a fresh pinned history
stream, never reuses a consumed reader. Failed attempts can leave uncommitted
Azure blocks for service-managed expiration, but do not replace committed data.
Custom history sources must deliver events serially and stop on handler errors
or context cancellation. Update preview source/store implementations and blob
metadata readers when upgrading.

### Versioning

Every system task is registered unversioned so it stays reachable when an
application enables default versioning. Because a strict worker advertises its
own version for unversioned registrations, `WithExportHistory()` allow-lists
both the system orchestrators and their activities, so the derived work-item
filters keep advertising them unversioned. Without it, a strict-version worker
constructs successfully but the service never dispatches export work to it.

## Client

```go
exportClient, err := exporthistory.NewClient(taskHubClient, exporthistory.ClientOptions{
    ContainerName: "history-exports",
})

job, err := exportClient.CreateJob(ctx, exporthistory.JobCreationOptions{
    Mode:                 exporthistory.ExportModeBatch,
    CompletedTimeFrom:    from,
    CompletedTimeTo:      to,
    MaxInstancesPerBatch: 200,
})
description, err := job.Describe(ctx)
page, err := exportClient.ListJobs(ctx, exporthistory.ExportJobQuery{JobIDPrefix: "nightly-"})
err = job.Delete(ctx)
```

### Validation

`JobCreationOptions.Normalize` applies the same rules as .NET:

- Batch mode requires `CompletedTimeFrom` and `CompletedTimeTo`, requires
  `CompletedTimeTo` to be strictly greater than `CompletedTimeFrom`, and rejects
  an upper bound in the future.
- Continuous mode rejects `CompletedTimeTo` and defaults `CompletedTimeFrom` to
  now.
- `MaxInstancesPerBatch` must be between 1 and 1000; it defaults to 100.
- `RuntimeStatus` accepts only `COMPLETED`, `FAILED`, and `TERMINATED`; an empty
  filter selects all three.
- A missing job ID is generated as a 32-character GUID.

The `ExportJob` entity re-validates creation options on the worker, whose clock
is independent of the client's. Everything is checked identically except the
"upper bound is not in the future" rule, which the entity relaxes by
`MaxCreationClockSkew` (5 minutes) so a worker running slightly behind does not
reject a window the client accepted. Clients stay strict, and the tolerance never
shifts the window a continuous job starts from.

### Errors

| Error | Sentinel | Raised when |
| --- | --- | --- |
| `ValidationError` | `ErrValidation` | Invalid options, destination, or client configuration |
| `NotFoundError` | `ErrJobNotFound` | Reading a job that does not exist |
| `InvalidTransitionError` | `ErrJobInvalidTransition` | An operation the lifecycle does not allow |
| `OperationError` | `ErrJobOperationFailed` | An operation orchestration failed for another reason |

Errors raised inside the entity carry stable cross-language error types, so a
client reconstructs the typed error across the orchestration boundary and
`errors.Is`/`errors.As` keep working.

## Service behavior and limitations

- **Extended sessions are not supported.**
- **Pagination.** DTS signals the end of the stream by omitting the continuation
  token. An empty page that still carries a token advances the cursor and the job
  keeps listing. If a non-empty final page omits its token, a continuous job
  re-scans that page on each idle cycle; deterministic blob names make the
  writes idempotent, but scanned and exported counters include the re-scan.
- **List visibility lag.** The service's instance-ID index can lag orchestration
  completion. A batch job whose first page is empty legitimately completes with
  nothing exported, so schedule a job after the window's instances are listable.
- **Retry semantics.** A per-instance export is attempted up to three times
  (retry delays 15s, then 30s) for transient failures such as a storage write
  error. Conditions retrying cannot fix, such as a missing or non-terminal
  instance, are collected immediately. A page whose instances still fail is
  attempted three times in total, waiting 1 minute before the second attempt and
  2 minutes before the third; the third attempt fails the page instead of
  waiting again.
- **Delete cleanup is separate** from entity deletion and targets only the
  captured generation. Older generation histories are retained after recreation
  and must be purged separately if no longer needed.

## Tests

Unit and orchestration-replay tests run with no external services. Live tests
skip unless their environment is configured:

```bash
# Azure Blob write path against Azurite
AZURITE_CONNECTION_STRING="..." go test ./exporthistory/

# End-to-end against a live Durable Task Scheduler plus Azurite
DTS_EMULATOR_ENDPOINT="http://127.0.0.1:8080" DTS_TASK_HUB=default \
  AZURITE_CONNECTION_STRING="..." \
  go test ./tests/durabletaskscheduler/ -run TestDTSExportHistory
```
