# Durable Task Scheduler SDK

The `durabletaskscheduler` package is the primary SDK surface for connecting Go
management clients and workers to Durable Task Scheduler (DTS).

## Configuration

Use a connection string from the environment:

```bash
export DTS_CONNECTION_STRING='Endpoint=https://<scheduler-host>;TaskHub=<task-hub>;Authentication=DefaultAzure'
go run ./samples/durabletaskscheduler
```

For the local DTS emulator:

```bash
export DTS_CONNECTION_STRING='Endpoint=http://127.0.0.1:8080;TaskHub=default;Authentication=None'
go run ./samples/durabletaskscheduler
```

Connection strings support the Azure Identity modes that have Go equivalents:
`DefaultAzure`, `ManagedIdentity`, `WorkloadIdentity`, `Environment`,
`AzureCLI`, `AzurePowerShell`, `InteractiveBrowser`, and `None`. Keys and
`Authentication` values are case-insensitive, surrounding whitespace is trimmed,
empty segments are skipped, and a repeated key uses its last value. Only the
first `=` in a segment separates the key from the value, so values may contain
`=`. Unknown keys, malformed segments, and a missing or blank `Endpoint`,
`TaskHub`, or `Authentication` are rejected. `VisualStudio` and
`VisualStudioCode` have no Go equivalent, and `TokenCredential` is programmatic
only: both are rejected with guidance to use `NewOptionsWithCredential` with any
`azcore.TokenCredential`. Plaintext `http://` endpoints are accepted only with
`Authentication=None`, which also requires `AllowInsecureConnection`.

Authentication-specific keys are `ClientID`, `TenantID`, `TokenFilePath`, and
the comma-separated `AdditionallyAllowedTenants`. Each mode consumes only the
fields Azure Identity for Go supports for it:

| Authentication | ClientID | TenantID | TokenFilePath | AdditionallyAllowedTenants |
| --- | --- | --- | --- | --- |
| `DefaultAzure` | ignored | used | ignored | used |
| `ManagedIdentity` | used | ignored | ignored | ignored |
| `WorkloadIdentity` | used | used | used | used |
| `Environment` | ignored | ignored | ignored | ignored |
| `AzureCLI` | ignored | used | ignored | used |
| `AzurePowerShell` | ignored | used | ignored | used |
| `InteractiveBrowser` | used | used | ignored | used |
| `None` | rejected | rejected | rejected | rejected |
| `TokenCredential` | rejected | rejected | rejected | rejected |

`Environment` is configured entirely by the `AZURE_*` environment variables, and
`WorkloadIdentity` falls back to them for any field left unset. Fields are
trimmed before use and blank `AdditionallyAllowedTenants` entries are dropped.
`None` and `TokenCredential` construct no Azure Identity credential, so
supplying identity fields for them fails validation instead of being silently
ignored.

Access tokens are requested for `Options.ResourceID` (default
`https://durabletask.io`) with trailing slashes removed and `/.default`
appended. Token acquisition failures are surfaced as retriable `Unavailable`
gRPC errors. Tokens and immutable authorization metadata are cached until the
credential's `RefreshOn` time (or five minutes before expiry), and concurrent
refreshes are coalesced. Every RPC carries `taskhub` and `x-user-agent` metadata; worker
connections add `workerid`. `Options.UserAgent` and `Options.WorkerID` override
the generated values and are rejected if they contain leading/trailing
whitespace or newlines. Unset worker IDs default to
`<hostname>,<pid>,<uuid>`.

`Options.HelloTimeout` (default 30 seconds) bounds the fail-fast `Hello`
handshake for both `NewClient` and the worker connection factory; the caller's
context still applies when it is shorter. Client channels use a default gRPC
service config that retries `UNAVAILABLE` up to five attempts with a 50 ms
initial backoff, 250 ms cap, and multiplier 2. Worker channels do not, because
the worker owns its own reconnect loop.

Individual gRPC messages are bounded to 64 MiB by default through
`Options.MaxReceiveMessageSize` and `Options.MaxSendMessageSize`. The worker uses a
3.9 MiB safety bound below the service ceiling. When
`Options.MaxSendMessageSize` is lower, that configured send limit becomes the
bound instead. The worker validates the final response after large-payload
externalization and fails an orchestration once
with `OrchestratorResponseTooLarge` if it still exceeds that limit. The legacy
`isPartial`/`chunkIndex` response fields are deprecated; the Go worker does not
use them because validation against the current DTS emulator did not provide a
portable chunking path. Configure large-payload storage or reduce per-turn
fan-out instead. Active streams
use a two-minute keepalive with a 20-second acknowledgement timeout; set
`Options.KeepaliveTime` to zero to disable it. The timeout is ignored while
keepalive is disabled. Values below 30 seconds are rejected to avoid
aggressive-ping disconnects.

`NewClient` creates and owns a management connection; call `Close` when done.
Options created with `NewOptions`, `NewOptionsFromConnectionString`, or
`NewOptionsWithCredential` recreate the channel after five consecutive
`Unavailable` responses (or unexpected `DeadlineExceeded` responses), with a
30-second minimum interval. Successful and application-level responses reset
the counter, while caller cancellation/deadlines and expected deadlines from
instance start/completion long polls do not count. Configure the thresholds with
`Options.ChannelRecreateFailureThreshold` and
`Options.ChannelRecreateMinInterval`. Authentication, interceptors, large
payload settings, and data conversion are preserved across replacements.
A hand-built `Options` value with a zero failure threshold disables recreation.

`client.NewTaskHubGrpcClient`, `client.NewTaskHubGrpcWorker`, and
`TaskHubGrpcClient.StartWorkItemListener` are lower-level APIs that borrow the
caller's `grpc.ClientConnInterface`; they never replace or close it.
`NewWorker` owns its channels, recreates them after transient disconnects, and
closes retired channels after their in-flight completions drain.
`client.NewTaskHubGrpcWorkerWithConnectionFactory` provides the same lifecycle
when its factory returns a non-nil closer, which transfers ownership to the
worker. Use an owning configuration to recover from a permanently wedged
channel. After a worker has started, `Unauthenticated` and `PermissionDenied`
stream or reconnect-handshake responses are retried with backoff so token
refresh and RBAC propagation can recover without restarting the worker. See
`client.NewTaskHubGrpcWorker` for the full ownership contract.

Reconnect and RPC retry delays are deterministic and always stay within
`[baseDelay, maxDelay]`. A stream that delivers at least one message before it
ends is treated as a drain and restarts the schedule at the base delay; a stream
that stays silent past `WithWorkerSilentDisconnectTimeout` before its first
message is treated as poisoned and keeps escalating.

`Options.MaximumTimerInterval` defaults to three days. Longer durable timers
are split into deterministic sequential timer actions that retain the original
deadline. Histories written before splitting remain compatible once the original
logical timer deadline fires. Changing the interval between two splitting
configurations is replay-breaking for affected in-flight orchestrations.

### Advanced management

`TaskHubGrpcClient` exposes bounded `QueryInstances` and `ListInstanceIDs`
operations with opaque continuation tokens, plus `RestartInstance`,
`RewindInstance`, batch/filter `PurgeInstances`,
`SkipGracefulOrchestrationTerminations`, and task-hub lifecycle RPCs. Queries
can filter locally by exact tag key/value pairs when the current wire contract
does not carry tag filters.

`GetOrchestrationHistory` returns API-owned history records with validated event
and approximate byte caps. `StreamOrchestrationHistory` invokes a callback in
service order without retaining the history in memory. Both preserve execution identity,
timestamps, failures, tags, context fields, rewind markers, and native entity
events. Serialized payloads remain raw until a `ReadInput`, `ReadResult`, or
`ReadData` helper applies the configured data converter.

The current DTS emulator supports query, restart, and batch purge, but has known
service limitations: `SkipGracefulOrchestrationTerminations`
is unimplemented, rewind can return success without transitioning the failed
instance, filtered purge can complete without deleting matches, and
`ListInstanceIds` can omit matching IDs. The emulator integration tests record
these limitations explicitly.

### Worker routing and capabilities

Use `client.WithTaskVersioning` for `None`, `Strict`, or `CurrentOrOlder`
acceptance and `Reject` or `Fail` mismatch handling. `VersioningOptions.DefaultVersion`
is applied to sub-orchestrations. When `Options.Versioning` is set, its default
also applies to top-level starts made by the DTS client.

Registrations use `AddOrchestratorNVersion` and `AddActivityNVersion`. Exact
name/version matches win. Unversioned fallback is allowed only for a logical
name with no versioned registrations. Activities inherit their parent
orchestration version unless explicitly overridden, including an explicit
unversioned `""` override. ContinueAsNew can migrate with
`task.WithContinueAsNewVersion`, including to
`task.UnversionedTaskVersion`.

The current DTS service accepts numeric versions in
`Major[.Minor[.Patch]]` form. The Go registry also supports case-insensitive
opaque version strings, but DTS applications should use numeric versions such as
`"1.0"` and `"2.0"`.

Use `client.WithAutoWorkItemFilters()` to derive filters from the registry, or
`client.WithWorkItemFilters` for an explicit override. Local enforcement is a
fallback for services that ignore the filter request. `CurrentOrOlder` ranges
cannot be represented by the protocol filter and are therefore enforced by the
worker. Service-side
filters can leave a task pending indefinitely when no worker advertises it,
whereas unfiltered delivery produces a deterministic task-not-found failure.
Auto-generated filters reject task kinds with no registrations and validate
that strict worker versions can resolve every advertised logical name.
Capability advertisement is explicit: history streaming is enabled by default,
while scheduled tasks use `durabletaskscheduler.WithScheduledTasks()`. Streamed
history accumulation is bounded to 100,000 events and 64 MiB by default; use
`client.WithMaxStreamedHistoryEvents` and
`client.WithMaxStreamedHistoryBytes` to choose different bounded limits. These
are per-work-item limits, so size worker concurrency with the aggregate memory
budget in mind. Exceeding either limit delays and abandons the work item until
the worker is reconfigured with a larger bounded limit.

### Recurring scheduled tasks

> **Mixed-SDK constraint:** this surface uses Go SDK-owned system entity and
> orchestrator names and state. Use it only in Go-only task hubs; interoperability
> with .NET workers sharing those system names and state is not defined.

Scheduled tasks use recurring UTC intervals, not cron expressions. Register the
system entity and orchestrators before creating the worker:

```go
registry := task.NewTaskRegistry()
registry.AddOrchestratorNVersion("Backup", "1.0", backup)
durabletaskscheduler.RegisterScheduledTasksWithDefaultVersion(registry, "1.0")

worker, _ := durabletaskscheduler.NewWorker(
    options,
    registry,
    logger,
    durabletaskscheduler.WithScheduledTasks(),
    client.WithAutoWorkItemFilters(),
)
```

Create and manage schedules through the DTS client:

```go
schedules := schedulerClient.ScheduledTasks()
handle, _ := schedules.Create(ctx, durabletaskscheduler.ScheduleCreationOptions{
    ScheduleID:              "nightly-backup",
    OrchestrationName:       "Backup",
    TypedOrchestrationInput: backupRequest,
    Interval:                24 * time.Hour,
    StartAt:                 firstRun,
    StartImmediatelyIfLate:  true,
    Tags:                    map[string]string{"team": "storage"},
    ContextFields:           api.ContextFields{"tenant": "north"},
})

description, _ := handle.Describe(ctx)
_ = handle.Pause(ctx)
_ = handle.Update(ctx, durabletaskscheduler.ScheduleUpdateOptions{Interval: &newInterval})
_ = handle.Resume(ctx)
_ = handle.Delete(ctx)
```

`Get` returns `nil, nil` for a missing schedule. `List` is continuation-based;
status and creation-time filters are applied after each entity page, so pages
can be underfilled. Schedule operations return typed errors compatible with
`errors.Is` and `errors.As`. A fixed target instance ID prevents overlapping
runs. Fixed target instance IDs cannot be combined with a retry policy because
each retry requires a distinct durable instance. Tags, context fields, and
retry policies use an internal launch
orchestrator because the entity-start wire action cannot carry them directly.
Explicit work-item filters must include the `Schedule` entity and both system
orchestrators; auto filters include them from the registry.

### Data converters

Set `Options.DataConverter` to configure one `api.DataConverter` for the DTS
client and worker. The default is `api.JSONDataConverter`. Conversion happens
before large-payload externalization and after hydration. Converter errors are
returned; the SDK never retries a payload with JSON. Raw input/output APIs and
serialized metadata fields bypass conversion. Converter identity is not stored
by the protocol, so deployments must retain backward decoding compatibility.
The legacy skip-graceful termination `reason` remains a plain protocol string
for cross-version service compatibility.

### Large payloads

Large payload support uses an opaque, integrity-checked reference encoded in
the existing string payload fields. Production DTS workloads should configure
the same Azure Blob store for every management client and worker:

```go
store, err := payload.NewAzureBlobStore(payload.AzureBlobStoreOptions{
    ConnectionString: os.Getenv("AzureWebJobsStorage"),
    Container:        "durabletask-payloads",
})
if err != nil {
    return err
}
options.LargePayloads = &api.LargePayloadOptions{
    Store:            store,
    Resolver:         store,
}
```

`payload.NewAzureBlobStore` emits the same self-describing
`blob:v2:<absolute-blob-url>` token as the .NET SDK and can read legacy .NET
`blob:v1` tokens. `payload.NewMemoryStore` is only for tests and single-process
experiments because references are lost on restart. `payload.NewFileStore`
requires every client and worker to share the same durable filesystem path;
container-local or other ephemeral storage is not safe.

Identity authentication uses `AccountURL` plus an `azcore.TokenCredential`.
Cross-account identity reads require an explicit `AllowedHosts` entry. Azure
Blob defaults match .NET: 256 KiB inclusive threshold, 10 MiB maximum, gzip
enabled, eight exponential retries, and the `durabletask-payloads` container.
Tokens are treated as untrusted: userinfo, SAS/query strings, fragments,
unapproved hosts, malformed paths, oversized downloads, and invalid integrity
metadata are rejected. Go-written blobs include size, SHA-256, and content-MD5
integrity data; .NET blobs without that metadata remain readable.

Workers advertise `LARGE_PAYLOADS` only when `LargePayloads` is configured.
Resolver implementations must treat reference locations as untrusted and enforce
their own scheme/account/path allow lists.

## Worker lifecycle

Use `Start` for background execution or `Run` for blocking execution. `Shutdown`
stops intake, allows in-flight execution and completion RPCs to drain, and
cancels them only if the shutdown context expires.

## Feature matrix

| Feature | Status |
| --- | --- |
| Schedule, bounded query/list, and wait for orchestrations | Supported |
| Tags on schedule, metadata, query, sub-orchestration, continue-as-new, restart, and rewind | Supported; distinct from immutable context fields |
| Restart and batch/filter purge | Supported; see emulator limitations above |
| Rewind | Client and wire support are complete; current emulator does not transition instances |
| Skip-graceful termination | Client and wire support are complete; current emulator returns `Unimplemented` |
| Task-hub create/delete | Client and wire support are complete; remote-service behavior is provider-specific |
| Raise events, suspend/resume, terminate, and single-instance purge | Supported |
| Orchestration and activity execution | Supported |
| Bounded orchestration/activity/entity concurrency | Supported |
| Work-item filters for orchestrations, activities, and entities | Supported |
| Completion tokens and abandon RPCs | Supported |
| Oversized orchestration responses | Blob-externalized before send; residual responses above the effective limit fail once with non-retriable guidance; the Go worker does not use deprecated legacy chunking |
| Health pings, silent-disconnect detection, auth/RBAC recovery, and channel recreation | Supported |
| Public orchestration history | Supported through buffered and callback-streaming API-owned records |
| Version-aware registry dispatch and controlled unversioned fallback | Supported |
| Default versions, activity inheritance, and ContinueAsNew migration | Supported; DTS must honor `newVersion` |
| Name/version work-item filters | Supported, auto-generated or explicit, advertised, and locally enforced |
| Pluggable application data conversion | Supported with shared client/worker configuration; default is JSON |
| Recurring interval schedules | Supported in Go-only task hubs; mixed-SDK interoperability for the SDK-owned system entity state is not defined |
| Scheduled-task capability | Supported; register system tasks and opt in with `durabletaskscheduler.WithScheduledTasks()` |
| Azure Blob `blob:v2` payloads | Supported with connection-string or identity authentication and .NET-compatible gzip/token semantics |
| Large-payload capability | Supported and advertised only when a store/resolver is configured |
| Durable entities | Supported: legacy and V2 work items, scheduled signals, calls, queries, and critical sections |
| Status-based instance-ID deduplication and replacement | Supported through `api.OrchestrationIDReusePolicy.DedupeStatuses` |
| History export jobs (preview) | Buffers each complete history in worker memory and writes `api.HistoryEvent` JSON/JSONL; the schema version defaults to preview value `1.0` and is caller-configurable, so assess memory and schema compatibility before enabling |
| Sandbox worker profiles | Not implemented |

The current V2 protobuf cannot carry per-operation trace context or request time
to an entity worker, and it has no properties map for legacy extended-session
state elision. DTS therefore sends entity state on every V2 work item; causal
trace metadata on entity-emitted actions is best-effort.

## Emulator tests

On Apple silicon with Apple Container, the current MCR emulator image runs
under Rosetta:

```bash
container image pull mcr.microsoft.com/dts/dts-emulator:latest
container run --detach --name dts-emulator \
  --arch amd64 --rosetta \
  --publish 8080:8080 --publish 8082:8082 \
  --env DTS_TASK_HUB_NAMES=default \
  mcr.microsoft.com/dts/dts-emulator:latest
```

The integration suite is environment-gated:

```bash
DTS_EMULATOR_ENDPOINT=http://127.0.0.1:8080 \
DTS_TASK_HUB=default \
go test ./tests/durabletaskscheduler -count=1
```

Azurite-backed blob tests additionally use:

```bash
export AZURITE_CONNECTION_STRING='DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=<development-key>;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;'
```
