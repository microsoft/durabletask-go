# Runnable DTS feature samples

These examples use Durable Task Scheduler, not an embedded storage backend.
Each sample has its own README, a bounded run, observable outcome assertions,
and cleanup of its own resources. `SAMPLE_OK <name>` is printed only after
validation and cleanup succeed. A build, a skipped run, or an unsupported
operation is not evidence that a feature works end to end.

## Feature coverage

| Sample | What it demonstrates | Additional prerequisites |
| --- | --- | --- |
| [durabletaskscheduler](durabletaskscheduler) | Orchestrations, activities, typed input/output, start/wait | None |
| [parallel](parallel) | Fan-out/fan-in, WhenAll/WhenAny, aggregation | None |
| [coroutines](coroutines) | Durable coroutines, wait groups, selection, cancellation scopes | None |
| [timers](timers) | Durable delays, scheduled starts, timer splitting, deterministic time/IDs | None |
| [externalevents](externalevents) | Typed and repeated events, timeouts, cross-instance events | Piped or interactive input |
| [suborchestrations](suborchestrations) | Child workflows, results, IDs, metadata and failures | None |
| [retries](retries) | Controlled retries, retry handlers, typed/non-retriable failures | None |
| [continueasnew](continueasnew) | Checkpoints, event carryover, continuation and history/turn budgets | None |
| [management](management) | Progress, queries/paging, lifecycle, restart, ID reuse, ID-scoped purge | None |
| [scheduledtasks](scheduledtasks) | Actual scheduled execution and schedule lifecycle | Go-only task hub |
| [versioning](versioning) | Worker versions, inheritance, fallback, migration and routing | None |
| [entity](entity) | State, signals/calls, registration models, factories and locked transfers | None |
| [dataconverter](dataconverter) | Custom serialization and raw versus typed payloads | None |
| [largepayloads](largepayloads) | Blob externalization, hydration, compression and bounds | Azure Blob Storage or Azurite |
| [history](history) | Buffered/streaming history, execution identity, typed readers and limits | None |
| [observability](observability) | Tags/context, replay-safe logs and metric hooks | None |
| [worker](worker) | Concurrency, graceful drain, restart/recovery and connection ownership | None |
| [authentication](authentication) | Azure connection-string and programmatic identity authentication | Azure DTS and an authorized identity |
| [distributedtracing](distributedtracing) | Application spans, caller propagation and actual OTLP receipt | OTLP collector and readable trace capture |
| [exporthistory](exporthistory) | Export jobs and downloaded JSON/JSONL objects | Blob storage and an explicitly isolated Go-only hub |
| [replayanalysis](replayanalysis) | Analyzer diagnostics and safe runnable counterparts | Local `cmd/orchestratorvet` sources |
| [serviceoperations](serviceoperations) | Hub-wide filtered purge and empty-entity maintenance | Deletes data; read the sample's warning |

Protocol bookkeeping, legacy wire compatibility and unsupported sandbox worker
profiles are not separate runnable features. SDK regression tests cover the
internal contracts. The catalogue demonstrates user-facing behavior.

## Run an example

Go 1.25 or later is required. Choose an example and configure `DTS_CONNECTION_STRING`.
Most examples can use either the emulator or Azure DTS:

```bash
export DTS_CONNECTION_STRING='Endpoint=http://127.0.0.1:8080;TaskHub=default;Authentication=None'
go run ./samples/durabletaskscheduler
go run ./samples/retries
```

Run commands are relative to the repository root unless a sample README says
otherwise. The distributed-tracing sample remains a nested Go module:

```bash
cd samples/distributedtracing
go run .
```

Examples use unique instance/entity/schedule/job IDs. Cleanup never searches a
shared hub for arbitrary data to delete. Activities may be delivered more than
once; failure injection is a teaching device, not a production idempotency store.

## Execute the actual programs end to end

The runner in `tests/samples` builds and executes the same programs, with their
normal environment variables. Select samples using Go's standard `-run` flag:

```bash
DTS_SAMPLES_E2E=1 \
  go test ./tests/samples -run '^TestSamplesE2E/retries$' -count=1 -v

DTS_SAMPLES_E2E=1 \
  go test ./tests/samples -run '^TestSamplesE2E/(timers|parallel)$' -count=1 -v
```

Set `DTS_SAMPLES_RACE=1` to build the sample processes with the race detector.
The external-event sample receives piped input automatically. The replay-analysis
sample also runs the real analyzer against safe and intentionally unsafe code.
The runner preserves timeouts, checks exit status and the sample's validation
receipt, and checks that every runnable sample has an entry and README.

Each sample validates its own prerequisites, identically whether invoked through
`go run` or the E2E runner. For example:

```bash
export DTS_CONNECTION_STRING='Endpoint=https://<scheduler-host>;TaskHub=<hub>;Authentication=DefaultAzure'
go run ./samples/authentication

DTS_SAMPLES_E2E=1 \
  go test ./tests/samples -run '^TestSamplesE2E/authentication$' -count=1 -timeout=5m -v
```

There is no target switching in the runner: every selected sample inherits the
same `DTS_CONNECTION_STRING`. Run maintenance separately: it performs hub-wide cleanup.
Follow the individual READMEs for storage, telemetry and safety settings; the
runner does not invent aliases, supply credentials or relax those checks.

Missing required configuration fails the selected example rather than silently
skipping it. Ordinary `go test ./...` leaves external-service runs disabled; that
default skip does not count as E2E coverage.

The `Sample E2E` workflow provisions isolated DTS, Azurite and an OTLP collector,
then runs the local examples using Go's test selection flags. It explicitly
excludes `authentication` and `serviceoperations`, which need approved Azure
targets. Temporary Azurite credentials are generated at runtime; cloud credentials
are not exposed to untrusted pull requests.

## Capability and safety boundaries

Rewind, skip-graceful termination and SDK task-hub lifecycle RPCs are intentionally
not supported. Provision and remove task hubs through Azure's control plane or
CLI. Filtered purge and instance listing have documented emulator limitations;
consult the [SDK feature matrix](../durabletaskscheduler/README.md#feature-matrix).

The administrative sample verifies state changes, not just acknowledgements.
An unsupported API, a success response that leaves the expected state unchanged,
or a missing precondition remains failed/blocked coverage. Do not turn such
results into a success or a skipped test just to make a dashboard green.

Hub-wide purge and entity maintenance are destructive.
Read the `serviceoperations` warning before running it against a task hub.
Storage/export examples must
use isolated destinations and task hubs so they cannot export or delete unrelated
application data.
