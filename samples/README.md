# Runnable DTS feature samples

These examples use Durable Task Scheduler, not an embedded storage backend.
Each sample has its own README, a bounded run, observable outcome assertions,
and cleanup of its own resources. `SAMPLE_OK <name>` is printed only after
validation and cleanup succeed. A build, a skipped run, or an unsupported
operation is not evidence that a feature works end to end.

## Feature coverage

| Sample | Feature family | E2E group |
| --- | --- | --- |
| [durabletaskscheduler](durabletaskscheduler) | Orchestrations, activities, typed input/output, start/wait | emulator |
| [parallel](parallel) | Fan-out/fan-in, WhenAll/WhenAny, aggregation | emulator |
| [coroutines](coroutines) | Durable coroutines, wait groups, selection, cancellation scopes | emulator |
| [timers](timers) | Durable delays, scheduled starts, timer splitting, deterministic time/IDs | emulator |
| [externalevents](externalevents) | Typed and repeated events, timeouts, cross-instance events | emulator |
| [suborchestrations](suborchestrations) | Child workflows, results, IDs, metadata and failures | emulator |
| [retries](retries) | Controlled retries, retry handlers, typed/non-retriable failures | emulator |
| [continueasnew](continueasnew) | Checkpoints, event carryover, continuation and history/turn budgets | emulator |
| [management](management) | Progress, queries/paging, lifecycle, restart, ID reuse, ID-scoped purge | emulator |
| [scheduledtasks](scheduledtasks) | Actual scheduled execution and schedule lifecycle | emulator |
| [versioning](versioning) | Worker versions, inheritance, fallback, migration and routing | emulator |
| [entity](entity) | State, signals/calls, registration models, factories and locked transfers | emulator |
| [dataconverter](dataconverter) | Custom serialization and raw versus typed payloads | emulator |
| [largepayloads](largepayloads) | Blob externalization, hydration, compression and bounds | storage |
| [history](history) | Buffered/streaming history, execution identity, typed readers and limits | emulator |
| [observability](observability) | Tags/context, replay-safe logs and metric hooks | emulator |
| [worker](worker) | Concurrency, graceful drain, restart/recovery and connection ownership | emulator |
| [authentication](authentication) | Real Azure connection-string and programmatic identity authentication | azure |
| [distributedtracing](distributedtracing) | Application spans, caller propagation and actual OTLP receipt | telemetry |
| [exporthistory](exporthistory) | Export jobs and verification of downloaded JSON/JSONL objects | storage |
| [replayanalysis](replayanalysis) | Real analyzer diagnostics and safe runnable counterparts | emulator |
| [serviceoperations](serviceoperations) | Hub-wide filtered purge and empty-entity maintenance | admin |

Protocol bookkeeping, legacy wire compatibility and unsupported sandbox worker
profiles are not separate runnable features. SDK regression tests cover the
internal contracts. The catalogue demonstrates user-facing behavior.

## Run an example

Go 1.25 or later is required. Point the normal examples at an isolated DTS emulator:

```bash
export DTS_CONNECTION_STRING='Endpoint=http://127.0.0.1:8080;TaskHub=default;Authentication=None'
go run ./samples/durabletaskscheduler
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

The standard-library runner in `tests/samples` builds and runs each selected
entry point as a separate process. It verifies exit status and the sample's
validation receipt. It also checks that every runnable sample has a catalogue
entry and run instructions.

```bash
DTS_SAMPLES_E2E=1 DTS_SAMPLES_GROUP=emulator \
  go test ./tests/samples -run 'TestSampleCatalogue|TestSamplesE2E' -count=1 -timeout=20m -v
```

Set `DTS_SAMPLES_RACE=1` to build the sample processes with the race detector.
The external-event sample receives piped input automatically. The replay-analysis
entry also runs the real analyzer against its safe and intentionally unsafe code.
There is no fake service or hand-built history substituting for these executions.

| Group | Required configuration |
| --- | --- |
| `emulator` | `DTS_CONNECTION_STRING` pointing at isolated DTS |
| `storage` | Isolated DTS with `DTS_SAMPLE_ISOLATED_TASKHUB=1`, plus `AZURE_STORAGE_CONNECTION_STRING` (`EXPORT_STORAGE_CONNECTION_STRING` is also accepted); set `DTS_SAMPLE_ALLOW_INSECURE_STORAGE=1` for loopback Azurite |
| `telemetry` | DTS, `OTEL_EXPORTER_OTLP_ENDPOINT`, and `OTEL_CAPTURE_FILE` written by the real collector |
| `azure` | `DTS_SAMPLES_AZURE_CONNECTION_STRING` and an authorized Azure Identity credential |
| `admin` | `DTS_SAMPLES_ADMIN_CONNECTION_STRING` for a **disposable `sample-*` hub** and `DTS_SAMPLE_ALLOW_HUB_MAINTENANCE=1` |
| `all` | All of the above; each process gets the appropriate DTS target |

For example, run the same authentication executable against a real Azure hub:

```bash
export DTS_SAMPLES_AZURE_CONNECTION_STRING='Endpoint=https://<scheduler-host>;TaskHub=<hub>;Authentication=DefaultAzure'
DTS_SAMPLES_E2E=1 DTS_SAMPLES_GROUP=azure \
  go test ./tests/samples -run TestSamplesE2E -count=1 -timeout=5m -v
```

When E2E is explicitly enabled, missing required configuration fails the selected
group instead of silently skipping it. Ordinary `go test ./...` leaves these
external-service runs disabled; that default skip does not count as E2E coverage.

The `Sample E2E` workflow runs emulator, storage and telemetry groups on real
isolated services. It generates temporary Azurite credentials rather than
committing account keys. Cloud groups are deliberately separate: use an approved
identity and target, not credentials exposed to untrusted pull requests.

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
The `admin` group must use a newly provisioned disposable hub, never the hub
used for ordinary shared examples. Remove that temporary control-plane resource
afterward through Azure's control plane. Storage/export examples must
use isolated destinations and task hubs so they cannot export or delete unrelated
application data.
