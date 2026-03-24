---
applyTo: "**/*.go"
---

# Go Source Instructions — durabletask-go

These instructions apply to all `.go` files **except** generated code (`internal/protos/`) and mocks (`tests/mocks/`). For those, see notes below.

---

## Formatting and Style

- All Go source must be `gofmt`-clean. The linter will catch violations.
- In core packages (e.g., `api/`, `backend/`, `task/`, `internal/`), use `any` instead of `interface{}` (enforced since PR #118). Existing uses of `interface{}` in samples or other non-core code may remain unless you are actively modifying that code.
- Receiver names must be consistent within a type — if existing methods on `sqliteBackend` use `be`, new methods must also use `be`.
- Unexported struct fields do not need doc comments. Exported fields and types do.
- Group imports: stdlib → external → internal (standard Go convention).

---

## Error Handling

The `errorlint` linter enforces these rules — violations will fail CI:

```go
// CORRECT: wrap with %w
return fmt.Errorf("CompleteOrchestrationWorkItem %s: %w", iid, err)

// CORRECT: use errors.Is/errors.As for checking
if errors.Is(err, backend.ErrNoWorkItems) { ... }

// WRONG: use == for error comparison
if err == backend.ErrNoWorkItems { ... }

// WRONG: fmt.Errorf without %w loses the error chain
return fmt.Errorf("failed: %s", err.Error())
```

Sentinel errors:
- `api/` package: `ErrInstanceNotFound`, `ErrNotStarted`, `ErrNotCompleted`, `ErrNoFailures`, `ErrDuplicateInstance`, `ErrIgnoreInstance`
- `backend/` package: `ErrTaskHubExists`, `ErrTaskHubNotFound`, `ErrNotInitialized`, `ErrWorkItemLockLost`, `ErrBackendAlreadyStarted`, `ErrOperationAborted`, `ErrNilHistoryEvent`, `ErrNilEventTimestamp`, `ErrNotExecutionStarted`, `ErrNoWorkItems`
- `task/` package: `ErrTaskBlocked`, `ErrTaskCanceled`

Do not invent new sentinel errors for conditions that already have one.

---

## Interface Implementation

When implementing `Backend`:
- `GetOrchestrationWorkItem` and `GetActivityWorkItem` must return `backend.ErrNoWorkItems` (not `nil, nil`) when the queue is empty.
- `CompleteOrchestrationWorkItem` must atomically write history + clear the work item lock.
- `AbandonOrchestrationWorkItem` clears the lock and sets `VisibleTime` based on `GetAbandonDelay()`. `RetryCount` is derived from the `NewEvents.DequeueCount` column at dequeue time (in `GetOrchestrationWorkItem`), not updated by abandon.

When implementing `Executor`:
- `ExecuteOrchestrator` receives `oldEvents` (completed history) and `newEvents` (inbox). The executor replays from the beginning on each call — it must not hold state between calls.
- `ExecuteActivity` runs in a separate goroutine from the orchestrator pipeline — it must be safe for concurrent execution.

---

## Concurrency Rules

- Do not use `sync.Mutex` where the existing `marusama/semaphore/v2` throttle already serializes access.
- The gRPC executor uses `sync.Map` keyed by `"{instanceID}/{taskID}"` for in-flight activities — follow this pattern if extending activity dispatch.
- Worker polling uses exponential backoff (`cenkalti/backoff/v4`): `InitialInterval: 50ms`, `MaxInterval: 5s`. Avoid introducing new hardcoded sleep durations outside the established worker patterns (including the existing extra 5s delay on unexpected errors in `backend/worker.go`).
- `StopAndDrain()` must block until all in-flight work items complete — use `sync.WaitGroup` or `semaphore.TryAcquire` patterns consistent with `worker.go`.

---

## Orchestration Determinism

These rules apply specifically to code inside `task/orchestrator.go` and any code called from within an orchestrator function:

- **Never use `time.Now()`** — use `OrchestrationContext.CurrentTimeUtc` (set from `OrchestratorStarted` event during replay).
- **Never read environment variables or files** inside an orchestrator function.
- **Never generate random values** — sub-orchestration IDs are derived deterministically from parent instance ID + action sequence number.
- The `ErrTaskBlocked` panic is the coroutine yield mechanism — it is caught by `taskExecutor.executeOrchestrator`. Do not catch it anywhere else.
- `IsReplaying` is `true` while replaying old events. Use it to gate logging and other side effects.

---

## Serialization Contracts

- History events: `proto.Marshal` / `proto.Unmarshal` via `backend.MarshalHistoryEvent` / `backend.UnmarshalHistoryEvent`.
- Orchestrator/activity I/O: JSON via `json.Marshal`. Errors must surface as `TaskFailed` events, not panics.
- `OrchestrationMetadata.MarshalJSON` produces camelCase JSON — if you add a field to `OrchestrationMetadata`, add both marshal and unmarshal handling with the correct JSON key.

---

## Database Backends

Both `backend/sqlite/sqlite.go` and `backend/postgres/postgres.go` implement the same `Backend` interface with structurally parallel code.

When fixing a bug or adding a feature:
1. Check whether the same issue exists in the parallel backend.
2. Apply the fix in both unless there is a documented reason why one backend differs.
3. The schemas are in `backend/sqlite/schema.sql` and `backend/postgres/schema.sql` — schema changes require migration handling.

**PostgreSQL-specific:** The test suite uses `POSTGRES_ENABLED=true` env var to gate postgres tests. Do not add new postgres-only tests without this guard.

---

## Generated Files — Do Not Edit

| File pattern | Generated by | How to regenerate |
|---|---|---|
| `internal/protos/*.pb.go` | `protoc` | See CI workflow step |
| `tests/mocks/*.go` | `mockery` | Run `mockery` against updated interfaces |

If you update the `Backend`, `Executor`, or `TaskWorker` interfaces, regenerate the corresponding mock in `tests/mocks/` — stale mocks will cause test compilation failures.

---

## Tracing

- Tracer name: `"durabletask"` — do not introduce a new tracer name.
- Span naming convention: `taskType||taskName` or `taskType||taskName||version`.
- Attribute keys: `durabletask.type`, `durabletask.task.name`, `durabletask.task.instance_id`, `durabletask.task.task_id`, `durabletask.task.version`, `durabletask.fire_at`.
- Trace context propagation uses W3C `traceparent` format stored in protobuf `TraceContext` — do not use custom header formats.
- Non-sampled spans produce `nil` TraceContext (not an empty struct) — check `traceContext == nil` before propagating.
