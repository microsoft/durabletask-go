# Copilot Instructions - durabletask-go

> These instructions apply to every Copilot interaction in this repository.
> They encode repo-specific facts, not generic advice.

## What This Repository Is

durabletask-go is the Go SDK for the Durable Task Framework
(https://github.com/microsoft/durabletask-protobuf).
It implements the orchestrator sidecar gRPC service (TaskHubSidecarService)
and the in-process worker runtime that executes orchestrators and activities.

Key facts before making any change:

| Fact | Value |
|------|-------|
| Module path | github.com/microsoft/durabletask-go |
| Go version | 1.23+ (CI tests 1.23.x and 1.24.x) |
| Wire format | Protocol Buffers 3 (shared across all Durable Task SDKs) |
| Proto source | submodules/durabletask-protobuf (git submodule - never edit generated code) |
| Generated files | internal/protos/*.pb.go - do not edit; regenerate with protoc |
| SQLite driver | modernc.org/sqlite - pure Go, CGO_ENABLED=0 |
| Test package | tests/ (integration tests + mocks in tests/mocks/) |
| Lint config | .golangci.yml - 7 linters; excludes internal/protos/ and tests/mocks/ |
| Status | Pre-production (current tags are v0.x); API stability is not guaranteed |

## Package Map

| Package | Role |
|---------|------|
| api/ | Public types: InstanceID, OrchestrationMetadata, sentinel errors, functional option types |
| task/ | In-process execution: OrchestrationContext, ActivityContext, Task, RetryPolicy, TaskRegistry |
| backend/ | Core interfaces (Backend, Executor, TaskHubWorker), worker loop, gRPC executor, runtime state |
| backend/sqlite/ | SQLite persistence backend |
| backend/postgres/ | PostgreSQL persistence backend |
| client/ | Out-of-process gRPC client (TaskHubGrpcClient) and stream-based work item listener |
| internal/helpers/ | History event and action constructors; tracing utilities |
| internal/protos/ | Protobuf-generated code - never edit directly |
| tests/ | Integration tests and mockery-generated mocks |
| main.go | gRPC server entry point |

## Coding Expectations

### Error handling

- Use sentinel errors from api/ and backend/ - do not introduce ad-hoc error strings.
- Use fmt.Errorf("...: %w", err) for wrapping; errors.Is/errors.As for checking.
- The errorlint linter enforces correct wrapping - run golangci-lint run before declaring done.
- Never swallow errors silently.

Sentinel errors in api/: ErrInstanceNotFound, ErrNotStarted, ErrNotCompleted,
ErrNoFailures, ErrDuplicateInstance, ErrIgnoreInstance

Sentinel errors in backend/: ErrTaskHubExists, ErrTaskHubNotFound, ErrNotInitialized,
ErrWorkItemLockLost, ErrBackendAlreadyStarted, ErrOperationAborted,
ErrNilHistoryEvent, ErrNilEventTimestamp, ErrNotExecutionStarted, ErrNoWorkItems

Sentinel errors in task/: ErrTaskBlocked, ErrTaskCanceled

### Interfaces and types

- In core packages (api/, backend/, task/, internal/), prefer any over interface{} (enforced since PR #118). Existing uses of interface{} in samples or other non-core code may remain unless you are actively modifying that code.
- Do not add a new interface unless at least two concrete implementations exist or are planned.

### Serialization contracts

- History events use proto.Marshal/proto.Unmarshal - changes require proto regeneration.
- Orchestrator/activity I/O is JSON via json.Marshal.
- OrchestrationMetadata has custom MarshalJSON/UnmarshalJSON with camelCase field names.
- Changing a serialized field name or removing a field is a BREAKING wire format change.

### Concurrency and determinism

- Orchestrator functions MUST be deterministic - they replay from history.
  Never use time.Now(), random numbers, or non-deterministic I/O inside an orchestrator.
- OrchestrationContext.IsReplaying is true during replay - gate side effects on !IsReplaying.
- task.Await() panics with ErrTaskBlocked as a coroutine mechanism (caught by taskExecutor).
  Do not recover from this panic in application code.
- Worker concurrency uses marusama/semaphore/v2.
- Worker polling uses cenkalti/backoff/v4: InitialInterval 50ms, MaxInterval 5s.

### gRPC and proto

- gRPC service name: TaskHubSidecarService.
  Helper IsDurableTaskGrpcRequest() matches /TaskHubSidecarService/ prefix.
- Regenerate protos:
    protoc --go_out=. --go-grpc_out=. -I ./submodules/durabletask-protobuf/protos orchestrator_service.proto
  using protoc-gen-go@v1.30 and protoc-gen-go-grpc@v1.3.

## Evidence-First Rule

Never propose a change based on assumption. Before editing any file:
1. Read the file and its direct callers/implementors.
2. Locate all tests that exercise the code path.
3. Check whether the Backend or Executor interface contract is involved.
4. If the change touches serialization, find all marshal/unmarshal call sites.
5. Confirm the change does not break replay determinism.

## Build and Test Expectations

All changes must pass:

    go vet ./...
    golangci-lint run
    go test ./tests/...
    POSTGRES_ENABLED=true go test ./tests/...       -coverpkg ./api,./task,./client,./backend/...,./internal/helpers

Do not claim a change is complete until go test ./tests/... passes locally.

Coverage packages: ./api,./task,./client,./backend/...,./internal/helpers
Excluded from lint: internal/protos/ (generated), tests/mocks/ (generated).

## Change-Scoping Discipline

- One concern per PR: do not mix a behavioral fix with cleanup or a parity addition.
- If a refactor changes behavior, document it explicitly in the PR.
- The SQLite and PostgreSQL backends are structurally parallel - if you fix a bug in one,
  check the other.
- The durabletask-protobuf submodule is shared across all SDKs - never bump it without
  cross-SDK awareness.

## Commit and PR Conventions

Follow Conventional Commits:
  feat:     new capability
  fix:      bug fix
  refactor: code restructuring without behavior change
  chore:    tooling, CI, dependency updates, generated code regeneration
  docs:     documentation only
  test:     test additions or fixes

No period at end of subject line. Imperative mood. Subject <= 72 characters.

## Review Expectations

A change is reviewer-ready when:
1. go vet ./... passes.
2. golangci-lint run passes.
3. go test ./tests/... passes.
4. New behavior has a test that is Red before the change and Green after.
5. Breaking changes (interface, serialization, wire protocol) are explicitly labeled.

## Documentation Update Expectations

- Update CHANGELOG.md under [Unreleased] (Keep a Changelog format) when behavior changes.
- If a public type/function signature changes, update all call sites in tests/ and samples/.
- Update README.md if public API usage examples change.

## What Not to Do

- Do not edit internal/protos/*.pb.go directly - regenerate with protoc.
- Do not edit tests/mocks/*.go directly - regenerate with mockery.
- Do not use interface{} in core packages (api/, backend/, task/, internal/) - use any.
- Do not introduce CGO dependencies.
- Do not add a new direct dependency without clear PR justification.
- Do not call time.Now() inside orchestrator functions.
- Do not recover from task.ErrTaskBlocked panics in application code.
- Do not add abstraction layers without 2+ concrete implementations or a documented use case.
