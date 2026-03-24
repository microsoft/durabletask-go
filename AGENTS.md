# AGENTS.md — durabletask-go

This file defines the operating constitution for all AI agents working in this repository.
It applies to GitHub Copilot agents, Claude, and any other AI system that reads repository context.

---

## What This Repository Is

`durabletask-go` is the Go implementation of the [Durable Task Framework](https://github.com/microsoft/durabletask-protobuf) — a cross-SDK protocol for building reliable, stateful orchestration workflows over gRPC.

Key constraints every agent must internalize:

| Constraint | Fact |
|---|---|
| Wire protocol | Protocol Buffers 3; shared with .NET, Python, Java SDKs |
| Proto source | `submodules/durabletask-protobuf` — submodule, not owned here |
| Generated code | `internal/protos/*.pb.go` — never edit; regenerate with `protoc` |
| Test command | `go test ./tests/... -coverpkg ./api,./task,./client,./backend/...,./internal/helpers` |
| Lint | `golangci-lint run` (7 linters; excludes `internal/protos/`, `tests/mocks/`) |
| Go version | ≥ 1.23 |
| Status | Pre-production (`v0.x`); APIs are explicitly unstable |

---

## Cross-Agent Operating Principles

These principles override any agent's default behavior.

### 1. Inspect Before Editing

Read the file, its callers, and its tests before proposing any change.
If you do not know the call path, search for it.
Do not propose changes based on analogies or pattern-matching to other codebases.

### 2. Evidence Before Claims

Every statement about the repository must be traceable to a specific file and line.
Statements like "this function is called here" or "this interface requires X" must cite evidence.
If evidence is missing, say "I did not find evidence for X — please verify."

### 3. Validate Before Declaring Done

A change is not done until:
- `go build ./...` passes
- `go vet ./...` passes
- `golangci-lint run` passes
- `go test ./tests/...` passes
- All of the above were run **after** the final code change, not before

Never declare a change done based on a "should work" judgment.

### 4. Minimize Blast Radius

- Fix the specific thing that is broken.
- Do not refactor while fixing a bug.
- Do not add features while fixing a bug.
- Do not clean up unrelated code in the same commit.
- Each PR has one concern.

### 5. Preserve Public Behavior Unless Change Is Intended

The public API surface (`api/`, `task/`, `backend/`, `client/`) is used by consumers of this module.
Before changing any exported type, function signature, or error return:
- Confirm the change is intentional (documented in the PR or issue).
- Check whether the change breaks the wire-format contract with other Durable Task SDKs.
- Update callers in `tests/` and examples.

### 6. Record Lessons Back Into the System

Every agent in `.github/agents/` has a mandatory self-improvement step.
When you discover a fact about the repository that is not encoded in any instruction file, encode it.
The Copilot OS improves by being written to, not just read from.

---

## Critical Orchestration-Specific Rules

Because this is a durable task orchestration engine, these rules are non-negotiable:

1. **Determinism**: Orchestrator functions are replayed from history. Any non-deterministic code (random, `time.Now()`, I/O) inside an orchestrator function is a correctness bug, not a style issue.

2. **ErrTaskBlocked is a coroutine mechanism**: `task.Await()` panics with `ErrTaskBlocked` to yield control. This panic is caught by the `taskExecutor`. Do not catch it in application code.

3. **Serialization contracts are cross-SDK**: Changing the wire format of history events, orchestration I/O, or gRPC request fields can break compatibility with other Durable Task SDKs. Always check the shared proto spec before modifying serialization.

4. **Work item locking is safety-critical**: If `CompleteOrchestrationWorkItem` fails to atomically clear the lock, work items become orphaned or double-processed. Any change to work item lifecycle code requires explicit atomicity verification.

5. **Both backends must be equivalent**: The SQLite and PostgreSQL backends implement the same interface. A bug fixed in one must be checked in the other.

---

## Agent Directory

| Agent | Purpose |
|---|---|
| `.github/agents/feature-designer.md` | Design new features before implementation |
| `.github/agents/implementation-planner.md` | Translate approved design into file-level plan |
| `.github/agents/test-specialist.md` | Write determinism-aware, Red-Green tests |
| `.github/agents/test-runner-validator.md` | Run and interpret the test suite |
| `.github/agents/code-review-specialist.md` | Evidence-backed PR code review |
| `.github/agents/comment-resolution-specialist.md` | Resolve reviewer comments with code or evidence |
| `.github/agents/feature-parity-specialist.md` | Analyze and close cross-SDK parity gaps |
| `.github/agents/bug-finder.md` | Find bugs through invariant scanning |
| `.github/agents/bug-fixer.md` | Fix proven bugs with regression tests |
| `.github/agents/change-verifier.md` | Final gate before declaring a change complete |
| `.github/agents/cleanup-refactor-specialist.md` | Safe, behavior-preserving cleanup |
| `.github/agents/deployment-readiness-reviewer.md` | Pre-release validation checklist |
| `.github/agents/self-reflection-improver.md` | Improve the Copilot OS itself |

## Skills Directory

| Skill | Purpose |
|---|---|
| `.github/skills/durabletask-go-test-execution/` | Exact test suite commands and flags |
| `.github/skills/durabletask-go-change-verification/` | Complete pre-PR validation sequence |
| `.github/skills/durabletask-go-feature-parity-analysis/` | Proto-grounded parity gap analysis |
| `.github/skills/durabletask-go-bug-forensics/` | Root cause tracing for orchestration bugs |
| `.github/skills/durabletask-go-code-review-evidence/` | Evidence collection for code review |
| `.github/skills/durabletask-go-release-readiness/` | Pre-tag release validation |
