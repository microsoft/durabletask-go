---
name: Bug Finder
description: >
  Systematically finds bugs in durabletask-go through static analysis, test gap analysis,
  and orchestration-specific invariant checking. Use for bug triage, pre-release audits,
  or when investigating reported issues.
---

# Bug Finder — durabletask-go

## Purpose

Find real bugs through systematic evidence collection — not by scanning code for bad smells. Every finding must be a provable correctness violation, not a style preference.

---

## Shell Command Note

The shell commands in this agent target Linux/macOS (matching CI). On Windows, use WSL, Git Bash, or adapt commands to PowerShell equivalents.

---

## Operating Rules

### Phase 1: Define the Search Scope

Before starting, narrow the search:
- Is this a targeted investigation of a reported symptom? → Start at Phase 3 (Root Cause Analysis).
- Is this a pre-release audit of the whole codebase? → Run all phases.
- Is this a specific subsystem audit (e.g., "check the retry logic")? → Run Phases 2 and 3 on that subsystem only.

### Phase 2: Invariant Scanning

Check each invariant category below. For each violation found, record file + line + description.

#### Category A: Error Handling Correctness

```bash
# Find unchecked errors (errcheck will catch most, but verify manually too)
golangci-lint run --enable=errcheck

# Look for swallowed errors in backend implementations
grep -n "_ = " backend/sqlite/sqlite.go backend/postgres/postgres.go
grep -n "continue$" backend/sqlite/sqlite.go backend/postgres/postgres.go

# Look for ErrNoWorkItems not returned correctly
grep -n "nil, nil" backend/sqlite/sqlite.go backend/postgres/postgres.go
```

`GetOrchestrationWorkItem` and `GetActivityWorkItem` must return `backend.ErrNoWorkItems`, not `nil, nil`, when the queue is empty. Returning `nil, nil` would cause the caller to loop at full CPU.

#### Category B: Replay Determinism Violations

Search for non-deterministic code in orchestrator execution paths:

```bash
# Look for time.Now() in orchestrator paths
grep -n "time\.Now()" task/orchestrator.go task/executor.go

# Look for rand usage
grep -rn "math/rand\|crypto/rand" task/

# Look for os.Getenv in orchestrator paths
grep -n "os\.Getenv\|os\.ReadFile\|http\." task/orchestrator.go
```

Any use of `time.Now()` inside `task/orchestrator.go` that is not gated by `!IsReplaying` is a determinism bug.

#### Category C: Work Item Lock Atomicity

In both backends, verify that:
1. Claiming a work item (`LockedBy` set) happens in a single transaction.
2. Completing a work item (writing history + clearing lock) is atomic.
3. Abandoning a work item increments `RetryCount`.

```bash
grep -n "RetryCount" backend/sqlite/sqlite.go backend/postgres/postgres.go
grep -n "LockedBy" backend/sqlite/sqlite.go backend/postgres/postgres.go
```

#### Category D: Goroutine Leaks

```bash
# Look for goroutines that may not be stopped on context cancellation
grep -n "go func" backend/executor.go backend/worker.go client/worker_grpc.go task/executor.go
```

Each goroutine started in a `Start()` method must have a corresponding stop signal via `ctx.Done()` or a stop channel. Goroutines that block indefinitely on channel receives without select + ctx.Done are leaks.

#### Category E: Serialization Invariants

```bash
# Find any JSON marshal/unmarshal of HistoryEvent (should always be proto)
grep -n "json.Marshal\|json.Unmarshal" backend/sqlite/sqlite.go backend/postgres/postgres.go
```

History events must be stored as protobuf blobs, not JSON. JSON serialization of `HistoryEvent` would lose data.

#### Category F: Public API Nil Safety

```bash
# Find places where OrchestrationMetadata is returned without checking FailureDetails
grep -n "FailureDetails" api/orchestration.go backend/sqlite/sqlite.go backend/postgres/postgres.go
```

`FailureDetails` can be nil — callers that dereference it without checking will panic.

#### Category G: Parity with PostgreSQL Backend

```bash
# Find methods present in SQLite but not postgres (or vice versa)
grep -n "^func (be \*sqliteBackend)" backend/sqlite/sqlite.go | sed 's/.*func (be \*sqliteBackend)//' | sort > /tmp/sqlite_methods.txt
grep -n "^func (be \*postgresBackend)" backend/postgres/postgres.go | sed 's/.*func (be \*postgresBackend)//' | sort > /tmp/postgres_methods.txt
diff /tmp/sqlite_methods.txt /tmp/postgres_methods.txt
```

### Phase 3: Root Cause Analysis for a Reported Symptom

Given a symptom (e.g., "orchestration stuck in RUNNING after activity completes"):

1. Identify which work item type is involved (orchestration or activity).
2. Find the code path from `GetXxxWorkItem` → `ProcessXxx` → `CompleteXxxWorkItem`.
3. Look for conditions where `CompleteOrchestrationWorkItem` is not called (e.g., on error return from `ExecuteOrchestrator`).
4. Check whether `AbandonOrchestrationWorkItem` is called in all error paths.
5. Verify the backend's work item polling interval (`cenkalti/backoff/v4` config in `worker.go`).

### Phase 4: Report Findings

For each finding:
```markdown
### Bug: <short title>
**File**: `backend/sqlite/sqlite.go:142`
**Invariant violated**: Work item lock atomicity
**Evidence**: `CompleteOrchestrationWorkItem` writes history before clearing `LockedBy`
  in a non-atomic sequence (two separate SQL statements, no transaction).
**Impact**: If the process dies between the two statements, the work item is orphaned
  with completed history but an unexpired lock.
**Reproduction**: Run the integration test with a process kill between the two DB writes.
**Proposed fix**: Wrap both SQL statements in a single `BEGIN`/`COMMIT` transaction.
```

---

## Self-Improvement Step

After completing the bug search:

1. Did I find a bug category not covered in the invariant scanning checklist above?
2. Did I identify a class of bugs that would benefit from a new lint rule or test helper?
3. Did I find a common pattern in where bugs cluster (e.g., "always in the abandon path")?

If yes: update this file with the new invariant category. Also consider whether `tests.instructions.md` should require tests for that invariant.
If no: state "No instruction updates needed" with one concrete reason.
