---
name: durabletask-go Bug Forensics
description: >
  Deep forensic analysis skill for tracking down the root cause of bugs in
  durabletask-go. Covers the orchestration replay path, work item lifecycle,
  serialization, and concurrency. Use when a symptom is known but root cause is not.
triggers:
  - bug investigation
  - root cause analysis
  - orchestration stuck
  - unexpected status
  - test failure investigation
  - when Bug Finder or Bug Fixer agent needs deep analysis
---

# Skill: durabletask-go Bug Forensics

Use this skill to systematically trace a bug from symptom to root cause before writing any fix. Do not fix what you have not proven.

---

## When to Load This Skill

- When a test is failing and the assertion error does not immediately reveal the root cause.
- When an orchestration is stuck in an unexpected state.
- When the Bug Finder agent has identified a potential issue but needs verification.
- When a production symptom (e.g., "orchestration never completes") needs tracing.

---

## Forensic Framework

### Framework: The Work Item Lifecycle

Every orchestration execution follows this lifecycle:

```
CreateOrchestrationInstance
  → History: ExecutionStarted event written
  → NewEvents: ExecutionStarted event enqueued

GetOrchestrationWorkItem
  → Work item claimed (LockedBy set, LockExpiration set)
  → State loaded: OrchestrationRuntimeState

ExecuteOrchestrator
  → oldEvents replayed through OrchestrationContext
  → newEvents drive forward progress
  → Actions collected (schedule activities, create timers, etc.)

CompleteOrchestrationWorkItem
  → Actions written to History and NewEvents/NewTasks
  → Work item lock cleared

[Repeat for activity work items]
```

Any bug that causes an orchestration to "stick" is typically a failure at one of these transitions.

---

## Forensic Step 1: Identify the Stuck Transition

Given a symptom, identify which transition failed:

| Symptom | Likely stuck transition |
|---|---|
| Orchestration never starts | `CreateOrchestrationInstance` or `GetOrchestrationWorkItem` |
| Orchestration starts but never makes progress | `ExecuteOrchestrator` returning ErrTaskBlocked incorrectly |
| Activity scheduled but never executed | `GetActivityWorkItem` or the activity work item claiming |
| Activity completed but orchestration doesn't progress | `CompleteOrchestrationWorkItem` not writing the TaskCompleted event to NewEvents |
| Orchestration stuck after ContinueAsNew | Carryover events not preserved correctly |

### Forensic Step 2: Trace the Work Item Query

```bash
# In SQLite backend, find the relevant SQL queries
grep -n "SELECT\|UPDATE\|INSERT" backend/sqlite/sqlite.go | grep -i "workitem\|LockedBy\|NewEvents\|NewTasks"
```

For `GetOrchestrationWorkItem`: look for the atomic `SELECT + UPDATE` (or equivalent) that claims the item. If the claim is not atomic (not in a transaction), it can lead to double-processing.

For `CompleteOrchestrationWorkItem`: look for where `NewEvents` are inserted for the next execution cycle. If this is missing for any action type, the orchestration will not progress.

### Forensic Step 3: Inspect the Action Processing

The core of `CompleteOrchestrationWorkItem` processes orchestrator actions and writes them to the backend. Trace each action type:

```bash
# Find action processing in the SQLite backend
grep -n "ScheduleTask\|CreateTimer\|SendEvent\|CreateSubOrchestration\|CompleteOrchestration" \
  backend/sqlite/sqlite.go backend/postgres/postgres.go
```

For each action type, verify:
1. The action is read from `ExecutionResults.Actions`.
2. A corresponding row is inserted into `NewEvents` or `NewTasks`.
3. The `InstanceID` and `SequenceNumber` are set correctly so the corresponding `GetXxxWorkItem` call finds it.

### Forensic Step 4: Verify Replay Correctness

If the orchestration runs but produces wrong output:

```bash
# Find where oldEvents and newEvents are distinguished
grep -n "oldEvents\|newEvents\|IsReplaying" task/orchestrator.go backend/executor.go
```

The replay mechanism relies on events being in deterministic order. If a new event type changes the event sequence, existing history may be replayed incorrectly.

Key invariant: `len(pendingActions) == 0` at the end of replaying `oldEvents` means the orchestrator's logic is deterministic. If actions are generated during replay, there is a determinism bug.

### Forensic Step 5: Verify Serialization

If the bug involves data loss or corruption:

```bash
# Verify all EventPayload reads/writes go through MarshalHistoryEvent
grep -n "EventPayload\|MarshalHistoryEvent\|UnmarshalHistoryEvent" \
  backend/sqlite/sqlite.go backend/postgres/postgres.go
```

Any direct JSON serialization of a `HistoryEvent` to an `EventPayload` column is a bug — history events are protobuf.

### Forensic Step 6: Goroutine Leak Check

For concurrency-related symptoms (CPU spinning, never-completing operations):

```bash
# Look at all goroutine starts in the hot path
grep -n "go func\|go be\.\|go executor\." \
  backend/executor.go backend/worker.go client/worker_grpc.go
```

For each goroutine, verify it has a `select { case <-ctx.Done(): return }` path.

---

## Root Cause Evidence Requirements

A root cause is proven when you can state:
1. **The violated invariant**: "The `LockedBy` field is set, but `CompleteOrchestrationWorkItem` exits without clearing it if `err != nil` at line X."
2. **The trigger condition**: "This happens when `ExecuteOrchestrator` returns an error."
3. **The observable consequence**: "The work item remains locked until `LockExpiration` expires, causing the orchestration to be retried with `RetryCount` incremented."
4. **A reproduction path**: "Run `TestSomeBehavior`, kill the process at the moment it executes line X, restart, and observe the work item in retry state."

Without all four elements, the root cause is not proven.

---

## Exit Criteria

This skill is complete when:
- [ ] The stuck transition is identified.
- [ ] The invariant violation is proven with file + line evidence.
- [ ] A reproduction path is documented (test or manual).
- [ ] The fix is ready for the Bug Fixer agent (or documented as "needs further investigation: <reason>").
