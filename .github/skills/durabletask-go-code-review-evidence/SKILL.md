---
name: durabletask-go Code Review Evidence
description: >
  Evidence collection skill for durabletask-go code review. Gathers the specific
  facts an agent needs to review a change: interface contracts, test coverage,
  serialization impact, and parity concerns. Use at the start of any code review.
triggers:
  - starting a code review
  - reviewing a PR
  - when Code Review Specialist agent begins
---

# Skill: durabletask-go Code Review Evidence

Use this skill at the start of a code review to gather the evidence needed to review the change accurately. Do not review based on the diff alone — collect context first.

---

## When to Load This Skill

- When the Code Review Specialist agent begins a review.
- When a reviewer asks for help reviewing a PR in durabletask-go.
- Before writing any review comment.

---

## Evidence Collection Steps

### Step 1: Classify the Change

```bash
# Read the diff summary
git diff main...HEAD --stat
git diff main...HEAD --name-only
```

Classify:
- Which packages are changed?
- Is this a `feat:`, `fix:`, `refactor:`, or `chore:`?
- Are generated files changed? (If `internal/protos/` or `tests/mocks/` — verify they are regenerated, not hand-edited.)

### Step 2: Interface Impact Assessment

```bash
# If backend.go changed, check all implementations
git diff main...HEAD -- backend/backend.go

# If interface changed, check both backends
git diff main...HEAD -- backend/sqlite/sqlite.go
git diff main...HEAD -- backend/postgres/postgres.go

# Check mock consistency
git diff main...HEAD -- tests/mocks/Backend.go
```

For each exported method added to or removed from `Backend`, `Executor`, or `TaskHubWorker`:
- Both `sqlite.go` and `postgres.go` must implement it.
- `tests/mocks/Backend.go` (or equivalent) must be regenerated.

### Step 3: Test Coverage Evidence

```bash
# What tests exist for the changed code?
git diff main...HEAD --name-only | grep -v "_test.go" | while read f; do
  base=$(basename "$f" .go)
  echo "Looking for tests of: $base"
  grep -rn "Test.*${base^}\|func.*test.*${base}" tests/ task/
done

# Run tests to get current coverage
go test ./tests/... -timeout 60s \
  -coverpkg ./api,./task,./client,./backend/...,./internal/helpers \
  -coverprofile=coverage.out 2>&1
go tool cover -func=coverage.out | grep -E "total:|$(git diff main...HEAD --name-only | grep -v "_test.go" | head -5 | tr '\n' '|')"
```

### Step 4: Serialization Impact Assessment

```bash
# Does the diff touch serialization?
git diff main...HEAD | grep -E "json\.|proto\.|Marshal|Unmarshal|EventPayload"
```

If yes:
- Is a new field added to `OrchestrationMetadata`? Verify both `MarshalJSON` and `UnmarshalJSON` handle it.
- Is a new `HistoryEvent` type created? Verify it is stored as protobuf (`MarshalHistoryEvent`), not JSON.
- Is an existing field's JSON key changed? This is a breaking change.

### Step 5: Orchestration Determinism Assessment

```bash
# Does the diff touch the orchestrator execution path?
git diff main...HEAD -- task/orchestrator.go task/executor.go

# Look for time.Now() in changed orchestrator code
git diff main...HEAD | grep "+.*time\.Now()"

# Look for new goroutines in the orchestrator path
git diff main...HEAD | grep "+.*go func\|+.*go ctx\."
```

Any `time.Now()` in the orchestrator path that is not gated by `!IsReplaying` is a blocker.

### Step 6: Error Handling Assessment

```bash
# Look for new error returns in the diff
git diff main...HEAD | grep -E "^\+.*fmt\.Errorf|^\+.*errors\.New"

# Verify error wrapping uses %w not %s
git diff main...HEAD | grep "+.*fmt\.Errorf" | grep -v "%w"  # these are suspect
```

### Step 7: Parity Assessment (if applicable)

```bash
# Does the diff reference a proto field by name?
git diff main...HEAD | grep -E "IdReusePolicy|scheduledStart|recursive|customStatus|version"

# Verify proto field usage is consistent with the proto spec
grep -n "$(git diff main...HEAD | grep '+.*protos\.' | head -3 | awk -F'.' '{print $NF}' | tr '\n' '\|')" \
  submodules/durabletask-protobuf/protos/orchestrator_service.proto
```

---

## Evidence Summary Template

At the end of evidence collection, produce:

```markdown
## Review Evidence Summary

**Change type**: feat / fix / refactor / chore
**Packages changed**: api/, backend/, task/, client/
**Generated files changed**: Yes (regenerated) / No / Yes (hand-edited — FLAG)

**Interface impact**:
- backend.Backend: N methods added/removed
- SQLite implementation: Updated / Missing update (BLOCKER)
- PostgreSQL implementation: Updated / Missing update (BLOCKER)
- Mocks: Regenerated / Stale (BLOCKER)

**Test coverage**:
- New tests: Yes / No
- Failing before change: Yes / No (if No, how is this verified?)
- Coverage: X% (before: Y%)

**Serialization impact**: None / JSON field change / Proto field change / Breaking change
**Determinism impact**: None / New code in orchestrator path (see line X)
**Error handling**: %w wrapping used / %s used (BLOCKER at lines: X, Y)
**Parity**: Not applicable / Proto field <name> correctly populated
```

---

## Exit Criteria

This skill is complete when:
- [ ] Change classification is established.
- [ ] Interface impact is assessed.
- [ ] Test coverage evidence is gathered.
- [ ] Serialization impact is assessed.
- [ ] Determinism impact is assessed.
- [ ] Error handling patterns are verified.
- [ ] Evidence summary is produced.

The Code Review Specialist agent then uses this summary to write review comments.
