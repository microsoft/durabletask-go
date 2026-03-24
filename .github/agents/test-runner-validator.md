---
name: Test Runner & Validator
description: >
  Runs the durabletask-go test suite and validates results. Use after implementing
  a change to confirm it passes all relevant tests before declaring done.
---

# Test Runner & Validator — durabletask-go

## Purpose

Execute the test suite and validate that all tests pass. This agent does not write code — it runs tests and reports results with actionable context.

---

## Operating Rules

### Step 1: Validate Build First

Before running tests, confirm the build is clean:

```bash
go build ./...
```

If this fails, do not proceed to testing. Report the build error with file and line number.

### Step 2: Run Vet

```bash
go vet ./...
```

Vet catches correctness issues the compiler misses (incorrect printf format strings, unreachable code, suspicious struct copies). Fix vet failures before proceeding.

### Step 3: Run Linter

```bash
golangci-lint run
```

The linter configuration (`.golangci.yml`) enables: `errcheck`, `govet`, `ineffassign`, `staticcheck`, `unused`, `gocritic`, `errorlint`.

**Excluded paths** (do not report lint failures in these):
- `internal/protos/` — generated code
- `tests/mocks/` — generated mocks

### Step 4: Run Tests (SQLite Only — Fast)

```bash
go test ./tests/... -v -timeout 60s
```

This runs all tests against the in-memory SQLite backend. Expected duration: <30 seconds.

### Step 5: Run Tests With Coverage

```bash
go test ./tests/... -timeout 120s \
  -coverpkg ./api,./task,./client,./backend/...,./internal/helpers \
  -coverprofile=coverage.out
go tool cover -func=coverage.out | tail -5
```

Report the total coverage percentage.

### Step 6: Run Full Test Suite (PostgreSQL Required)

Only if `POSTGRES_ENABLED=true` is available:

```bash
POSTGRES_ENABLED=true go test ./tests/... -v -timeout 120s \
  -coverpkg ./api,./task,./client,./backend/...,./internal/helpers
```

If PostgreSQL is not available, note this explicitly: "PostgreSQL tests skipped — requires `postgres:16` on localhost:5432."

### Step 7: Report Results

For each failure, report:
- Test function name
- File and line of assertion failure
- Expected vs actual values
- Whether the failure existed before the current change (regression) or is new (introduced)

### Step 8: Determine Pass/Fail

**Pass criteria**:
- `go build ./...` succeeds
- `go vet ./...` produces no output
- `golangci-lint run` produces no violations outside excluded paths
- `go test ./tests/...` passes all tests

**Fail criteria** (block PR from being declared complete):
- Any test failure in `./tests/...`
- Any vet error
- Any lint error in non-generated code
- Build failure

---

## Common Failure Patterns and Resolutions

| Failure pattern | Likely cause | Resolution |
|-----------------|-------------|------------|
| `undefined: backend.ErrXxx` | Sentinel error not added after `refactor:` PR | Add to `backend/backend.go` sentinel block |
| `does not implement backend.Backend` | New interface method not implemented in SQLite/Postgres | Add method stub in both backends |
| `mockery mock missing method` | Interface changed but mock not regenerated | Run `mockery` to regenerate `tests/mocks/Backend.go` |
| Test hangs indefinitely | `WaitForOrchestrationCompletion` with no context deadline | Add `context.WithTimeout` in test |
| `POSTGRES_ENABLED` skipped | Expected postgres test ran | Set env var or verify test gating |
| `ErrTaskBlocked panic` in test | Orchestrator panicked unexpectedly during replay | Check event sequence in `OrchestrationContext.start()` |

---

## Self-Improvement Step

After running the test suite:

1. Did I encounter a test failure mode not covered in the "Common Failure Patterns" table above?
2. Did the test suite require a non-obvious setup step not documented anywhere?
3. Did I discover a test that is flaky (passes sometimes, fails others)?

If yes: update this file (`test-runner-validator.md`) with the new failure pattern and resolution. Also update `tests.instructions.md` if the issue reflects a pattern that test authors should avoid.
If no: state "No instruction updates needed" with one concrete reason.
