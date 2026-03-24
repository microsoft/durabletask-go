---
name: durabletask-go Test Execution
description: >
  Stepwise skill for running the durabletask-go test suite with correct flags,
  coverage targets, and environment setup. Loads when an agent needs to
  execute tests or validate test results.
triggers:
  - running tests
  - validating test results
  - checking test coverage
  - before claiming implementation complete
---

# Skill: durabletask-go Test Execution

Use this skill whenever you need to run the durabletask-go test suite. The test suite has specific flags and environment requirements — do not improvise.

---

## When to Load This Skill

- After implementing a change, before declaring it complete.
- When the Test Runner & Validator agent is active.
- When the Change Verifier agent reaches the test step.
- When investigating a test failure.

---

## Prerequisites

```bash
# Verify go is installed and correct version
go version
# Must be >= 1.23

# Verify golangci-lint is installed
golangci-lint --version
# Used in CI: golangci-lint-action@v7

# For PostgreSQL tests (optional):
# postgres:16 must be running on localhost:5432
# Credentials: default postgres user with password "postgres" (matches CI POSTGRES_PASSWORD)
```

---

## Execution Steps

### Step 1: Build Check

```bash
go build ./...
```

Expected: no output, exit 0. Any output = failure.

### Step 2: Vet

```bash
go vet ./...
```

Expected: no output, exit 0.

### Step 3: Lint

```bash
golangci-lint run
```

Expected: no violations outside `internal/protos/` and `tests/mocks/`.

Configuration file: `.golangci.yml`
Enabled linters: `errcheck`, `govet`, `ineffassign`, `staticcheck`, `unused`, `gocritic`, `errorlint`

### Step 4: Run Tests — Fast (SQLite Only)

```bash
go test ./tests/... -timeout 120s
```

Expected: `ok  	github.com/microsoft/durabletask-go/tests`

This runs the full integration test suite against an in-memory SQLite backend. No external services required.

### Step 5: Run Tests with Coverage

```bash
go test ./tests/... -timeout 120s \
  -coverpkg ./api,./task,./client,./backend/...,./internal/helpers \
  -coverprofile=coverage.out

go tool cover -func=coverage.out | grep "total:"
```

Coverage packages are fixed:
- `./api` — public API types
- `./task` — orchestrator/activity runtime
- `./client` — gRPC client
- `./backend/...` — Backend interface + SQLite + PostgreSQL implementations + workers
- `./internal/helpers` — history event constructors and tracing utilities

**Not included in coverage**: `internal/protos/` (generated), `tests/mocks/` (generated).

### Step 6: Run Specific Test

```bash
go test ./tests/... -run TestFunctionName -v -timeout 30s
```

Use `-v` to see per-test output. Use `-run TestFunctionName` to narrow to a specific test function.

### Step 7: Run PostgreSQL Tests (Optional — Requires postgres:16)

```bash
POSTGRES_ENABLED=true go test ./tests/... -timeout 180s \
  -coverpkg ./api,./task,./client,./backend/...,./internal/helpers
```

If PostgreSQL is not available, tests with `os.Getenv("POSTGRES_ENABLED") == "true"` guards will be skipped automatically.

---

## Interpreting Results

| Output pattern | Meaning | Action |
|---|---|---|
| `FAIL	github.com/microsoft/durabletask-go/tests [build failed]` | Compilation error in test code | Fix the compilation error first |
| `--- FAIL: TestXxx (0.00s)` | Test assertion failed | Read the assertion error, find the failing expectation |
| `panic: ...` | Uncaught panic in test or production code | Check if `ErrTaskBlocked` is leaking out of the orchestrator |
| `FAIL (timeout)` | Test timed out | Usually indicates orchestration stuck waiting for event; add context deadline |
| `SKIP: ...` | Test skipped | Check the skip condition; likely `POSTGRES_ENABLED` guard |

---

## Exit Criteria

This skill is complete when:
- [ ] `go build ./...` exits 0.
- [ ] `go vet ./...` produces no output.
- [ ] `golangci-lint run` produces no violations in non-generated code.
- [ ] `go test ./tests/...` passes all tests with 0 failures.
- [ ] Coverage percentage has been reported (regression check is the caller's responsibility).
