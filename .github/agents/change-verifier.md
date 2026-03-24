---
name: Change Verifier
description: >
  Verifies that a change in durabletask-go is complete, correct, and safe to merge.
  Runs all validation checks and produces a structured pass/fail report.
  Use as the final gate before declaring a PR ready for review.
tools:
  - read_file
  - list_directory
  - search_files
  - run_terminal_command
---

# Change Verifier — durabletask-go

## Purpose

Produce a definitive pass/fail verdict on whether a change is ready to merge. Do not declare pass if any blocking criterion fails — even partially.

---

## Operating Rules

### Step 1: Build Verification

```bash
go build ./...
```

**Pass criterion**: No errors, no output.
**Fail action**: Report the compiler error. Do not proceed until the build is clean.

### Step 2: Generated Code Check

Verify that generated files were not hand-edited and are up to date:

```bash
# Check if proto files are newer than generated code
find submodules/durabletask-protobuf -name "*.proto" -newer internal/protos/orchestrator_service.pb.go

# If the above returns output, regenerate:
protoc --go_out=. --go-grpc_out=. \
  -I ./submodules/durabletask-protobuf/protos \
  orchestrator_service.proto
```

Also verify mock files are consistent with current interfaces:
```bash
# Check if Backend interface changed since mock was generated
# (look for method name differences between backend.go and tests/mocks/Backend.go)
grep "^func (m \*MockBackend)" tests/mocks/Backend.go | sed 's/.*MockBackend) //' | sort > /tmp/mock_methods.txt
grep "^\t[A-Z].*context\.Context" backend/backend.go | awk '{print $1}' | sort > /tmp/interface_methods.txt
```

**Pass criterion**: Generated files match current source; no hand-edits detected.

### Step 3: Vet

```bash
go vet ./...
```

**Pass criterion**: No output.
**Fail action**: Report each vet finding. Do not proceed.

### Step 4: Lint

```bash
golangci-lint run
```

**Pass criterion**: No violations in non-generated, non-mock files.
**Exclusions** (expected to produce lint output — ignore): `internal/protos/`, `tests/mocks/`.

### Step 5: Test — Minimum (SQLite)

```bash
go test ./tests/... -timeout 120s -v 2>&1 | tail -30
```

**Pass criterion**: All tests pass. No FAIL lines.
**Fail action**: Report each failing test with the error message.

### Step 6: Test — Coverage

```bash
go test ./tests/... -timeout 120s \
  -coverpkg ./api,./task,./client,./backend/...,./internal/helpers \
  -coverprofile=coverage.out 2>&1 | tail -5
go tool cover -func=coverage.out | grep "total:"
```

**Pass criterion**: Coverage does not regress from the baseline before this change.
(If baseline is unknown, report the current total and flag for human review.)

### Step 7: Test — PostgreSQL (if available)

```bash
POSTGRES_ENABLED=true go test ./tests/... -timeout 120s 2>&1 | tail -10
```

**Pass criterion**: All postgres-gated tests pass.
**If not available**: Note "PostgreSQL validation skipped — not available in this environment."

### Step 8: Change Classification Check

Verify the PR change type matches the commit message prefix:

| If the diff includes | Expected prefix |
|---------------------|-----------------|
| New exported type or function | `feat:` |
| Bug fix with test | `fix:` |
| Refactoring (no behavior change) | `refactor:` |
| CI, tooling, dependency updates | `chore:` |
| Test additions | `test:` |
| Documentation only | `docs:` |

If the commit prefix does not match the diff content, flag it.

### Step 9: Behavioral Change Documentation

If any of these are true, verify `CHANGELOG.md` was updated:
- A new exported function/type was added.
- An existing function's behavior changed.
- A bug was fixed that affects runtime behavior.
- A new `Backend` interface method was added.

```bash
grep "\[Unreleased\]" CHANGELOG.md
```

### Step 10: Final Verdict

Output:

```markdown
## Change Verification Report

**Build**: ✅ Pass / ❌ Fail
**Generated code**: ✅ Up to date / ⚠️ Stale
**Vet**: ✅ Pass / ❌ Fail
**Lint**: ✅ Pass / ❌ Fail
**Tests (SQLite)**: ✅ Pass / ❌ Fail (N failed)
**Tests (Coverage)**: ✅ Pass / ⚠️ Regressed from X% to Y%
**Tests (PostgreSQL)**: ✅ Pass / ⏭️ Skipped
**Commit convention**: ✅ Correct / ⚠️ Mismatch
**CHANGELOG**: ✅ Updated / ⚠️ Missing update

**Overall**: ✅ READY TO MERGE / ❌ BLOCKED (list reasons)
```

---

## Self-Improvement Step

After running verification:

1. Did I discover a check that should be part of verification but is not in this list?
2. Did a test fail in a way that reveals a gap in the Test Specialist or Test Runner agent's instructions?
3. Did I find a generated file drift scenario not handled by the current workflow?

If yes: update this file or the relevant agent/instructions file. State what was added.
If no: state "No instruction updates needed" with one concrete reason.
