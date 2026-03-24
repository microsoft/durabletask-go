---
name: durabletask-go Change Verification
description: >
  Comprehensive change verification skill for durabletask-go. Validates build, vet, lint,
  tests, generated code consistency, and documentation. Use as the final gate before
  declaring any change complete.
triggers:
  - before declaring a change complete
  - before opening a PR
  - when the Change Verifier agent runs
---

# Skill: durabletask-go Change Verification

Use this skill as the mandatory final gate before declaring any code change complete. It combines build, test, lint, and consistency checks into a single pass/fail verdict.

---

## When to Load This Skill

- Immediately before pushing a branch for review.
- When the Change Verifier agent is active.
- After addressing PR review comments.

---

## Execution Sequence

Run all steps in order. Do not skip a step if an earlier step fails — report all failures.

### Gate 1: Build

```bash
go build ./...
```

**Pass**: No output, exit 0.
**Fail**: Report the error. This blocks all subsequent steps.

### Gate 2: Generated Code Consistency

```bash
# Check whether proto files are newer than generated code
stat -c "%Y %n" \
  submodules/durabletask-protobuf/protos/orchestrator_service.proto \
  internal/protos/orchestrator_service.pb.go \
  internal/protos/orchestrator_service_grpc.pb.go
```

If the proto file has a newer modification time than the generated files, regenerate:
```bash
protoc --go_out=. --go-grpc_out=. \
  -I ./submodules/durabletask-protobuf/protos \
  orchestrator_service.proto
```

Also check that mock files match their source interfaces:
```bash
# If backend.Backend interface changed, check mock is updated
grep "func (m \*MockBackend)" tests/mocks/Backend.go | wc -l
grep "^\t[A-Z]" backend/backend.go | grep "context.Context" | wc -l
# The method counts should be consistent
```

**Pass**: Generated code is up to date or was regenerated successfully.

### Gate 3: Vet

```bash
go vet ./...
```

**Pass**: No output.

### Gate 4: Lint

```bash
golangci-lint run
```

**Pass**: No violations in non-excluded paths.
**Excluded** (expected to have lint flags): `internal/protos/`, `tests/mocks/`.

### Gate 5: Tests

```bash
go test ./tests/... -timeout 120s
```

**Pass**: All tests pass. `go test` exits 0.

### Gate 6: Coverage Baseline

```bash
go test ./tests/... -timeout 120s \
  -coverpkg ./api,./task,./client,./backend/...,./internal/helpers \
  -coverprofile=coverage.out
go tool cover -func=coverage.out | grep "total:"
```

**Pass**: Total coverage ≥ pre-change baseline (if known). If baseline is unknown, record the current percentage.

### Gate 7: Commit Convention

Verify the commit message starts with a valid Conventional Commits prefix:
- `feat:`, `fix:`, `refactor:`, `chore:`, `docs:`, `test:`

And that the prefix matches the type of change in the diff.

### Gate 8: CHANGELOG

If the change introduces or modifies behavior visible to SDK consumers:
```bash
grep "\[Unreleased\]" CHANGELOG.md
```

**Pass**: `[Unreleased]` section exists and contains an entry describing this change.

### Gate 9: No Debug Leftovers

```bash
grep -rn "fmt.Println\|log.Print\b\|t.Log(" --include="*.go" \
  api/ task/ backend/ client/ internal/helpers/ | \
  grep -v "_test.go"
```

**Pass**: No debug print statements in non-test production code.

---

## Verdict Format

```
Build:             ✅ / ❌
Generated code:    ✅ / ⚠️ (stale — regenerated) / ❌
Vet:               ✅ / ❌
Lint:              ✅ / ❌
Tests:             ✅ N passed / ❌ N failed
Coverage:          X% (baseline: Y%)
Commit convention: ✅ / ⚠️
CHANGELOG:         ✅ / ⚠️ (missing)
Debug leftovers:   ✅ / ⚠️ (list)

Overall: READY / BLOCKED
```

If BLOCKED, list each blocking issue with the file and line.

---

## Exit Criteria

All of the following must be true:
- [ ] `go build ./...` exits 0
- [ ] `go vet ./...` produces no output
- [ ] `golangci-lint run` produces no non-excluded violations
- [ ] `go test ./tests/...` passes all tests
- [ ] Generated files are consistent with source
- [ ] Commit prefix matches change type
- [ ] CHANGELOG updated (if behavioral change)
- [ ] No debug prints in production code
