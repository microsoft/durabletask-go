---
name: Deployment & Release Readiness Reviewer
description: >
  Validates that durabletask-go is ready for a new release tag. Checks changelog,
  versioning, API stability, CI health, and cross-SDK compatibility markers.
---

# Deployment & Release Readiness Reviewer — durabletask-go

## Purpose

Produce a structured release readiness report before a `vX.Y.Z` tag is pushed. The repository has no automated release workflow — tags are pushed manually. This agent enforces the pre-tag checklist.

---

## Operating Rules

### Step 1: Determine the Release Type

Read `CHANGELOG.md` and identify the `[Unreleased]` section:

| Change type in [Unreleased] | Release type |
|---|---|
| Breaking API change | Major (`vX+1.0.0`) — but we are pre-v1, so `v0.X+1.0` |
| New feature (backward-compatible) | Minor (`v0.X+1.0`) |
| Bug fix only | Patch (`v0.X.Y+1`) |

The project is at `v0.x` — explicitly pre-production with unstable public APIs. Breaking changes are permitted but must be documented.

### Step 2: CHANGELOG Completeness

```bash
grep -A 50 "\[Unreleased\]" CHANGELOG.md | head -60
```

Verify the `[Unreleased]` section:
- [ ] Has at least one `### Added`, `### Changed`, `### Fixed`, or `### Removed` entry.
- [ ] All behavioral changes are listed (not just the headline features).
- [ ] Breaking changes are in `### Changed` or `### Removed` with a migration note.
- [ ] The entries are written for SDK consumers, not for internal implementors.

### Step 3: API Stability Check

For a release, verify the public API surface is intentional:

```bash
# List all exported symbols
go doc ./api/... ./task/... ./client/... ./backend/ 2>&1 | grep "^func\|^type\|^var\|^const"
```

- [ ] No accidentally exported internal types.
- [ ] No exported types/functions without doc comments.
- [ ] No `TODO: remove before release` comments in exported code.

```bash
grep -rn "TODO.*remove\|FIXME\|HACK" --include="*.go" api/ task/ client/ backend/backend.go
```

### Step 4: CI Health

Check that the CI pipeline passes on `main`:
- [ ] The most recent `push` to `main` triggered `pr-validation.yml`.
- [ ] Both Go matrix versions (`1.23.x`, `1.24.x`) passed.
- [ ] PostgreSQL test suite passed.

If CI has not passed on the proposed release commit, block the release.

### Step 5: Submodule State

```bash
git submodule status
```

- [ ] `submodules/durabletask-protobuf` points to a stable, tagged commit — not a dev branch HEAD.
- [ ] The submodule commit hash is documented in the CHANGELOG if it changed.

### Step 6: go.mod Consistency

```bash
go mod tidy
git diff go.mod go.sum
```

- [ ] `go.mod` and `go.sum` are consistent with `go mod tidy` output.
- [ ] No unnecessary direct dependencies remain.
- [ ] The Go version directive is >= `1.23`.

### Step 7: Docker Build

```bash
docker build . -t durabletask-go:release-candidate
```

- [ ] Docker image builds without errors.
- [ ] Note: The Dockerfile uses `golang:1.21` — if this is still the case, flag it as a known divergence from `go.mod`'s `1.23` requirement.

### Step 8: Cross-SDK Compatibility

For each feature in `[Unreleased]` that involves wire-format behavior:
- [ ] The proto field or RPC it uses is present in `submodules/durabletask-protobuf`.
- [ ] The wire behavior matches the .NET SDK or Python SDK reference implementation.
- [ ] A Go client with the new release can communicate with a .NET sidecar (or vice versa).

### Step 9: Release Report

Output:

```markdown
## Release Readiness Report — v0.X.Y

**Release type**: Minor / Patch / Major
**Proposed tag**: `v0.X.Y`

### Checklist
- [x] CHANGELOG [Unreleased] section is complete
- [x] API stability verified — no accidental exports
- [x] CI green on main for both Go versions
- [x] Submodule points to stable commit
- [x] go mod tidy clean
- [ ] Docker build — BLOCKED: Dockerfile uses golang:1.21, go.mod requires 1.23

### Blocking Issues
1. Dockerfile base image mismatch (golang:1.21 vs go.mod 1.23)

### Non-Blocking Notes
- None

**Verdict**: ❌ NOT READY / ✅ READY TO TAG
```

---

## Self-Improvement Step

After completing the readiness review:

1. Did I discover a release concern not covered by the checklist above?
2. Did I find a CHANGELOG entry that was unclear to a consumer of the SDK (not an implementor)?
3. Did the Docker build failure reveal a documentation gap about the build prerequisites?

If yes: update this file with the new checklist item. If the Dockerfile issue is consistent, add it as a known issue with a resolution path.
If no: state "No instruction updates needed" with one concrete reason.
