---
name: durabletask-go Release Readiness
description: >
  Pre-release validation skill for durabletask-go. Validates CHANGELOG, API surface,
  CI health, submodule state, and cross-SDK compatibility before tagging a release.
triggers:
  - preparing a release
  - before pushing a version tag
  - when Deployment Readiness Reviewer agent runs
---

# Skill: durabletask-go Release Readiness

Use this skill before pushing a `vX.Y.Z` tag to validate that the release is complete and correct. The repository has no automated release workflow — tags are pushed manually.

---

## When to Load This Skill

- When the Deployment & Release Readiness Reviewer agent is active.
- When a maintainer asks "is this ready to release?"
- Before pushing a git tag.

---

## Step 1: Determine Release Version

```bash
# Check the most recent tag
git tag --sort=-version:refname | head -5

# Read the [Unreleased] section
grep -A 100 "\[Unreleased\]" CHANGELOG.md | head -50
```

Determine the release type:

| `[Unreleased]` contains | Release type |
|---|---|
| `### Added` (new capability) | Minor: `v0.X+1.0` |
| `### Fixed` or `### Changed` (no new API) | Patch: `v0.X.Y+1` |
| `### Removed` or breaking `### Changed` | Minor at minimum: `v0.X+1.0` |

The project is pre-v1 (`v0.x`). "Breaking" is permitted but must be listed under `### Changed` or `### Removed`.

---

## Step 2: CHANGELOG Validation

```bash
grep -n "\[Unreleased\]\|\[0\." CHANGELOG.md | head -10
```

**Required**:
- [ ] `[Unreleased]` section exists.
- [ ] At least one entry under `Added`, `Changed`, `Fixed`, or `Removed`.
- [ ] Every behavioral change since the last tag is listed.
- [ ] No `<!-- TODO -->` placeholders.
- [ ] Entries are written from the consumer's perspective (not the implementor's).

**Format check**: The project follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## Step 3: CI Health Verification

```bash
# Check that main is passing CI
# (requires GitHub CLI or manual verification)
gh run list --branch main --limit 5 --workflow pr-validation.yml
```

**Required**:
- [ ] The most recent `pr-validation.yml` run on `main` succeeded.
- [ ] Both Go `1.23.x` and `1.24.x` matrix jobs passed.
- [ ] PostgreSQL integration tests passed.

---

## Step 4: API Surface Check

```bash
# List all exported symbols in public packages
go doc ./api/... 2>&1 | grep -E "^(func|type|var|const)"
go doc ./task/... 2>&1 | grep -E "^(func|type|var|const)"
go doc ./client/... 2>&1 | grep -E "^(func|type|var|const)"
go doc ./backend 2>&1 | grep -E "^(func|type|var|const)"
```

**Checks**:
- [ ] No accidentally exported internal types (e.g., `sqliteBackend`).
- [ ] All exported types and functions have doc comments.
- [ ] No `TODO: remove before v1` or `TODO: deprecate` in exported code.

```bash
grep -rn "TODO.*v1\|TODO.*deprecat\|TODO.*remove" --include="*.go" api/ task/ client/ backend/backend.go
```

---

## Step 5: Submodule State

```bash
git submodule status
```

**Required**:
- [ ] `submodules/durabletask-protobuf` is at a specific commit (not a detached HEAD from an untracked commit).
- [ ] If the submodule was updated since the last release, note the new commit hash in the CHANGELOG.

```bash
# Show the submodule commit hash and its date
cd submodules/durabletask-protobuf && git log --oneline -3 && cd ../..
```

---

## Step 6: go.mod Hygiene

```bash
go mod tidy
git diff go.mod go.sum
```

**Required**:
- [ ] `go mod tidy` produces no changes (i.e., `go.mod` and `go.sum` are already clean).
- [ ] Go version directive is `>= 1.23`.
- [ ] No `replace` directives pointing to local paths.

---

## Step 7: Docker Build

```bash
docker build . -t durabletask-go:release-check
```

**Required**: Image builds without errors.

**Known issue to watch for**: The Dockerfile uses `FROM golang:1.21` but `go.mod` requires `1.23`. If this discrepancy is still present, flag it as a known limitation but do not block the release unless it causes actual build failures.

---

## Step 8: Final Validation Run

```bash
POSTGRES_ENABLED=true go test ./tests/... -timeout 180s \
  -coverpkg ./api,./task,./client,./backend/...,./internal/helpers
```

**Required**: All tests pass, including PostgreSQL tests.

---

## Step 9: Tag the Release

If all checks pass:

```bash
# Update CHANGELOG.md: rename [Unreleased] to [vX.Y.Z] with today's date
# Then:
git add CHANGELOG.md
git commit -m "chore: release v0.X.Y"
git tag v0.X.Y
git push origin main
git push origin v0.X.Y
```

**Do not push the tag until all previous steps pass.**

---

## Release Readiness Report Template

```markdown
## Release Readiness Report

**Proposed version**: v0.X.Y
**Release type**: Minor / Patch

### Validation Results
- CHANGELOG:      ✅ Complete / ❌ Incomplete (details)
- CI health:      ✅ Green / ❌ Failing (details)
- API surface:    ✅ Clean / ⚠️ Issues (details)
- Submodule:      ✅ Stable / ⚠️ Untracked commit
- go.mod hygiene: ✅ Clean / ❌ Dirty (run go mod tidy)
- Docker build:   ✅ Pass / ❌ Fail
- Full test suite:✅ Pass / ❌ Fail (N failures)

**Verdict**: ✅ READY TO TAG / ❌ BLOCKED
**Blocking issues**: (list)
**Tag command**: `git tag v0.X.Y && git push origin v0.X.Y`
```

---

## Exit Criteria

This skill is complete when:
- [ ] CHANGELOG is validated and complete.
- [ ] CI is green on main.
- [ ] API surface check passes.
- [ ] go.mod is tidy.
- [ ] Full test suite (including PostgreSQL) passes.
- [ ] Release readiness report is produced.
- [ ] Tag has been pushed (or blocking issues are documented).
