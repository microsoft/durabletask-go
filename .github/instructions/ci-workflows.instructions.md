---
applyTo: ".github/workflows/**/*.yml"
---

# CI Workflow Instructions — durabletask-go

These instructions apply to all files under `.github/workflows/`.

---

## Current Workflow: `pr-validation.yml`

The single CI workflow validates PRs and pushes to `main`. Key facts:

| Setting | Value |
|---------|-------|
| Triggers | `push` to `main` (non-`.md`), `pull_request` to `main` (non-`.md`), `workflow_dispatch` |
| Matrix | Go `1.24.x` and `1.23.x` on `ubuntu-latest` |
| PostgreSQL | `postgres:16` service container on port `5432` |
| Protoc version | `25.x` |
| `protoc-gen-go` | `v1.30` |
| `protoc-gen-go-grpc` | `v1.3` |
| Lint action | `golangci/golangci-lint-action@v7` |

**Do not change tool versions without verifying compatibility** with both Go matrix versions and the current `.golangci.yml` configuration.

---

## Step Order (must be preserved)

1. `actions/checkout@v4` with `fetch-depth: 1` and `submodules: true`
2. `actions/setup-go@v5`
3. `go get .`
4. Install `protoc` via `arduino/setup-protoc@v3`
5. Install `protoc-gen-go` and `protoc-gen-go-grpc`
6. Run `protoc` to regenerate `internal/protos/*.pb.go`
7. `go vet ./...`
8. `golangci/golangci-lint-action@v7`
9. `go test ./tests/... -coverpkg ...` with `POSTGRES_ENABLED=true`

The protoc regeneration step (6) runs **before** vet and tests. This ensures generated code is always current and that tests compile against the latest proto definitions.

---

## Submodule Requirement

The workflow checks out with `submodules: true`. Any change to the `durabletask-protobuf` submodule reference must be committed in `.gitmodules` and the submodule hash updated. Do not add workflow steps that bypass the submodule checkout.

---

## Adding New CI Steps

If adding a new CI step:
- Place it before `go test` but after `golangci-lint` unless it has a strict ordering requirement.
- Do not add steps that run only on specific Go matrix versions without a documented reason.
- Do not add steps that require secrets without verifying the secrets are configured in the repository settings.
- New service containers must include a health check (see the `postgres` service container as the pattern).

---

## Path Filters

The workflow uses path filtering to skip CI for `.md`-only changes:
```yaml
paths-ignore:
  - '**.md'
```
Do not remove this filter — it avoids running full CI for documentation-only PRs.

If adding a new trigger, preserve the path filter pattern.

---

## Adding a Release Workflow

The repository currently has no release or publish workflow. Tags are pushed manually.
If adding a release workflow:
- Use semantic versioning (`v0.x.y` format consistent with `CHANGELOG.md`).
- Trigger on `push` to tags matching `v*`.
- Do not automate `go mod` publishing without confirming the repository's release process.
