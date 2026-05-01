# Durable Task Protobuf Files

This directory contains the protocol buffer definitions used by the Durable Task Framework Go SDK. The files in this directory are vendored from the [microsoft/durabletask-protobuf](https://github.com/microsoft/durabletask-protobuf) repository.

## Directory Structure

- `protos/` - Contains the vendored proto files
- `PROTO_SOURCE_COMMIT_HASH` - Records the upstream source URL, branch/ref, and commit hash that the proto files were last synced from
- `update-proto.sh` - Helper script to refresh the vendored proto file and commit hash from upstream

## Updating the proto files

Run the helper script from the repository root to download the latest `orchestrator_service.proto` from upstream and update `PROTO_SOURCE_COMMIT_HASH`:

```bash
# Update from the default branch (main)
./vendored/durabletask-protobuf/update-proto.sh

# Update from a specific branch, tag, or commit SHA
./vendored/durabletask-protobuf/update-proto.sh <branch-tag-or-sha>
```

After running the script, regenerate the Go gRPC bindings so that the committed `internal/protos/*.pb.go` files match the updated `.proto`:

```bash
protoc --go_out=. --go-grpc_out=. -I ./vendored/durabletask-protobuf/protos orchestrator_service.proto
```

Commit the updated proto file, `PROTO_SOURCE_COMMIT_HASH`, and the regenerated `.pb.go` files together.
