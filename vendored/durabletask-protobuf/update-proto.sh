#!/usr/bin/env bash
# Downloads the latest orchestrator_service.proto from the
# microsoft/durabletask-protobuf repository and writes provenance
# information (source URL, branch/ref, commit hash, file URL) to
# PROTO_SOURCE_COMMIT_HASH.
#
# Usage:
#   ./vendored/durabletask-protobuf/update-proto.sh [ref]
#
# `ref` may be a branch name, tag name, or commit SHA.
# If omitted, "main" is used.

set -euo pipefail

REF="${1:-main}"
REPO="microsoft/durabletask-protobuf"
REPO_URL="https://github.com/${REPO}"
PROTO_PATH="protos/orchestrator_service.proto"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROTO_DIR="${SCRIPT_DIR}/protos"
HASH_FILE="${SCRIPT_DIR}/PROTO_SOURCE_COMMIT_HASH"

mkdir -p "${PROTO_DIR}"

CURL_AUTH=()
if [[ -n "${GITHUB_TOKEN:-}" ]]; then
    CURL_AUTH=(-H "Authorization: Bearer ${GITHUB_TOKEN}")
fi

echo "Resolving latest commit for ${PROTO_PATH} on ${REPO}@${REF}..."
COMMIT_API_URL="https://api.github.com/repos/${REPO}/commits?path=${PROTO_PATH}&sha=${REF}&per_page=1"
COMMIT_RESPONSE=$(curl -fsSL \
    "${CURL_AUTH[@]}" \
    -H "Accept: application/vnd.github.v3+json" \
    "${COMMIT_API_URL}")

# Extract the first "sha": "<hash>" value from the JSON response.
# Use sed -nE so a missing match leaves COMMIT_HASH empty and the
# validation block below can emit a clear error, instead of the
# pipeline aborting silently under `set -euo pipefail`.
COMMIT_HASH=$(printf '%s' "${COMMIT_RESPONSE}" \
    | sed -nE 's/.*"sha"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/p' \
    | head -n 1)

if [[ -z "${COMMIT_HASH}" ]]; then
    echo "Failed to resolve commit hash from ${COMMIT_API_URL}" >&2
    echo "Response was:" >&2
    printf '%s\n' "${COMMIT_RESPONSE}" >&2
    exit 1
fi

echo "Downloading ${PROTO_PATH}@${COMMIT_HASH}..."
curl -fsSL \
    "https://raw.githubusercontent.com/${REPO}/${COMMIT_HASH}/${PROTO_PATH}" \
    -o "${PROTO_DIR}/orchestrator_service.proto"

cat > "${HASH_FILE}" <<EOF
Source: ${REPO_URL}
Branch: ${REF}
Commit: ${COMMIT_HASH}
URL:    ${REPO_URL}/blob/${COMMIT_HASH}/${PROTO_PATH}
EOF

echo "Updated ${PROTO_DIR}/orchestrator_service.proto"
echo "Recorded provenance for commit ${COMMIT_HASH} in ${HASH_FILE}"
echo
echo "Next step: regenerate the Go bindings:"
echo "  protoc --go_out=. --go-grpc_out=. -I ./vendored/durabletask-protobuf/protos orchestrator_service.proto"