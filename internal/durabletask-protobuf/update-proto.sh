#!/usr/bin/env bash
# Downloads the latest orchestrator_service.proto from the
# microsoft/durabletask-protobuf repository and writes the source
# commit hash to PROTO_SOURCE_COMMIT_HASH.
#
# Usage:
#   ./internal/durabletask-protobuf/update-proto.sh [branch]
#
# If [branch] is omitted, "main" is used.

set -euo pipefail

BRANCH="${1:-main}"
REPO="microsoft/durabletask-protobuf"
PROTO_PATH="protos/orchestrator_service.proto"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROTO_DIR="${SCRIPT_DIR}/protos"
HASH_FILE="${SCRIPT_DIR}/PROTO_SOURCE_COMMIT_HASH"

mkdir -p "${PROTO_DIR}"

CURL_AUTH=()
if [[ -n "${GITHUB_TOKEN:-}" ]]; then
    CURL_AUTH=(-H "Authorization: Bearer ${GITHUB_TOKEN}")
fi

echo "Resolving latest commit for ${PROTO_PATH} on ${REPO}@${BRANCH}..."
COMMIT_API_URL="https://api.github.com/repos/${REPO}/commits?path=${PROTO_PATH}&sha=${BRANCH}&per_page=1"
COMMIT_RESPONSE=$(curl -fsSL \
    "${CURL_AUTH[@]}" \
    -H "Accept: application/vnd.github.v3+json" \
    "${COMMIT_API_URL}")

# Extract the first "sha": "<hash>" value from the JSON response.
COMMIT_HASH=$(printf '%s' "${COMMIT_RESPONSE}" \
    | grep -o '"sha"[[:space:]]*:[[:space:]]*"[^"]*"' \
    | head -n 1 \
    | sed -E 's/.*"sha"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/')

if [[ -z "${COMMIT_HASH}" ]]; then
    echo "Failed to resolve commit hash from ${COMMIT_API_URL}" >&2
    exit 1
fi

echo "Downloading ${PROTO_PATH}@${COMMIT_HASH}..."
curl -fsSL \
    "https://raw.githubusercontent.com/${REPO}/${COMMIT_HASH}/${PROTO_PATH}" \
    -o "${PROTO_DIR}/orchestrator_service.proto"

echo "${COMMIT_HASH}" > "${HASH_FILE}"

echo "Updated ${PROTO_DIR}/orchestrator_service.proto"
echo "Recorded commit hash ${COMMIT_HASH} in ${HASH_FILE}"
echo
echo "Next step: regenerate the Go bindings:"
echo "  protoc --go_out=. --go-grpc_out=. -I ./internal/durabletask-protobuf/protos orchestrator_service.proto"
