#!/usr/bin/env bash
#
# build-remote.sh -- Build the duckpond amd64 container image locally and push
#                    it to linux.local via SSH.
#
# Usage:
#   ./build-remote.sh              # build + push
#   ./build-remote.sh --build-only # build without pushing
#
set -euo pipefail

HOST=jmacd@linux.local
IMAGE_NAME=duckpond
IMAGE_TAG=latest-amd64
LOCAL_TAG="${IMAGE_NAME}:${IMAGE_TAG}"
TARBALL="/tmp/duckpond-amd64.tar"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "=== Building ${LOCAL_TAG} for linux/amd64 ==="
docker buildx build \
    --platform linux/amd64 \
    --tag "${LOCAL_TAG}" \
    --load \
    "${REPO_ROOT}"

if [[ "${1:-}" == "--build-only" ]]; then
    echo ""
    echo "=== Build complete (skipping push) ==="
    echo "Image: ${LOCAL_TAG}"
    exit 0
fi

echo ""
echo "=== Saving image to ${TARBALL} ==="
docker save "${LOCAL_TAG}" -o "${TARBALL}"
SIZE=$(du -h "${TARBALL}" | cut -f1)
echo "  Image size: ${SIZE}"

echo ""
echo "=== Pushing to ${HOST} ==="
scp "${TARBALL}" "${HOST}:/tmp/duckpond-amd64.tar"

echo ""
echo "=== Loading into podman on ${HOST} ==="
ssh "${HOST}" "podman load -i /tmp/duckpond-amd64.tar && podman tag docker.io/library/${LOCAL_TAG} ${LOCAL_TAG} && rm /tmp/duckpond-amd64.tar"

echo ""
echo "=== Done ==="
echo "Image '${LOCAL_TAG}' is now available on ${HOST}"
echo ""
echo "Next steps:"
echo "  ./setup-remote.sh     # first-time init"
echo "  ./update-remote.sh    # update configs"
echo "  ./run-remote.sh       # ingest data"
echo "  ./generate-remote.sh  # build site"
