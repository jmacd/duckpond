#!/usr/bin/env bash
#
# build.sh — Build the duckpond arm64 container image locally and push
#             it to the BeaglePlay via SSH.
#
# This replaces the stale GHCR pull with a fresh local build, so every
# code change is testable on the device immediately.
#
# Usage:
#   ./build.sh              # build + push
#   ./build.sh --build-only # build without pushing
#
set -euo pipefail

HOST=debian@septicplaystation.local
IMAGE_NAME=duckpond
IMAGE_TAG=latest-arm64
LOCAL_TAG="${IMAGE_NAME}:${IMAGE_TAG}"
TARBALL="/tmp/duckpond-arm64.tar"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "=== Building ${LOCAL_TAG} for linux/arm64 ==="
docker buildx build \
    --platform linux/arm64 \
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
scp "${TARBALL}" "${HOST}:/tmp/duckpond-arm64.tar"

echo ""
echo "=== Loading into podman on ${HOST} ==="
ssh "${HOST}" "podman load -i /tmp/duckpond-arm64.tar && podman tag docker.io/library/${LOCAL_TAG} ${LOCAL_TAG} && rm /tmp/duckpond-arm64.tar"

echo ""
echo "=== Done ==="
echo "Image '${LOCAL_TAG}' is now available on ${HOST}"
echo ""
echo "Next steps:"
echo "  ./setup.sh     # first-time init"
echo "  ./update.sh    # update configs"
echo "  ./run.sh       # ingest data"
echo "  ./generate.sh  # build site"
