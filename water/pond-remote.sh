#!/usr/bin/env bash
#
# pond-remote.sh -- Run pond on linux.local via podman + SSH.
#
# Uses a locally-built amd64 container image pushed via build-remote.sh.
# Run ./build-remote.sh first to build and push the image.
#

HOST=jmacd@linux.local
IMAGE=${DUCKPOND_IMAGE:-duckpond:latest-amd64}
VOLUME=pond-water

# Remote paths
REMOTE_CONFIG=/home/jmacd/water-config
REMOTE_DATA=/home/data
REMOTE_OUTPUT=/home/jmacd/water-site-output

# Ensure mount points exist
ssh ${HOST} "mkdir -p ${REMOTE_CONFIG} ${REMOTE_OUTPUT}"

ssh ${HOST} \
    podman run --pull=never -ti --rm \
    -v "${VOLUME}:/pond" \
    -v "${REMOTE_CONFIG}:/config:ro" \
    -v "${REMOTE_DATA}:/data:ro" \
    -v "${REMOTE_OUTPUT}:/output" \
    -e POND=/pond \
    -e RUST_LOG=${RUST_LOG:-info} \
    "${IMAGE}" "$@"
