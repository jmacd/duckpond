#!/usr/bin/env bash
#
# pond.sh — Run pond on the BeaglePlay via podman + SSH.
#
# Uses a locally-built arm64 container image pushed via build.sh.
# Run ./build.sh first to build and push the image.
#

HOST=debian@septicplaystation.local
IMAGE=duckpond:latest-arm64
VOLUME=pond-data

# Remote paths
REMOTE_CONFIG=/home/debian/config
REMOTE_DATA=/home/data
REMOTE_OUTPUT=/home/debian/site-output

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
