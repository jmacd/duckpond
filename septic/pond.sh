#!/usr/bin/env bash
#
# pond.sh — Run pond on the BeaglePlay via podman + SSH.
#
# Uses the pre-built arm64 container image from GHCR.
# No cross-compilation needed.
#

HOST=debian@septicplaystation.local
IMAGE=ghcr.io/jmacd/duckpond/duckpond:latest-arm64
VOLUME=pond-data

# Remote paths
REMOTE_CONFIG=/home/debian/config
REMOTE_DATA=/home/data
REMOTE_OUTPUT=/home/debian/site-output

# Ensure mount points exist
ssh ${HOST} "mkdir -p ${REMOTE_CONFIG} ${REMOTE_OUTPUT}"

ssh ${HOST} \
    podman run --pull=always -ti --rm \
    -v "${VOLUME}:/pond" \
    -v "${REMOTE_CONFIG}:/config:ro" \
    -v "${REMOTE_DATA}:/data:ro" \
    -v "${REMOTE_OUTPUT}:/output" \
    -e POND=/pond \
    -e RUST_LOG=${RUST_LOG:-info} \
    -e R2_ENDPOINT=${R2_ENDPOINT} \
    -e R2_KEY=${R2_KEY} \
    -e R2_SECRET=${R2_SECRET} \
    "${IMAGE}" "$@"
