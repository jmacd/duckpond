#!/bin/sh

HOST=debian@septicplaystation.local
# Project root and binary paths
PROJECT_ROOT=$(cd "$(dirname "$0")/.." && pwd)
LOCAL_BIN=${PROJECT_ROOT}/target/aarch64-unknown-linux-gnu/debug/pond
# Remote paths
REMOTE_BIN=/home/debian/bin/pond
REMOTE_POND=/home/debian/pond
REMOTE_CONFIG=/home/debian/config
REMOTE_DATA=/home/data

# Build the cross-compiled binary
build_binary() {
    echo "Building cross-compiled binary..."
    (cd "${PROJECT_ROOT}" && cargo zigbuild --target aarch64-unknown-linux-gnu --bin pond)
}

# Sync binary to remote host
sync_binary() {
    if [ ! -f "${LOCAL_BIN}" ]; then
        echo "Error: Binary not found at ${LOCAL_BIN}" >&2
        exit 1
    fi
    
    # Create remote bin directory if needed
    ssh ${HOST} "mkdir -p $(dirname ${REMOTE_BIN})"
    
    # Use rsync for efficient sync (only copies if changed)
    rsync -avz --progress "${LOCAL_BIN}" "${HOST}:${REMOTE_BIN}"
}

# Build and sync
build_binary
sync_binary

ssh ${HOST} \
    POND=${REMOTE_POND} \
    RUST_LOG=${RUST_LOG:-info} \
    R2_ENDPOINT=${R2_ENDPOINT} \
    R2_KEY=${R2_KEY} \
    R2_SECRET=${R2_SECRET} \
    ${REMOTE_BIN} "$@"
