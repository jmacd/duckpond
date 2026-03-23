#!/bin/bash
# Shared environment for linux example scripts.
# Sourced by other scripts, not run directly.

export POND=/home/jmacd/pond
export MINIO_ENDPOINT=http://localhost:9000
export MINIO_ACCESS_KEY=caspar
export MINIO_SECRET_KEY=watertown
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
export RUST_LOG=info
