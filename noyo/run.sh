#!/bin/sh

ROOT=/Volumes/sourcecode/src/duckpond
NOYO=${ROOT}/noyo
POND=${NOYO}/pond
EXE=${ROOT}/target/release/pond

export RUST_LOG=info
export POND

cargo build --release

${EXE} run /etc/hydrovu
