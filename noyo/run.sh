#!/bin/sh

ROOT=/Volumes/sourcecode/src/duckpond
NOYO=${ROOT}/noyo
POND=${NOYO}/pond
EXE=${ROOT}/target/debug/pond

export RUST_LOG=info
export POND

cargo build

${EXE} run /etc/hydrovu collect
