#!/bin/sh
set -x
set -e

ROOT=/Volumes/sourcecode/src/duckpond
SEPTIC=${ROOT}/septic
POND=${SEPTIC}/pond
REPLICA=${SEPTIC}/replica

EXE=${ROOT}/target/release/pond

export POND

cargo build --release

${EXE} init

${EXE} mkdir -p /etc/system.d

${EXE} mkdir -p /ingest

${EXE} mknod remote /etc/system.d/1-backup --config-path ${SEPTIC}/backup.yaml

${EXE} mknod logfile-ingest /etc/ingest --config-path ${SEPTIC}/ingest.yaml

