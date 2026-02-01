#!/bin/sh

ROOT=/Volumes/sourcecode/src/duckpond
SEPTIC=${ROOT}/septic
POND=${SEPTIC}/pond
EXE=${ROOT}/target/release/pond
OUTDIR=./export

export POND

cargo build --release

${EXE} mknod remote /etc/system.d/1-backup --overwrite --config-path ${SEPTIC}/backup.yaml

${EXE} mknod logfile-ingest /etc/ingest --overwrite --config-path ${SEPTIC}/ingest.yaml
