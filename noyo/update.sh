#!/bin/sh

ROOT=/Volumes/sourcecode/src/duckpond
NOYO=${ROOT}/noyo
POND=${NOYO}/pond
EXE=${ROOT}/target/debug/pond
OUTDIR=./export

export POND

cargo build

${EXE} mknod dynamic-dir /combined --overwrite --config-path ${NOYO}/combine.yaml

${EXE} mknod dynamic-dir /singled --overwrite --config-path ${NOYO}/single.yaml

${EXE} mknod dynamic-dir /reduced --overwrite --config-path ${NOYO}/reduce.yaml

${EXE} mknod dynamic-dir /templates --overwrite --config-path ${NOYO}/template.yaml
