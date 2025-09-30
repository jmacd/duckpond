#!/bin/sh

ROOT=/Volumes/sourcecode/src/duckpond
NOYO=${ROOT}/noyo
POND=${NOYO}/pond
EXE=${ROOT}/target/debug/pond
OUTDIR=./export

export POND

# Temporary
cargo build

rm -rf ${OUTDIR}

${EXE} mknod dynamic-dir /templates --overwrite --config-path ${NOYO}/template.yaml

${EXE} export --pattern '/reduced/**/*.series' --pattern '/templates/**/*.md' --dir ${OUTDIR} --temporal "year,month"
