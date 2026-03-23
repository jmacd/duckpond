#!/bin/sh
set -x
set -e

ROOT=/Volumes/sourcecode/src/duckpond
NOYO=${ROOT}/noyo
POND=${NOYO}/pond
EXE=${ROOT}/target/debug/pond
OUTDIR=./export

export RUST_BACKTRACE=1
export POND_MAX_ALLOC_MB=1000

export POND

cargo build

rm -rf ${OUTDIR}
mkdir -p ${OUTDIR}

# Sitegen: exports data + renders HTML in one step
${EXE} run /system/etc/90-sitegen build ${OUTDIR}

echo Now, run:
echo npx vite ${OUTDIR} --port 4174 --open
