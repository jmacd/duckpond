#!/bin/sh

set -x -i

ROOT=/Volumes/sourcecode/src/duckpond
NOYO=${ROOT}/noyo
POND=${NOYO}/pond
EXE=${ROOT}/target/release/pond
#EXE=${ROOT}/target/debug/pond
OUTDIR=./export

export RUST_BACKTRACE=1
export POND_MAX_ALLOC_MB=1000
#export RUST_LOG=tlogfs=debug

export POND

cargo build
#cargo build --release

rm -rf ${OUTDIR}
mkdir -p ${OUTDIR}

# Sitegen: exports data + renders HTML in one step
${EXE} run /etc/site.yaml build ${OUTDIR}
