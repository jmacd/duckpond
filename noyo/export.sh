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

# Parameters
${EXE} export --pattern '/reduced/single_param/*/*.series' --pattern '/templates/params/*' --dir ${OUTDIR} --temporal "year,month"

# Site detail
${EXE} export --pattern '/reduced/single_site/*/*.series' --pattern '/templates/sites/*' --dir ${OUTDIR} --temporal "year,month"

# Index page
${EXE} export --pattern '/templates/index/*' --dir ${OUTDIR}

# Page template (for notebook-kit --template)
${EXE} export --pattern '/templates/page/*' --dir ${OUTDIR}
