#!/bin/sh

ROOT=/Volumes/sourcecode/src/duckpond
NOYO=${ROOT}/noyo
POND=${NOYO}/pond
EXE=${ROOT}/target/release/pond
OUTDIR=./export

export RUST_BACKTRACE=1
export POND_MAX_ALLOC_MB=1000
#export RUST_LOG=debug

export POND

cargo build --release

rm -rf ${OUTDIR}

# Parameters
${EXE} export --pattern '/reduced/single_param/*/*.series' --pattern '/templates/params/param=*' --dir OUTDIR --temporal "year,month"

# Site detail
${EXE} export --pattern '/reduced/single_site/*/*.series' --pattern '/templates/sites/site=*' --dir OUTDIR --temporal "year,month"
