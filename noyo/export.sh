#!/bin/sh

ROOT=/Volumes/sourcecode/src/duckpond
NOYO=${ROOT}/noyo
POND=${NOYO}/pond
BUILD=release
EXE=${ROOT}/target/${BUILD}/pond
OUTDIR=./export

export RUST_LOG=info

export POND

cargo build --${BUILD}

rm -rf ${OUTDIR}

# Parameters
${EXE} export --pattern '/reduced/single_param/*/*.series' --pattern '/templates/params/param=*' --dir OUTDIR --temporal "year,month"

# Site detail
${EXE} export --pattern '/reduced/single_site/*/*.series' --pattern '/templates/sites/site=*' --dir OUTDIR --temporal "year,month"
