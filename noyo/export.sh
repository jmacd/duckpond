#!/bin/sh

ROOT=/Volumes/sourcecode/src/duckpond
NOYO=${ROOT}/noyo
POND=${NOYO}/pond
EXE=${ROOT}/target/debug/pond
OUTDIR=./export

export POND

cargo build

rm -rf ${OUTDIR}

# Two steps:
#POND=noyo/pond cargo run export --pattern '/reduced/single_param/*/*.series' --pattern '/templates/params/param=*' --dir OUTDIR --temporal "year,month"

${EXE} export --pattern '/reduced/**/*.series' --pattern '/templates/**/*.md' --dir ${OUTDIR} --temporal "year,month"
