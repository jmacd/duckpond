#!/bin/sh

ROOT=/Volumes/sourcecode/src/duckpond
NOYO=${ROOT}/noyo
POND=${NOYO}/pond
EXE=${ROOT}/target/debug/pond

export POND

${EXE} export --pattern '/reduced/**/*.series' --pattern '/templates/**/*.md' --dir ./export --temporal "year,month"
