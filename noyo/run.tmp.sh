#!/bin/sh
set -x
set -e

ROOT=/Volumes/sourcecode/src/duckpond
NOYO=${ROOT}/noyo
POND=${NOYO}/pond
REPLICA=${NOYO}/replica

EXE=${ROOT}/target/debug/pond

export POND

rm -rf ${NOYO}/export

cargo build --release --bin pond

${EXE} copy host://${NOYO}/data.html.tmpl /etc
${EXE} copy host://${NOYO}/index.html.tmpl /etc
${EXE} copy host://${NOYO}/nav.html.tmpl /etc

${EXE} mknod dynamic-dir /templates --overwrite --config-path ${NOYO}/template.yaml

${NOYO}/export.sh
