#!/bin/sh
set -x
set -e

ROOT=/Volumes/sourcecode/src/duckpond
NOYO=${ROOT}/noyo
POND=${NOYO}/pond
EXE=${ROOT}/target/release/pond

export POND

cargo build --release

${EXE} init

${EXE} mkdir -p /etc/system.d

${EXE} copy ${NOYO}/data.md.tmpl /etc

${EXE} mknod remote /etc/system.d/backup --config-path ${NOYO}/backup.yaml

${EXE} mknod hydrovu /etc/hydrovu --config-path ${NOYO}/hydrovu.yaml

${EXE} mknod dynamic-dir /combined --config-path ${NOYO}/combine.yaml

${EXE} mknod dynamic-dir /singled --config-path ${NOYO}/single.yaml

${EXE} mknod dynamic-dir /reduced --config-path ${NOYO}/reduce.yaml

${EXE} mknod dynamic-dir /templates --config-path ${NOYO}/template.yaml
