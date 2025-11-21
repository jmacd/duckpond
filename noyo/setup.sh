#!/bin/sh
set -x
set -e

ROOT=/Volumes/sourcecode/src/duckpond
NOYO=${ROOT}/noyo
POND=${NOYO}/pond
REPLICA=${NOYO}/replica

EXE=${ROOT}/target/release/pond

export POND

cargo build --release

${EXE} init

${EXE} mkdir -p /etc

${EXE} copy ${NOYO}/data.md.tmpl /etc

# Disable backup
#${EXE} mkdir /etc/system.d
#${EXE} mknod remote /etc/system.d/backup --config-path ${NOYO}/backup.yaml

# Disable hydrovu
#${EXE} mknod hydrovu /etc/hydrovu --config-path ${NOYO}/hydrovu.yaml

# Copy-out hydrovu data
COPY=${NOYO}/copy
#rm -rf ${COPY}
#mkdir ${COPY}
#POND=${REPLICA} ${EXE} copy '/hydrovu/**/*.series' host://${COPY}

# Copy-in hydrovu data
${EXE} copy host://${COPY} /

# Configure export pipeline

${EXE} mknod dynamic-dir /combined --config-path ${NOYO}/combine.yaml

${EXE} mknod dynamic-dir /singled --config-path ${NOYO}/single.yaml

${EXE} mknod dynamic-dir /reduced --config-path ${NOYO}/reduce.yaml

${EXE} mknod dynamic-dir /templates --config-path ${NOYO}/template.yaml
