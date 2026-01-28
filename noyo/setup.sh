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

${EXE} copy host://${NOYO}/data.html.tmpl /etc
${EXE} copy host://${NOYO}/index.html.tmpl /etc
${EXE} copy host://${NOYO}/nav.html.tmpl /etc

${EXE} mkdir -p /laketech

${EXE} copy host://${NOYO}/laketech /laketech/data

# Disable backup
${EXE} mkdir /etc/system.d
#${EXE} mknod remote /etc/test_backup --config-path ${NOYO}/backup.yaml
#RUST_LOG=tlogfs=debug,remote=debug
${EXE} mknod remote /etc/system.d/1-backup --config-path ${NOYO}/backup.yaml

# Disable hydrovu
${EXE} mknod hydrovu /etc/hydrovu --config-path ${NOYO}/hydrovu.yaml

# Copy-out hydrovu data
#COPY=${NOYO}/copy
#rm -rf ${COPY}
#mkdir ${COPY}
#POND=${REPLICA} ${EXE} copy '/hydrovu/**/*.series' host://${COPY}

# Copy-in hydrovu data
#${EXE} copy host://${COPY} /

# Configure export pipeline

${EXE} mknod dynamic-dir /combined --config-path ${NOYO}/combine.yaml

${EXE} mknod dynamic-dir /singled --config-path ${NOYO}/single.yaml

${EXE} mknod dynamic-dir /reduced --config-path ${NOYO}/reduce.yaml

${EXE} mknod dynamic-dir /templates --config-path ${NOYO}/template.yaml

${EXE} mknod column-rename /etc/hydro_rename --config-path ${NOYO}/hrename.yaml
