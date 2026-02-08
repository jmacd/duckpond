#!/bin/sh
set -x
set -e

ROOT=/Volumes/sourcecode/src/duckpond
NOYO=${ROOT}/noyo
POND=${NOYO}/pond
REPLICA=${NOYO}/replica

EXE=${ROOT}/target/debug/pond

export POND

cargo build

${EXE} init

${EXE} mkdir -p /etc

# Sitegen markdown pages
${EXE} mkdir -p /etc/site
${EXE} copy host://${NOYO}/site/index.md /etc/site/index.md
${EXE} copy host://${NOYO}/site/data.md /etc/site/data.md
${EXE} copy host://${NOYO}/site/sidebar.md /etc/site/sidebar.md

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

${EXE} mknod sitegen /etc/site.yaml --config-path ${NOYO}/site.yaml

${EXE} mknod column-rename /etc/hydro_rename --config-path ${NOYO}/hrename.yaml
