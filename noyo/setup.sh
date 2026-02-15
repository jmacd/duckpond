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

${EXE} mkdir -p /etc/system.d

${EXE} copy host://${NOYO}/site /etc/site

${EXE} mkdir -p /laketech

${EXE} copy host://${NOYO}/laketech /laketech/data

# Import archived instrument data (if any exported Parquet files exist)
# Archive Parquet lives in noyo/hydrovu/ — same pattern as noyo/laketech/
if [ -d "${NOYO}/hydrovu" ]; then
  ${EXE} copy host://${NOYO}/hydrovu /hydrovu
fi

${EXE} mknod remote /etc/system.d/1-backup --config-path ${NOYO}/backup.yaml

${EXE} mknod hydrovu /etc/hydrovu --config-path ${NOYO}/hydrovu.yaml

${EXE} mknod dynamic-dir /combined --config-path ${NOYO}/combine.yaml

${EXE} mknod dynamic-dir /singled --config-path ${NOYO}/single.yaml

${EXE} mknod dynamic-dir /reduced --config-path ${NOYO}/reduce.yaml

${EXE} mknod sitegen /etc/site.yaml --config-path ${NOYO}/site.yaml

${EXE} mknod column-rename /etc/hydro_rename --config-path ${NOYO}/hrename.yaml
