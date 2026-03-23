#!/bin/sh
set -x
set -e

ROOT=/Volumes/sourcecode/src/duckpond
NOYO=${ROOT}/noyo
POND=${NOYO}/pond
REPLICA=${NOYO}/replica

EXE=${ROOT}/target/debug/pond

# Source private credentials (HydroVu API keys, etc.)
if [ -f ~/.zshrc.private ]; then
  . ~/.zshrc.private
fi

export POND

cargo build

${EXE} init

${EXE} mkdir -p /system/run

${EXE} mkdir -p /system/etc

${EXE} copy host://${NOYO}/site /system/site

${EXE} mkdir -p /laketech

${EXE} copy host://${NOYO}/laketech /laketech/data

# Import archived instrument data (if any exported Parquet files exist)
# Archive Parquet lives in noyo/hydrovu/ — same pattern as noyo/laketech/
if [ -d "${NOYO}/hydrovu" ]; then
  ${EXE} copy host://${NOYO}/hydrovu /hydrovu
fi

${EXE} mknod remote /system/run/1-backup --config-path ${NOYO}/backup.yaml

${EXE} mknod hydrovu /system/etc/20-hydrovu --config-path ${NOYO}/hydrovu.yaml

${EXE} mknod dynamic-dir /combined --config-path ${NOYO}/combine.yaml

${EXE} mknod dynamic-dir /singled --config-path ${NOYO}/single.yaml

${EXE} mknod dynamic-dir /reduced --config-path ${NOYO}/reduce.yaml

${EXE} mknod sitegen /system/etc/90-sitegen --config-path ${NOYO}/site.yaml

${EXE} mknod column-rename /system/etc/10-hrename --config-path ${NOYO}/hrename.yaml
