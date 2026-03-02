#!/bin/sh

ROOT=/Volumes/sourcecode/src/duckpond
NOYO=${ROOT}/noyo
POND=${NOYO}/pond
EXE=${ROOT}/target/debug/pond
OUTDIR=./export

export POND

cargo build

# Re-copy page templates from host (overwrites existing versions in pond)
${EXE} copy host://${NOYO}/site/index.md /system/site/index.md
${EXE} copy host://${NOYO}/site/data.md /system/site/data.md
${EXE} copy host://${NOYO}/site/sidebar.md /system/site/sidebar.md

${EXE} mknod remote /system/run/1-backup --overwrite --config-path ${NOYO}/backup.yaml

${EXE} mknod hydrovu /system/etc/20-hydrovu --overwrite --config-path ${NOYO}/hydrovu.yaml

${EXE} mknod dynamic-dir /combined --overwrite --config-path ${NOYO}/combine.yaml

${EXE} mknod dynamic-dir /singled --overwrite --config-path ${NOYO}/single.yaml

${EXE} mknod dynamic-dir /reduced --overwrite --config-path ${NOYO}/reduce.yaml

${EXE} mknod sitegen /system/etc/90-sitegen --overwrite --config-path ${NOYO}/site.yaml

${EXE} mknod column-rename /system/etc/10-hrename --overwrite --config-path ${NOYO}/hrename.yaml

${EXE} set-temporal-bounds /hydrovu/devices/6582334615060480/NoyoCenterVulink_2_active.series \
  --min-time "2024-01-01 00:00:00" \
  --max-time "2024-05-30 23:59:59"
