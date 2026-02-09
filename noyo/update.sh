#!/bin/sh

ROOT=/Volumes/sourcecode/src/duckpond
NOYO=${ROOT}/noyo
POND=${NOYO}/pond
EXE=${ROOT}/target/debug/pond
OUTDIR=./export

export POND

cargo build

# Re-copy page templates from host (overwrites existing versions in pond)
${EXE} copy host://${NOYO}/site/index.md /etc/site/index.md
${EXE} copy host://${NOYO}/site/data.md /etc/site/data.md
${EXE} copy host://${NOYO}/site/sidebar.md /etc/site/sidebar.md

${EXE} mknod remote /etc/system.d/1-backup --overwrite --config-path ${NOYO}/backup.yaml

${EXE} mknod hydrovu /etc/hydrovu --overwrite --config-path ${NOYO}/hydrovu.yaml

${EXE} mknod dynamic-dir /combined --overwrite --config-path ${NOYO}/combine.yaml

${EXE} mknod dynamic-dir /singled --overwrite --config-path ${NOYO}/single.yaml

${EXE} mknod dynamic-dir /reduced --overwrite --config-path ${NOYO}/reduce.yaml

${EXE} mknod sitegen /etc/site.yaml --overwrite --config-path ${NOYO}/site.yaml

${EXE} mknod column-rename /etc/hydro_rename --overwrite --config-path ${NOYO}/hrename.yaml

${EXE} set-temporal-bounds /hydrovu/devices/6582334615060480/NoyoCenterVulink_2_active.series \
  --min-time "2024-01-01 00:00:00" \
  --max-time "2024-05-30 23:59:59"
