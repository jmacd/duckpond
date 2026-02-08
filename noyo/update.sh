#!/bin/sh

ROOT=/Volumes/sourcecode/src/duckpond
NOYO=${ROOT}/noyo
POND=${NOYO}/pond
EXE=${ROOT}/target/release/pond
OUTDIR=./export

export POND

cargo build --release

${EXE} mknod hydrovu /etc/hydrovu --overwrite --config-path ${NOYO}/hydrovu.yaml

${EXE} mknod dynamic-dir /combined --overwrite --config-path ${NOYO}/combine.yaml

${EXE} mknod dynamic-dir /singled --overwrite --config-path ${NOYO}/single.yaml

${EXE} mknod dynamic-dir /reduced --overwrite --config-path ${NOYO}/reduce.yaml

${EXE} mknod sitegen /etc/site.yaml --overwrite --config-path ${NOYO}/site.yaml

${EXE} copy host://${NOYO}/data.html.tmpl /etc

${EXE} set-temporal-bounds /hydrovu/devices/6582334615060480/NoyoCenterVulink_2_active.series \
  --min-time "2024-01-01 00:00:00" \
  --max-time "2024-05-30 23:59:59"

