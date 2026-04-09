#!/bin/sh
set -x
set -e

SCRIPTS=$(cd "$(dirname "$0")" && pwd)
POND_DIR=${SCRIPTS}/pond

export POND=${POND_DIR}

# Load credentials (HydroVu keys, S3 creds)
if [ -f "${SCRIPTS}/deploy.env" ]; then
  . "${SCRIPTS}/deploy.env"
fi
if [ -f ~/.zshrc.private ]; then
  . ~/.zshrc.private
fi

CARGO="cargo run --release -p cmd --"

# Wipe and initialize
rm -rf "${POND_DIR}"
${CARGO} init

${CARGO} mkdir -p /system/run
${CARGO} mkdir -p /system/etc
${CARGO} mkdir -p /laketech

${CARGO} copy host://${SCRIPTS}/site /system/site
${CARGO} copy host://${SCRIPTS}/laketech /laketech/data

# Import archived instrument data (if any exported Parquet files exist)
if [ -d "${SCRIPTS}/hydrovu" ]; then
  ${CARGO} copy host://${SCRIPTS}/hydrovu /hydrovu
fi

# Install factory nodes
${CARGO} mknod remote /system/run/1-backup --config-path "${SCRIPTS}/backup.yaml"
${CARGO} mknod hydrovu /system/etc/20-hydrovu --config-path ${SCRIPTS}/hydrovu.yaml
${CARGO} mknod dynamic-dir /combined --config-path ${SCRIPTS}/combine.yaml
${CARGO} mknod dynamic-dir /singled --config-path ${SCRIPTS}/single.yaml
${CARGO} mknod dynamic-dir /reduced --config-path ${SCRIPTS}/reduce.yaml
${CARGO} mknod sitegen /system/etc/90-sitegen --config-path ${SCRIPTS}/site.yaml
${CARGO} mknod column-rename /system/etc/10-hrename --config-path ${SCRIPTS}/hrename.yaml

echo
echo "=== Setup complete ==="
echo "Next: ./run.sh       # collect from HydroVu"
echo "Then: ./export.sh    # build the site"
