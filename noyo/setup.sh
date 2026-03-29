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

# Expand env vars in backup.yaml
export S3_URL S3_ENDPOINT S3_ACCESS_KEY S3_SECRET_KEY
BACKUP_CFG=$(mktemp)
envsubst < "${SCRIPTS}/backup.yaml" > "${BACKUP_CFG}"

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

${CARGO} mknod remote /system/run/1-backup --config-path "${BACKUP_CFG}"
${CARGO} mknod hydrovu /system/etc/20-hydrovu --config-path ${SCRIPTS}/hydrovu.yaml
${CARGO} mknod dynamic-dir /combined --config-path ${SCRIPTS}/combine.yaml
${CARGO} mknod dynamic-dir /singled --config-path ${SCRIPTS}/single.yaml
${CARGO} mknod dynamic-dir /reduced --config-path ${SCRIPTS}/reduce.yaml
${CARGO} mknod sitegen /system/etc/90-sitegen --config-path ${SCRIPTS}/site.yaml
${CARGO} mknod column-rename /system/etc/10-hrename --config-path ${SCRIPTS}/hrename.yaml

rm -f "${BACKUP_CFG}"

echo
echo "=== Setup complete ==="
echo "Next: ./run.sh       # collect from HydroVu"
echo "Then: ./export.sh    # build the site"
