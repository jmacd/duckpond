#!/bin/sh
set -x
set -e

SCRIPTS=$(cd "$(dirname "$0")" && pwd)
export POND=${SCRIPTS}/pond

# Load credentials (HydroVu keys, S3 creds)
if [ -f "${SCRIPTS}/deploy.env" ]; then
  . "${SCRIPTS}/deploy.env"
fi
if [ -f ~/.zshrc.private ]; then
  . ~/.zshrc.private
fi

CARGO="cargo run --release -p cmd --"

# Re-copy page templates from host
for f in index.md data.md sidebar.md params.md sites.md; do
    ${CARGO} copy host://${SCRIPTS}/site/${f} /system/site/${f}
done

${CARGO} mknod remote /system/run/1-backup --overwrite --config-path "${SCRIPTS}/backup.yaml"
${CARGO} mknod hydrovu /system/etc/20-hydrovu --overwrite --config-path ${SCRIPTS}/hydrovu.yaml
${CARGO} mknod dynamic-dir /combined --overwrite --config-path ${SCRIPTS}/combine.yaml
${CARGO} mknod dynamic-dir /singled --overwrite --config-path ${SCRIPTS}/single.yaml
${CARGO} mknod dynamic-dir /reduced --overwrite --config-path ${SCRIPTS}/reduce.yaml
${CARGO} mknod sitegen /system/etc/90-sitegen --overwrite --config-path ${SCRIPTS}/site.yaml
${CARGO} mknod column-rename /system/etc/10-hrename --overwrite --config-path ${SCRIPTS}/hrename.yaml

echo
echo "=== Update complete ==="
echo "Next: ./export.sh  # rebuild the site"
