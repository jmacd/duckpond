#!/bin/sh
#
# update.sh — Update configs and recreate factory nodes in the local pond.
#
# Use this after editing any .yaml or site/* files.
#
set -x
set -e

SCRIPTS=$(cd "$(dirname "$0")" && pwd)
DATA_DIR=${SCRIPTS}/data

export POND=${SCRIPTS}/pond

# Load deployment config (for S3 credentials)
. "${SCRIPTS}/deploy.env"

CARGO="cargo run --release -p cmd --"

# Generate ingest config with absolute paths
INGEST_CFG=$(mktemp)
cat > "${INGEST_CFG}" <<EOF
archived_pattern: ${DATA_DIR}/septicstation.json.*
active_pattern: ${DATA_DIR}/septicstation.json
pond_path: /ingest
EOF

# Update site templates in the pond
for f in index.md data.md sidebar.md; do
    ${CARGO} copy host:///${SCRIPTS}/site/${f} /etc/site/${f}
done

# Recreate factory nodes with --overwrite
${CARGO} mknod logfile-ingest /etc/ingest --overwrite --config-path "${INGEST_CFG}"
${CARGO} mknod remote /system/run/1-backup --overwrite --config-path "${SCRIPTS}/backup.yaml"
${CARGO} mknod dynamic-dir /reduced --overwrite --config-path ${SCRIPTS}/reduce.yaml
${CARGO} mknod sitegen /etc/site.yaml --overwrite --config-path ${SCRIPTS}/site.yaml

rm -f "${INGEST_CFG}"

echo
echo "=== Update complete ==="
echo "Next: ./generate.sh  # rebuild the site"
