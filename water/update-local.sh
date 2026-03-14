#!/bin/sh
#
# update-local.sh -- Update configs and recreate factory nodes in the local pond.
#
# Use this after editing any .yaml, site/*, or content/* files.
#
set -x
set -e

SCRIPTS=$(cd "$(dirname "$0")" && pwd)
DATA_DIR=${SCRIPTS}/data

export POND=${SCRIPTS}/pond

CARGO="cargo run --release -p cmd --"

# Generate ingest config with absolute paths
INGEST_CFG=$(mktemp)
cat > "${INGEST_CFG}" <<EOF
archived_pattern: ${DATA_DIR}/casparwater-*.json
active_pattern: ${DATA_DIR}/casparwater.json
pond_path: /ingest
EOF

# Recreate factory nodes with --overwrite
${CARGO} mknod logfile-ingest /etc/ingest --overwrite --config-path "${INGEST_CFG}"
${CARGO} mknod dynamic-dir /reduced --overwrite --config-path ${SCRIPTS}/reduce-remote.yaml
${CARGO} mknod dynamic-dir /analysis --overwrite --config-path ${SCRIPTS}/analysis-remote.yaml
${CARGO} mknod sitegen /etc/site.yaml --overwrite --config-path ${SCRIPTS}/site.yaml

rm -f "${INGEST_CFG}"

echo
echo "=== Update complete ==="
echo "Next: ./generate-local.sh  # rebuild the site"
