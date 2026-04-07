#!/bin/sh
#
# setup-local.sh -- Initialize a local water pond and install factory nodes.
#
# Prerequisites:
#   - deploy.env configured (cp deploy.env.example deploy.env)
#   - cargo build works in the workspace root
#
# The pond is stored in ./pond/ and data is rsynced to ./data/.
#
set -x
set -e

SCRIPTS=$(cd "$(dirname "$0")" && pwd)
POND_DIR=${SCRIPTS}/pond
DATA_DIR=${SCRIPTS}/data

export POND=${POND_DIR}

# Load deployment config (for rsync source)
. "${SCRIPTS}/deploy.env"

# Cargo run helper
CARGO="cargo run --release -p cmd --"

# Sync data from remote (skip if data already exists locally)
mkdir -p "${DATA_DIR}"
if [ -z "$(ls -A "${DATA_DIR}" 2>/dev/null)" ]; then
  rsync -chavzP --update --stats ${DEPLOY_HOST}:${DEPLOY_DATA_DIR}/ ${DATA_DIR}/
fi

# Generate ingest config with absolute paths
INGEST_CFG=$(mktemp)
cat > "${INGEST_CFG}" <<EOF
archived_pattern: ${DATA_DIR}/casparwater-*.json
active_pattern: ${DATA_DIR}/casparwater.json
pond_path: /ingest
EOF

# Wipe and initialize
rm -rf "${POND_DIR}"
${CARGO} init

# Create directory structure
${CARGO} mkdir -p /system/run
${CARGO} mkdir -p /etc/system.d
${CARGO} mkdir -p /ingest

# Copy site templates and content into the pond
${CARGO} copy host:///${SCRIPTS}/site /site
${CARGO} copy host:///${SCRIPTS}/content /content

# Install factory nodes
${CARGO} mknod logfile-ingest /etc/ingest --config-path "${INGEST_CFG}"
${CARGO} mknod remote /system/run/1-backup --config-path "${SCRIPTS}/backup.yaml"
${CARGO} mknod dynamic-dir /reduced --config-path ${SCRIPTS}/reduce-remote.yaml
${CARGO} mknod dynamic-dir /analysis --config-path ${SCRIPTS}/analysis-remote.yaml
${CARGO} mknod sitegen /etc/site.yaml --config-path ${SCRIPTS}/site.yaml

rm -f "${INGEST_CFG}"

echo
echo "=== Setup complete ==="
echo "Next: ./run-local.sh       # ingest data"
echo "Then: ./generate-local.sh  # build the site"
