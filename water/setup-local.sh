#!/bin/sh
#
# setup-local.sh -- Initialize a local water pond and install factory nodes.
#
# Prerequisites:
#   - rsync data from remote first:
#     rsync -chavzP --update --stats jmacd@linux.local:/home/data/ ./data/
#   - cargo build works in the workspace root
#
# The pond is stored in ./pond/ (relative to this script's directory).
# Data files are read from ./data/ via absolute paths.
#
set -x
set -e

SCRIPTS=$(cd "$(dirname "$0")" && pwd)
POND_DIR=${SCRIPTS}/pond
DATA_DIR=${SCRIPTS}/data
HOST=jmacd@linux.local
REMOTE_DATA=/home/data

export POND=${POND_DIR}

# Cargo run helper
CARGO="cargo run --release -p cmd --"

# Sync data from remote (--update preserves newer local copies)
mkdir -p "${DATA_DIR}"
rsync -chavzP --update --stats ${HOST}:${REMOTE_DATA}/ ${DATA_DIR}/

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
${CARGO} mkdir -p /etc/system.d
${CARGO} mkdir -p /ingest

# Copy site templates and content into the pond
${CARGO} copy host:///${SCRIPTS}/site /site
${CARGO} copy host:///${SCRIPTS}/content /content

# Install factory nodes
${CARGO} mknod logfile-ingest /etc/ingest --config-path "${INGEST_CFG}"
${CARGO} mknod dynamic-dir /reduced --config-path ${SCRIPTS}/reduce-remote.yaml
${CARGO} mknod sitegen /etc/site.yaml --config-path ${SCRIPTS}/site.yaml

rm -f "${INGEST_CFG}"

echo
echo "=== Setup complete ==="
echo "Next: ./run-local.sh       # ingest data"
echo "Then: ./generate-local.sh  # build the site"
