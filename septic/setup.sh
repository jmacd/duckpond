#!/bin/sh
#
# setup.sh — Initialize a local septic pond and install all factory nodes.
#
# Run this ONCE on a fresh pond. To update configs later, use update.sh.
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

# Load deployment config (for rsync source and S3 credentials)
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
archived_pattern: ${DATA_DIR}/septicstation.json.*
active_pattern: ${DATA_DIR}/septicstation.json
pond_path: /ingest
EOF

# Expand env vars in backup.yaml (S3 credentials from deploy.env)
export S3_URL S3_ENDPOINT S3_ACCESS_KEY S3_SECRET_KEY S3_ALLOW_HTTP
BACKUP_CFG=$(mktemp)
envsubst < "${SCRIPTS}/backup.yaml" > "${BACKUP_CFG}"

# Wipe and initialize
rm -rf "${POND_DIR}"
${CARGO} init

# Create directory structure
${CARGO} mkdir -p /system/run
${CARGO} mkdir -p /ingest
${CARGO} mkdir -p /etc

# Copy site templates into the pond
${CARGO} copy host:///${SCRIPTS}/site /etc/site

# Install factory nodes
${CARGO} mknod logfile-ingest /etc/ingest --config-path "${INGEST_CFG}"
${CARGO} mknod remote /system/run/1-backup --config-path "${BACKUP_CFG}"
${CARGO} mknod dynamic-dir /reduced --config-path ${SCRIPTS}/reduce.yaml
${CARGO} mknod sitegen /etc/site.yaml --config-path ${SCRIPTS}/site.yaml

rm -f "${INGEST_CFG}" "${BACKUP_CFG}"

echo
echo "=== Setup complete ==="
echo "Next: ./run.sh          # sync + ingest data"
echo "Then: ./generate.sh     # build the site"

