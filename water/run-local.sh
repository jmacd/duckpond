#!/bin/sh
#
# run-local.sh -- Sync data from remote and ingest into the local pond.
#
set -x
set -e

SCRIPTS=$(cd "$(dirname "$0")" && pwd)
DATA_DIR=${SCRIPTS}/data

export POND=${SCRIPTS}/pond

# Load deployment config (for rsync source)
. "${SCRIPTS}/deploy.env"

CARGO="cargo run --release -p cmd --"

# Sync latest data from remote (skip if data already exists locally)
if [ -z "$(ls -A "${DATA_DIR}" 2>/dev/null)" ]; then
  rsync -chavzP --update --stats ${DEPLOY_HOST}:${DEPLOY_DATA_DIR}/ ${DATA_DIR}/
fi

# Ingest new/updated files
${CARGO} run /etc/ingest
