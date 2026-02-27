#!/bin/sh
#
# run-local.sh -- Ingest log data from local data directory.
#
set -x
set -e

SCRIPTS=$(cd "$(dirname "$0")" && pwd)
DATA_DIR=${SCRIPTS}/data
HOST=jmacd@linux.local
REMOTE_DATA=/home/data

export POND=${SCRIPTS}/pond

CARGO="cargo run --release -p cmd --"

# Sync latest data from remote (--update preserves newer local copies)
rsync -chavzP --update --stats ${HOST}:${REMOTE_DATA}/ ${DATA_DIR}/

# Ingest new/updated files
${CARGO} run /etc/ingest
