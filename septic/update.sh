#!/bin/sh
#
# update.sh — Push updated configs to the remote and recreate factory nodes.
#
# Use this after editing any .yaml or site/* files locally.
#
set -x
set -e

HOST=debian@septicplaystation.local
REMOTE_CONFIG=/home/debian/config

SCRIPTS=$(cd "$(dirname "$0")" && pwd)
EXE=${SCRIPTS}/pond.sh

# Copy all config files to remote host
ssh ${HOST} "mkdir -p ${REMOTE_CONFIG}/site"
scp \
    ingest.yaml \
    backup.yaml \
    reduce.yaml \
    site.yaml \
    ${HOST}:${REMOTE_CONFIG}/
scp site/index.md site/data.md site/sidebar.md ${HOST}:${REMOTE_CONFIG}/site/

# Update site templates in the pond
# (config is mounted at /config inside container)
${EXE} copy host:///config/site /etc/site --overwrite

# Recreate factory nodes with --overwrite
${EXE} mknod logfile-ingest /etc/ingest --overwrite --config-path /config/ingest.yaml

${EXE} mknod remote /etc/system.d/1-backup --overwrite --config-path /config/backup.yaml

${EXE} mknod dynamic-dir /reduced --overwrite --config-path /config/reduce.yaml

${EXE} mknod sitegen /etc/site.yaml --overwrite --config-path /config/site.yaml
