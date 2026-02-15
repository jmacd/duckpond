#!/bin/sh
#
# setup.sh — Initialize the septic station pond and install all factory nodes.
#
# Run this ONCE on a fresh pond. To update configs later, use update.sh.
#
# Prerequisites:
#   - pond.sh can reach septicplaystation.local
#   - podman installed on the BeaglePlay
#   - R2_ENDPOINT, R2_KEY, R2_SECRET set for backup
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

# Wipe the podman volume for a clean start
ssh ${HOST} "podman volume rm -f pond-data && podman volume create pond-data"

# Initialize the pond
${EXE} init

# Create directory structure
${EXE} mkdir -p /etc/system.d
${EXE} mkdir -p /ingest

# Copy site templates into the pond
# (config is mounted at /config inside container)
${EXE} copy host:///config/site /etc/site

# Install factory nodes
# (ingest reads from /data which is the host data dir mounted into container)
${EXE} mknod logfile-ingest /etc/ingest --config-path /config/ingest.yaml

${EXE} mknod remote /etc/system.d/1-backup --config-path /config/backup.yaml

${EXE} mknod dynamic-dir /reduced --config-path /config/reduce.yaml

${EXE} mknod sitegen /etc/site.yaml --config-path /config/site.yaml

echo "=== Setup complete ==="
echo "Next steps:"
echo "  ./run.sh          # ingest data from logfiles"
echo "  ./generate.sh     # build the static site"

