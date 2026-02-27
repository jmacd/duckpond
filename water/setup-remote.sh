#!/bin/sh
#
# setup-remote.sh -- Initialize the water pond on linux.local and install
#                    all factory nodes.
#
# Run this ONCE on a fresh pond. To update configs later, use update-remote.sh.
#
# Prerequisites:
#   - pond-remote.sh can reach linux.local
#   - podman installed on linux.local
#
set -x
set -e

HOST=jmacd@linux.local
REMOTE_CONFIG=/home/jmacd/water-config

SCRIPTS=$(cd "$(dirname "$0")" && pwd)
EXE=${SCRIPTS}/pond-remote.sh

# Copy all config files to remote host
ssh ${HOST} "mkdir -p ${REMOTE_CONFIG}/site ${REMOTE_CONFIG}/content"
scp \
    ingest.yaml \
    reduce-remote.yaml \
    site-remote.yaml \
    ${HOST}:${REMOTE_CONFIG}/
scp site/index.md site/data.md site/sidebar.md ${HOST}:${REMOTE_CONFIG}/site/
scp content/*.md ${HOST}:${REMOTE_CONFIG}/content/

# Wipe the podman volume for a clean start
ssh ${HOST} "podman volume rm pond-water 2>/dev/null; podman volume create pond-water"

# Initialize the pond
${EXE} init

# Create directory structure
${EXE} mkdir -p /etc/system.d
${EXE} mkdir -p /ingest

# Copy site templates and content into the pond
# (config is mounted at /config inside container)
${EXE} copy host:///config/site /site
${EXE} copy host:///config/content /content

# Install factory nodes
# (ingest reads from /data which is the host data dir mounted into container)
${EXE} mknod logfile-ingest /etc/ingest --config-path /config/ingest.yaml

${EXE} mknod dynamic-dir /reduced --config-path /config/reduce-remote.yaml

${EXE} mknod sitegen /etc/site.yaml --config-path /config/site-remote.yaml

echo "=== Setup complete ==="
echo "Next steps:"
echo "  ./run-remote.sh          # ingest data from logfiles"
echo "  ./generate-remote.sh     # build the static site"
