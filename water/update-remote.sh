#!/bin/sh
#
# update-remote.sh -- Push updated configs to the remote and recreate
#                     factory nodes.
#
# Use this after editing any .yaml, site/*, or content/* files locally.
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

# Update site templates and content in the pond
# (config is mounted at /config inside container)
# Update site templates and content in the pond
${EXE} copy host:///config/site /site
${EXE} copy host:///config/content /content

# Recreate factory nodes with --overwrite
${EXE} mknod logfile-ingest /etc/ingest --overwrite --config-path /config/ingest.yaml

${EXE} mknod dynamic-dir /reduced --overwrite --config-path /config/reduce-remote.yaml

${EXE} mknod sitegen /etc/site.yaml --overwrite --config-path /config/site-remote.yaml
