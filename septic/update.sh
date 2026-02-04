#!/bin/sh
set -x
set -e

HOST=debian@septicplaystation.local
REMOTE_CONFIG=/home/debian/config

SCRIPTS=$(cd "$(dirname "$0")" && pwd)
EXE=${SCRIPTS}/pond.sh

# Copy updated config files to remote host
scp backup.yaml ingest.yaml ${HOST}:${REMOTE_CONFIG}/

${EXE} mknod remote /etc/system.d/1-backup --overwrite --config-path ${REMOTE_CONFIG}/backup.yaml

${EXE} mknod logfile-ingest /etc/ingest --overwrite --config-path ${REMOTE_CONFIG}/ingest.yaml
