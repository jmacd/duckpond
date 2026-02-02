#!/bin/sh
set -x
set -e

HOST=debian@septicplaystation.local
REMOTE_CONFIG=/home/debian/config
REMOTE_POND=/home/debian/pond

SCRIPTS=$(cd "$(dirname "$0")" && pwd)
EXE=${SCRIPTS}/pond.sh

# Copy config files to remote host
scp backup.yaml ingest.yaml ${HOST}:${REMOTE_CONFIG}/

# Ensure the pond data directory exists on remote host
ssh ${HOST} "mkdir -p ${REMOTE_POND}"

${EXE} init

${EXE} mkdir -p /etc/system.d

${EXE} mkdir -p /ingest

${EXE} mknod remote /etc/system.d/1-backup --config-path ${REMOTE_CONFIG}/backup.yaml

${EXE} mknod logfile-ingest /etc/ingest --config-path ${REMOTE_CONFIG}/ingest.yaml

