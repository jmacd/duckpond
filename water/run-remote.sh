#!/bin/sh
#
# run-remote.sh -- Ingest new log data from the OTelJSON rotated logfiles.
#
# Temporal-reduce is a dynamic factory (computed on read), so it doesn't
# need an explicit run step.  The reduced aggregations are materialized
# when sitegen reads them during generate-remote.sh.
#
set -x
set -e

SCRIPTS=$(cd "$(dirname "$0")" && pwd)
EXE=${SCRIPTS}/pond-remote.sh

# Ingest new log data from rotated OTelJSON files
${EXE} run /etc/ingest
