#!/bin/sh
#
# generate.sh — Run the sitegen build to produce static HTML.
#
# The built site lands on the remote at /home/debian/site-output/.
# After running, you can rsync it down or serve it locally.
#
set -x
set -e

SCRIPTS=$(cd "$(dirname "$0")" && pwd)
EXE=${SCRIPTS}/pond.sh

HOST=debian@septicplaystation.local
REMOTE_OUTDIR=/home/debian/site-output
LOCAL_OUTDIR=${SCRIPTS}/export

# Clear remote output dir
ssh ${HOST} "rm -rf ${REMOTE_OUTDIR} && mkdir -p ${REMOTE_OUTDIR}"

# Run sitegen build (output dir is mounted at /output inside container)
${EXE} run /etc/site.yaml build /output

# Rsync the built site to local
rm -rf ${LOCAL_OUTDIR}
mkdir -p ${LOCAL_OUTDIR}
rsync -avz ${HOST}:${REMOTE_OUTDIR}/ ${LOCAL_OUTDIR}/

echo
echo "Site generated at: ${LOCAL_OUTDIR}"
echo "To preview: npx vite ${LOCAL_OUTDIR} --port 4174 --open"
