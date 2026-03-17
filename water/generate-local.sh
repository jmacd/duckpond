#!/bin/sh
#
# generate-local.sh -- Run the sitegen build to produce static HTML locally.
#
set -x
set -e

SCRIPTS=$(cd "$(dirname "$0")" && pwd)
export POND=${SCRIPTS}/pond

LOCAL_OUTDIR=${SCRIPTS}/export
CARGO="cargo run --release -p cmd --"

# Clear and recreate output dir
rm -rf "${LOCAL_OUTDIR}"
mkdir -p "${LOCAL_OUTDIR}"

# Run sitegen build
${CARGO} run /etc/site.yaml build "${LOCAL_OUTDIR}"

echo
echo "Site generated at: ${LOCAL_OUTDIR}"
echo "To preview: cd ${LOCAL_OUTDIR} && python3 -m http.server 4177"
