#!/bin/sh
#
# generate.sh -- Build the combined site from all imported sources.
#
set -x
set -e

SCRIPTS=$(cd "$(dirname "$0")" && pwd)
export POND=${SCRIPTS}/pond
export RUST_BACKTRACE=1
export POND_MAX_ALLOC_MB=1000

LOCAL_OUTDIR=${SCRIPTS}/export
CARGO="cargo run --release -p cmd --"

# Clear and recreate output dir
rm -rf "${LOCAL_OUTDIR}"
mkdir -p "${LOCAL_OUTDIR}"

# Run sitegen build
${CARGO} run /system/etc/90-sitegen build "${LOCAL_OUTDIR}"

echo
echo "Site generated at: ${LOCAL_OUTDIR}"
echo "To preview: npx vite ${LOCAL_OUTDIR} --port 4175 --open"
