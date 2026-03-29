#!/bin/sh
set -x
set -e

SCRIPTS=$(cd "$(dirname "$0")" && pwd)
export POND=${SCRIPTS}/pond
export RUST_BACKTRACE=1
export POND_MAX_ALLOC_MB=1000

LOCAL_OUTDIR=${SCRIPTS}/export
CARGO="cargo run --release -p cmd --"

rm -rf ${LOCAL_OUTDIR}
mkdir -p ${LOCAL_OUTDIR}

${CARGO} run /system/etc/90-sitegen build ${LOCAL_OUTDIR}

echo
echo "Site generated at: ${LOCAL_OUTDIR}"
echo "To preview:"
echo "  SITE_ROOT=${LOCAL_OUTDIR} BASE_PATH=/noyo-harbor/ npx vite --config ${SCRIPTS}/../testsuite/browser/vite.config.js --port 4174 --open"
