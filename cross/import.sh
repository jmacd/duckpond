#!/bin/sh
#
# import.sh -- Pull data from all three source ponds.
#
# Run this after setup.sh to download the actual data from remote backups.
# Re-run periodically to pull new data from the source ponds.
#
set -x
set -e

SCRIPTS=$(cd "$(dirname "$0")" && pwd)
export POND=${SCRIPTS}/pond

CARGO="cargo run --release -p cmd --"

echo "=== Pulling data from source ponds ==="

echo ""
echo "--- Noyo ---"
${CARGO} run /system/etc/10-noyo pull

echo ""
echo "--- Septic ---"
${CARGO} run /system/etc/11-septic pull

echo ""
echo "--- Water ---"
${CARGO} run /system/etc/12-water pull

echo ""
echo "=== Import complete ==="
echo ""

# Show what we got
echo "=== Imported content ==="
${CARGO} list '/sources/**'

echo ""
echo "Next: ./generate.sh  # build the combined site"
