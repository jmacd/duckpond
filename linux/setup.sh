#!/bin/bash
# Setup DuckPond for Linux log collection on watershop.local
#
# Destroys any existing pond at $POND, clears the MinIO bucket,
# and initializes everything from scratch.
#
# Prerequisites:
#   - pond binary installed (~/.cargo/bin/pond)
#   - MinIO running at localhost:9000
#   - mc (MinIO client) installed at ~/.local/bin/mc
#
# Usage:
#   ./linux/setup.sh
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/env.sh"

echo "=== Tearing down existing state ==="
rm -rf "$POND"
mc rm --recursive --force local/duckpond-linux 2>/dev/null || true
mc mb local/duckpond-linux --ignore-existing

echo "=== Initializing pond at $POND ==="
pond init

echo "=== Creating directory structure ==="
pond mkdir /logs
pond mkdir /logs/watershop
pond mkdir /system
pond mkdir /system/etc
pond mkdir /system/run

echo "=== Installing journal-ingest factory ==="
pond mknod --config-path "$SCRIPT_DIR/journal-ingest.yaml" \
    journal-ingest /system/etc/journal

echo "=== Installing remote backup factory ==="
pond mknod --config-path "$SCRIPT_DIR/backup.yaml" \
    remote /system/run/20-backup

echo "=== Installing sitegen config ==="
pond mkdir /site
for f in "$SCRIPT_DIR"/site/*.md; do
    name=$(basename "$f")
    pond copy "host:///$f" "/site/$name"
done
pond mknod --config-path "$SCRIPT_DIR/site.yaml" \
    sitegen /system/etc/site

echo "=== Running initial journal collection ==="
pond run /system/etc/journal push

echo "=== Verifying ==="
pond list '/logs/watershop/' | tail -5
echo ""
pond run /system/run/20-backup show 2>&1 | tail -5
echo ""
echo "Setup complete. Use './linux/install-timer.sh' to enable automatic collection."
