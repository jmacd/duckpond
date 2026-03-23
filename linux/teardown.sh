#!/bin/bash
# Tear down the pond and MinIO bucket.
#
# Usage:
#   ./linux/teardown.sh
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/env.sh"

echo "=== Stopping timer if active ==="
systemctl --user stop pond-journal.timer 2>/dev/null || true
systemctl --user disable pond-journal.timer 2>/dev/null || true

echo "=== Removing pond ==="
rm -rf "$POND"

echo "=== Clearing MinIO bucket ==="
mc rm --recursive --force local/duckpond-linux 2>/dev/null || true
mc rb local/duckpond-linux 2>/dev/null || true

echo "=== Removing cron log ==="
rm -f "$HOME/pond-cron.log"

echo "Teardown complete."
