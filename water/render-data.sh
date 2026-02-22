#!/bin/bash
# Render the Caspar Water site with live data from casparwater-sample.json
#
# LEGACY: This builds a temporary pond. For the faster hostmount approach,
# use render.sh instead (no pond required).
#
# This script builds a temporary pond, ingests the sample oteljson data,
# creates temporal-reduce aggregations, and runs sitegen to produce
# a static site with interactive charts.
#
# Usage:
#   ./render-data.sh              # Build and open in browser
#   ./render-data.sh --no-open    # Build only
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTDIR="${SCRIPT_DIR}/dist"
POND_DIR="${SCRIPT_DIR}/.pond-data"
SAMPLE="${SCRIPT_DIR}/casparwater-sample.json"

if [[ ! -f "${SAMPLE}" ]]; then
    echo "ERROR: ${SAMPLE} not found"
    echo "Place the casparwater oteljson sample file alongside this script."
    exit 1
fi

pond() { cargo run --quiet --manifest-path "${REPO_ROOT}/Cargo.toml" -- "$@"; }

export POND="${POND_DIR}"
export RUST_LOG=info

echo "=== Building pond from sample data ==="

# Clean slate
rm -rf "${POND_DIR}"
pond init

# Create directory structure
pond mkdir -p /etc
pond mkdir -p /content
pond mkdir -p /site

# Copy the sample oteljson file to the pond root
# (reduce.yaml references oteljson:///casparwater-sample.json)
pond copy "host://${SAMPLE}" "/casparwater-sample.json"

echo ""
echo "=== Ingested data ==="
pond list '/casparwater-sample.json'

# Create temporal-reduce dynamic directory
pond mknod dynamic-dir /reduced --config-path "${SCRIPT_DIR}/reduce.yaml"

echo ""
echo "=== Reduced data tree ==="
pond list '/reduced/**/*'

# Copy content pages into the pond
for f in "${SCRIPT_DIR}"/content/*.md; do
    pond copy "host://${f}" "/content/$(basename "$f")"
done

# Copy site templates into the pond
for f in "${SCRIPT_DIR}"/site/*.md; do
    pond copy "host://${f}" "/site/$(basename "$f")"
done

# Create sitegen factory
pond mknod sitegen /site.yaml --config-path "${SCRIPT_DIR}/site.yaml"

# Generate the site
echo ""
echo "=== Generating site ==="
rm -rf "${OUTDIR}"
mkdir -p "${OUTDIR}"
pond run /site.yaml build "${OUTDIR}"

echo ""
echo "=== Site generated ==="
echo "Output: ${OUTDIR}"
find "${OUTDIR}" -name '*.html' | sort | sed 's|^|  |'

echo ""
echo "Data files:"
find "${OUTDIR}" -name '*.parquet' | wc -l | xargs echo "  parquet files:"

# Open in browser unless --no-open
if [[ "$1" != "--no-open" ]]; then
    BROWSER_DIR="${REPO_ROOT}/testsuite/browser"
    if [[ -f "${BROWSER_DIR}/package.json" ]]; then
        echo ""
        echo "=== Opening in browser ==="
        (cd "${BROWSER_DIR}" && SITE_ROOT="${OUTDIR}" npx vite --port 4176 --open)
    else
        open "${OUTDIR}/index.html"
    fi
fi
