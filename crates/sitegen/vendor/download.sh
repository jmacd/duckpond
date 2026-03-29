#!/bin/bash
# Download and bundle JavaScript vendor dependencies for offline use.
#
# Run this once (requires network) to populate vendor/dist/.
# After that, sitegen generates sites that work fully offline
# with no CDN dependency.
#
# Prerequisites: Node.js >= 18, npm
#
# Usage:
#   cd crates/sitegen/vendor && bash download.sh
#
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIST_DIR="${SCRIPT_DIR}/dist"
WORK_DIR="${SCRIPT_DIR}/.work"

echo "=== Downloading vendor dependencies ==="

rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}" "${DIST_DIR}"

cd "${WORK_DIR}"
npm init -y --silent > /dev/null 2>&1 || true

# Pin exact versions to match what chart.js/overlay.js expect
echo "Installing packages..."
npm install --save \
    @duckdb/duckdb-wasm@1.29.0 \
    @observablehq/plot@0.6 \
    d3@7 \
    esbuild \
    > /dev/null 2>&1

# -- DuckDB-WASM: copy dist files directly --
echo "Copying DuckDB-WASM files..."
cp node_modules/@duckdb/duckdb-wasm/dist/duckdb-eh.wasm "${DIST_DIR}/"
cp node_modules/@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js "${DIST_DIR}/"

# Bundle the DuckDB ESM wrapper (tree-shakes away Node.js code)
cat > _duckdb.mjs << 'EOF'
export * from "@duckdb/duckdb-wasm";
EOF
npx esbuild _duckdb.mjs --bundle --format=esm --outfile="${DIST_DIR}/duckdb-browser.mjs" --minify 2>&1

# -- Observable Plot + D3: bundle into a single file --
echo "Bundling Observable Plot + D3..."
cat > _plot.mjs << 'EOF'
export * as Plot from "@observablehq/plot";
export * as d3 from "d3";
EOF
npx esbuild _plot.mjs --bundle --format=esm --outfile="${DIST_DIR}/plot-d3-bundle.mjs" --minify 2>&1

# -- Cleanup --
cd "${SCRIPT_DIR}"
rm -rf "${WORK_DIR}"

echo ""
echo "=== Vendor files ready ==="
ls -lh "${DIST_DIR}/"
echo ""
echo "Total:"
du -sh "${DIST_DIR}"
