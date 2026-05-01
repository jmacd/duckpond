#!/bin/bash
# Download and bundle JavaScript vendor dependencies for offline use.
#
# Run this once (requires network) to populate vendor/dist/.
# After that, sitegen generates sites that work fully offline
# with no CDN dependency.
#
# If brotli and gzip are available, pre-compressed (.br, .gz) variants
# are created alongside the originals for production serving.
#
# Prerequisites: Node.js >= 18, npm
# Optional: brotli (for .br pre-compression), gzip (for .gz)
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

# -- Cleanup work directory --
cd "${SCRIPT_DIR}"
rm -rf "${WORK_DIR}"

# -- Pre-compress for production serving --
# Brotli and gzip pre-compressed files let Caddy/nginx serve compressed
# responses without on-the-fly compression overhead. These are optional;
# the site works without them, just with larger transfers.

HAS_BROTLI=false
HAS_GZIP=false
if command -v brotli >/dev/null 2>&1; then
    HAS_BROTLI=true
fi
if command -v gzip >/dev/null 2>&1; then
    HAS_GZIP=true
fi

if [ "$HAS_BROTLI" = true ] || [ "$HAS_GZIP" = true ]; then
    echo "Pre-compressing vendor files..."
    for f in "${DIST_DIR}"/*.mjs "${DIST_DIR}"/*.js "${DIST_DIR}"/*.wasm; do
        [ -f "$f" ] || continue
        if [ "$HAS_BROTLI" = true ]; then
            brotli -f -k -q 11 "$f"
        fi
        if [ "$HAS_GZIP" = true ]; then
            gzip -f -k -9 "$f"
        fi
    done
else
    echo "Note: brotli/gzip not found; skipping pre-compression."
    echo "  Install brotli for smaller production downloads."
fi

echo ""
echo "=== Vendor files ready ==="
ls -lh "${DIST_DIR}/"
echo ""
echo "Total:"
du -sh "${DIST_DIR}"
