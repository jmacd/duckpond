#!/bin/bash
# Render the Caspar Water site locally
#
# Uses hostmount: no pond required. The host directory IS the filesystem.
#
# Usage:
#   ./render.sh              # Build and open in browser
#   ./render.sh --no-open    # Build only
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTDIR="${SCRIPT_DIR}/dist"

pond() { cargo run --quiet --manifest-path "${REPO_ROOT}/Cargo.toml" -- "$@"; }

export RUST_LOG=info

# Generate site directly from host filesystem
echo "=== Generating site ==="
rm -rf "${OUTDIR}"
mkdir -p "${OUTDIR}"
pond run -d "${SCRIPT_DIR}" host+sitegen:///site.yaml build "${OUTDIR}"

echo ""
echo "=== Site generated ==="
echo "Output: ${OUTDIR}"
find "${OUTDIR}" -name '*.html' | sort | sed 's|^|  |'

# Open in browser unless --no-open
if [[ "$1" != "--no-open" ]]; then
    BROWSER_DIR="${REPO_ROOT}/testsuite/browser"
    if [[ -f "${BROWSER_DIR}/package.json" ]]; then
        echo ""
        echo "=== Opening in browser ==="
        (cd "${BROWSER_DIR}" && SITE_ROOT="${OUTDIR}" npx vite --port 4175 --open)
    else
        open "${OUTDIR}/index.html"
    fi
fi
