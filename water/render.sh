#!/bin/bash
# Render the Caspar Water site locally
#
# Usage:
#   ./render.sh              # Build and open in browser
#   ./render.sh --no-open    # Build only
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTDIR="${SCRIPT_DIR}/dist"
POND_DIR="${SCRIPT_DIR}/.pond"

pond() { cargo run --quiet --manifest-path "${REPO_ROOT}/Cargo.toml" -- "$@"; }

export POND="${POND_DIR}"
export RUST_LOG=info

# Fresh pond every time
echo "=== Initializing pond ==="
rm -rf "${POND_DIR}"
pond init

# Copy content files
echo "=== Loading content ==="
pond mkdir /content
for f in "${SCRIPT_DIR}"/content/*.md; do
    pond copy "host://${f}" "/content/$(basename "$f")"
done
echo "  $(ls "${SCRIPT_DIR}"/content/*.md | wc -l | tr -d ' ') content files"

# Copy site templates
echo "=== Loading site templates ==="
pond mkdir /etc
pond mkdir /etc/site
for f in "${SCRIPT_DIR}"/site/*.md; do
    pond copy "host://${f}" "/etc/site/$(basename "$f")"
done

# Create sitegen node with config
pond mknod sitegen /etc/site.yaml --config-path "${SCRIPT_DIR}/site.yaml"

# Generate site
echo "=== Generating site ==="
rm -rf "${OUTDIR}"
mkdir -p "${OUTDIR}"
pond run /etc/site.yaml build "${OUTDIR}"

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
