#!/bin/bash
# Browser validation for sitegen output.
#
# Orchestrates two stages:
#   1. Generate the site via test 201 in Docker (produces /tmp/test-output/)
#   2. Serve with Vite + validate with Puppeteer on the host
#
# Prerequisites: Node.js >=18, Docker
# First-time:    cd testsuite/browser && npm install
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTSUITE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="/tmp/test-output"
PORT=4174
VITE_PID=""

cleanup() {
    if [[ -n "${VITE_PID}" ]]; then
        kill "${VITE_PID}" 2>/dev/null || true
        wait "${VITE_PID}" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# ── Stage 1: Generate site in Docker ────────────────────────

echo "=== Stage 1: Generating site via test 201 ==="
bash "${TESTSUITE_DIR}/run-test.sh" --output "${OUTPUT_DIR}" 201

if [[ ! -f "${OUTPUT_DIR}/index.html" ]]; then
    echo "ERROR: Site generation failed — no index.html in ${OUTPUT_DIR}"
    exit 1
fi
echo "  ✓ Site generated in ${OUTPUT_DIR}"

# ── Stage 2: Serve + validate ───────────────────────────────

echo ""
echo "=== Stage 2: Browser validation ==="

# Ensure dependencies are installed
if [[ ! -d "${SCRIPT_DIR}/node_modules" ]]; then
    echo "Installing browser test dependencies..."
    (cd "${SCRIPT_DIR}" && npm install)
fi

# Start Vite in background
echo "Starting Vite on port ${PORT}..."
(cd "${SCRIPT_DIR}" && SITE_ROOT="${OUTPUT_DIR}" npx vite --port ${PORT} &)
VITE_PID=$!

# Wait for Vite to be ready
for i in $(seq 1 30); do
    if curl -s "http://localhost:${PORT}/index.html" > /dev/null 2>&1; then
        break
    fi
    if [[ $i -eq 30 ]]; then
        echo "ERROR: Vite did not start within 30 seconds"
        exit 1
    fi
    sleep 1
done
echo "  ✓ Vite serving at http://localhost:${PORT}"

# Run Puppeteer validation
echo ""
BASE_URL="http://localhost:${PORT}" node "${SCRIPT_DIR}/validate.mjs"
