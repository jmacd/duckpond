#!/bin/bash
# Browser validation for sitegen output.
#
# Orchestrates four stages:
#   1. Generate root site via test 201 in Docker   → /tmp/test-output/
#   2. Serve at / with Vite + validate with Puppeteer (root deployment)
#   3. Generate subdir site via test 205 in Docker  → /tmp/test-output-subdir/
#   4. Serve at /myapp/ with Vite + validate with Puppeteer (subdir deployment)
#
# Prerequisites: Node.js >=22, Docker
# First-time:    cd testsuite/browser && npm install
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTSUITE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT_OUTPUT="/tmp/test-output"
SUBDIR_OUTPUT="/tmp/test-output-subdir"
PORT=4174
VITE_PID=""

start_vite() {
    local site_root="$1"
    local base_path="${2:-/}"

    SITE_ROOT="${site_root}" BASE_PATH="${base_path}" \
        npx vite --port ${PORT} --config "${SCRIPT_DIR}/vite.config.js" &
    VITE_PID=$!

    local url="http://localhost:${PORT}${base_path}"
    for i in $(seq 1 30); do
        if curl -sf "${url}index.html" > /dev/null 2>&1; then
            echo "  ✓ Vite serving at ${url}"
            return 0
        fi
        sleep 1
    done
    echo "ERROR: Vite did not start within 30 seconds (tried ${url}index.html)"
    return 1
}

stop_vite() {
    if [[ -n "${VITE_PID}" ]]; then
        kill "${VITE_PID}" 2>/dev/null || true
        wait "${VITE_PID}" 2>/dev/null || true
        VITE_PID=""
    fi
}

cleanup() {
    stop_vite
}
trap cleanup EXIT

# Ensure dependencies are installed
if [[ ! -d "${SCRIPT_DIR}/node_modules" ]]; then
    echo "Installing browser test dependencies..."
    (cd "${SCRIPT_DIR}" && npm install)
fi

# ── Stage 1: Generate root site via test 201 ────────────────

echo "=== Stage 1: Generating root site via test 201 ==="
bash "${TESTSUITE_DIR}/run-test.sh" --output "${ROOT_OUTPUT}" 201

if [[ ! -f "${ROOT_OUTPUT}/index.html" ]]; then
    echo "ERROR: Root site generation failed — no index.html in ${ROOT_OUTPUT}"
    exit 1
fi
echo "  ✓ Root site generated in ${ROOT_OUTPUT}"

# ── Stage 2: Validate root site ─────────────────────────────

echo ""
echo "=== Stage 2: Browser validation (root: /) ==="

start_vite "${ROOT_OUTPUT}" "/"
BASE_URL="http://localhost:${PORT}" BASE_PATH="/" SITE_ROOT="${ROOT_OUTPUT}" \
    node "${SCRIPT_DIR}/validate.mjs"
stop_vite

# ── Stage 3: Generate subdir site in Docker ──────────────────

echo ""
echo "=== Stage 3: Generating subdir site (base_url: /myapp/) ==="

rm -rf "${SUBDIR_OUTPUT}"
mkdir -p "${SUBDIR_OUTPUT}"

docker run --rm \
    -e POND=/pond \
    -e RUST_LOG=info \
    -v "${SUBDIR_OUTPUT}:/output" \
    -v "${TESTSUITE_DIR}/tests/205-sitegen-base-url.sh:/test/run.sh:ro" \
    duckpond-test:latest \
    -c "/bin/bash /test/run.sh && cp -r /tmp/sitegen-subdir/* /output/" > /tmp/test-202-subdir.log 2>&1

if [[ ! -f "${SUBDIR_OUTPUT}/index.html" ]]; then
    echo "ERROR: Subdir site generation failed — no index.html"
    echo "Log:"
    tail -20 /tmp/test-202-subdir.log
    exit 1
fi
echo "  ✓ Subdir site generated in ${SUBDIR_OUTPUT}"

# ── Stage 4: Validate subdir site ───────────────────────────

echo ""
echo "=== Stage 4: Browser validation (subdir: /myapp/) ==="

start_vite "${SUBDIR_OUTPUT}" "/myapp/"
BASE_URL="http://localhost:${PORT}" BASE_PATH="/myapp/" SITE_ROOT="${SUBDIR_OUTPUT}" \
    node "${SCRIPT_DIR}/validate.mjs"
stop_vite

echo ""
echo "=== All browser validation PASSED ==="
