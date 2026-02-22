#!/bin/bash
# Browser validation for sitegen output.
#
# Orchestrates:
#   1. Generate root site via test 201 in Docker   → /tmp/test-output/
#   2. Serve at / with Vite, run all browser tests (root deployment)
#   3. Generate subdir site via test 205 in Docker  → /tmp/test-output-subdir/
#   4. Serve at /myapp/ with Vite, run all browser tests (subdir deployment)
#
# Browser tests live in browser/tests/NNN-name.mjs, numbered in the same
# space as testsuite/tests/ (200-series).  All discovered tests are run
# against each deployment.
#
# Prerequisites: Node.js >=22, Docker
# First-time:    cd testsuite/browser && npm install
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTSUITE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TESTS_DIR="${SCRIPT_DIR}/tests"
ROOT_OUTPUT="/tmp/test-output"
SUBDIR_OUTPUT="/tmp/test-output-subdir"
PORT=4174
VITE_PID=""

# Forward --no-rebuild to run-test.sh calls
RUN_TEST_EXTRA_ARGS=()
for arg in "$@"; do
    case "$arg" in
        --no-rebuild|-n) RUN_TEST_EXTRA_ARGS+=("--no-rebuild") ;;
    esac
done

start_vite() {
    local site_root="$1"
    local base_path="${2:-/}"

    # Kill anything already listening on our port
    lsof -ti:${PORT} 2>/dev/null | xargs kill -9 2>/dev/null; true
    sleep 0.5

    SITE_ROOT="${site_root}" BASE_PATH="${base_path}" \
        npx vite --port ${PORT} --strictPort --config "${SCRIPT_DIR}/vite.config.js" &
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
    # Also kill anything still listening on the port (orphaned processes)
    lsof -ti:${PORT} 2>/dev/null | xargs kill -9 2>/dev/null; true
}

cleanup() {
    stop_vite
}
trap cleanup EXIT

# Discover browser tests (NNN-name.mjs in tests/)
BROWSER_TESTS=()
for t in "${TESTS_DIR}"/[0-9]*-*.mjs; do
    [[ -f "$t" ]] && BROWSER_TESTS+=("$t")
done

if [[ ${#BROWSER_TESTS[@]} -eq 0 ]]; then
    echo "ERROR: No browser tests found in ${TESTS_DIR}/"
    exit 1
fi

echo "Browser tests discovered:"
for t in "${BROWSER_TESTS[@]}"; do
    echo "  $(basename "$t")"
done

# Ensure dependencies are installed
if [[ ! -d "${SCRIPT_DIR}/node_modules" ]]; then
    echo "Installing browser test dependencies..."
    (cd "${SCRIPT_DIR}" && npm install)
fi

# Run all browser tests against a served site.
# Args: base_url base_path site_root
run_browser_tests() {
    local base_url="$1"
    local base_path="$2"
    local site_root="$3"

    for t in "${BROWSER_TESTS[@]}"; do
        local name
        name="$(basename "$t")"
        echo ""
        echo "--- ${name} ---"
        BASE_URL="${base_url}" BASE_PATH="${base_path}" SITE_ROOT="${site_root}" \
            node "$t"
    done
}

# ── Stage 1: Generate root site via test 201 ────────────────

echo "=== Stage 1: Generating root site via test 201 ==="
bash "${TESTSUITE_DIR}/run-test.sh" "${RUN_TEST_EXTRA_ARGS[@]}" --output "${ROOT_OUTPUT}" 201

if [[ ! -f "${ROOT_OUTPUT}/index.html" ]]; then
    echo "ERROR: Root site generation failed — no index.html in ${ROOT_OUTPUT}"
    exit 1
fi
echo "  ✓ Root site generated in ${ROOT_OUTPUT}"

# ── Stage 2: Run browser tests (root: /) ────────────────────

echo ""
echo "=== Stage 2: Browser tests (root: /) ==="

start_vite "${ROOT_OUTPUT}" "/"
run_browser_tests "http://localhost:${PORT}" "/" "${ROOT_OUTPUT}"
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
    -v "${TESTSUITE_DIR}/testdata:/data:ro" \
    duckpond-test:latest \
    -c "/bin/bash /test/run.sh && cp -r /tmp/sitegen-subdir/* /output/" > /tmp/test-202-subdir.log 2>&1

if [[ ! -f "${SUBDIR_OUTPUT}/index.html" ]]; then
    echo "ERROR: Subdir site generation failed — no index.html"
    echo "Log:"
    tail -20 /tmp/test-202-subdir.log
    exit 1
fi
echo "  ✓ Subdir site generated in ${SUBDIR_OUTPUT}"

# ── Stage 4: Run browser tests (subdir: /myapp/) ────────────

echo ""
echo "=== Stage 4: Browser tests (subdir: /myapp/) ==="

start_vite "${SUBDIR_OUTPUT}" "/myapp/"
run_browser_tests "http://localhost:${PORT}" "/myapp/" "${SUBDIR_OUTPUT}"
stop_vite

echo ""
echo "=== All browser tests PASSED ==="
