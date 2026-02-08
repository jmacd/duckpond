#!/bin/bash
# Run a DuckPond test in a fresh container
#
# Usage:
#   ./run-test.sh 201                       # Run test, inspect on failure
#   ./run-test.sh --inspect 201             # Run test, always open for inspection
#   ./run-test.sh --interactive             # Interactive shell in container
#   ./run-test.sh --inline 'pond init'      # Run inline commands
#
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_NAME="duckpond-test:latest"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

# Parse arguments
INTERACTIVE=false
INLINE_SCRIPT=""
SAVE_RESULT=false
SCRIPT_FILE=""
VERBOSE=false
NO_REBUILD=false
OUTPUT_DIR=""
USER_OUTPUT_DIR=""
INSPECT=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --output|-o)
            OUTPUT_DIR="$2"
            USER_OUTPUT_DIR="$2"
            shift 2
            ;;
        --inspect)
            INSPECT=true
            shift
            ;;
        --interactive|-i)
            INTERACTIVE=true
            shift
            ;;
        --inline)
            INLINE_SCRIPT="$2"
            shift 2
            ;;
        --save-result|-s)
            SAVE_RESULT=true
            shift
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --no-rebuild|-n)
            NO_REBUILD=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [options] [script.sh]"
            echo ""
            echo "Options:"
            echo "  --interactive, -i    Start interactive shell in container"
            echo "  --inline 'cmd'       Run inline shell commands"
            echo "  --save-result, -s    Save output to results/ directory"
            echo "  --verbose, -v        Show container output in real-time"
            echo "  --no-rebuild, -n     Skip automatic rebuild (use existing image)"
            echo "  --output, -o DIR     Mount DIR as /output in the container (for capturing results)"
            echo "  --inspect            Copy output for inspection (always; default on failure)"
            echo "  --help, -h           Show this help"
            echo ""
            echo "Examples:"
            echo "  $0 201                           # Run test 201"
            echo "  $0 --inspect 201                 # Run and open output for inspection"
            echo "  $0 --interactive                  # Interactive shell"
            echo "  $0 --inline 'pond init && pond list /'"
            exit 0
            ;;
        *)
            SCRIPT_FILE="$1"
            shift
            ;;
    esac
done

# Auto-rebuild unless --no-rebuild specified
if [[ "${NO_REBUILD}" == "false" ]]; then
    echo "=== Auto-rebuilding pond (debug mode) ==="
    "${SCRIPT_DIR}/build-image.sh" --quiet
    echo ""
elif ! docker image inspect "${IMAGE_NAME}" &>/dev/null; then
    echo "ERROR: Image ${IMAGE_NAME} not found."
    echo "Run: ./build-image.sh (or remove --no-rebuild)"
    exit 1
fi

# Create results directory
mkdir -p "${SCRIPT_DIR}/results"
mkdir -p "${SCRIPT_DIR}/tests"

# Helper to save results
save_result() {
    local exit_code=$1
    local output_file=$2
    local result_type
    
    if [[ ${exit_code} -eq 0 ]]; then
        result_type="success"
    else
        result_type="failure"
    fi
    
    local result_file="${SCRIPT_DIR}/results/${TIMESTAMP}-${result_type}.md"
    local result_type_upper
    result_type_upper=$(echo "${result_type}" | tr '[:lower:]' '[:upper:]')
    
    cat > "${result_file}" << EOF
# Test Result: ${result_type_upper}

**Timestamp**: ${TIMESTAMP}
**Exit Code**: ${exit_code}

## Script

\`\`\`bash
$(cat "${SCRIPT_FILE:-/dev/null}" 2>/dev/null || echo "${INLINE_SCRIPT}")
\`\`\`

## Output

\`\`\`
$(cat "${output_file}")
\`\`\`
EOF

    echo "Result saved to: ${result_file}"
}

# Run mode: Interactive
if [[ "${INTERACTIVE}" == "true" ]]; then
    echo "=== Interactive Test Mode ==="
    echo "Container: ${IMAGE_NAME}"
    echo "POND=/pond (fresh for each run)"
    echo ""
    echo "Useful commands:"
    echo "  pond --help           # CLI help"
    echo "  pond init             # Initialize pond"
    echo "  exit                  # Exit container"
    echo ""
    
    docker run --rm -it \
        -e POND=/pond \
        -e RUST_LOG=info \
        "${IMAGE_NAME}"
    exit 0
fi

# Run mode: Inline script
if [[ -n "${INLINE_SCRIPT}" ]]; then
    echo "=== Running Inline Test ==="
    
    TEMP_OUTPUT=$(mktemp)
    
    set +e
    docker run --rm \
        -e POND=/pond \
        -e RUST_LOG=info \
        "${IMAGE_NAME}" \
        -c "set -e; ${INLINE_SCRIPT}" 2>&1 | tee "${TEMP_OUTPUT}"
    EXIT_CODE=${PIPESTATUS[0]}
    set -e
    
    echo ""
    echo "=== Test ${EXIT_CODE:-0 == 0 ? 'SUCCEEDED' : 'FAILED'} (exit: ${EXIT_CODE}) ==="
    
    if [[ "${SAVE_RESULT}" == "true" ]]; then
        save_result "${EXIT_CODE}" "${TEMP_OUTPUT}"
    fi
    
    rm -f "${TEMP_OUTPUT}"
    exit ${EXIT_CODE}
fi

# Run mode: Script file
if [[ -n "${SCRIPT_FILE}" ]]; then
    # Support numeric shorthand: 032 -> tests/032-*.sh
    if [[ "${SCRIPT_FILE}" =~ ^[0-9]+$ ]]; then
        PATTERN="${SCRIPT_FILE}"
        # Zero-pad to 3 digits
        while [[ ${#PATTERN} -lt 3 ]]; do
            PATTERN="0${PATTERN}"
        done
        # Look in tests/
        FOUND=$(find "${SCRIPT_DIR}/tests" -maxdepth 1 -name "${PATTERN}-*.sh" 2>/dev/null | head -1)
        if [[ -n "${FOUND}" ]]; then
            SCRIPT_FILE="${FOUND}"
            echo "Resolved ${PATTERN} -> $(basename "${SCRIPT_FILE}")"
        else
            echo "ERROR: No test found matching ${PATTERN}-*.sh in tests/"
            exit 1
        fi
    fi
    
    # Resolve script path
    if [[ ! "${SCRIPT_FILE}" = /* ]]; then
        if [[ -f "${SCRIPT_DIR}/${SCRIPT_FILE}" ]]; then
            SCRIPT_FILE="${SCRIPT_DIR}/${SCRIPT_FILE}"
        elif [[ -f "${PWD}/${SCRIPT_FILE}" ]]; then
            SCRIPT_FILE="${PWD}/${SCRIPT_FILE}"
        fi
    fi
    
    if [[ ! -f "${SCRIPT_FILE}" ]]; then
        echo "ERROR: Script not found: ${SCRIPT_FILE}"
        exit 1
    fi
    
    SCRIPT_NAME=$(basename "${SCRIPT_FILE}")

    # Always capture output to /tmp/test-output for inspection
    # (--output overrides this if the user specifies a custom dir)
    if [[ -z "${OUTPUT_DIR}" ]]; then
        OUTPUT_DIR="/tmp/test-output"
    fi
    rm -rf "${OUTPUT_DIR}"
    mkdir -p "${OUTPUT_DIR}"
    OUTPUT_MOUNT=(-v "${OUTPUT_DIR}:/output")

    # Full log always goes to a known file for post-hoc inspection
    LOG_FILE="/tmp/test-${SCRIPT_NAME%.sh}.log"

    echo "=== Running Test: ${SCRIPT_NAME} ==="
    echo "Log: ${LOG_FILE}"

    # Run the script in the container — all output to log file, NOT to stdout
    set +e
    docker run --rm \
        -e POND=/pond \
        -e RUST_LOG=info \
        -v "${SCRIPT_FILE}:/test/run.sh:ro" \
        "${OUTPUT_MOUNT[@]}" \
        "${IMAGE_NAME}" \
        -c "/bin/bash /test/run.sh" > "${LOG_FILE}" 2>&1
    EXIT_CODE=$?
    set -e

    # Print a short summary
    # Print a short summary — extract from the test's own results line
    RESULTS_LINE=$(grep -E '^=== Results:' "${LOG_FILE}" 2>/dev/null || true)
    TOTAL=$(grep -cE '^  [✓✗]' "${LOG_FILE}" 2>/dev/null || echo 0)
    PASSED=$(grep -c '^  ✓' "${LOG_FILE}" 2>/dev/null || echo 0)
    FAILED_COUNT=$(grep -c '^  ✗' "${LOG_FILE}" 2>/dev/null || echo 0)

    if [[ ${EXIT_CODE} -eq 0 ]]; then
        echo "=== PASSED ${PASSED}/${TOTAL} checks ==="
    else
        echo "=== FAILED (exit: ${EXIT_CODE}) — ${PASSED}/${TOTAL} checks passed ==="
        # Show only the failing lines
        grep '^  ✗' "${LOG_FILE}" 2>/dev/null | sed 's/^/  /'
        # Show the last few lines for context on crashes/errors
        if [[ ${FAILED_COUNT} -eq 0 ]]; then
            echo "  (no check failures — last 10 lines of log:)"
            tail -10 "${LOG_FILE}" | sed 's/^/  /'
        fi
    fi
    
    if [[ "${SAVE_RESULT}" == "true" ]]; then
        save_result "${EXIT_CODE}" "${LOG_FILE}"
    fi

    # On failure, --inspect, or explicit --output, print how to inspect the output
    if [[ ${EXIT_CODE} -ne 0 ]] || [[ "${INSPECT}" == "true" ]] || [[ -n "${USER_OUTPUT_DIR}" ]]; then
        echo ""
        echo "Full log: ${LOG_FILE}  ($(wc -l < "${LOG_FILE}") lines)"
        echo "Output:   ${OUTPUT_DIR}"
        ls "${OUTPUT_DIR}" 2>/dev/null | head -20 | sed 's/^/  /'
        # If it looks like a sitegen site, print the serve command
        if [[ -f "${OUTPUT_DIR}/index.html" ]]; then
            echo ""
            echo "To inspect the site in a browser:"
            echo "  (cd ${SCRIPT_DIR}/browser && SITE_ROOT=${OUTPUT_DIR} npx vite --port 4174 --open)"
        fi
    fi

    exit ${EXIT_CODE}
fi

# No mode specified - default to interactive
echo "No script specified. Starting interactive mode..."
exec "$0" --interactive
