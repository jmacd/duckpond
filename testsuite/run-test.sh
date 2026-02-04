#!/bin/bash
# Run a DuckPond test in a fresh container
#
# Usage:
#   ./run-test.sh                           # Interactive mode
#   ./run-test.sh script.sh                 # Run a script file
#   ./run-test.sh --inline 'pond init'      # Run inline commands
#   ./run-test.sh --save-result script.sh   # Run and save result to results/
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

while [[ $# -gt 0 ]]; do
    case $1 in
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
            echo "  --help, -h           Show this help"
            echo ""
            echo "Examples:"
            echo "  $0 --interactive"
            echo "  $0 tests/001-basic-init.sh"
            echo "  $0 --inline 'pond init && pond list /'"
            echo "  $0 --save-result tests/test.sh"
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
    echo "=== Running Test: ${SCRIPT_NAME} ==="
    
    TEMP_OUTPUT=$(mktemp)
    
    # Run the script in the container
    set +e
    docker run --rm \
        -e POND=/pond \
        -e RUST_LOG=info \
        -v "${SCRIPT_FILE}:/test/run.sh:ro" \
        "${IMAGE_NAME}" \
        -c "/bin/bash /test/run.sh" 2>&1 | tee "${TEMP_OUTPUT}"
    EXIT_CODE=${PIPESTATUS[0]}
    set -e
    
    echo ""
    if [[ ${EXIT_CODE} -eq 0 ]]; then
        echo "=== Test SUCCEEDED ==="
    else
        echo "=== Test FAILED (exit: ${EXIT_CODE}) ==="
    fi
    
    if [[ "${SAVE_RESULT}" == "true" ]]; then
        save_result "${EXIT_CODE}" "${TEMP_OUTPUT}"
    fi
    
    rm -f "${TEMP_OUTPUT}"
    exit ${EXIT_CODE}
fi

# No mode specified - default to interactive
echo "No script specified. Starting interactive mode..."
exec "$0" --interactive
