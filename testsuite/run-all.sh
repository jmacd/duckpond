#!/bin/bash
# Run all integration tests and report results
#
# Usage:
#   ./run-all.sh           # Run all tests (rebuilds image first)
#   ./run-all.sh --stop    # Stop on first failure
#   ./run-all.sh --no-rebuild  # Skip rebuild (use existing image)
#   ./run-all.sh --skip-browser # Skip slow browser/Puppeteer tests
#
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STOP_ON_FAILURE=false
NO_REBUILD=false
SKIP_BROWSER=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --stop|-s)
            STOP_ON_FAILURE=true
            shift
            ;;
        --no-rebuild|-n)
            NO_REBUILD=true
            shift
            ;;
        --skip-browser|-B)
            SKIP_BROWSER=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [--stop] [--no-rebuild] [--skip-browser]"
            echo ""
            echo "Options:"
            echo "  --stop, -s           Stop on first failure"
            echo "  --no-rebuild, -n     Skip rebuild (use existing image)"
            echo "  --skip-browser, -B   Skip slow browser/Puppeteer tests (REQUIRES: host)"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Build once upfront (not per-test)
if [[ "${NO_REBUILD}" == "false" ]]; then
    echo "=== Building test image ==="
    "${SCRIPT_DIR}/build-image.sh" --quiet
    echo ""
fi

# Find all tests
TESTS=$(ls "$SCRIPT_DIR/tests/"*.sh 2>/dev/null | sort)

if [ -z "$TESTS" ]; then
    echo "No tests found in $SCRIPT_DIR/tests/"
    exit 0
fi

# Track results
PASSED=0
FAILED=0
FAILED_NAMES=""

echo "========================================"
echo "Running all integration tests"
echo "========================================"
echo ""

for test in $TESTS; do
    name=$(basename "$test")
    echo "--- Running: $name ---"

    # Tests marked '# REQUIRES: host' run directly on the host (not in Docker).
    # They need tools like Node.js/Puppeteer that aren't in the test container.
    if head -25 "$test" | grep -q '# REQUIRES: host'; then
        if [[ "${SKIP_BROWSER}" == "true" ]]; then
            echo "  (skipped — browser test, use without --skip-browser to include)"
            echo ""
            continue
        fi
        runner=(bash "$test" "--no-rebuild")
        echo "  (host-only test — running locally)"
    # Tests marked '# REQUIRES: compose' need docker compose (e.g., MinIO for S3).
    elif head -25 "$test" | grep -q '# REQUIRES: compose'; then
        runner=("$SCRIPT_DIR/run-test.sh" "--no-rebuild" "--compose" "$test")
        echo "  (compose test — starting MinIO)"
    else
        runner=("$SCRIPT_DIR/run-test.sh" "--no-rebuild" "$test")
    fi

    if "${runner[@]}" > /tmp/test-output-$$.txt 2>&1; then
        echo "✓ PASSED: $name"
        ((PASSED++))
    else
        echo "✗ FAILED: $name"
        ((FAILED++))
        FAILED_NAMES="$FAILED_NAMES  - $name\n"
        
        # Show last 20 lines on failure
        echo "--- Last 20 lines of output ---"
        tail -20 /tmp/test-output-$$.txt
        echo "-------------------------------"
        
        if [ "$STOP_ON_FAILURE" = true ]; then
            echo ""
            echo "Stopping on first failure (--stop)"
            rm -f /tmp/test-output-$$.txt
            exit 1
        fi
    fi
    echo ""
done

rm -f /tmp/test-output-$$.txt

# Summary
echo "========================================"
echo "SUMMARY"
echo "========================================"
echo "Passed: $PASSED"
echo "Failed: $FAILED"

if [ $FAILED -gt 0 ]; then
    echo ""
    echo "Failed tests:"
    echo -e "$FAILED_NAMES"
    exit 1
else
    echo ""
    echo "All tests passed!"
    exit 0
fi
