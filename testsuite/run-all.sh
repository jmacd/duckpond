#!/bin/bash
# Run all integration tests and report results
#
# Usage:
#   ./run-all.sh           # Run all tests
#   ./run-all.sh --stop    # Stop on first failure
#
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STOP_ON_FAILURE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --stop|-s)
            STOP_ON_FAILURE=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [--stop]"
            echo ""
            echo "Options:"
            echo "  --stop, -s    Stop on first failure"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

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
    
    if "$SCRIPT_DIR/run-test.sh" "$test" > /tmp/test-output-$$.txt 2>&1; then
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
