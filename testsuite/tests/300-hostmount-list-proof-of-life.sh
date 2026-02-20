#!/bin/bash
# EXPERIMENT: Phase 3 proof-of-life -- pond list on host filesystem via hostmount
# EXPECTED: `pond list host+file:///path` lists host files without needing a pond
set -e

echo "=== TEST: Hostmount list proof-of-life ==="

# Create a test directory structure on the host filesystem
mkdir -p /tmp/hostmount-test/data/subdir
echo "hello world" > /tmp/hostmount-test/data/file1.txt
echo "second file" > /tmp/hostmount-test/data/file2.csv
echo "nested file" > /tmp/hostmount-test/data/subdir/nested.txt

echo "--- Test 1: List host directory with host+file:// URL ---"
OUTPUT=$(pond list "host+file:///tmp/hostmount-test/data/*" 2>&1)
echo "$OUTPUT"

# Verify we see the files (not an error about POND)
echo "$OUTPUT" | grep -q "file1.txt" || { echo "FAIL: file1.txt not found in output"; exit 1; }
echo "$OUTPUT" | grep -q "file2.csv" || { echo "FAIL: file2.csv not found in output"; exit 1; }
echo "PASS: host+file:// URL lists host files"

echo ""
echo "--- Test 2: List host directory with glob pattern ---"
OUTPUT=$(pond list "host+file:///tmp/hostmount-test/data/**/*" 2>&1)
echo "$OUTPUT"

echo "$OUTPUT" | grep -q "nested.txt" || { echo "FAIL: nested.txt not found in recursive glob"; exit 1; }
echo "PASS: recursive glob works on host filesystem"

echo ""
echo "--- Test 3: List host directory with -d flag ---"
OUTPUT=$(pond list -d /tmp/hostmount-test/data "host+file:///*" 2>&1)
echo "$OUTPUT"

echo "$OUTPUT" | grep -q "file1.txt" || { echo "FAIL: -d flag listing failed"; exit 1; }
echo "PASS: -d flag works with host listing"

echo ""
echo "--- Test 4: No POND required for host-only operation ---"
# Unset POND to prove we don't need it
unset POND
OUTPUT=$(pond list "host+file:///tmp/hostmount-test/data/*" 2>&1)
echo "$OUTPUT"

echo "$OUTPUT" | grep -q "file1.txt" || { echo "FAIL: host listing should work without POND"; exit 1; }
echo "PASS: host listing works without POND env var"

# Cleanup
rm -rf /tmp/hostmount-test

echo ""
echo "=== ALL HOSTMOUNT LIST TESTS PASSED ==="
