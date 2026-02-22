#!/bin/bash
# EXPERIMENT: Phase 3 proof-of-life -- pond list on host filesystem via hostmount
# EXPECTED: `pond list host+file:///path` lists host files without needing a pond
set -e
source check.sh

echo "=== Experiment: Hostmount list proof-of-life ==="

# Create a test directory structure on the host filesystem
mkdir -p /tmp/hostmount-test/data/subdir
echo "hello world" > /tmp/hostmount-test/data/file1.txt
echo "second file" > /tmp/hostmount-test/data/file2.csv
echo "nested file" > /tmp/hostmount-test/data/subdir/nested.txt

echo ""
echo "--- List with host+file:// URL ---"
OUTPUT1=$(pond list "host+file:///tmp/hostmount-test/data/*" 2>&1)
echo "$OUTPUT1"

echo ""
echo "--- List with recursive glob ---"
OUTPUT2=$(pond list "host+file:///tmp/hostmount-test/data/**/*" 2>&1)
echo "$OUTPUT2"

echo ""
echo "--- List with -d flag ---"
OUTPUT3=$(pond list -d /tmp/hostmount-test/data "host+file:///*" 2>&1)
echo "$OUTPUT3"

echo ""
echo "--- List without POND env var ---"
unset POND
OUTPUT4=$(pond list "host+file:///tmp/hostmount-test/data/*" 2>&1)
echo "$OUTPUT4"

# Cleanup
rm -rf /tmp/hostmount-test

echo ""
echo "--- Verification ---"

check 'echo "$OUTPUT1" | grep -q "file1.txt"'  "host+file:// lists file1.txt"
check 'echo "$OUTPUT1" | grep -q "file2.csv"'  "host+file:// lists file2.csv"
check 'echo "$OUTPUT2" | grep -q "nested.txt"' "recursive glob finds nested.txt"
check 'echo "$OUTPUT3" | grep -q "file1.txt"'  "-d flag works with host listing"
check 'echo "$OUTPUT4" | grep -q "file1.txt"'  "host listing works without POND env var"

check_finish
