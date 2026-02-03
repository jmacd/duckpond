#!/bin/bash
# EXPERIMENT: Exercise all pond list pattern variations
# DESCRIPTION: Test directory vs file listing, trailing slash, root patterns
# EXPECTED: Clear understanding of how different patterns behave
#
set -e

echo "=== Experiment: List Pattern Variations ==="
echo ""

pond init
echo "✓ pond init succeeded"
echo ""

# Create a directory structure with files at various levels
echo "=== Setting up test structure ==="
pond mkdir /data
pond mkdir /data/csv
pond mkdir /data/json
pond mkdir /sensors
pond mkdir /sensors/temp
pond mkdir /sensors/humidity

# Create temp files for copying
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

echo "root-file" > "$TMPDIR/root.txt"
echo "data-readme" > "$TMPDIR/README.md"
echo "csv-file1" > "$TMPDIR/file1.csv"
echo "csv-file2" > "$TMPDIR/file2.csv"
echo "json-config" > "$TMPDIR/config.json"
echo "temp-reading" > "$TMPDIR/temp-reading.dat"
echo "humidity-reading" > "$TMPDIR/humidity-reading.dat"

# Copy files at different levels
pond copy "host://$TMPDIR/root.txt" /root.txt
pond copy "host://$TMPDIR/README.md" /data/README.md
pond copy "host://$TMPDIR/file1.csv" /data/csv/file1.csv
pond copy "host://$TMPDIR/file2.csv" /data/csv/file2.csv
pond copy "host://$TMPDIR/config.json" /data/json/config.json
pond copy "host://$TMPDIR/temp-reading.dat" /sensors/temp/reading.dat
pond copy "host://$TMPDIR/humidity-reading.dat" /sensors/humidity/reading.dat

echo "✓ Created test structure:"
echo "  /root.txt"
echo "  /data/README.md"
echo "  /data/csv/file1.csv"
echo "  /data/csv/file2.csv"
echo "  /data/json/config.json"
echo "  /sensors/temp/reading.dat"
echo "  /sensors/humidity/reading.dat"
echo ""

# ============================================================
# TEST 1: Default behavior (no pattern)
# ============================================================
echo "=== TEST 1: pond list (default - all files recursively) ==="
pond list
echo ""

# ============================================================
# TEST 2: Root directory listing
# ============================================================
echo "=== TEST 2: pond list / (root directory entries) ==="
pond list /
echo ""

# ============================================================
# TEST 3: Trailing slash (list directory contents)
# ============================================================
echo "=== TEST 3: pond list /data/ (trailing slash - directory contents) ==="
pond list /data/
echo ""

echo "=== TEST 3b: pond list /data/csv/ (nested directory contents) ==="
pond list /data/csv/
echo ""

# ============================================================
# TEST 4: Without trailing slash (exact match)
# ============================================================
echo "=== TEST 4: pond list /data (no trailing slash - matches 'data' entry) ==="
pond list /data
echo ""

echo "=== TEST 4b: pond list data (relative path, no leading slash) ==="
pond list data
echo ""

# ============================================================
# TEST 5: Glob patterns
# ============================================================
echo "=== TEST 5: pond list '/*' (explicit root glob) ==="
pond list '/*'
echo ""

echo "=== TEST 5b: pond list '/data/*' (explicit directory contents) ==="
pond list '/data/*'
echo ""

echo "=== TEST 5c: pond list '**/*.csv' (recursive file type match) ==="
pond list '**/*.csv'
echo ""

echo "=== TEST 5d: pond list '/sensors/*/reading.dat' (wildcard in path) ==="
pond list '/sensors/*/reading.dat'
echo ""

# ============================================================
# TEST 6: Recursive patterns
# ============================================================
echo "=== TEST 6: pond list '/data/**/*' (all under /data recursively) ==="
pond list '/data/**/*'
echo ""

echo "=== TEST 6b: pond list '/sensors/**' (double-star at end) ==="
pond list '/sensors/**'
echo ""

# ============================================================
# TEST 7: Edge cases
# ============================================================
echo "=== TEST 7: pond list '/nonexistent/' (non-existent directory) ==="
pond list '/nonexistent/' || echo "(no matches - expected)"
echo ""

echo "=== TEST 7b: pond list '/data/csv/file1.csv' (exact file path) ==="
pond list '/data/csv/file1.csv'
echo ""

echo "=== TEST 7c: pond list '/data/csv/file1.csv/' (file with trailing slash) ==="
pond list '/data/csv/file1.csv/' || echo "(error or no matches - file is not a directory)"
echo ""

# ============================================================
# SUMMARY
# ============================================================
echo "=== Pattern Behavior Summary ==="
echo ""
echo "| Pattern              | Meaning                                    |"
echo "|----------------------|--------------------------------------------|"
echo "| (none)               | All files recursively (**/*) - DEFAULT     |"
echo "| /                    | Root directory entries only                |"
echo "| /dir/                | Contents of /dir (trailing slash = /dir/*) |"
echo "| /dir                 | Entry named 'dir' exactly                  |"
echo "| /*                   | Root entries (explicit glob)               |"
echo "| /dir/*               | Contents of /dir (explicit glob)           |"
echo "| **/*.ext             | All files with extension recursively       |"
echo "| /path/**/file        | Recursive match under path                 |"
echo ""
echo "=== Experiment Complete ==="
