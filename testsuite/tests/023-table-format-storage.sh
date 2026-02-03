#!/bin/bash
# EXPERIMENT: Table format requires Parquet files
# DESCRIPTION: Verify --format=table rejects CSV with helpful error message
# EXPECTED: CSV with --format=table fails with message about PAR1 magic bytes
#
set -e

echo "=== Experiment: Table Format Storage ==="
echo ""

pond init

cat > /tmp/data.csv << 'EOF'
timestamp,temperature
2024-01-01T00:00:00Z,20.0
2024-01-01T01:00:00Z,25.0
EOF

pond mkdir /data

echo "=== Test 1: Copy CSV as --format=data (raw) - should succeed ==="
pond copy host:///tmp/data.csv /data/raw.csv --format=data
pond list '/data/raw.csv'
echo "✓ CSV with --format=data succeeded"

echo ""
echo "=== Test 2: Copy CSV as --format=table - should fail with helpful error ==="
ERROR_OUTPUT=$(pond copy host:///tmp/data.csv /data/table.csv --format=table 2>&1 || true)
echo "$ERROR_OUTPUT"

# Verify the error message is helpful
if echo "$ERROR_OUTPUT" | grep -q "not a valid parquet file"; then
    echo "✓ Error mentions 'not a valid parquet file'"
else
    echo "✗ Missing expected error about parquet validation"
    exit 1
fi

if echo "$ERROR_OUTPUT" | grep -q "PAR1 magic bytes"; then
    echo "✓ Error mentions 'PAR1 magic bytes'"
else
    echo "✗ Missing expected error about PAR1 magic bytes"
    exit 1
fi

if echo "$ERROR_OUTPUT" | grep -q "Use --format=data"; then
    echo "✓ Error suggests using --format=data"
else
    echo "✗ Missing suggestion to use --format=data"
    exit 1
fi

echo ""
echo "=== Experiment Complete ==="
echo "VERIFIED: --format=table correctly rejects non-Parquet files with helpful error"
