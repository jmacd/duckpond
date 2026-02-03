#!/bin/bash
# EXPERIMENT: Investigate table format storage
# DESCRIPTION: Check if --format=table converts CSV to Parquet or just changes metadata
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

echo "=== Test 1: Copy CSV as --format=data (raw) ==="
pond copy host:///tmp/data.csv /data/raw.csv --format=data
pond describe /data/raw.csv
echo "Size after data format:"
pond list '/data/raw.csv'

echo ""
echo "=== Test 2: Copy CSV as --format=table ==="
pond copy host:///tmp/data.csv /data/table.csv --format=table
pond describe /data/table.csv
echo "Size after table format:"
pond list '/data/table.csv'

echo ""
echo "=== Test 3: Check if sizes differ (Parquet would be different size) ==="
pond list '/data/*'

echo ""
echo "=== Test 4: Try to cat the table file raw ==="
pond cat /data/table.csv 2>&1 || echo "Failed to cat table format"

echo ""
echo "=== Experiment Complete ==="
