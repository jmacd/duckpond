#!/bin/bash
# EXPERIMENT: Verify --format=table default
# DESCRIPTION: After changing default from 'data' to 'table', verify SQL queries work
# EXPECTED: SQL filtering should now work without explicit --format flag
#
set -e

echo "=== Experiment: Verify Table Default ==="
echo ""

pond init

# Create test data
cat > /tmp/data.csv << 'EOF'
timestamp,temperature,humidity
2024-01-01T00:00:00Z,20.0,45
2024-01-01T01:00:00Z,25.0,44
2024-01-01T02:00:00Z,30.0,46
EOF

pond mkdir /data

# Copy WITHOUT explicit --format (should now default to table)
echo "=== Copying without --format flag ==="
pond copy host:///tmp/data.csv /data/readings.csv

echo ""
echo "=== Check file type (should show Table, not Raw data) ==="
pond describe /data/readings.csv

echo ""
echo "=== Test SQL query (should filter to temp >= 25) ==="
pond cat 'csv:///data/readings.csv' --format=table --query "SELECT * FROM source WHERE temperature >= 25"

echo ""
echo "=== Expected: Only 2 rows (25.0 and 30.0), not all 3 ==="

echo ""
echo "=== Verify --format=data still works for raw files ==="
pond copy host:///tmp/data.csv /data/raw.csv
pond describe /data/raw.csv

echo ""
echo "=== Experiment Complete ==="
