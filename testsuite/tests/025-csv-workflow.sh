#!/bin/bash
# EXPERIMENT: Verify CSV workflow and --format=table validation
# DESCRIPTION: 
#   1. --format=table on CSV should fail with helpful error
#   2. CSV should be copied as --format=data (default)
#   3. CSV should be queried using csv:// URL scheme
#
set -e

echo "=== Experiment: CSV Workflow ==="
echo ""

pond init

cat > /tmp/data.csv << 'EOF'
timestamp,temperature,humidity
2024-01-01T00:00:00Z,20.0,45
2024-01-01T01:00:00Z,25.0,44
2024-01-01T02:00:00Z,30.0,46
EOF

pond mkdir /data

#############################
# TEST 1: --format=table should reject CSV
#############################
echo "=== Test 1: --format=table should reject CSV ==="
if pond copy host:///tmp/data.csv /data/bad.csv --format=table 2>&1; then
    echo "ERROR: Should have rejected CSV with --format=table"
    exit 1
else
    echo "✓ Correctly rejected CSV with --format=table"
fi

#############################
# TEST 2: Default --format=data works for CSV
#############################
echo ""
echo "=== Test 2: Copy CSV with default --format=data ==="
pond copy host:///tmp/data.csv /data/readings.csv
pond describe /data/readings.csv
echo "✓ CSV copied as raw data"

#############################
# TEST 3: Query CSV using csv:// URL scheme
#############################
echo ""
echo "=== Test 3: Query CSV with csv:// scheme ==="
echo "--- All rows (with SELECT *) ---"
pond cat csv:///data/readings.csv --format=table --query "SELECT * FROM source"

echo ""
echo "--- With SQL filter (temp >= 25) ---"
pond cat csv:///data/readings.csv --format=table --query "SELECT * FROM source WHERE temperature >= 25"

echo ""
echo "=== Experiment Complete ==="
