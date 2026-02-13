#!/bin/bash
# EXPERIMENT: Investigate --query flag behavior
# DESCRIPTION: Determine why SQL queries appear to not filter data
# HYPOTHESIS: CSV files may be stored as 'file:data' not 'file:table', bypassing SQL
#
set -e

echo "=== Experiment: Query Flag Investigation ==="
echo ""

pond init

# Create test data with clear filter boundary
cat > /tmp/data.csv << 'EOF'
timestamp,temperature,humidity
2024-01-01T00:00:00Z,20.0,45
2024-01-01T01:00:00Z,25.0,44
2024-01-01T02:00:00Z,30.0,46
EOF

pond mkdir /data

#############################
# TEST 1: Copy as .csv extension
#############################
echo "=== Test 1: Copy as .csv ==="
pond copy host:///tmp/data.csv /data/readings.csv

echo ""
echo "--- List with details ---"
pond list '/data/*'

echo ""
echo "--- Raw cat ---"
pond cat /data/readings.csv

echo ""
echo "--- With --query (should filter to temp >= 25) ---"
pond cat 'csv:///data/readings.csv' --format=table --query "SELECT * FROM source WHERE temperature >= 25"

#############################
# TEST 2: Check file metadata via describe
#############################
echo ""
echo "=== Test 2: Describe file ==="
pond describe /data/readings.csv || echo "(describe may not work on this file type)"

#############################
# TEST 3: Try different table name
#############################
echo ""
echo "=== Test 3: Try 'series' as table name ==="
pond cat 'csv:///data/readings.csv' --format=table --query "SELECT * FROM series WHERE temperature >= 25" 2>&1 || echo "(may fail)"

#############################
# TEST 4: Check control table for file type
#############################
echo ""
echo "=== Test 4: Control table - what type is the file? ==="
pond control --sql "SELECT * FROM control_table WHERE record_type LIKE '%file%' OR record_type LIKE '%copy%' ORDER BY txn_seq DESC LIMIT 5" 2>&1 || echo "(control sql may not work)"

#############################
# TEST 5: Try copy with explicit type hint?
#############################
echo ""
echo "=== Test 5: Check copy help for type options ==="
pond copy --help

echo ""
echo "=== Experiment Complete ==="
echo ""
echo "FINDINGS:"
echo "- Check if file type in listing shows 'table' or 'data'"
echo "- Check if --query silently ignores non-table files"
echo "- Check if there's a way to force table type on copy"
