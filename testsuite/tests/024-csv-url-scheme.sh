#!/bin/bash
# EXPERIMENT: Test CSV querying with csv:// URL scheme
# DESCRIPTION: The provider layer supports csv:// URLs - test if that works for querying
# HYPOTHESIS: Using csv:// prefix instead of file:// should enable CSV querying
#
set -e

echo "=== Experiment: CSV URL Scheme ==="
echo ""

pond init

cat > /tmp/data.csv << 'EOF'
timestamp,temperature,humidity
2024-01-01T00:00:00Z,20.0,45
2024-01-01T01:00:00Z,25.0,44
2024-01-01T02:00:00Z,30.0,46
EOF

pond mkdir /data

# Copy as raw data (FilePhysicalVersion)
echo "=== Copy as --format=data (raw bytes) ==="
pond copy host:///tmp/data.csv /data/readings.csv --format=data
pond describe /data/readings.csv

echo ""
echo "=== Try querying with csv:// URL scheme ==="
# This should use CsvProvider instead of ParquetFormat
pond cat csv:///data/readings.csv --format=table --query "SELECT * FROM source WHERE temperature >= 25" 2>&1 || echo "csv:// scheme failed"

echo ""
echo "=== Try querying with file:// URL scheme ==="
# This currently fails because it assumes Parquet
pond cat file:///data/readings.csv --format=table --query "SELECT * FROM source WHERE temperature >= 25" 2>&1 || echo "file:// scheme failed (expected)"

echo ""
echo "=== Check if we can use table:// scheme ==="
pond cat table:///data/readings.csv --format=table --query "SELECT * FROM source WHERE temperature >= 25" 2>&1 || echo "table:// scheme failed"

echo ""
echo "=== Experiment Complete ==="
