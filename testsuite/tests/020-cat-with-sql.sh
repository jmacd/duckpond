#!/bin/bash
# EXPERIMENT: Query CSV with SQL
# DESCRIPTION: Test using --sql flag with pond cat
# EXPECTED: SQL query filters and transforms data correctly
#
set -e

echo "=== Experiment: Cat with SQL ==="

pond init

# Create test data
cat > /tmp/data.csv << 'EOF'
timestamp,temperature,humidity
2024-01-01T00:00:00Z,22.5,45
2024-01-01T01:00:00Z,23.1,44
2024-01-01T02:00:00Z,22.8,46
2024-01-01T03:00:00Z,24.0,43
2024-01-01T04:00:00Z,21.5,48
EOF

pond mkdir /sensors
pond copy host:///tmp/data.csv /sensors/readings.csv
echo "✓ setup complete"

# Query: Filter rows
echo ""
echo "=== Query: WHERE temperature > 23 ==="
pond cat /sensors/readings.csv --format=table --query "SELECT * FROM source WHERE temperature > 23"

# Query: Aggregate
echo ""
echo "=== Query: AVG temperature ==="
pond cat /sensors/readings.csv --format=table --query "SELECT AVG(temperature) as avg_temp, AVG(humidity) as avg_humidity FROM source"

# Query: Transform
echo ""
echo "=== Query: Temperature in Fahrenheit ==="
pond cat /sensors/readings.csv --format=table --query "SELECT timestamp, temperature * 9/5 + 32 as temp_f FROM source"

echo ""
echo "=== Experiment Complete ==="
