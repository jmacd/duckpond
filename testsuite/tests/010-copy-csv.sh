#!/bin/bash
# EXPERIMENT: Copy CSV file into pond
# DESCRIPTION: Test copying a CSV file and reading it back
# EXPECTED: File is copied, content is readable with cat
#
set -e

echo "=== Experiment: Copy CSV ==="

pond init

# Create test data
cat > /tmp/data.csv << 'EOF'
timestamp,temperature,humidity
2024-01-01T00:00:00Z,22.5,45
2024-01-01T01:00:00Z,23.1,44
2024-01-01T02:00:00Z,22.8,46
EOF
echo "✓ created test CSV"

# Create destination directory
pond mkdir /sensors

# Copy file (use host:// prefix for external files)
pond copy host:///tmp/data.csv /sensors/readings.csv
echo "✓ copied to /sensors/readings.csv"

# Verify with list
echo ""
echo "=== Listing /sensors ==="
pond list '/sensors/*'

# Read content
echo ""
echo "=== Content of /sensors/readings.csv ==="
pond cat /sensors/readings.csv

echo ""
echo "=== Experiment Complete ==="
