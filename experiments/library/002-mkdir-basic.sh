#!/bin/bash
# EXPERIMENT: Create directories with mkdir
# DESCRIPTION: Test creating single and nested directories
# EXPECTED: Directories are created and visible in listing
#
set -e

echo "=== Experiment: Mkdir Basic ==="

pond init
echo "✓ pond init succeeded"

# Create single directory
pond mkdir /data
echo "✓ created /data"

# Create nested directories
pond mkdir /sensors
pond mkdir /sensors/temperature
pond mkdir /sensors/humidity
echo "✓ created /sensors hierarchy"

# Verify structure
echo ""
echo "=== Listing root ==="
pond list '/*'

echo ""
echo "=== Listing /sensors ==="
pond list '/sensors/*'

echo ""
echo "=== Experiment Complete ==="
