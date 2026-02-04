#!/bin/bash
# EXPERIMENT: Basic pond initialization
# DESCRIPTION: Test that pond init creates a working pond filesystem
# EXPECTED: Init succeeds, list shows empty root directory
#
set -e

echo "=== Experiment: Basic Init ==="

# Initialize pond
pond init
echo "✓ pond init succeeded"

# List root directory
echo ""
echo "=== Listing root directory ==="
pond list / || echo "(empty - no files yet)"

echo ""
echo "=== Experiment Complete ==="
