#!/bin/bash
# EXPERIMENT: Test pond copy overwrite semantics
# DESCRIPTION: Unix cp overwrites by default. Does pond copy behave the same
#   way when copying to a path that already exists?
# EXPECTED: Second copy should succeed and the file should contain the new content.
#
set -e

check() {
  if eval "$1"; then
    echo "  ✓ $2"
  else
    echo "  ✗ $2"
    FAIL=1
  fi
}

FAIL=0

echo "=== Experiment: Copy Overwrite Semantics ==="
echo ""

pond init
pond mkdir -p /etc/site

# --- Round 1: initial copy ---
echo "version1" > /tmp/test-overwrite.md
pond copy host:///tmp/test-overwrite.md /etc/site/page.md
V1=$(pond cat /etc/site/page.md)
check 'echo "$V1" | grep -q "version1"' "first copy stores version1"

# --- Round 2: overwrite with new content ---
echo "version2" > /tmp/test-overwrite.md
pond copy host:///tmp/test-overwrite.md /etc/site/page.md
V2=$(pond cat /etc/site/page.md)
check 'echo "$V2" | grep -q "version2"' "second copy overwrites with version2"
check '! echo "$V2" | grep -q "version1"' "old content is gone"

# --- Round 3: overwrite with a third version to confirm repeatability ---
echo "version3" > /tmp/test-overwrite.md
pond copy host:///tmp/test-overwrite.md /etc/site/page.md
V3=$(pond cat /etc/site/page.md)
check 'echo "$V3" | grep -q "version3"' "third copy overwrites with version3"

echo ""
echo "=== Results: ==="
exit $FAIL
