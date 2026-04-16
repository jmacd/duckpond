#!/bin/bash
# EXPERIMENT: pond apply -- initialize a pond with multiple configs
# DESCRIPTION: Test that pond apply creates nodes from YAML files with
#   kind/path/version prelude, auto-creates parent directories, and
#   is idempotent on re-apply.
# EXPECTED: All nodes created on first apply, unchanged on second apply.
set -e
source check.sh

echo "=== Experiment: pond apply -- init ==="

pond init

# ==============================================================================
# Step 1: Create config files with prelude format
# ==============================================================================

echo ""
echo "--- Step 1: Create apply config files ---"

cat > /tmp/derived-a.yaml << 'YAML'
kind: sql-derived-table
path: /data/derived-a
version: v1
---
patterns:
  source: "table:///data/*.table"
query: "SELECT 1 AS value"
YAML

cat > /tmp/derived-b.yaml << 'YAML'
kind: sql-derived-table
path: /data/derived-b
version: v1
---
patterns:
  source: "table:///data/*.table"
query: "SELECT 2 AS value"
YAML

echo "Config files created"

# ==============================================================================
# Step 2: Apply -- should create both nodes and parent dirs
# ==============================================================================

echo ""
echo "--- Step 2: First apply (create) ---"

APPLY_OUT=$(pond apply -f /tmp/derived-a.yaml /tmp/derived-b.yaml 2>&1)
echo "$APPLY_OUT"

check 'echo "$APPLY_OUT" | grep -q "created.*derived-a"' "derived-a created"
check 'echo "$APPLY_OUT" | grep -q "created.*derived-b"' "derived-b created"
check 'echo "$APPLY_OUT" | grep -q "2 created"'          "summary shows 2 created"

# Verify nodes exist
LIST_OUT=$(pond list "/data/*")
echo "$LIST_OUT"
check 'echo "$LIST_OUT" | grep -q "derived-a"' "derived-a in listing"
check 'echo "$LIST_OUT" | grep -q "derived-b"' "derived-b in listing"

# ==============================================================================
# Step 3: Re-apply -- should be idempotent (unchanged)
# ==============================================================================

echo ""
echo "--- Step 3: Second apply (idempotent) ---"

REAPPLY_OUT=$(pond apply -f /tmp/derived-a.yaml /tmp/derived-b.yaml 2>&1)
echo "$REAPPLY_OUT"

check 'echo "$REAPPLY_OUT" | grep -q "unchanged"'          "reports unchanged"
check 'echo "$REAPPLY_OUT" | grep -q "no transaction"'     "no transaction committed"

# ==============================================================================
# Step 4: Check transaction log -- only one write transaction for the apply
# ==============================================================================

echo ""
echo "--- Step 4: Verify transaction log ---"

LOG_OUT=$(pond log --limit 5)
echo "$LOG_OUT"

# First apply should show as one transaction with "apply" metadata
check 'echo "$LOG_OUT" | grep -q "apply"' "transaction log contains apply"

check_finish
