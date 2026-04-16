#!/bin/bash
# EXPERIMENT: pond apply -- update config and validation failure
# DESCRIPTION: Test that pond apply updates changed configs, and that
#   validation failures abort the entire transaction (no partial writes).
# EXPECTED: Update succeeds for valid changes; invalid config prevents
#   the entire apply from running (pre-validation catches errors before
#   any transaction is opened).
set -e
source check.sh

echo "=== Experiment: pond apply -- update and validation ==="

pond init

# ==============================================================================
# Step 1: Initial apply -- create a node
# ==============================================================================

echo ""
echo "--- Step 1: Initial apply ---"

cat > /tmp/derived.yaml << 'YAML'
kind: sql-derived-table
path: /data/derived
version: v1
---
patterns:
  source: "table:///data/*.table"
query: "SELECT 1 AS value"
YAML

INIT_OUT=$(pond apply -f /tmp/derived.yaml 2>&1)
echo "$INIT_OUT"
check 'echo "$INIT_OUT" | grep -q "created"' "node created on first apply"

# ==============================================================================
# Step 2: Update with changed config -- should update
# ==============================================================================

echo ""
echo "--- Step 2: Update with changed config ---"

cat > /tmp/derived.yaml << 'YAML'
kind: sql-derived-table
path: /data/derived
version: v1
---
patterns:
  source: "table:///data/*.table"
query: "SELECT 42 AS answer"
YAML

UPDATE_OUT=$(pond apply -f /tmp/derived.yaml 2>&1)
echo "$UPDATE_OUT"
check 'echo "$UPDATE_OUT" | grep -q "updated"' "node updated with new config"
check 'echo "$UPDATE_OUT" | grep -q "1 updated"' "summary shows 1 updated"

# ==============================================================================
# Step 3: Apply with invalid config -- should fail pre-validation
# ==============================================================================

echo ""
echo "--- Step 3: Apply with invalid factory config ---"

cat > /tmp/bad.yaml << 'YAML'
kind: sql-derived-table
path: /data/bad-node
version: v1
---
not_a_valid_key: "this should fail validation"
YAML

BAD_OUT=$(pond apply -f /tmp/bad.yaml 2>&1 || true)
echo "$BAD_OUT"
check 'echo "$BAD_OUT" | grep -qi "invalid\|error\|unknown"' "invalid config rejected"

# The bad node should NOT exist (pre-validation prevents txn)
BAD_LIST=$(pond list "/data/bad*" 2>&1 || true)
check '! echo "$BAD_LIST" | grep -q "bad-node"' "bad node was not created"

# ==============================================================================
# Step 4: Batch apply where one file is invalid -- entire batch fails
# ==============================================================================

echo ""
echo "--- Step 4: Batch with one invalid file aborts all ---"

cat > /tmp/good.yaml << 'YAML'
kind: sql-derived-table
path: /data/good-node
version: v1
---
patterns:
  source: "table:///data/*.table"
query: "SELECT 99 AS good"
YAML

# bad.yaml from step 3 is still on disk
BATCH_OUT=$(pond apply -f /tmp/good.yaml /tmp/bad.yaml 2>&1 || true)
echo "$BATCH_OUT"
check 'echo "$BATCH_OUT" | grep -qi "invalid\|error\|unknown"' "batch rejected due to bad file"

# Neither node should exist -- good-node was not created because
# pre-validation failed before any transaction was opened.
GOOD_LIST=$(pond list "/data/good*" 2>&1 || true)
check '! echo "$GOOD_LIST" | grep -q "good-node"' "good-node not created (batch aborted)"

# ==============================================================================
# Step 5: Original node from step 2 is still intact
# ==============================================================================

echo ""
echo "--- Step 5: Original node survives failed applies ---"

ORIG_LIST=$(pond list "/data/*")
echo "$ORIG_LIST"
check 'echo "$ORIG_LIST" | grep -q "derived"' "original derived node still exists"

check_finish
