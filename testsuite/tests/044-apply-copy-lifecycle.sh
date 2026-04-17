#!/bin/bash
# EXPERIMENT: pond apply -- copy kind and full lifecycle
# DESCRIPTION: Test that pond apply handles copy resources (host->pond),
#   verifies ordering (mkdir before copy before mknod), and exercises
#   the full setup-like lifecycle in a single transaction.
# EXPECTED: Directories created, host files copied, factory nodes installed.
set -e
source check.sh

echo "=== Experiment: pond apply -- copy and lifecycle ==="

pond init

# ==============================================================================
# Step 1: Create host content to copy
# ==============================================================================

echo ""
echo "--- Step 1: Create host content ---"

mkdir -p /tmp/apply-test/content
mkdir -p /tmp/apply-test/templates

cat > /tmp/apply-test/content/index.md << 'MD'
---
title: Home
---
# Welcome
MD

cat > /tmp/apply-test/content/about.md << 'MD'
---
title: About
---
# About us
MD

cat > /tmp/apply-test/templates/sidebar.md << 'MD'
- [Home](/)
- [About](/about)
MD

echo "Host content created"

# ==============================================================================
# Step 2: Apply multi-resource file with mkdir + copy + mknod
# ==============================================================================

echo ""
echo "--- Step 2: Apply full lifecycle config ---"

cat > /tmp/apply-test/pond.yaml << 'YAML'
version: v1
kind: mkdir
metadata:
  path: /data
---
version: v1
kind: copy
metadata:
  path: /content
spec:
  source: "host:///tmp/apply-test/content"
---
version: v1
kind: copy
metadata:
  path: /templates
spec:
  source: "host:///tmp/apply-test/templates"
---
version: v1
kind: mknod
metadata:
  path: /data/derived
spec:
  factory: sql-derived-table
  config:
    patterns:
      source: "table:///data/*.table"
    query: "SELECT 1 AS value"
YAML

APPLY_OUT=$(pond apply -f /tmp/apply-test/pond.yaml 2>&1)
echo "$APPLY_OUT"

check 'echo "$APPLY_OUT" | grep -q "created.*/data$"'          "mkdir /data created"
check 'echo "$APPLY_OUT" | grep -q "created.*/content"'        "copy /content created"
check 'echo "$APPLY_OUT" | grep -q "created.*/templates"'      "copy /templates created"
check 'echo "$APPLY_OUT" | grep -q "created.*/data/derived"'   "mknod /data/derived created"

# ==============================================================================
# Step 3: Verify copied content
# ==============================================================================

echo ""
echo "--- Step 3: Verify copied content ---"

LIST_CONTENT=$(pond list "/content/*")
echo "$LIST_CONTENT"
check 'echo "$LIST_CONTENT" | grep -q "index.md"'   "index.md copied"
check 'echo "$LIST_CONTENT" | grep -q "about.md"'    "about.md copied"

LIST_TEMPLATES=$(pond list "/templates/*")
echo "$LIST_TEMPLATES"
check 'echo "$LIST_TEMPLATES" | grep -q "sidebar.md"' "sidebar.md copied"

# Read back copied file content
CONTENT=$(pond cat /content/index.md)
echo "$CONTENT"
check 'echo "$CONTENT" | grep -q "Welcome"' "index.md content correct"

# ==============================================================================
# Step 4: Verify factory node
# ==============================================================================

echo ""
echo "--- Step 4: Verify factory node ---"

LIST_DATA=$(pond list "/data/*")
echo "$LIST_DATA"
check 'echo "$LIST_DATA" | grep -q "derived"' "derived node exists"

# ==============================================================================
# Step 5: Verify single transaction
# ==============================================================================

echo ""
echo "--- Step 5: Verify transaction log ---"

LOG_OUT=$(pond log --limit 5)
echo "$LOG_OUT"
check 'echo "$LOG_OUT" | grep -q "apply"' "apply in transaction log"
# Should be exactly 2 transactions: init + apply
check 'echo "$LOG_OUT" | grep -c "seq=" | grep -q "2"' "exactly 2 transactions"

# ==============================================================================
# Step 6: Copy with overwrite=false should fail if exists
# ==============================================================================

echo ""
echo "--- Step 6: Copy without overwrite fails on existing ---"

cat > /tmp/apply-test/dup.yaml << 'YAML'
version: v1
kind: copy
metadata:
  path: /content
spec:
  source: "host:///tmp/apply-test/content"
YAML

DUP_OUT=$(pond apply -f /tmp/apply-test/dup.yaml 2>&1 || true)
echo "$DUP_OUT"
check 'echo "$DUP_OUT" | grep -q "already exists"' "copy rejects existing destination"

# ==============================================================================
# Step 7: Copy with overwrite=true should succeed
# ==============================================================================

echo ""
echo "--- Step 7: Copy with overwrite succeeds ---"

cat > /tmp/apply-test/overwrite.yaml << 'YAML'
version: v1
kind: copy
metadata:
  path: /content
spec:
  source: "host:///tmp/apply-test/content"
  overwrite: true
YAML

OW_OUT=$(pond apply -f /tmp/apply-test/overwrite.yaml 2>&1)
echo "$OW_OUT"
check 'echo "$OW_OUT" | grep -q "created.*/content"' "overwrite copy succeeded"

# Cleanup
rm -rf /tmp/apply-test

check_finish
