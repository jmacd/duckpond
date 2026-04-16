#!/bin/bash
# EXPERIMENT: Git-ingest factory — status command
# DESCRIPTION: Verify the status subcommand reports sync state correctly
#              before and after pulling.
# EXPECTED: Status shows "not yet synced" before pull, shows commit SHA after.
set -e

echo "=== Experiment: Git-Ingest Status Command ==="

# --- Create git repo ----------------------------------------------------------
REPO_DIR=/tmp/test-blog-status
rm -rf "$REPO_DIR"
mkdir -p "$REPO_DIR"
cd "$REPO_DIR"
git init
git config user.email "test@example.com"
git config user.name "Test User"
echo "hello" > hello.txt
git add -A
git commit -m "Initial"
COMMIT_SHA=$(git rev-parse HEAD)
echo "Commit SHA: $COMMIT_SHA"
cd /

# --- Initialize pond ----------------------------------------------------------
pond init

cat > /tmp/git-ingest.yaml << EOF
url: file://${REPO_DIR}
ref: main
pond_path: data
EOF

pond mkdir /system
pond mkdir /system/etc
pond mknod git-ingest /system/etc/src --config-path /tmp/git-ingest.yaml

# --- Status before pull -------------------------------------------------------
echo ""
echo "--- Status before pull ---"
RUST_LOG=info pond run /system/etc/src status 2>&1 | tee /tmp/status1.log
if grep -q "not yet synced" /tmp/status1.log; then
    echo "Pre-pull status: CORRECT"
else
    echo "FAIL: Expected 'not yet synced'"
    exit 1
fi

# --- Pull ----------------------------------------------------------------------
echo ""
echo "--- Pull ---"
RUST_LOG=info pond run /system/etc/src pull

# --- Status after pull --------------------------------------------------------
echo ""
echo "--- Status after pull ---"
RUST_LOG=info pond run /system/etc/src status 2>&1 | tee /tmp/status2.log
if grep -q "$COMMIT_SHA" /tmp/status2.log; then
    echo "Post-pull status shows commit SHA: CORRECT"
else
    echo "FAIL: Expected commit SHA in status"
    cat /tmp/status2.log
    exit 1
fi

if grep -q "entries: 1" /tmp/status2.log; then
    echo "Post-pull entry count: CORRECT"
else
    echo "FAIL: Expected 'entries: 1'"
    exit 1
fi

echo ""
echo "=== Experiment Complete ==="
