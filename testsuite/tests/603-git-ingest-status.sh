#!/bin/bash
# EXPERIMENT: Git-ingest factory — status command
# DESCRIPTION: Verify the status subcommand reports state correctly
#              before and after pulling.
# EXPECTED: Status shows "not yet fetched" before pull, shows commit info after.
set -e

echo "=== Experiment: Git-Ingest Status Command ==="

# --- Create git repo ----------------------------------------------------------
REPO_DIR=/tmp/test-blog-status
rm -rf "$REPO_DIR"
mkdir -p "$REPO_DIR"
cd "$REPO_DIR"
git init -b main
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
EOF

pond mkdir /system
pond mkdir /system/etc
pond mknod git-ingest /system/etc/src --config-path /tmp/git-ingest.yaml

# --- Status before pull -------------------------------------------------------
echo ""
echo "--- Status before pull ---"
RUST_LOG=info pond run /system/etc/src status 2>&1 | tee /tmp/status1.log
if grep -q "not yet fetched" /tmp/status1.log; then
    echo "Pre-pull status: CORRECT"
else
    echo "FAIL: Expected 'not yet fetched'"
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
if grep -q "at commit" /tmp/status2.log; then
    echo "Post-pull status shows commit: CORRECT"
else
    echo "FAIL: Expected commit info in status"
    cat /tmp/status2.log
    exit 1
fi

# --- Verify file is readable via dynamic dir ----------------------------------
echo ""
echo "--- Verify content ---"
CONTENT=$(pond cat /system/etc/src/hello.txt)
if [ "$CONTENT" = "hello" ]; then
    echo "hello.txt: CORRECT"
else
    echo "FAIL: hello.txt (got: $CONTENT)"
    exit 1
fi

echo ""
echo "=== Experiment Complete ==="
