#!/bin/bash
# EXPERIMENT: Git-ingest factory — incremental update via dynamic dir
# DESCRIPTION: Pull from git, modify the repo, pull again. Since the
#              dynamic dir reads from the bare repo, changes are visible
#              immediately after fetch.
# EXPECTED: Second pull reflects new/changed/deleted files in the tree.
set -e

echo "=== Experiment: Git-Ingest Incremental Update ==="

# --- Create initial git repo -------------------------------------------------
REPO_DIR=/tmp/test-blog-incremental
rm -rf "$REPO_DIR"
mkdir -p "$REPO_DIR"
cd "$REPO_DIR"
git init -b main
git config user.email "test@example.com"
git config user.name "Test User"

echo "# Blog" > README.md
echo "Post one" > post1.md
echo "Post two" > post2.md
echo "To be deleted" > delete-me.md

git add -A
git commit -m "Initial commit"
cd /

# --- Initialize pond and do first pull ---------------------------------------
pond init

cat > /tmp/git-ingest.yaml << EOF
url: file://${REPO_DIR}
ref: main
EOF

pond mkdir /system
pond mkdir /system/etc
pond mknod git-ingest /system/etc/gitblog --config-path /tmp/git-ingest.yaml

echo "--- First pull ---"
RUST_LOG=info pond run /system/etc/gitblog pull

# Verify initial state
CONTENT=$(pond cat /system/etc/gitblog/post1.md)
[ "$CONTENT" = "Post one" ] && echo "post1.md: CORRECT" || { echo "FAIL: post1.md"; exit 1; }

CONTENT=$(pond cat /system/etc/gitblog/delete-me.md)
[ "$CONTENT" = "To be deleted" ] && echo "delete-me.md: CORRECT" || { echo "FAIL: delete-me.md"; exit 1; }

# --- Modify the git repo -----------------------------------------------------
cd "$REPO_DIR"
echo "Post one UPDATED" > post1.md
echo "Brand new post" > post3.md
rm delete-me.md
git add -A
git commit -m "Update, add, and delete"
cd /

# --- Second pull (fetch updates) ---------------------------------------------
echo ""
echo "--- Second pull ---"
RUST_LOG=info pond run /system/etc/gitblog pull

# --- Verify changes ----------------------------------------------------------
echo ""
echo "=== Verification ==="

# Updated file
CONTENT=$(pond cat /system/etc/gitblog/post1.md)
if [ "$CONTENT" = "Post one UPDATED" ]; then
    echo "post1.md updated: CORRECT"
else
    echo "FAIL: post1.md not updated (got: $CONTENT)"
    exit 1
fi

# Unchanged file
CONTENT=$(pond cat /system/etc/gitblog/post2.md)
if [ "$CONTENT" = "Post two" ]; then
    echo "post2.md unchanged: CORRECT"
else
    echo "FAIL: post2.md changed unexpectedly (got: $CONTENT)"
    exit 1
fi

# New file
CONTENT=$(pond cat /system/etc/gitblog/post3.md)
if [ "$CONTENT" = "Brand new post" ]; then
    echo "post3.md added: CORRECT"
else
    echo "FAIL: post3.md not found or wrong (got: $CONTENT)"
    exit 1
fi

# Deleted file should be gone
if pond cat /system/etc/gitblog/delete-me.md 2>/dev/null; then
    echo "FAIL: delete-me.md should have been removed"
    exit 1
else
    echo "delete-me.md removed: CORRECT"
fi

echo ""
echo "=== Listing /system/etc/gitblog/ ==="
pond list /system/etc/gitblog/

echo ""
echo "=== Experiment Complete ==="
