#!/bin/bash
# EXPERIMENT: Git-ingest factory — symlinks and nested directories
# DESCRIPTION: Test that symlinks and deeply nested directory trees
#              from git are correctly mirrored in the pond.
# EXPECTED: Symlinks appear as symlinks in the pond, nested dirs created.
set -e

echo "=== Experiment: Git-Ingest Symlinks and Nested Dirs ==="

# --- Create git repo with symlinks and nesting --------------------------------
REPO_DIR=/tmp/test-blog-symlinks
rm -rf "$REPO_DIR"
mkdir -p "$REPO_DIR"
cd "$REPO_DIR"
git init
git config user.email "test@example.com"
git config user.name "Test User"

# Create nested directory structure
mkdir -p a/b/c
echo "deep file" > a/b/c/deep.txt
echo "level-b file" > a/b/mid.txt
echo "root file" > root.txt

# Create a symlink
ln -s root.txt link-to-root.txt
ln -s a/b/c/deep.txt link-to-deep.txt

git add -A
git commit -m "Nested dirs and symlinks"
cd /

# --- Initialize pond and pull -------------------------------------------------
pond init

cat > /tmp/git-ingest.yaml << EOF
url: file://${REPO_DIR}
ref: main
pond_path: content
EOF

pond mkdir /system
pond mkdir /system/etc
pond mknod git-ingest /system/etc/gitrepo --config-path /tmp/git-ingest.yaml

RUST_LOG=info pond run /system/etc/gitrepo pull

# --- Verify structure ---------------------------------------------------------
echo ""
echo "=== Full listing ==="
pond list -r /content/

echo ""
echo "=== Verify nested file ==="
CONTENT=$(pond cat /content/a/b/c/deep.txt)
if [ "$CONTENT" = "deep file" ]; then
    echo "a/b/c/deep.txt: CORRECT"
else
    echo "FAIL: a/b/c/deep.txt (got: $CONTENT)"
    exit 1
fi

CONTENT=$(pond cat /content/a/b/mid.txt)
if [ "$CONTENT" = "level-b file" ]; then
    echo "a/b/mid.txt: CORRECT"
else
    echo "FAIL: a/b/mid.txt (got: $CONTENT)"
    exit 1
fi

echo ""
echo "=== Verify symlinks ==="
# Symlinks should exist and be readable
# (pond cat follows symlinks)
CONTENT=$(pond cat /content/link-to-root.txt)
if [ "$CONTENT" = "root file" ]; then
    echo "link-to-root.txt -> root.txt: CORRECT"
else
    echo "FAIL: link-to-root.txt (got: $CONTENT)"
    exit 1
fi

echo ""
echo "=== Experiment Complete ==="
