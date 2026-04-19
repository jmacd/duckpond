#!/bin/bash
# EXPERIMENT: Git-ingest factory — prefix filtering
# DESCRIPTION: Verify that the prefix config field filters the git tree
#              to only show files under the prefix, with the prefix stripped.
# EXPECTED: Only files under the prefix appear; other files are invisible.
set -e

echo "=== Experiment: Git-Ingest Prefix Filtering ==="

# --- Create git repo with multiple top-level dirs ----------------------------
REPO_DIR=/tmp/test-prefix
rm -rf "$REPO_DIR"
mkdir -p "$REPO_DIR"
cd "$REPO_DIR"
git init -b main
git config user.email "test@example.com"
git config user.name "Test User"

# Files outside the prefix
echo "Root readme" > README.md
mkdir -p src
echo "fn main() {}" > src/main.rs

# Files inside the prefix
mkdir -p site/content
echo "# Blog Post" > site/content/post.md
mkdir -p site/templates
echo "<html></html>" > site/templates/base.html
mkdir -p site/img
echo "SVG" > site/img/logo.svg

# Nested prefix test
mkdir -p config/noyo/site
echo "noyo index" > config/noyo/site/index.md
echo "noyo data" > config/noyo/site/data.yaml

git add -A
git commit -m "Multi-dir repo"
cd /

# --- Test 1: prefix = "site" ------------------------------------------------
echo ""
echo "--- Test 1: prefix=site ---"
pond init

cat > /tmp/git-prefix1.yaml << EOF
url: file://${REPO_DIR}
ref: main
prefix: site
EOF

pond mkdir /system
pond mkdir /system/etc
pond mknod git-ingest /system/etc/site --config-path /tmp/git-prefix1.yaml
RUST_LOG=info pond run /system/etc/site pull

echo "Listing /system/etc/site/:"
pond list /system/etc/site/

# Should see content/, templates/, img/ — NOT README.md or src/
CONTENT=$(pond cat /system/etc/site/content/post.md)
if [ "$CONTENT" = "# Blog Post" ]; then
    echo "content/post.md: CORRECT"
else
    echo "FAIL: content/post.md (got: $CONTENT)"
    exit 1
fi

HTML=$(pond cat /system/etc/site/templates/base.html)
if [ "$HTML" = "<html></html>" ]; then
    echo "templates/base.html: CORRECT"
else
    echo "FAIL: templates/base.html (got: $HTML)"
    exit 1
fi

# Files outside prefix should NOT be visible
if pond cat /system/etc/site/README.md 2>/dev/null; then
    echo "FAIL: README.md should not be visible under prefix=site"
    exit 1
else
    echo "README.md excluded: CORRECT"
fi

if pond cat /system/etc/site/src/main.rs 2>/dev/null; then
    echo "FAIL: src/main.rs should not be visible under prefix=site"
    exit 1
else
    echo "src/main.rs excluded: CORRECT"
fi

# --- Test 2: nested prefix = "config/noyo/site" -----------------------------
echo ""
echo "--- Test 2: prefix=config/noyo/site ---"

cat > /tmp/git-prefix2.yaml << EOF
url: file://${REPO_DIR}
ref: main
prefix: config/noyo/site
EOF

pond mknod git-ingest /system/etc/noyo --config-path /tmp/git-prefix2.yaml
RUST_LOG=info pond run /system/etc/noyo pull

echo "Listing /system/etc/noyo/:"
pond list /system/etc/noyo/

CONTENT=$(pond cat /system/etc/noyo/index.md)
if [ "$CONTENT" = "noyo index" ]; then
    echo "index.md: CORRECT"
else
    echo "FAIL: index.md (got: $CONTENT)"
    exit 1
fi

CONTENT=$(pond cat /system/etc/noyo/data.yaml)
if [ "$CONTENT" = "noyo data" ]; then
    echo "data.yaml: CORRECT"
else
    echo "FAIL: data.yaml (got: $CONTENT)"
    exit 1
fi

echo ""
echo "=== Experiment Complete ==="
