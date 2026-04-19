#!/bin/bash
# EXPERIMENT: Git-ingest factory — basic pull and dynamic dir listing
# DESCRIPTION: Create a local git repo, configure git-ingest factory as
#              a dynamic directory, pull to cache, verify files are readable.
# EXPECTED: After pull, the dynamic dir lists git tree and pond cat reads blobs.
set -e

echo "=== Experiment: Git-Ingest Dynamic Dir ==="

# --- Create a local git repo with some files --------------------------------
REPO_DIR=/tmp/test-blog
rm -rf "$REPO_DIR"
mkdir -p "$REPO_DIR"
cd "$REPO_DIR"
git init -b main
git config user.email "test@example.com"
git config user.name "Test User"

echo "# My Blog" > README.md
mkdir -p posts
echo "Hello, world!" > posts/first.md
echo "Second post content" > posts/second.md
mkdir -p assets
echo "body { color: black; }" > assets/style.css

git add -A
git commit -m "Initial commit"
echo "Created test repo at $REPO_DIR"
cd /

# --- Initialize pond and create factory -------------------------------------
pond init

cat > /tmp/git-ingest.yaml << EOF
url: file://${REPO_DIR}
ref: main
EOF

pond mkdir /system
pond mkdir /system/etc
pond mknod git-ingest /system/etc/blog --config-path /tmp/git-ingest.yaml
echo "Created git-ingest factory node"

# --- Pull from git (fetch into bare cache) -----------------------------------
RUST_LOG=info pond run /system/etc/blog pull

# --- Verify files appear via dynamic dir -------------------------------------
echo ""
echo "=== Listing /system/etc/blog/ ==="
pond list /system/etc/blog/

echo ""
echo "=== Listing /system/etc/blog/posts/ ==="
pond list /system/etc/blog/posts/

echo ""
echo "=== Verify file contents ==="

README_CONTENT=$(pond cat /system/etc/blog/README.md)
if [ "$README_CONTENT" = "# My Blog" ]; then
    echo "README.md: CORRECT"
else
    echo "README.md: WRONG (got: $README_CONTENT)"
    exit 1
fi

FIRST_CONTENT=$(pond cat /system/etc/blog/posts/first.md)
if [ "$FIRST_CONTENT" = "Hello, world!" ]; then
    echo "posts/first.md: CORRECT"
else
    echo "posts/first.md: WRONG (got: $FIRST_CONTENT)"
    exit 1
fi

CSS_CONTENT=$(pond cat /system/etc/blog/assets/style.css)
if [ "$CSS_CONTENT" = "body { color: black; }" ]; then
    echo "assets/style.css: CORRECT"
else
    echo "assets/style.css: WRONG (got: $CSS_CONTENT)"
    exit 1
fi

echo ""
echo "=== Experiment Complete ==="
