#!/bin/bash
# EXPERIMENT: Git-ingest factory — env var expansion in dynamic directory
# DESCRIPTION: Verify that ${env:VAR} placeholders in git-ingest config
#              are expanded when the dynamic directory is accessed.
#              This exercises the FactoryRegistry::create_directory env
#              expansion path (not pond run, which always expanded).
# EXPECTED: After pull with an env-var ref, pond cat reads files correctly.
#           Without the env var set, the default value is used.
set -e

echo "=== Experiment: Git-Ingest Env Var Expansion ==="

# --- Create a local git repo with branches ----------------------------------
REPO_DIR=/tmp/test-envref
rm -rf "$REPO_DIR"
mkdir -p "$REPO_DIR"
cd "$REPO_DIR"
git init -b main
git config user.email "test@example.com"
git config user.name "Test User"

echo "main content" > page.md
git add -A
git commit -m "Initial on main"

git checkout -b dev
echo "dev content" > page.md
git add -A
git commit -m "Change on dev"

git checkout main
cd /

# --- Test 1: ref uses ${env:VAR:-default} with default ----------------------
echo ""
echo "--- Test 1: env var with default (GIT_TEST_REF unset -> main) ---"

# Ensure GIT_TEST_REF is NOT set
unset GIT_TEST_REF

pond init

cat > /tmp/git-envref.yaml << 'EOF'
url: file:///tmp/test-envref
ref: "${env:GIT_TEST_REF:-main}"
EOF

pond mkdir /system
pond mkdir /system/etc
pond mknod git-ingest /system/etc/content --config-path /tmp/git-envref.yaml

# Pull — the run path expands env vars
RUST_LOG=info pond run /system/etc/content pull

# Read via dynamic directory — this triggers create_directory, which must
# also expand the env var to find the correct ref
CONTENT=$(pond cat /system/etc/content/page.md)
if [ "$CONTENT" = "main content" ]; then
    echo "Default ref (main): CORRECT"
else
    echo "FAIL: expected 'main content', got: $CONTENT"
    exit 1
fi

# --- Test 2: ref uses ${env:VAR:-default} with var set ----------------------
echo ""
echo "--- Test 2: env var set (GIT_TEST_REF=dev) ---"

# Re-init a fresh pond
rm -rf "$POND"
pond init

export GIT_TEST_REF=dev

pond mkdir /system
pond mkdir /system/etc
pond mknod git-ingest /system/etc/content --config-path /tmp/git-envref.yaml

RUST_LOG=info pond run /system/etc/content pull

CONTENT=$(pond cat /system/etc/content/page.md)
if [ "$CONTENT" = "dev content" ]; then
    echo "Env ref (dev): CORRECT"
else
    echo "FAIL: expected 'dev content', got: $CONTENT"
    exit 1
fi

unset GIT_TEST_REF

echo ""
echo "=== Experiment Complete ==="
