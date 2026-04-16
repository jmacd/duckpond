#!/bin/bash
# EXPERIMENT: Sitegen build footer with version identifier
# EXPECTED: Every generated page contains a <footer> with a DuckPond
#           build identifier (version or git SHA).
set -e
source check.sh

echo "=== Experiment: Sitegen Build Footer ==="

SITE_ROOT="/tmp/footer-test"
OUTDIR="${OUTPUT:-/output}"

rm -rf "${SITE_ROOT}"
rm -rf "${OUTDIR:?}"/* 2>/dev/null || true
mkdir -p "${OUTDIR}"

# Create minimal site structure
mkdir -p "${SITE_ROOT}/templates"

cat > "${SITE_ROOT}/templates/index.md" <<'EOF'
---
title: Home
---
# Welcome
This is the home page.
EOF

cat > "${SITE_ROOT}/templates/sidebar.md" <<'EOF'
- [Home](/)
EOF

cat > "${SITE_ROOT}/site.yaml" <<'EOF'

site:
  title: "Footer Test"
  base_url: "/"

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/templates/index.md"

partials:
  sidebar: "/templates/sidebar.md"

EOF

# Build the site using hostmount
pond run \
    -d "${SITE_ROOT}" \
    "host+sitegen:///site.yaml" \
    build "${OUTDIR}"

echo "=== VERIFICATION ==="

# Check that index.html exists
check 'test -f "${OUTDIR}/index.html"' "index.html exists"

# Check that the build footer is present
check_contains "${OUTDIR}/index.html" "footer contains build-footer class" "build-footer"
check_contains "${OUTDIR}/index.html" "footer mentions DuckPond" "DuckPond"

# The footer should contain either a version (v0.38.0) or a git SHA
if grep -q 'DuckPond v[0-9]' "${OUTDIR}/index.html"; then
    echo "[OK] Footer contains version identifier"
elif grep -q 'DuckPond [0-9a-f]' "${OUTDIR}/index.html"; then
    echo "[OK] Footer contains git SHA identifier"
else
    echo "[FAIL] Footer does not contain a recognizable build identifier"
    grep "build-footer" "${OUTDIR}/index.html" || true
    exit 1
fi

echo "[OK] Build footer verified"

check_finish
