#!/bin/bash
# EXPERIMENT: Sitegen build footer with configurable text and variables
# EXPECTED: Footer renders custom text with ${DATE} and ${BUILD} expansion.
#           No footer when footer config is absent.
set -e
source check.sh

echo "=== Experiment: Sitegen Build Footer ==="

SITE_ROOT="/tmp/footer-test"
OUTDIR="/tmp/footer-output"

rm -rf "${SITE_ROOT}"
rm -rf "${OUTDIR}"
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

# --- Test 1: No footer configured → no footer element ---
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

pond run \
    -d "${SITE_ROOT}" \
    "host+sitegen:///site.yaml" \
    build "${OUTDIR}"

echo "=== Test 1: No footer configured ==="
check_not_contains "${OUTDIR}/index.html" "no footer element when unconfigured" "build-footer"

# --- Test 2: Footer with ${BUILD} variable ---
rm -rf "${OUTDIR}"
mkdir -p "${OUTDIR}"

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

footer: "Test Site | ${BUILD}"

EOF

pond run \
    -d "${SITE_ROOT}" \
    "host+sitegen:///site.yaml" \
    build "${OUTDIR}"

echo "=== Test 2: Footer with BUILD variable ==="
check_contains "${OUTDIR}/index.html" "footer contains build-footer class" "build-footer"
check_contains "${OUTDIR}/index.html" "footer contains custom text" "Test Site"
check_contains "${OUTDIR}/index.html" "footer mentions DuckPond" "DuckPond"

# The footer should contain either a version (v0.47.0) or a git SHA
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
