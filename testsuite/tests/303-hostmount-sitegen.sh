#!/bin/bash
# EXPERIMENT: Sitegen via host+sitegen:// URL (no pond required)
# EXPECTED: Generates a static site from host filesystem content using
#           the hostmount path. No pond init needed. Content, templates,
#           and config all live on the host.
set -e
source check.sh

echo "=== Experiment: Hostmount Sitegen ==="

SITE_ROOT="/tmp/host-sitegen"
OUTDIR="${OUTPUT:-/output}"

# Clean up from any prior run
rm -rf "${SITE_ROOT}"
rm -rf "${OUTDIR:?}"/* 2>/dev/null || true
mkdir -p "${OUTDIR}"

# ==============================================================================
# Step 1: Create host directory structure
# ==============================================================================

echo ""
echo "--- Step 1: Create host directory structure ---"

mkdir -p "${SITE_ROOT}/content"
mkdir -p "${SITE_ROOT}/site"

echo "directory tree:"
find "${SITE_ROOT}" -type d | sort

# ==============================================================================
# Step 2: Create content files on host
# ==============================================================================

echo ""
echo "--- Step 2: Create content files ---"

cat > "${SITE_ROOT}/content/alpha.md" << 'MD'
---
title: Alpha Page
weight: 10
layout: page
---

## Alpha

This is the alpha page rendered from the host filesystem.
MD

cat > "${SITE_ROOT}/content/beta.md" << 'MD'
---
title: Beta Page
weight: 20
layout: page
---

## Beta

This is the beta page with some **bold** text and a [link](https://example.com).
MD

cat > "${SITE_ROOT}/content/gamma.md" << 'MD'
---
title: Gamma Page
weight: 30
layout: page
hidden: true
---

## Gamma

This page is hidden -- it renders but does not appear in navigation.
MD

echo "content files:"
ls -la "${SITE_ROOT}/content/"

# ==============================================================================
# Step 3: Create site templates on host
# ==============================================================================

echo ""
echo "--- Step 3: Create site templates ---"

cat > "${SITE_ROOT}/site/index.md" << 'MD'
---
title: Host Site
layout: default
---

# Host Site

Welcome to the site generated from the host filesystem.
MD

cat > "${SITE_ROOT}/site/sidebar.md" << 'MD'
## [Home]({{ base_url /}})

{{ content_nav content="pages" /}}
MD

echo "template files:"
ls -la "${SITE_ROOT}/site/"

# ==============================================================================
# Step 4: Create sitegen config on host
# ==============================================================================

echo ""
echo "--- Step 4: Create sitegen config ---"

cat > "${SITE_ROOT}/site.yaml" << 'YAML'
factory: sitegen

site:
  title: "Host Site"
  base_url: "/"

content:
  - name: "pages"
    pattern: "/content/*.md"

exports: []

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/site/index.md"
    routes:
      - name: "pages"
        type: content
        slug: ""
        content: "pages"

partials:
  sidebar: "/site/sidebar.md"

static_assets: []
YAML

echo "config:"
cat "${SITE_ROOT}/site.yaml"

# ==============================================================================
# Step 5: Run sitegen via hostmount (NO POND)
# ==============================================================================

echo ""
echo "--- Step 5: Run sitegen via host+sitegen:// ---"
echo "Command: pond run -d ${SITE_ROOT} host+sitegen:///site.yaml build ${OUTDIR}"

pond run -d "${SITE_ROOT}" host+sitegen:///site.yaml build "${OUTDIR}"

echo "sitegen complete"

# ==============================================================================
# Step 6: Verify output
# ==============================================================================

echo ""
echo "--- Verification ---"

check '[ -f "${OUTDIR}/index.html" ]'  "index.html exists"
check '[ -f "${OUTDIR}/alpha.html" ]'  "alpha.html exists"
check '[ -f "${OUTDIR}/beta.html" ]'   "beta.html exists"
check '[ -f "${OUTDIR}/gamma.html" ]'  "gamma.html exists (hidden page still renders)"
check '[ -f "${OUTDIR}/style.css" ]'   "style.css generated"

check_contains "${OUTDIR}/alpha.html"  "alpha uses page layout"       'class="content-page"'
check_contains "${OUTDIR}/alpha.html"  "alpha has article wrapper"    '<article>'
check_contains "${OUTDIR}/alpha.html"  "heading anchors present"      'id="alpha"'
check_contains "${OUTDIR}/alpha.html"  "sidebar navigation present"   'nav-list'
check_contains "${OUTDIR}/alpha.html"  "active page highlighting"     'class="active"'
check_contains "${OUTDIR}/index.html"  "index uses default layout"    'class="hero"'

check_not_contains "${OUTDIR}/alpha.html"  "hidden page excluded from nav"  'Gamma Page'
check_not_contains "${OUTDIR}/alpha.html"  "no CDN scripts in page layout"  'chart.js'

echo ""
echo "Generated $(find ${OUTDIR} -name '*.html' | wc -l | tr -d ' ') HTML files from host filesystem (no pond)"
find "${OUTDIR}" -name '*.html' -o -name '*.css' | sort

check_finish
