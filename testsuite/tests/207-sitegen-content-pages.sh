#!/bin/bash
# EXPERIMENT: Sitegen — content page route type
# EXPECTED: Content stage globs markdown files, parses frontmatter,
#           generates one page per file with sidebar navigation.
#           No export stages — pure content site.
set -e
source check.sh

echo "=== Experiment: Sitegen Content Pages ==="

pond init
OUTDIR="${OUTPUT:-/output}"

# ══════════════════════════════════════════════════════════════════════════════
# Step 1: Create synthetic content files
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 1: Create content files ---"

pond mkdir /content

cat > /tmp/alpha.md << 'MD'
---
title: Alpha Page
weight: 10
layout: page
---

## Alpha

This is the alpha page. It should appear first in navigation.
MD

cat > /tmp/beta.md << 'MD'
---
title: Beta Page
weight: 20
layout: page
---

## Beta

This is the beta page with a [link](https://example.com).

### Sub-heading

Some more text under a sub-heading.
MD

cat > /tmp/gamma.md << 'MD'
---
title: Gamma Page
weight: 30
layout: page
hidden: true
---

## Gamma

This page is hidden — it should render but not appear in navigation.
MD

pond copy host:///tmp/alpha.md /content/alpha.md
pond copy host:///tmp/beta.md /content/beta.md
pond copy host:///tmp/gamma.md /content/gamma.md

echo "✓ Content files loaded"
pond list /content

# ══════════════════════════════════════════════════════════════════════════════
# Step 2: Create site templates
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 2: Create site templates ---"

pond mkdir /etc
pond mkdir /etc/site

cat > /tmp/index.md << 'MD'
---
title: Test Site
layout: default
---

# Test Site

Welcome to the test site.
MD
pond copy host:///tmp/index.md /etc/site/index.md

cat > /tmp/sidebar.md << 'MD'
## [Home]({{ base_url /}})

{{ content_nav content="pages" /}}
MD
pond copy host:///tmp/sidebar.md /etc/site/sidebar.md

echo "✓ Templates created"

# ══════════════════════════════════════════════════════════════════════════════
# Step 3: Create site.yaml and run sitegen
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 3: Run sitegen ---"

cat > /tmp/site.yaml << 'YAML'
factory: sitegen

site:
  title: "Test Site"
  base_url: "/"

content:
  - name: "pages"
    pattern: "/content/*.md"

exports: []

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/etc/site/index.md"
    routes:
      - name: "pages"
        type: content
        slug: ""
        content: "pages"

partials:
  sidebar: "/etc/site/sidebar.md"

static_assets: []
YAML

pond mknod sitegen /etc/site.yaml --config-path /tmp/site.yaml

rm -rf "${OUTDIR:?}"/* 2>/dev/null || true
mkdir -p "${OUTDIR}"
pond run /etc/site.yaml build "${OUTDIR}"

# ══════════════════════════════════════════════════════════════════════════════
# Step 4: Verify content pages
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Verification ---"

check '[ -f "${OUTDIR}/index.html" ]'  "index.html exists"
check '[ -f "${OUTDIR}/alpha.html" ]'  "alpha.html exists"
check '[ -f "${OUTDIR}/beta.html" ]'   "beta.html exists"
check '[ -f "${OUTDIR}/gamma.html" ]'  "gamma.html exists (hidden page still renders)"

check_contains "${OUTDIR}/alpha.html"  "alpha uses page layout"       'class="content-page"'
check_contains "${OUTDIR}/alpha.html"  "alpha has article wrapper"    '<article>'
check_contains "${OUTDIR}/alpha.html"  "heading anchor on alpha"      'id="alpha"'
check_contains "${OUTDIR}/beta.html"   "sub-heading anchor on beta"   'id="sub-heading"'
check_contains "${OUTDIR}/alpha.html"  "sidebar navigation present"   'nav-list'
check_contains "${OUTDIR}/alpha.html"  "active page highlighting"     'class="active"'
check_contains "${OUTDIR}/index.html"  "index uses default layout"    'class="hero"'

check_not_contains "${OUTDIR}/alpha.html"  "hidden page excluded from nav"  'Gamma Page'
check_not_contains "${OUTDIR}/alpha.html"  "no CDN scripts in page layout"  'chart.js'

echo ""
echo "Generated $(find ${OUTDIR} -name '*.html' | wc -l | tr -d ' ') HTML files"
find ${OUTDIR} -name '*.html' | sort

check_finish
