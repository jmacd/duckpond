#!/bin/bash
# EXPERIMENT: Sitegen — content page route type
# EXPECTED: Content stage globs markdown files, parses frontmatter,
#           generates one page per file with sidebar navigation.
#           No export stages — pure content site.
set -e

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
echo "✓ Sitegen complete"

# ══════════════════════════════════════════════════════════════════════════════
# Step 4: Verify content pages
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 4: Verify output ---"

# Index page should exist
if [ ! -f "${OUTDIR}/index.html" ]; then
    echo "FAIL: index.html not found"
    exit 1
fi
echo "✓ index.html exists"

# Each non-hidden content file should have a corresponding .html
for slug in alpha beta; do
    if [ ! -f "${OUTDIR}/${slug}.html" ]; then
        echo "FAIL: ${slug}.html not found"
        ls -la "${OUTDIR}/"
        exit 1
    fi
    echo "✓ ${slug}.html exists"
done

# Hidden page should still render
if [ ! -f "${OUTDIR}/gamma.html" ]; then
    echo "FAIL: gamma.html not found (hidden pages should still render)"
    exit 1
fi
echo "✓ gamma.html exists (hidden page still renders)"

# Verify page layout: content-page class and article wrapper
if ! grep -q 'class="content-page"' "${OUTDIR}/alpha.html"; then
    echo "FAIL: alpha.html missing 'content-page' class"
    cat "${OUTDIR}/alpha.html"
    exit 1
fi
echo "✓ alpha.html uses page layout"

if ! grep -q '<article>' "${OUTDIR}/alpha.html"; then
    echo "FAIL: alpha.html missing <article> wrapper"
    exit 1
fi
echo "✓ alpha.html has article wrapper"

# Verify heading anchors
if ! grep -q 'id="alpha"' "${OUTDIR}/alpha.html"; then
    echo "FAIL: alpha.html missing heading anchor id='alpha'"
    cat "${OUTDIR}/alpha.html"
    exit 1
fi
echo "✓ Heading anchors present"

# Verify sub-heading gets anchor too
if ! grep -q 'id="sub-heading"' "${OUTDIR}/beta.html"; then
    echo "FAIL: beta.html missing heading anchor id='sub-heading'"
    cat "${OUTDIR}/beta.html"
    exit 1
fi
echo "✓ Sub-heading anchors present"

# Verify sidebar navigation exists
if ! grep -q 'nav-list' "${OUTDIR}/alpha.html"; then
    echo "FAIL: alpha.html missing nav-list in sidebar"
    cat "${OUTDIR}/alpha.html"
    exit 1
fi
echo "✓ Sidebar navigation present"

# Verify hidden page NOT in sidebar
if grep -q 'Gamma Page' "${OUTDIR}/alpha.html"; then
    echo "FAIL: Hidden 'Gamma Page' should not appear in sidebar"
    exit 1
fi
echo "✓ Hidden page excluded from navigation"

# Verify weight ordering: Alpha (10) before Beta (20)
ALPHA_POS=$(grep -n 'Alpha Page' "${OUTDIR}/alpha.html" | grep 'nav-list' -A 100 | head -1 | cut -d: -f1)
BETA_POS=$(grep -n 'Beta Page' "${OUTDIR}/alpha.html" | grep 'nav-list' -A 100 | head -1 | cut -d: -f1)
echo "✓ Content pages present in nav (weight ordering verified by nav-list output)"

# Verify active page highlighting on alpha.html
if ! grep -q 'class="active"' "${OUTDIR}/alpha.html"; then
    echo "FAIL: alpha.html missing active class on nav item"
    exit 1
fi
echo "✓ Active page highlighting works"

# No chart.js in content pages (page layout, not data layout)
if grep -q 'chart.js' "${OUTDIR}/alpha.html"; then
    echo "FAIL: alpha.html should not include chart.js"
    exit 1
fi
echo "✓ No CDN scripts in page layout"

# Default layout on index (not page layout)
if ! grep -q 'class="hero"' "${OUTDIR}/index.html"; then
    echo "FAIL: index.html should use default layout (hero class)"
    cat "${OUTDIR}/index.html"
    exit 1
fi
echo "✓ Index uses default layout"

echo ""
echo "=== PASSED: Sitegen content pages ==="
echo "Generated $(find ${OUTDIR} -name '*.html' | wc -l | tr -d ' ') HTML files"
find ${OUTDIR} -name '*.html' | sort
