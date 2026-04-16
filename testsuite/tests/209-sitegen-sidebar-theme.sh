#!/bin/bash
# EXPERIMENT: Sitegen — sidebar navigation, theme overrides, github_url
# EXPECTED: Sidebar config in site.yaml drives navigation structure.
#           Sections collapse/expand correctly. Theme overrides appear in CSS.
#           GitHub icon is conditional on github_url. Direct-link entries work.
#           Section headings without href link to first child.
set -e
source check.sh

echo "=== Experiment: Sitegen Sidebar & Theme ==="

SITE_ROOT="/tmp/host-sidebar-theme"
OUTDIR="${OUTPUT:-/output}"

rm -rf "${SITE_ROOT}"
rm -rf "${OUTDIR:?}"/* 2>/dev/null || true
mkdir -p "${OUTDIR}"

# ══════════════════════════════════════════════════════════════════════════════
# Step 1: Create site structure with export-based template pages
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 1: Create site structure ---"

mkdir -p "${SITE_ROOT}/site"
mkdir -p "${SITE_ROOT}/data/group-a"
mkdir -p "${SITE_ROOT}/data/group-b"

# Create some minimal parquet files for export matching
# (Use pond to create them via host+table)
for name in alpha beta; do
  python3 -c "
import struct, io
# Minimal valid parquet: PAR1 magic + empty + PAR1
buf = b'PAR1'
buf += b'\x00' * 100
buf += b'PAR1'
open('${SITE_ROOT}/data/group-a/${name}.parquet', 'wb').write(buf)
open('${SITE_ROOT}/data/group-b/${name}.parquet', 'wb').write(buf)
"
done

# Index page
cat > "${SITE_ROOT}/site/index.md" << 'MD'
---
title: Home
layout: default
---

# Test Site

Welcome.
MD

# Data page template
cat > "${SITE_ROOT}/site/data.md" << 'MD'
---
title: "{{ $0 }}"
layout: page
---

# {{ $0 }}

{{ breadcrumb /}}
MD

# Sidebar partial
cat > "${SITE_ROOT}/site/sidebar.md" << 'MD'
{{ content_nav /}}
MD

# ══════════════════════════════════════════════════════════════════════════════
# Step 2: Build site with sidebar config, theme, and github_url
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 2: Build site ---"

cat > "${SITE_ROOT}/site.yaml" << 'YAML'

site:
  title: "Sidebar Test"
  base_url: "/"
  github_url: "https://github.com/test/repo"

content:
  - name: "pages"
    pattern: "/content/*.md"

exports: []

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/site/index.md"
  - name: "group-a"
    type: static
    slug: "a"
    page: "/site/data.md"
  - name: "group-b"
    type: static
    slug: "b"
    page: "/site/data.md"

partials:
  sidebar: "/site/sidebar.md"

sidebar:
  - label: "Home"
    href: "/"
  - label: "Group A"
    href: "/a/index.html"
    children:
      - label: "Item A1"
        href: "/a/alpha.html"
      - label: "Item A2"
        href: "/a/beta.html"
  - label: "Group B"
    children:
      - label: "Item B1"
        href: "/b/alpha.html"
      - label: "Item B2"
        href: "/b/beta.html"

static: []

theme:
  accent: "#1a1a2e"
  accent-light: "#16213e"
  nav-btn: "rgba(100, 100, 200, 0.8)"
  nav-btn-active: "rgba(50, 50, 150, 0.9)"
YAML

pond run -d "${SITE_ROOT}" host+sitegen:///site.yaml build "${OUTDIR}"

# ══════════════════════════════════════════════════════════════════════════════
# Step 3: Verify sidebar structure
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Verification: Sidebar Structure ---"

check '[ -f "${OUTDIR}/index.html" ]'  "index.html exists"
check '[ -f "${OUTDIR}/style.css" ]'   "style.css exists"

# Home page: all sections collapsed (no active child)
check_contains     "${OUTDIR}/index.html"  "home link present"            'href="/"'
check_contains     "${OUTDIR}/index.html"  "sidebar has ul structure"     '<ul>'
check_not_contains "${OUTDIR}/index.html"  "no sections expanded on home" 'subnav expanded'

# Direct link entry (Home)
check_contains     "${OUTDIR}/index.html"  "Home is a link"              '<a href="/">Home</a>'

# Group A: has explicit href -- renders as <a>
check_contains     "${OUTDIR}/index.html"  "Group A is a link"           'href="/a/index.html"'
check_contains     "${OUTDIR}/index.html"  "Group A label"               '>Group A<'

# Group B: no href -- links to first child
check_contains     "${OUTDIR}/index.html"  "Group B links to first child"  'href="/b/alpha.html" class="nav-heading">Group B<'

# Children present
check_contains     "${OUTDIR}/index.html"  "Item A1 in sidebar"          'Item A1'
check_contains     "${OUTDIR}/index.html"  "Item B2 in sidebar"          'Item B2'

# Subnav structure
check_contains     "${OUTDIR}/index.html"  "subnav class present"        'class="subnav"'

# ══════════════════════════════════════════════════════════════════════════════
# Step 4: Verify theme overrides in CSS
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Verification: Theme Overrides ---"

check_contains     "${OUTDIR}/theme.css"   "accent override in CSS"      'accent: #1a1a2e'
check_contains     "${OUTDIR}/theme.css"   "accent-light override"       'accent-light: #16213e'
check_contains     "${OUTDIR}/theme.css"   "nav-btn override"            'nav-btn: rgba(100, 100, 200, 0.8)'
check_contains     "${OUTDIR}/theme.css"   "nav-btn-active override"     'nav-btn-active: rgba(50, 50, 150, 0.9)'

# Base CSS variables still present (not replaced, overridden)
check_contains     "${OUTDIR}/style.css"   "base nav-heading styles"     '.sidebar .nav-heading'

# ══════════════════════════════════════════════════════════════════════════════
# Step 5: Verify GitHub URL
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Verification: GitHub URL ---"

check_contains     "${OUTDIR}/index.html"  "GitHub link present"         'github.com/test/repo'
check_contains     "${OUTDIR}/index.html"  "GitHub icon SVG"             'Source on GitHub'

# ══════════════════════════════════════════════════════════════════════════════
# Step 6: Verify title (no duplication)
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Verification: Page Titles ---"

check_contains     "${OUTDIR}/index.html"  "home title correct"          '<title>Home -- Sidebar Test</title>'
check_not_contains "${OUTDIR}/index.html"  "no duplicate site title"     'Sidebar Test -- Sidebar Test'

echo ""
echo "Generated files:"
find ${OUTDIR} -type f | sort

check_finish
