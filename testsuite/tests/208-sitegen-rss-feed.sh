#!/bin/bash
# EXPERIMENT: Sitegen -- RSS feed generation from blog content
# EXPECTED: When site_url is configured, sitegen generates feed.xml with
#           blog posts (section=Blog, non-hidden, with dates), sorted
#           newest-first. RSS icon appears in HTML top bar. Autodiscovery
#           link appears in <head>. When site_url is absent, no feed.
set -e
source check.sh

echo "=== Experiment: Sitegen RSS Feed ==="

SITE_ROOT="/tmp/host-rss-feed"
OUTDIR="${OUTPUT:-/output}"

rm -rf "${SITE_ROOT}"
rm -rf "${OUTDIR:?}"/* 2>/dev/null || true
mkdir -p "${OUTDIR}"

# ══════════════════════════════════════════════════════════════════════════════
# Step 1: Create content files with blog posts and non-blog pages
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 1: Create content files ---"

mkdir -p "${SITE_ROOT}/content"
mkdir -p "${SITE_ROOT}/site"

cat > "${SITE_ROOT}/content/blog.md" << 'MD'
---
title: Blog
weight: 50
layout: page
section: Main
---

# Blog

{{ blog_grid content="pages" section="Blog" /}}
MD

cat > "${SITE_ROOT}/content/first-post.md" << 'MD'
---
title: First Post
layout: blog
section: Blog
date: "2025-01-15"
summary: "This is the first blog post about getting started."
---

## Getting Started

Welcome to the first post.
MD

cat > "${SITE_ROOT}/content/second-post.md" << 'MD'
---
title: Second Post
layout: blog
section: Blog
date: "2025-03-10"
summary: "A newer post with more content."
---

## More Content

This is a second, newer blog post.
MD

cat > "${SITE_ROOT}/content/hidden-post.md" << 'MD'
---
title: Hidden Draft
layout: blog
section: Blog
date: "2025-04-01"
summary: "This should not appear in the feed."
hidden: true
---

## Draft

This post is hidden.
MD

cat > "${SITE_ROOT}/content/about.md" << 'MD'
---
title: About
weight: 10
layout: page
section: Main
---

## About Us

This is not a blog post, it should not be in the feed.
MD

# ══════════════════════════════════════════════════════════════════════════════
# Step 2: Create site templates
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 2: Create site templates ---"

cat > "${SITE_ROOT}/site/index.md" << 'MD'
---
title: Test Blog Site
layout: default
---

# Test Blog

Welcome.
MD

cat > "${SITE_ROOT}/site/sidebar.md" << 'MD'
{{ content_nav content="pages" /}}
MD

# ══════════════════════════════════════════════════════════════════════════════
# Step 3: Build with site_url (RSS should be generated)
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 3: Build with site_url ---"

cat > "${SITE_ROOT}/site.yaml" << 'YAML'
factory: sitegen

site:
  title: "Test Blog"
  base_url: "/"
  site_url: "https://testblog.example.com"

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

sidebar:
  - "Main"
YAML

echo "Command: pond run -d ${SITE_ROOT} host+sitegen:///site.yaml build ${OUTDIR}"
pond run -d "${SITE_ROOT}" host+sitegen:///site.yaml build "${OUTDIR}"

# ══════════════════════════════════════════════════════════════════════════════
# Step 4: Verify RSS feed
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Verification: RSS Feed ---"

check '[ -f "${OUTDIR}/feed.xml" ]'  "feed.xml exists"

# Channel metadata
check_contains "${OUTDIR}/feed.xml"  "channel title"               '<title>Test Blog</title>'
check_contains "${OUTDIR}/feed.xml"  "channel link"                'https://testblog.example.com'

# Blog posts present
check_contains "${OUTDIR}/feed.xml"  "first post in feed"          'First Post'
check_contains "${OUTDIR}/feed.xml"  "second post in feed"         'Second Post'
check_contains "${OUTDIR}/feed.xml"  "first post summary"          'This is the first blog post'
check_contains "${OUTDIR}/feed.xml"  "second post summary"         'A newer post with more content'
check_contains "${OUTDIR}/feed.xml"  "first post link"             'first-post.html'
check_contains "${OUTDIR}/feed.xml"  "second post link"            'second-post.html'
check_contains "${OUTDIR}/feed.xml"  "absolute URLs in feed"       'https://testblog.example.com/'

# Ordering: Second Post (2025-03-10) should appear before First Post (2025-01-15)
check 'python3 -c "
import sys
xml = open(\"${OUTDIR}/feed.xml\").read()
second = xml.find(\"Second Post\")
first = xml.find(\"First Post\")
assert second < first, f\"Second Post at {second} should be before First Post at {first}\"
print(\"  order OK: newest first\")
"' "newest post appears first in feed"

# Exclusions
check_not_contains "${OUTDIR}/feed.xml"  "hidden post excluded"    'Hidden Draft'
check_not_contains "${OUTDIR}/feed.xml"  "non-blog page excluded"  'About'

# ══════════════════════════════════════════════════════════════════════════════
# Step 5: Verify RSS icon in HTML
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Verification: RSS Icon in HTML ---"

check_contains "${OUTDIR}/index.html"       "RSS icon on homepage"         'RSS Feed'
check_contains "${OUTDIR}/index.html"       "feed link in top bar"         'feed.xml'
check_contains "${OUTDIR}/first-post.html"  "RSS icon on blog post"        'RSS Feed'
check_contains "${OUTDIR}/about.html"       "RSS icon on content page"     'RSS Feed'

# RSS autodiscovery link in <head>
check_contains "${OUTDIR}/index.html"       "autodiscovery link"           'application/rss+xml'
check_contains "${OUTDIR}/first-post.html"  "autodiscovery on blog post"   'application/rss+xml'

# GitHub icon still present
check_contains "${OUTDIR}/index.html"       "GitHub icon still present"    'github.com'

# ══════════════════════════════════════════════════════════════════════════════
# Step 6: Build WITHOUT site_url (RSS should be skipped)
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 6: Build without site_url ---"

OUTDIR2="${OUTDIR}-nofeed"
rm -rf "${OUTDIR2}" 2>/dev/null || true
mkdir -p "${OUTDIR2}"

cat > "${SITE_ROOT}/site-nofeed.yaml" << 'YAML'
factory: sitegen

site:
  title: "Test Blog"
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

sidebar:
  - "Main"
YAML

pond run -d "${SITE_ROOT}" host+sitegen:///site-nofeed.yaml build "${OUTDIR2}"

check '[ ! -f "${OUTDIR2}/feed.xml" ]'         "no feed.xml without site_url"
check_not_contains "${OUTDIR2}/index.html"      "no RSS icon without site_url"     'RSS Feed'
check_not_contains "${OUTDIR2}/index.html"      "no autodiscovery without site_url" 'application/rss+xml'

rm -rf "${OUTDIR2}"

echo ""
echo "Generated files:"
find ${OUTDIR} -type f | sort

check_finish
