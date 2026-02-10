#!/bin/bash
# EXPERIMENT: Sitegen base_url — non-root deployment path
# EXPECTED: When base_url is "/subdir/", all generated navigation links,
#           breadcrumbs, and shortcodes produce URLs prefixed with /subdir/.
#           Layout asset references (style.css, chart.js) stay at "/" since
#           Vite handles base path rewriting for those.
set -e

echo "=== Experiment: Sitegen base_url ==="

pond init

OUTDIR=/tmp/sitegen-baseurl-test

# ══════════════════════════════════════════════════════════════════════════════
# Step 1: Minimal synthetic data (same minimal pipeline as 204)
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 1: Create minimal data ---"

# Use recent dates so chart.js default 3-month window shows data.
# Generate 6 months of data ending ~now so there's always overlap.
START_DATE=$(date -u -d "180 days ago" +%Y-%m-%dT00:00:00Z 2>/dev/null \
          || date -u -v-180d +%Y-%m-%dT00:00:00Z)
END_DATE=$(date -u +%Y-%m-%dT00:00:00Z)

cat > /tmp/sensors.yaml << YAML
entries:
  - name: "north_temp"
    factory: "synthetic-timeseries"
    config:
      start: "${START_DATE}"
      end:   "${END_DATE}"
      interval: "1h"
      points:
        - name: "temperature.C"
          components:
            - type: sine
              amplitude: 4.0
              period: "24h"
              offset: 14.0
  - name: "north_do"
    factory: "synthetic-timeseries"
    config:
      start: "${START_DATE}"
      end:   "${END_DATE}"
      interval: "1h"
      points:
        - name: "do.mgL"
          components:
            - type: sine
              amplitude: 1.5
              period: "24h"
              offset: 8.0
YAML

cat > /tmp/combined.yaml << 'YAML'
entries:
  - name: "NorthDock"
    factory: "timeseries-join"
    config:
      inputs:
        - pattern: "series:///sensors/north_temp"
          scope: TempProbe
        - pattern: "series:///sensors/north_do"
          scope: DOProbe
YAML

cat > /tmp/single.yaml << 'YAML'
entries:
  - name: "Temperature"
    factory: "timeseries-pivot"
    config:
      pattern: "/combined/*"
      columns:
        - "TempProbe.temperature.C"
  - name: "DO"
    factory: "timeseries-pivot"
    config:
      pattern: "/combined/*"
      columns:
        - "DOProbe.do.mgL"
YAML

cat > /tmp/reduce.yaml << 'YAML'
entries:
  - name: "single_param"
    factory: "temporal-reduce"
    config:
      in_pattern: "/singled/*"
      out_pattern: "$0"
      time_column: "timestamp"
      resolutions: ["1h"]
      aggregations:
        - type: "avg"
          columns: ["*"]
        - type: "min"
          columns: ["*"]
        - type: "max"
          columns: ["*"]
YAML

pond mknod dynamic-dir /sensors --config-path /tmp/sensors.yaml
pond mknod dynamic-dir /combined --config-path /tmp/combined.yaml
pond mknod dynamic-dir /singled --config-path /tmp/single.yaml
pond mknod dynamic-dir /reduced --config-path /tmp/reduce.yaml
echo "✓ Data pipeline created"

# ══════════════════════════════════════════════════════════════════════════════
# Step 2: Create markdown pages
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 2: Create pages ---"

pond mkdir -p /etc/site

cat > /tmp/index.md << 'MD'
---
title: "Base URL Test"
layout: default
---

# Test Page

Some content here.
MD

cat > /tmp/data.md << 'MD'
---
title: "{{ $0 }}"
layout: data
---

# {{ $0 }}

{{ breadcrumb /}}

{{ chart /}}
MD

cat > /tmp/sidebar.md << 'MD'
## Test Site

### Overview

- [Home]({{ base_url /}})

### By Parameter

{{ nav_list collection="params" base="/params" /}}
MD

pond copy host:///tmp/index.md /etc/site/index.md
pond copy host:///tmp/data.md /etc/site/data.md
pond copy host:///tmp/sidebar.md /etc/site/sidebar.md
echo "✓ Pages loaded"

# ══════════════════════════════════════════════════════════════════════════════
# Step 3A: Generate site with base_url: "/" (root — the default)
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 3A: Sitegen with base_url: / ---"

cat > /tmp/site-root.yaml << 'YAML'
factory: sitegen

site:
  title: "Root Base URL Test"
  base_url: "/"

exports:
  - name: "params"
    pattern: "/reduced/single_param/*/*.series"
    temporal: ["year", "month"]

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/etc/site/index.md"
  - name: "params"
    type: static
    slug: "params"
    routes:
      - name: "param-detail"
        type: template
        slug: "$0"
        page: "/etc/site/data.md"
        export: "params"

partials:
  sidebar: "/etc/site/sidebar.md"

static_assets: []
YAML

pond mknod sitegen /etc/site-root.yaml --config-path /tmp/site-root.yaml

ROOT_DIR=/tmp/sitegen-root
rm -rf "${ROOT_DIR}"
mkdir -p "${ROOT_DIR}"
pond run /etc/site-root.yaml build "${ROOT_DIR}"
echo "✓ Root site generated"

# ══════════════════════════════════════════════════════════════════════════════
# Step 3B: Generate site with base_url: "/myapp/" (subdir deployment)
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 3B: Sitegen with base_url: /myapp/ ---"

cat > /tmp/site-subdir.yaml << 'YAML'
factory: sitegen

site:
  title: "Subdir Base URL Test"
  base_url: "/myapp/"

exports:
  - name: "params"
    pattern: "/reduced/single_param/*/*.series"
    temporal: ["year", "month"]

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/etc/site/index.md"
  - name: "params"
    type: static
    slug: "params"
    routes:
      - name: "param-detail"
        type: template
        slug: "$0"
        page: "/etc/site/data.md"
        export: "params"

partials:
  sidebar: "/etc/site/sidebar.md"

static_assets: []
YAML

pond mknod sitegen /etc/site-subdir.yaml --config-path /tmp/site-subdir.yaml

SUB_DIR=/tmp/sitegen-subdir
rm -rf "${SUB_DIR}"
mkdir -p "${SUB_DIR}"
pond run /etc/site-subdir.yaml build "${SUB_DIR}"
echo "✓ Subdir site generated"

# ══════════════════════════════════════════════════════════════════════════════
# Step 4: Verification
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "=== VERIFICATION ==="

PASS=0
FAIL=0

check_contains() {
  if grep -qF "$3" "$1" 2>/dev/null; then
    echo "  ✓ $2"
    PASS=$((PASS + 1))
  else
    echo "  ✗ $2 — expected: '$3'"
    FAIL=$((FAIL + 1))
  fi
}

check_not_contains() {
  if grep -qF "$3" "$1" 2>/dev/null; then
    echo "  ✗ $2 — found unwanted: '$3'"
    FAIL=$((FAIL + 1))
  else
    echo "  ✓ $2"
    PASS=$((PASS + 1))
  fi
}

# ── Root site checks (base_url: "/") ────────────────────────

echo ""
echo "--- Root site (base_url: /) ---"

echo ""
echo "  Nav links:"
check_contains "${ROOT_DIR}/index.html" "root: nav_list links are /params/..." 'href="/params/Temperature.html"'
check_contains "${ROOT_DIR}/index.html" "root: nav_list DO link" 'href="/params/DO.html"'

echo ""
echo "  Home link:"
check_contains "${ROOT_DIR}/index.html" "root: Home link is /" 'href="/"'

echo ""
echo "  Breadcrumbs:"
check_contains "${ROOT_DIR}/params/Temperature.html" "root: breadcrumb Home href=/" 'href="/"'
check_contains "${ROOT_DIR}/params/Temperature.html" "root: breadcrumb params href=/params" 'href="/params"'

echo ""
echo "  Layout assets (should be / for Vite):"
check_contains "${ROOT_DIR}/index.html" "root: style.css at /style.css" 'href="/style.css"'
check_contains "${ROOT_DIR}/params/Temperature.html" "root: chart.js at /chart.js" 'src="/chart.js"'

echo ""
echo "  Chart data manifest paths (should be /data/...):"
check_contains "${ROOT_DIR}/params/Temperature.html" "root: manifest files start with /data/" '"file":"/data/'
check_not_contains "${ROOT_DIR}/params/Temperature.html" "root: manifest files have no double slash" '"file":"//data/'

# ── Subdir site checks (base_url: "/myapp/") ────────────────

echo ""
echo "--- Subdir site (base_url: /myapp/) ---"

echo ""
echo "  Nav links:"
check_contains "${SUB_DIR}/index.html" "subdir: nav_list links are /myapp/params/..." 'href="/myapp/params/Temperature.html"'
check_contains "${SUB_DIR}/index.html" "subdir: nav_list DO link" 'href="/myapp/params/DO.html"'

echo ""
echo "  Home link:"
check_contains "${SUB_DIR}/index.html" "subdir: Home link is /myapp/" 'href="/myapp/"'

echo ""
echo "  Breadcrumbs:"
check_contains "${SUB_DIR}/params/Temperature.html" "subdir: breadcrumb Home href=/myapp/" 'href="/myapp/"'
check_contains "${SUB_DIR}/params/Temperature.html" "subdir: breadcrumb params href=/myapp/params" 'href="/myapp/params"'

echo ""
echo "  Layout assets (should still be / — Vite handles base):"
check_contains "${SUB_DIR}/index.html" "subdir: style.css still at /style.css" 'href="/style.css"'
check_contains "${SUB_DIR}/params/Temperature.html" "subdir: chart.js still at /chart.js" 'src="/chart.js"'

echo ""
echo "  Chart data manifest paths (should be /myapp/data/...):"
check_contains "${SUB_DIR}/params/Temperature.html" "subdir: manifest files start with /myapp/data/" '"file":"/myapp/data/'
check_not_contains "${SUB_DIR}/params/Temperature.html" "subdir: manifest files have no double slash" '"file":"/myapp//data/'
check_not_contains "${SUB_DIR}/params/Temperature.html" "subdir: manifest files NOT bare /data/" '"file":"/data/'

echo ""
echo "  No double slashes:"
check_not_contains "${SUB_DIR}/index.html" "subdir: no // in nav links" 'href="/myapp//params'
check_not_contains "${SUB_DIR}/params/Temperature.html" "subdir: no // in breadcrumbs" 'href="/myapp//"'

echo ""
echo "  Active nav link highlighting (Temperature page):"
check_contains "${SUB_DIR}/params/Temperature.html" "subdir: active class on Temperature" 'class="active"'
check_contains "${SUB_DIR}/params/Temperature.html" "subdir: aria-current on Temperature" 'aria-current="page"'

# ── Output files identical structure ─────────────────────────

echo ""
echo "--- Both sites have same file structure ---"

ROOT_FILES=$(cd "${ROOT_DIR}" && find . -name '*.html' | sort)
SUB_FILES=$(cd "${SUB_DIR}" && find . -name '*.html' | sort)
if [ "${ROOT_FILES}" = "${SUB_FILES}" ]; then
  echo "  ✓ HTML file structure matches"
  PASS=$((PASS + 1))
else
  echo "  ✗ HTML file structure differs"
  echo "    Root: ${ROOT_FILES}"
  echo "    Sub:  ${SUB_FILES}"
  FAIL=$((FAIL + 1))
fi

echo ""
echo "=== Results: ${PASS} passed, ${FAIL} failed ==="

if [ "${FAIL}" -gt 0 ]; then
  echo ""
  echo "DEBUG: Root index.html sidebar:"
  grep -E 'href=|src=' "${ROOT_DIR}/index.html" | head -20
  echo ""
  echo "DEBUG: Subdir index.html sidebar:"
  grep -E 'href=|src=' "${SUB_DIR}/index.html" | head -20
  echo ""
  echo "DEBUG: Subdir params/Temperature.html breadcrumbs:"
  grep -i 'breadcrumb' "${SUB_DIR}/params/Temperature.html" | head -10
  exit 1
fi

echo ""
echo "=== Test 205 PASSED ==="
