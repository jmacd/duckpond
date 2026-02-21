#!/bin/bash
# EXPERIMENT: Sitegen — HTML pass-through in markdown
# EXPECTED: Raw HTML blocks (script, link, div) in markdown pages must
#           survive the markdown renderer without being wrapped in <p> tags
#           or otherwise mangled.
#
# This specifically tests that inline <script> blocks in markdown are
# rendered verbatim, which is needed for pages that embed Leaflet maps
# or other client-side JS.
set -e
source check.sh

echo "=== Experiment: Sitegen HTML Pass-Through ==="

pond init

OUTDIR=/tmp/sitegen-html-test

# ══════════════════════════════════════════════════════════════════════════════
# Step 1: Minimal synthetic data (just enough for sitegen to run)
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 1: Create minimal data ---"

cat > /tmp/sensors.yaml << 'YAML'
entries:
  - name: "north_temp"
    factory: "synthetic-timeseries"
    config:
      start: "2025-01-01T00:00:00Z"
      end:   "2025-01-02T00:00:00Z"
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
      start: "2025-01-01T00:00:00Z"
      end:   "2025-01-02T00:00:00Z"
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
# Step 2: Create markdown pages with embedded HTML/JS
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 2: Create pages with embedded HTML ---"

pond mkdir -p /etc/site

# Index page with inline script block — the key test case
cat > /tmp/index.md << 'HTMLMD'
---
title: "HTML Pass-Through Test"
layout: default
---

# Test Page

Some markdown content here.

<div id="test-container" style="height:100px; background:#333;"></div>

<link rel="stylesheet" href="https://example.com/test.css">
<script type="module">
const el = document.getElementById("test-container");
const items = [1, 2, 3];

items.forEach(i => {
  const div = document.createElement("div");
  div.textContent = `Item ${i}`;
  el.appendChild(div);
});

console.log("Script loaded successfully");
</script>
HTMLMD

# Data page (simple, no HTML issues expected)
cat > /tmp/data.md << 'MD'
---
title: "{{ $0 }}"
layout: data
---

# {{ $0 }}

{{ breadcrumb /}}

{{ chart /}}
MD

# Sidebar
cat > /tmp/sidebar.md << 'MD'
## Test Site

### Overview

- [Home](/)

### By Parameter

{{ nav_list collection="params" base="/params" /}}
MD

pond copy host:///tmp/index.md /etc/site/index.md
pond copy host:///tmp/data.md /etc/site/data.md
pond copy host:///tmp/sidebar.md /etc/site/sidebar.md
echo "✓ Pages loaded"

# ══════════════════════════════════════════════════════════════════════════════
# Step 3: Create site.yaml and run sitegen
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 3: Run sitegen ---"

cat > /tmp/site.yaml << 'YAML'
factory: sitegen

site:
  title: "HTML Pass-Through Test"
  base_url: "/"

exports:
  - name: "params"
    pattern: "/reduced/single_param/*/*.series"

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

pond mknod sitegen /etc/site.yaml --config-path /tmp/site.yaml

rm -rf "${OUTDIR}"
mkdir -p "${OUTDIR}"
pond run /etc/site.yaml build "${OUTDIR}"
echo "✓ Sitegen complete"

# ══════════════════════════════════════════════════════════════════════════════
# Step 4: Verify HTML pass-through
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "=== VERIFICATION ==="

echo ""
echo "--- Verification ---"

INDEX="${OUTDIR}/index.html"

echo ""
echo "--- Script block integrity ---"
check_contains "$INDEX" "script tag present" '<script type="module">'
check_contains "$INDEX" "JS const preserved" 'const el = document.getElementById("test-container")'
check_contains "$INDEX" "JS forEach preserved" 'items.forEach(i =>'
check_contains "$INDEX" "JS console.log preserved" 'console.log("Script loaded successfully")'
check_contains "$INDEX" "closing script tag" '</script>'

echo ""
echo "--- No markdown contamination inside script ---"
check_not_contains "$INDEX" "no <p> inside script" '<p>const'
check_not_contains "$INDEX" "no <p> around forEach" '<p>items.forEach'
check_not_contains "$INDEX" "no <em> from underscores" '<em>'
check_not_contains "$INDEX" "no <code> inside script" '<code>'

echo ""
echo "--- Other HTML blocks preserved ---"
check_contains "$INDEX" "div with id preserved" '<div id="test-container"'
check_contains "$INDEX" "link tag preserved" '<link rel="stylesheet"'

echo ""
echo "--- Markdown still renders ---"
check_contains "$INDEX" "h1 rendered" '<h1 id="test-page">Test Page</h1>'
check_contains "$INDEX" "paragraph rendered" '<p>Some markdown content here.</p>'

check_finish
