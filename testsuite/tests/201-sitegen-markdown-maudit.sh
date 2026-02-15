#!/bin/bash
# EXPERIMENT: Sitegen factory — Markdown + Maud site generation
# EXPECTED: Full pipeline from synthetic waveforms through join/pivot/reduce
#           then sitegen factory renders Markdown pages to HTML using Maud layouts.
#           Produces index.html + per-param + per-site pages.
#
# Pipeline: synthetic-timeseries → timeseries-join → timeseries-pivot
#           → temporal-reduce → sitegen (pond run /etc/site.yaml build)
#
# Data: 2 sites (NorthDock, SouthDock) × 2 params (Temperature, DO)
#       1 year (2025) at 1h resolution
set -e

echo "=== Experiment: Sitegen Factory — Markdown + Maud ==="

pond init

OUTDIR=/tmp/sitegen-dist

# ══════════════════════════════════════════════════════════════════════════════
# Step 1: Create synthetic sensor data (same as test 200)
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 1: Create synthetic sensors ---"

cat > /tmp/sensors.yaml << 'YAML'
entries:
  - name: "north_temperature"
    factory: "synthetic-timeseries"
    config:
      start: "2025-01-01T00:00:00Z"
      end:   "2025-12-31T23:00:00Z"
      interval: "1h"
      points:
        - name: "temperature.C"
          components:
            - type: sine
              amplitude: 4.0
              period: "24h"
              offset: 14.0
            - type: sine
              amplitude: 6.0
              period: "8760h"
              phase: -1.5708

  - name: "north_do"
    factory: "synthetic-timeseries"
    config:
      start: "2025-01-01T00:00:00Z"
      end:   "2025-12-31T23:00:00Z"
      interval: "1h"
      points:
        - name: "do.mgL"
          components:
            - type: sine
              amplitude: 1.5
              period: "24h"
              offset: 8.0
              phase: 3.1416
            - type: sine
              amplitude: 2.0
              period: "8760h"
              phase: 1.5708

  - name: "south_temperature"
    factory: "synthetic-timeseries"
    config:
      start: "2025-01-01T00:00:00Z"
      end:   "2025-12-31T23:00:00Z"
      interval: "1h"
      points:
        - name: "temperature.C"
          components:
            - type: triangle
              amplitude: 3.5
              period: "24h"
              offset: 15.0
            - type: sine
              amplitude: 5.5
              period: "8760h"
              phase: -1.5708

  - name: "south_do"
    factory: "synthetic-timeseries"
    config:
      start: "2025-01-01T00:00:00Z"
      end:   "2025-12-31T23:00:00Z"
      interval: "1h"
      points:
        - name: "do.mgL"
          components:
            - type: sine
              amplitude: 1.2
              period: "24h"
              offset: 7.5
              phase: 3.1416
            - type: square
              amplitude: 0.5
              period: "4380h"
              offset: 0.0
            - type: sine
              amplitude: 1.8
              period: "8760h"
              phase: 1.5708
YAML

pond mknod dynamic-dir /sensors --config-path /tmp/sensors.yaml
echo "✓ /sensors created"

# ══════════════════════════════════════════════════════════════════════════════
# Step 2: Create per-site joins at /combined
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 2: Create per-site joins ---"

cat > /tmp/combined.yaml << 'YAML'
entries:
  - name: "NorthDock"
    factory: "timeseries-join"
    config:
      inputs:
        - pattern: "series:///sensors/north_temperature"
          scope: TempProbe
        - pattern: "series:///sensors/north_do"
          scope: DOProbe

  - name: "SouthDock"
    factory: "timeseries-join"
    config:
      inputs:
        - pattern: "series:///sensors/south_temperature"
          scope: TempProbe
        - pattern: "series:///sensors/south_do"
          scope: DOProbe
YAML

pond mknod dynamic-dir /combined --config-path /tmp/combined.yaml
echo "✓ /combined created"

# ══════════════════════════════════════════════════════════════════════════════
# Step 3: Create per-param pivots at /singled
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 3: Create per-param pivots ---"

cat > /tmp/singled.yaml << 'YAML'
entries:
  - name: "Temperature"
    factory: "timeseries-pivot"
    config:
      pattern: "series:///combined/*"
      columns:
        - "TempProbe.temperature.C"

  - name: "DO"
    factory: "timeseries-pivot"
    config:
      pattern: "series:///combined/*"
      columns:
        - "DOProbe.do.mgL"
YAML

pond mknod dynamic-dir /singled --config-path /tmp/singled.yaml
echo "✓ /singled created"

# ══════════════════════════════════════════════════════════════════════════════
# Step 4: Create temporal reduce at /reduced
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 4: Create temporal reduce (1h avg/min/max) ---"

cat > /tmp/reduce.yaml << 'YAML'
entries:
  - name: "single_site"
    factory: "temporal-reduce"
    config:
      in_pattern: "/combined/*"
      out_pattern: "$0"
      time_column: "timestamp"
      resolutions: ["1h", "2h", "4h", "12h", "24h"]
      aggregations:
        - type: "avg"
          columns: ["*"]
        - type: "min"
          columns: ["*"]
        - type: "max"
          columns: ["*"]

  - name: "single_param"
    factory: "temporal-reduce"
    config:
      in_pattern: "/singled/*"
      out_pattern: "$0"
      time_column: "timestamp"
      resolutions: ["1h", "2h", "4h", "12h", "24h"]
      aggregations:
        - type: "avg"
          columns: ["*"]
        - type: "min"
          columns: ["*"]
        - type: "max"
          columns: ["*"]
YAML

pond mknod dynamic-dir /reduced --config-path /tmp/reduce.yaml
echo "✓ /reduced created"

echo ""
echo "Reduced directory tree:"
pond list /reduced/**

# ══════════════════════════════════════════════════════════════════════════════
# Step 5: Load markdown pages + site.yaml into pond
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 5: Load sitegen pages into pond ---"

pond mkdir /etc
pond mkdir /etc/site

# Write markdown pages into pond
cat > /tmp/index.md << 'MD'
---
title: "Synthetic Example — DuckPond"
layout: default
---

# Synthetic Example

DuckPond End-to-End Pipeline Demo

## About This Example

This dashboard is generated entirely from **synthetic waveform data**
using the full DuckPond pipeline:

- **synthetic-timeseries** — generates sine/triangle/square waveforms
- **timeseries-join** — combines parameters per site
- **timeseries-pivot** — extracts one parameter across all sites
- **temporal-reduce** — downsamples to hourly avg/min/max
- **sitegen** — renders Markdown pages to HTML with Maud layouts
- **pond export** — writes Parquet + HTML to disk

## Navigation

Use the sidebar to explore data **by parameter** or **by site**.
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
## 🌡️ Synthetic Example

### Overview

- [Home](/)

### By Parameter

{{ nav_list collection="params" base="/params" /}}

### By Site

{{ nav_list collection="sites" base="/sites" /}}
MD

pond copy host:///tmp/index.md   /etc/site/index.md
pond copy host:///tmp/data.md    /etc/site/data.md
pond copy host:///tmp/sidebar.md /etc/site/sidebar.md
echo "✓ markdown pages loaded"

# Write site.yaml config
cat > /tmp/site.yaml << 'YAML'
{% raw %}
factory: sitegen

site:
  title: "Synthetic Example"
  base_url: "/"

exports:
  - name: "params"
    pattern: "/reduced/single_param/*/*.series"
    temporal: ["year"]
  - name: "sites"
    pattern: "/reduced/single_site/*/*.series"
    temporal: ["year"]

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
  - name: "sites"
    type: static
    slug: "sites"
    routes:
      - name: "site-detail"
        type: template
        slug: "$0"
        page: "/etc/site/data.md"
        export: "sites"

partials:
  sidebar: "/etc/site/sidebar.md"

static_assets: []
{% endraw %}
YAML

pond mknod sitegen /etc/site.yaml --config-path /tmp/site.yaml
echo "✓ site.yaml loaded (factory=sitegen)"

echo ""
echo "Pond /etc listing:"
pond list '/etc/**'

# ══════════════════════════════════════════════════════════════════════════════
# Step 6: Run sitegen factory
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 6: Run sitegen factory ---"
rm -rf "${OUTDIR}"
mkdir -p "${OUTDIR}"

pond run /etc/site.yaml build "${OUTDIR}"
echo "✓ sitegen complete"

# ══════════════════════════════════════════════════════════════════════════════
# Step 7: Verification
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "=== VERIFICATION ==="

echo ""
echo "Output directory structure:"
find "${OUTDIR}" -type f | sort

echo ""
echo "File counts:"
HTML_COUNT=$(find "${OUTDIR}" -name '*.html' | wc -l | tr -d ' ')
echo "  HTML files: ${HTML_COUNT}"

# Check expected files
PASS=0
FAIL=0

check_file() {
  if [ -f "$1" ]; then
    echo "  ✓ $2"
    PASS=$((PASS + 1))
  else
    echo "  ✗ MISSING: $2"
    FAIL=$((FAIL + 1))
  fi
}

check_contains() {
  if grep -qF "$3" "$1" 2>/dev/null; then
    echo "  ✓ $2 contains '$3'"
    PASS=$((PASS + 1))
  else
    echo "  ✗ $2 missing '$3'"
    FAIL=$((FAIL + 1))
  fi
}

echo ""
echo "--- File existence ---"
check_file "${OUTDIR}/index.html" "index.html"
check_file "${OUTDIR}/params/Temperature.html" "params/Temperature.html"
check_file "${OUTDIR}/params/DO.html" "params/DO.html"
check_file "${OUTDIR}/sites/NorthDock.html" "sites/NorthDock.html"
check_file "${OUTDIR}/sites/SouthDock.html" "sites/SouthDock.html"
check_file "${OUTDIR}/style.css" "style.css"
check_file "${OUTDIR}/chart.js" "chart.js"

echo ""
echo "--- Content checks ---"
check_contains "${OUTDIR}/index.html" "index.html" "Synthetic Example"
check_contains "${OUTDIR}/index.html" "index.html" "</html>"
check_contains "${OUTDIR}/params/Temperature.html" "params/Temperature.html" "Temperature"
check_contains "${OUTDIR}/params/Temperature.html" "params/Temperature.html" "chart-container"
check_contains "${OUTDIR}/sites/NorthDock.html" "sites/NorthDock.html" "NorthDock"
check_contains "${OUTDIR}/sites/NorthDock.html" "sites/NorthDock.html" 'type="module"'

echo ""
echo "--- Asset checks ---"
check_contains "${OUTDIR}/style.css" "style.css" "sidebar-width"
check_contains "${OUTDIR}/style.css" "style.css" ".chart-container"
check_contains "${OUTDIR}/chart.js" "chart.js" "chart-data"

echo ""
echo "--- Data export checks (Hive-partitioned) ---"

# With temporal: ["year"], data is exported as Hive-partitioned parquet:
# data/<group>/<param>/res=<R>/year=<Y>/<file>.parquet
# We check that partitioned directories exist and contain .parquet files.

check_has_parquet_in() {
  local dir="$1"
  local label="$2"
  local count
  count=$(find "$dir" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
  if [ "$count" -gt 0 ]; then
    echo "  ✓ ${label} (${count} parquet files)"
    PASS=$((PASS + 1))
  else
    echo "  ✗ MISSING parquet in: ${label}"
    FAIL=$((FAIL + 1))
  fi
}

for res in 1h 2h 4h 12h 24h; do
  check_has_parquet_in "${OUTDIR}/data/single_param/Temperature/res=${res}" "data/Temperature res=${res}"
  check_has_parquet_in "${OUTDIR}/data/single_param/DO/res=${res}" "data/DO res=${res}"
  check_has_parquet_in "${OUTDIR}/data/single_site/NorthDock/res=${res}" "data/NorthDock res=${res}"
  check_has_parquet_in "${OUTDIR}/data/single_site/SouthDock/res=${res}" "data/SouthDock res=${res}"
done

# Manifest URLs should reference partitioned paths (contain year=)
check_contains "${OUTDIR}/params/Temperature.html" "Temperature manifest has partitioned 1h" 'single_param/Temperature/res=1h/'
check_contains "${OUTDIR}/params/Temperature.html" "Temperature manifest has partitioned 24h" 'single_param/Temperature/res=24h/'

echo ""
echo "--- Navigation checks ---"
check_contains "${OUTDIR}/index.html" "index.html sidebar" 'href="/params/Temperature.html"'
check_contains "${OUTDIR}/index.html" "index.html sidebar" 'href="/sites/NorthDock.html"'

echo ""
echo "--- Temporal bounds checks ---"
# start_time/end_time should be non-zero in the chart manifest
# The manifest contains JSON with "start_time": <epoch_seconds>
if grep -oP '"start_time":\s*\d+' "${OUTDIR}/params/Temperature.html" | grep -v '"start_time": *0' | head -1 > /dev/null 2>&1; then
  echo "  ✓ Temperature manifest has non-zero start_time"
  PASS=$((PASS + 1))
else
  echo "  ✗ Temperature manifest start_time is 0 or missing"
  FAIL=$((FAIL + 1))
fi

echo ""
echo "--- Layout checks ---"
check_contains "${OUTDIR}/index.html" "index.html (default layout)" 'class="hero"'
check_contains "${OUTDIR}/params/Temperature.html" "params/Temperature.html (data layout)" 'class="data-page"'

echo ""
echo "=== Results: ${PASS} passed, ${FAIL} failed ==="

if [ "${FAIL}" -gt 0 ]; then
  echo ""
  echo "FAILED — showing generated files for debugging:"
  for f in $(find "${OUTDIR}" -name '*.html' | head -5); do
    echo ""
    echo "=== HEAD: ${f} ==="
    head -20 "${f}"
  done
  exit 1
fi

# Copy output to /output if mounted (use: ./run-test.sh 201 --output /tmp/sitegen-output)
if [ -d /output ]; then
  cp -r "${OUTDIR}/"* /output/
  echo "✓ Output copied to /output"
fi

echo ""
echo "=== Test 201 PASSED ==="
