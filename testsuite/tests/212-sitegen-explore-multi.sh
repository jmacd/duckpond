#!/bin/bash
# EXPERIMENT: Sitegen data explorer (Stage 1) — multiple selectable datasets
# EXPECTED: With a `explore:` config block naming several export stages, the
#           `{{ explore }}` shortcode emits a multi-dataset manifest:
#             - one entry per configured dataset (combined / singled / reduced)
#             - each carries its export stage's unioned `columns` list
#             - the explorer page works without an `export:` on its own route
set -e
source check.sh

echo "=== Experiment: Sitegen Data Explorer (Stage 1, multi-dataset) ==="

pond init --birthplace test-host

OUTDIR=/tmp/sitegen-explore-multi

# ──────────────────────────────────────────────────────────────────────────────
# Step 1: Minimal synthetic data pipeline (sensors -> combined -> singled -> reduced)
# ──────────────────────────────────────────────────────────────────────────────

echo ""
echo "--- Step 1: Create minimal data ---"

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
      resolutions: ["1h", "4h", "24h"]
      aggregations:
        - type: "avg"
          columns: ["*"]
        - type: "min"
          columns: ["*"]
        - type: "max"
          columns: ["*"]
YAML

pond mknod dynamic-dir /sensors  --config-path /tmp/sensors.yaml
pond mknod dynamic-dir /combined --config-path /tmp/combined.yaml
pond mknod dynamic-dir /singled  --config-path /tmp/single.yaml
pond mknod dynamic-dir /reduced  --config-path /tmp/reduce.yaml
echo "✓ Data pipeline created"

# ──────────────────────────────────────────────────────────────────────────────
# Step 2: Markdown pages (home + explore)
# ──────────────────────────────────────────────────────────────────────────────

echo ""
echo "--- Step 2: Create pages ---"

pond mkdir -p /site

cat > /tmp/index.md << 'MD'
---
title: "Explorer Test"
layout: default
---

# Explorer Test

- [Explore the data](/explore/)
MD

# Note: no explore args; the multi-dataset manifest comes from `explore:` config.
cat > /tmp/explore.md << 'MD'
---
title: "Data Explorer"
layout: explore
---

# Data Explorer

{{ explore /}}
MD

# A chart page bound to an export stage; with `explore.url` set it should emit
# the "Explore this data" cross-link (data-explore-url) for chart.js to use.
cat > /tmp/chart.md << 'MD'
---
title: "Chart Page"
layout: data
---

# Chart Page

{{ chart /}}
MD

pond copy host:///tmp/index.md   /site/index.md
pond copy host:///tmp/explore.md /site/explore.md
pond copy host:///tmp/chart.md   /site/chart.md
echo "✓ Pages loaded"

# ──────────────────────────────────────────────────────────────────────────────
# Step 3: Sitegen factory — three export stages, three explorer datasets
# ──────────────────────────────────────────────────────────────────────────────

echo ""
echo "--- Step 3: Create sitegen factory ---"

cat > /tmp/site.yaml << 'YAML'

site:
  title: "Explorer Test Site"
  base_url: "/"

exports:
  - name: "reduced_params"
    pattern: "/reduced/single_param/*/*.series"
    target_points: 1500
  - name: "singled_params"
    pattern: "/singled/*"
    target_points: 1500
  - name: "combined_join"
    pattern: "/combined/*"
    target_points: 1500

explore:
  url: "/explore/"
  datasets:
    - export: "reduced_params"
      table: "reduced"
      label: "Reduced parameters"
    - export: "singled_params"
      table: "singled"
      label: "Singled series"
    - export: "combined_join"
      table: "combined"
      label: "Combined join"

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/site/index.md"
  - name: "explore"
    type: static
    slug: "explore"
    page: "/site/explore.md"
  - name: "chart"
    type: static
    slug: "chart"
    page: "/site/chart.md"
    export: "reduced_params"

YAML

pond mknod sitegen /site.yaml --config-path /tmp/site.yaml
echo "✓ sitegen factory created"

# ──────────────────────────────────────────────────────────────────────────────
# Step 4: Build
# ──────────────────────────────────────────────────────────────────────────────

echo ""
echo "--- Step 4: Run sitegen build ---"
rm -rf "${OUTDIR}"
mkdir -p "${OUTDIR}"
pond run /site.yaml build "${OUTDIR}"
echo "✓ sitegen complete"

echo ""
echo "Output directory structure:"
find "${OUTDIR}" -type f | sort

# ──────────────────────────────────────────────────────────────────────────────
# Step 5: Verification
# ──────────────────────────────────────────────────────────────────────────────

echo ""
echo "=== VERIFICATION ==="

EXPLORE_HTML="${OUTDIR}/explore/index.html"

check '[ -f "${EXPLORE_HTML}" ]'             "explore/index.html"
check '[ -f "${OUTDIR}/explore.js" ]'        "explore.js asset shipped"
check '[ -f "${OUTDIR}/duckdb-shared.js" ]'  "duckdb-shared.js asset shipped"

check_contains "${EXPLORE_HTML}" "explore container" 'id="explore"'
check_contains "${EXPLORE_HTML}" "datasets manifest"  'class="datasets"'

# All three configured datasets are present in the manifest.
check_contains "${EXPLORE_HTML}" "reduced dataset"  '"table":"reduced"'
check_contains "${EXPLORE_HTML}" "singled dataset"  '"table":"singled"'
check_contains "${EXPLORE_HTML}" "combined dataset" '"table":"combined"'
check_contains "${EXPLORE_HTML}" "reduced label"    'Reduced parameters'
check_contains "${EXPLORE_HTML}" "singled label"    'Singled series'
check_contains "${EXPLORE_HTML}" "combined label"   'Combined join'

# Each dataset carries an export-stage column list for the up-front schema view.
# (The timestamp column is intentionally excluded from the manifest schema; the
# explorer's DESCRIBE refines the display with the full typed schema at runtime.)
check_contains "${EXPLORE_HTML}" "manifest carries non-empty columns" '"columns":["'

# Manifest references exported parquet files.
check_contains "${EXPLORE_HTML}" "datasets manifest has parquet urls" '.parquet'

# Explore page must NOT load chart.js (it is not a chart page).
check_not_contains "${EXPLORE_HTML}" "explore page (no chart.js)" 'chart.js'

# Stage 2 ergonomics ship in the explorer assets (client-side features).
check_contains "${OUTDIR}/explore.js" "column browser" 'renderColumnChips'
check_contains "${OUTDIR}/explore.js" "canned example queries" 'renderExamples'
check_contains "${OUTDIR}/explore.js" "result paging" 'renderPage'
check_contains "${OUTDIR}/explore.js" "json download button" 'Download JSON'
check_contains "${OUTDIR}/explore.js" "json serializer import" 'rowsToJson'
check_contains "${OUTDIR}/explore.js" "parquet download button" 'Download Parquet'
check_contains "${OUTDIR}/explore.js" "parquet copy writer" 'FORMAT PARQUET'
check_contains "${OUTDIR}/explore.js" "vega chart view" 'renderChart'
check_contains "${OUTDIR}/explore.js" "vega lazy import" 'vega-bundle.mjs'
check_contains "${OUTDIR}/explore.js" "vega-lite spec" 'vega-lite/v5'
check '[ -f "${OUTDIR}/vendor/vega-bundle.mjs" ]' "vega bundle vendored"
check_contains "${OUTDIR}/duckdb-shared.js" "shared json helper" 'export function rowsToJson'

# Stage 2 cross-link: chart pages emit an "Explore this data" link when
# `explore.url` is configured, and the explorer accepts the handed-over files.
CHART_HTML="${OUTDIR}/chart/index.html"
check '[ -f "${CHART_HTML}" ]'                "chart/index.html"
check_contains "${CHART_HTML}" "chart cross-link url" 'data-explore-url="/explore/"'
check_contains "${OUTDIR}/chart.js" "chart explore button" 'Explore this data'
check_contains "${OUTDIR}/chart.js" "chart reads explore url" 'exploreUrl'
check_contains "${OUTDIR}/explore.js" "explorer accepts handed-over files" 'chart_data'
check_contains "${OUTDIR}/explore.js" "explorer full-screen toggle" 'explore-fullscreen'

echo ""
echo "=== Data explorer Stage 1 (multi-dataset) verified ==="

if [ -d /output ]; then
  cp -r "${OUTDIR}/"* /output/ 2>/dev/null || true
fi
