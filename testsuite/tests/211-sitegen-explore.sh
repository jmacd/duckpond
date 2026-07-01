#!/bin/bash
# EXPERIMENT: Sitegen data explorer (Stage 0) — explore layout + datasets manifest
# EXPECTED: A static route with `export:` set and the `{{ explore }}` shortcode
#           produces a single site-wide explore page that:
#             - uses the `explore` layout (loads explore.js, not chart.js)
#             - carries a `<script class="datasets">` JSON manifest listing the
#               exported reduced parquet files (url/start_time/end_time)
#             - ships the explore.js + duckdb-shared.js assets
set -e
source check.sh

echo "=== Experiment: Sitegen Data Explorer (Stage 0) ==="

pond init --birthplace test-host

OUTDIR=/tmp/sitegen-explore

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

cat > /tmp/explore.md << 'MD'
---
title: "Data Explorer"
layout: explore
---

# Data Explorer

{{ explore table="reduced" label="Reduced parameters" /}}
MD

pond copy host:///tmp/index.md   /site/index.md
pond copy host:///tmp/explore.md /site/explore.md
echo "✓ Pages loaded"

# ──────────────────────────────────────────────────────────────────────────────
# Step 3: Sitegen factory — explore page is a static route bound to the export
# ──────────────────────────────────────────────────────────────────────────────

echo ""
echo "--- Step 3: Create sitegen factory ---"

cat > /tmp/site.yaml << 'YAML'

site:
  title: "Explorer Test Site"
  base_url: "/"

exports:
  - name: "params"
    pattern: "/reduced/single_param/*/*.series"
    target_points: 1500

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/site/index.md"
  - name: "explore"
    type: static
    slug: "explore"
    page: "/site/explore.md"
    export: "params"

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

# Page + shared assets exist
check '[ -f "${EXPLORE_HTML}" ]'                "explore/index.html"
check '[ -f "${OUTDIR}/explore.js" ]'          "explore.js asset shipped"
check '[ -f "${OUTDIR}/duckdb-shared.js" ]'    "duckdb-shared.js asset shipped"

# Explore page wiring
check_contains "${EXPLORE_HTML}" "explore page" 'id="explore"'
check_contains "${EXPLORE_HTML}" "explore page" 'class="datasets"'
check_contains "${EXPLORE_HTML}" "explore page loads explore.js" 'explore.js'
check_contains "${EXPLORE_HTML}" "explore page dataset table name" '"table":"reduced"'
check_contains "${EXPLORE_HTML}" "explore page dataset label" 'Reduced parameters'

# Manifest references exported parquet files
check_contains "${EXPLORE_HTML}" "datasets manifest has parquet urls" '.parquet'

# Explore page must NOT load chart.js (it is not a chart page)
check_not_contains "${EXPLORE_HTML}" "explore page (no chart.js)" 'chart.js'

# Shared module exports the helpers used by both chart.js and explore.js
check_contains "${OUTDIR}/duckdb-shared.js" "shared init helper" 'export async function initDuckdb'
check_contains "${OUTDIR}/duckdb-shared.js" "shared registry helper" 'export function createFileRegistry'
check_contains "${OUTDIR}/duckdb-shared.js" "shared csv helper" 'export function rowsToCsv'

# chart.js still imports from the shared module (refactor intact)
check_contains "${OUTDIR}/chart.js" "chart.js imports shared module" './duckdb-shared.js'

echo ""
echo "=== Data explorer Stage 0 verified ==="

if [ -d /output ]; then
  cp -r "${OUTDIR}/"* /output/ 2>/dev/null || true
fi
