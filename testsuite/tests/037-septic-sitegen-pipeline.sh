#!/bin/bash
# TEST: Septic station full pipeline — logfile-ingest → oteljson → temporal-reduce → sitegen
# DESCRIPTION: End-to-end test of the septic example pipeline:
#   1. logfile-ingest factory ingests sample OTelJSON logfile
#   2. temporal-reduce with oteljson:// in_pattern creates dynamic aggregations
#   3. sitegen factory renders Markdown pages with chart data to HTML
#   This validates the complete pipeline that runs on the BeaglePlay.
#
# REQUIRES: testsuite/testdata/septic-sample.json (auto-mounted at /data/)
set -e

echo "=== Test: Septic Station Full Pipeline ==="
echo ""

# testdata/ is auto-mounted at /data/ by run-test.sh
SAMPLE=/data/septic-sample.json
if [ ! -f "$SAMPLE" ]; then
    echo "SKIP: septic-sample.json not found at $SAMPLE"
    exit 0
fi
echo "Using sample: ${SAMPLE} ($(wc -c < "$SAMPLE" | tr -d ' ') bytes)"

OUTDIR=/tmp/septic-site

pond init

# ══════════════════════════════════════════════════════════════════════════════
# Step 1: Set up directory structure
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 1: Create directory structure ---"

pond mkdir -p /etc/system.d
pond mkdir -p /ingest

echo "✓ Directories created"

# ══════════════════════════════════════════════════════════════════════════════
# Step 2: Ingest via logfile-ingest factory (not manual copy)
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 2: Set up logfile-ingest factory ---"

# The sample file simulates a rotated logfile.
# In production, active_pattern is the growing logfile and archived_pattern
# matches rotated copies.  Here we only have the sample as an archived file.
cat > /tmp/ingest.yaml << EOF
archived_pattern: ${SAMPLE}
active_pattern: /data/septicstation-active.json
pond_path: /ingest
EOF

cat /tmp/ingest.yaml

pond mknod logfile-ingest /etc/ingest --config-path /tmp/ingest.yaml

echo "✓ logfile-ingest factory created"

echo ""
echo "--- Step 2b: Run logfile-ingest ---"
RUST_LOG=info pond run /etc/ingest 2>&1 | tee /tmp/ingest-run.log

echo ""
echo "--- Ingested files ---"
pond list '/ingest/*'

echo ""
echo "--- Describe ingested file ---"
pond describe /ingest/septic-sample.json

# ══════════════════════════════════════════════════════════════════════════════
# Step 3: Verify oteljson:// works on ingested data
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 3: Verify oteljson:// reads ingested data ---"
pond cat oteljson:///ingest/septic-sample.json --format=table --sql "SELECT COUNT(*) AS row_count FROM source"
pond describe oteljson:///ingest/septic-sample.json
echo "✓ oteljson:// URL scheme works on ingested data"

# ══════════════════════════════════════════════════════════════════════════════
# Step 4: Create temporal-reduce factory
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 4: Create temporal-reduce (dynamic-dir) ---"

cat > /tmp/reduce.yaml << 'EOF'
entries:
  - name: "pumps"
    factory: "temporal-reduce"
    config:
      in_pattern: "oteljson:///ingest/septic-sample.json"
      out_pattern: "data"
      time_column: "timestamp"
      resolutions: [1h, 6h, 1d]
      aggregations:
        - type: "avg"
          columns:
            - "orenco_RT_Pump1_Amps"
            - "orenco_RT_Pump2_Amps"
            - "orenco_DT_Pump3_Amps"
            - "orenco_DT_Pump4_Amps"
        - type: "min"
          columns:
            - "orenco_RT_Pump1_Amps"
            - "orenco_RT_Pump2_Amps"
            - "orenco_DT_Pump3_Amps"
            - "orenco_DT_Pump4_Amps"
        - type: "max"
          columns:
            - "orenco_RT_Pump1_Amps"
            - "orenco_RT_Pump2_Amps"
            - "orenco_DT_Pump3_Amps"
            - "orenco_DT_Pump4_Amps"

  - name: "environment"
    factory: "temporal-reduce"
    config:
      in_pattern: "oteljson:///ingest/septic-sample.json"
      out_pattern: "data"
      time_column: "timestamp"
      resolutions: [1h, 6h, 1d]
      aggregations:
        - type: "avg"
          columns:
            - "septicstation_temperature"
            - "septicstation_pressure"
            - "septicstation_humidity"
        - type: "min"
          columns:
            - "septicstation_temperature"
            - "septicstation_pressure"
            - "septicstation_humidity"
        - type: "max"
          columns:
            - "septicstation_temperature"
            - "septicstation_pressure"
            - "septicstation_humidity"
EOF

pond mknod dynamic-dir /reduced --config-path /tmp/reduce.yaml
echo "✓ dynamic-dir with pumps + environment created"

echo ""
echo "--- Reduced directory tree ---"
pond list '/reduced/**/*'

echo ""
echo "--- Verify reduced data is queryable ---"
pond cat /reduced/pumps/data/res=1h.series --format=table --sql "SELECT COUNT(*) AS hour_rows FROM source"
pond cat /reduced/environment/data/res=1d.series --format=table --sql "SELECT COUNT(*) AS day_rows FROM source"

# ══════════════════════════════════════════════════════════════════════════════
# Step 5: Load site templates into pond
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 5: Load sitegen pages ---"

pond mkdir /etc/site

cat > /tmp/index.md << 'MD'
---
title: "Septic Station Monitor"
layout: default
---

# Septic Station Monitor

## About

This site monitors an Orenco septic system via Modbus registers and a
BME280 environment sensor.

## Data Sources

| Source | Interval | Metrics |
|--------|----------|---------|
| **BME280** | ~10 min | Temperature, Pressure, Humidity |
| **Modbus (Orenco)** | ~5 min | Pump amps, flow, zone valves |

Use the sidebar to explore the data at different time resolutions.
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
## Septic Station

- [Home](/)
- [Pump Amps](/data/pumps.html)
- [Environment](/data/environment.html)
MD

pond copy host:///tmp/index.md   /etc/site/index.md
pond copy host:///tmp/data.md    /etc/site/data.md
pond copy host:///tmp/sidebar.md /etc/site/sidebar.md
echo "✓ markdown pages loaded"

# ══════════════════════════════════════════════════════════════════════════════
# Step 6: Create sitegen factory
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 6: Create sitegen factory ---"

cat > /tmp/site.yaml << 'YAML'
factory: sitegen

site:
  title: "Septic Station Monitor"
  base_url: "/"

exports:
  - name: "metrics"
    pattern: "/reduced/*/*/*.series"
    temporal: ["year", "month"]

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/etc/site/index.md"
  - name: "data"
    type: static
    slug: "data"
    routes:
      - name: "metric-detail"
        type: template
        slug: "$0"
        page: "/etc/site/data.md"
        export: "metrics"

partials:
  sidebar: "/etc/site/sidebar.md"

static_assets: []
YAML

pond mknod sitegen /etc/site.yaml --config-path /tmp/site.yaml
echo "✓ sitegen factory created"

echo ""
echo "--- Pond tree ---"
pond list '/etc/**'

# ══════════════════════════════════════════════════════════════════════════════
# Step 7: Run sitegen build
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 7: Run sitegen build ---"
rm -rf "${OUTDIR}"
mkdir -p "${OUTDIR}"

pond run /etc/site.yaml build "${OUTDIR}"
echo "✓ sitegen complete"

# ══════════════════════════════════════════════════════════════════════════════
# Step 8: Verification
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "=== VERIFICATION ==="

echo ""
echo "Output directory structure:"
find "${OUTDIR}" -type f | sort

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
check_file "${OUTDIR}/style.css" "style.css"
check_file "${OUTDIR}/chart.js" "chart.js"

# Template routes: data/pumps.html and data/environment.html
# $0 from pattern "/reduced/*/*.series" matches "pumps" and "environment"
check_file "${OUTDIR}/data/pumps.html" "data/pumps.html"
check_file "${OUTDIR}/data/environment.html" "data/environment.html"

echo ""
echo "--- Content checks ---"
check_contains "${OUTDIR}/index.html" "index.html" "Septic Station Monitor"
check_contains "${OUTDIR}/index.html" "index.html" "</html>"

# Data pages should have chart containers
check_contains "${OUTDIR}/data/pumps.html" "pumps page" "chart-container"
check_contains "${OUTDIR}/data/environment.html" "environment page" "chart-container"

echo ""
echo "--- Layout checks ---"
check_contains "${OUTDIR}/index.html" "index.html (default layout)" 'class="hero"'
check_contains "${OUTDIR}/data/pumps.html" "pumps (data layout)" 'class="data-page"'
check_contains "${OUTDIR}/data/environment.html" "environment (data layout)" 'class="data-page"'

echo ""
echo "--- Navigation checks ---"
# Sidebar should contain links to both data pages
check_contains "${OUTDIR}/index.html" "sidebar has pumps link" 'href="/data/pumps.html"'
check_contains "${OUTDIR}/index.html" "sidebar has environment link" 'href="/data/environment.html"'

echo ""
echo "--- Data export checks ---"
# Hive-partitioned parquet under data/metrics/septic/res=<R>/year=<Y>/month=<M>/
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

for res in 1h 6h 1d; do
  check_has_parquet_in "${OUTDIR}/data/pumps/data/res=${res}" "data/pumps/data/res=${res}"
  check_has_parquet_in "${OUTDIR}/data/environment/data/res=${res}" "data/environment/data/res=${res}"
done

echo ""
echo "--- Asset checks ---"
check_contains "${OUTDIR}/style.css" "style.css" "sidebar-width"
check_contains "${OUTDIR}/chart.js" "chart.js" "chart-data"

echo ""
echo "=== Results: ${PASS} passed, ${FAIL} failed ==="

if [ "${FAIL}" -gt 0 ]; then
  echo ""
  echo "FAILED — showing generated files for debugging:"
  for f in $(find "${OUTDIR}" -name '*.html' | head -5); do
    echo ""
    echo "=== ${f} ==="
    head -30 "${f}"
  done
  exit 1
fi

# Copy output to /output if mounted
if [ -d /output ]; then
  cp -r "${OUTDIR}/"* /output/
  echo "✓ Output copied to /output"
fi

echo ""
echo "=== Test 037 PASSED ==="
