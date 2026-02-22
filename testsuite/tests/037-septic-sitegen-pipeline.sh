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
source check.sh

echo "=== Experiment: Septic Station Full Pipeline ==="
echo ""

# testdata/ is auto-mounted at /data/ by run-test.sh
# Prefer the full sample if available (via --data), fall back to testdata excerpt
if [ -f /data/septicstation-sample.json ]; then
    SAMPLE=/data/septicstation-sample.json
elif [ -f /data/septic-sample.json ]; then
    SAMPLE=/data/septic-sample.json
else
    echo "SKIP: no septic sample data found at /data/"
    exit 0
fi
echo "Using sample: ${SAMPLE} ($(wc -c < "$SAMPLE" | tr -d ' ') bytes)"

# Derive the ingested filename (basename of the sample file)
INGESTED_NAME=$(basename "$SAMPLE")
INGESTED_PATH="/ingest/${INGESTED_NAME}"

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
pond describe "${INGESTED_PATH}"

# ══════════════════════════════════════════════════════════════════════════════
# Step 3: Verify oteljson:// works on ingested data
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 3: Verify oteljson:// reads ingested data ---"
pond cat "oteljson://${INGESTED_PATH}" --format=table --sql "SELECT COUNT(*) AS row_count FROM source"
pond describe "oteljson://${INGESTED_PATH}"
echo "✓ oteljson:// URL scheme works on ingested data"

# ══════════════════════════════════════════════════════════════════════════════
# Step 4: Create temporal-reduce factory
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 4: Create temporal-reduce (dynamic-dir) ---"

cat > /tmp/reduce.yaml << EOF
entries:
  - name: "pumps"
    factory: "temporal-reduce"
    config:
      in_pattern: "oteljson://${INGESTED_PATH}"
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

  - name: "cycle-times"
    factory: "temporal-reduce"
    config:
      in_pattern: "oteljson://${INGESTED_PATH}"
      out_pattern: "data"
      time_column: "timestamp"
      resolutions: [1h, 6h, 1d]
      aggregations:
        - type: "avg"
          columns:
            - "orenco_RT_Pump1_CT"
            - "orenco_RT_Pump2_CT"
            - "orenco_DT_Pump3_CT"
            - "orenco_DT_Pump4_CT"
        - type: "min"
          columns:
            - "orenco_RT_Pump1_CT"
            - "orenco_RT_Pump2_CT"
            - "orenco_DT_Pump3_CT"
            - "orenco_DT_Pump4_CT"
        - type: "max"
          columns:
            - "orenco_RT_Pump1_CT"
            - "orenco_RT_Pump2_CT"
            - "orenco_DT_Pump3_CT"
            - "orenco_DT_Pump4_CT"

  - name: "pump-modes"
    factory: "temporal-reduce"
    config:
      in_pattern: "oteljson://${INGESTED_PATH}"
      out_pattern: "data"
      time_column: "timestamp"
      resolutions: [1h, 6h, 1d]
      aggregations:
        - type: "avg"
          columns:
            - "orenco_RT_PumpMode"
            - "orenco_DT_PumpMode"
        - type: "min"
          columns:
            - "orenco_RT_PumpMode"
            - "orenco_DT_PumpMode"
        - type: "max"
          columns:
            - "orenco_RT_PumpMode"
            - "orenco_DT_PumpMode"

  - name: "flow-totals"
    factory: "temporal-reduce"
    config:
      in_pattern: "oteljson://${INGESTED_PATH}"
      out_pattern: "data"
      time_column: "timestamp"
      resolutions: [1h, 6h, 1d]
      aggregations:
        - type: "avg"
          columns:
            - "orenco_RT_TotalCount"
            - "orenco_RT_TotalFlow"
            - "orenco_RT_TotalTime"
            - "orenco_DT_TotalCount"
            - "orenco_DT_TotalFlow"
            - "orenco_DT_TotalTime"
        - type: "min"
          columns:
            - "orenco_RT_TotalCount"
            - "orenco_RT_TotalFlow"
            - "orenco_RT_TotalTime"
            - "orenco_DT_TotalCount"
            - "orenco_DT_TotalFlow"
            - "orenco_DT_TotalTime"
        - type: "max"
          columns:
            - "orenco_RT_TotalCount"
            - "orenco_RT_TotalFlow"
            - "orenco_RT_TotalTime"
            - "orenco_DT_TotalCount"
            - "orenco_DT_TotalFlow"
            - "orenco_DT_TotalTime"

  - name: "dose-zones"
    factory: "temporal-reduce"
    config:
      in_pattern: "oteljson://${INGESTED_PATH}"
      out_pattern: "data"
      time_column: "timestamp"
      resolutions: [1h, 6h, 1d]
      aggregations:
        - type: "avg"
          columns:
            - "orenco_Zone1_CountTday"
            - "orenco_Zone1_TimeTday"
            - "orenco_Zone2_CountTday"
            - "orenco_Zone2_TimeTday"
            - "orenco_Zone3_CountTday"
            - "orenco_Zone3_TimeTday"
        - type: "min"
          columns:
            - "orenco_Zone1_CountTday"
            - "orenco_Zone1_TimeTday"
            - "orenco_Zone2_CountTday"
            - "orenco_Zone2_TimeTday"
            - "orenco_Zone3_CountTday"
            - "orenco_Zone3_TimeTday"
        - type: "max"
          columns:
            - "orenco_Zone1_CountTday"
            - "orenco_Zone1_TimeTday"
            - "orenco_Zone2_CountTday"
            - "orenco_Zone2_TimeTday"
            - "orenco_Zone3_CountTday"
            - "orenco_Zone3_TimeTday"

  - name: "environment"
    factory: "temporal-reduce"
    config:
      in_pattern: "oteljson://${INGESTED_PATH}"
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
echo "✓ dynamic-dir with 6 groups created"

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

pond mkdir /site

cat > /tmp/index.md << 'MD'
---
title: "Septic Station Monitor"
layout: default
---

# Septic Station Monitor

## About

This site monitors an Orenco septic system via Modbus registers and a
BME280 environment sensor.

## Data Groups

| Group | Metrics | Description |
|-------|---------|-------------|
| [**Pump Amps**](/data/pumps.html) | RT Pump 1–2, DT Pump 3–4 | Motor current draw |
| [**Cycle Times**](/data/cycle-times.html) | RT Pump 1–2 CT, DT Pump 3–4 CT | Cumulative pump cycle counts |
| [**Pump Modes**](/data/pump-modes.html) | RT PumpMode, DT PumpMode | Operating mode registers |
| [**Flow Totals**](/data/flow-totals.html) | RT/DT TotalCount, TotalFlow, TotalTime | Cumulative flow counters |
| [**Dose Zones**](/data/dose-zones.html) | Zone 1–3 CountTday, TimeTday | Daily zone valve activity |
| [**Environment**](/data/environment.html) | Temperature, Pressure, Humidity | BME280 sensor on BeaglePlay |

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

### Pumps
- [Pump Amps](/data/pumps.html)
- [Cycle Times](/data/cycle-times.html)
- [Pump Modes](/data/pump-modes.html)

### Flow & Zones
- [Flow Totals](/data/flow-totals.html)
- [Dose Zones](/data/dose-zones.html)

### Environment
- [BME280](/data/environment.html)
MD

pond copy host:///tmp/index.md   /site/index.md
pond copy host:///tmp/data.md    /site/data.md
pond copy host:///tmp/sidebar.md /site/sidebar.md
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

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/site/index.md"
  - name: "data"
    type: static
    slug: "data"
    routes:
      - name: "metric-detail"
        type: template
        slug: "$0"
        page: "/site/data.md"
        export: "metrics"

partials:
  sidebar: "/site/sidebar.md"

static_assets: []
YAML

pond mknod sitegen /site.yaml --config-path /tmp/site.yaml
echo "✓ sitegen factory created"

echo ""
echo "--- Pond tree ---"
pond list '/site/**'

# ══════════════════════════════════════════════════════════════════════════════
# Step 7: Run sitegen build
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 7: Run sitegen build ---"
rm -rf "${OUTDIR}"
mkdir -p "${OUTDIR}"

pond run /site.yaml build "${OUTDIR}"
echo "✓ sitegen complete"

# ══════════════════════════════════════════════════════════════════════════════
# Step 8: Verification
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "=== VERIFICATION ==="

echo ""
echo "Output directory structure:"
find "${OUTDIR}" -type f | sort

check_has_parquet_in() {
  local dir="$1"
  local label="$2"
  local count
  count=$(find "$dir" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
  if [ "$count" -gt 0 ]; then
    echo "  ✓ ${label} (${count} parquet files)"
    _CHECK_PASS=$((_CHECK_PASS + 1))
  else
    echo "  ✗ MISSING parquet in: ${label}"
    _CHECK_FAIL=$((_CHECK_FAIL + 1))
  fi
}

echo ""
echo "--- File existence ---"
check '[ -f "${OUTDIR}/index.html" ]'  "index.html"
check '[ -f "${OUTDIR}/style.css" ]'   "style.css"
check '[ -f "${OUTDIR}/chart.js" ]'    "chart.js"

# Template routes: one page per group
# $0 from pattern "/reduced/*/*/*/*.series" matches group names
check '[ -f "${OUTDIR}/data/pumps.html" ]'        "data/pumps.html"
check '[ -f "${OUTDIR}/data/cycle-times.html" ]'   "data/cycle-times.html"
check '[ -f "${OUTDIR}/data/pump-modes.html" ]'    "data/pump-modes.html"
check '[ -f "${OUTDIR}/data/flow-totals.html" ]'   "data/flow-totals.html"
check '[ -f "${OUTDIR}/data/dose-zones.html" ]'    "data/dose-zones.html"
check '[ -f "${OUTDIR}/data/environment.html" ]'   "data/environment.html"

echo ""
echo "--- Content checks ---"
check_contains "${OUTDIR}/index.html" "index.html" "Septic Station Monitor"
check_contains "${OUTDIR}/index.html" "index.html" "</html>"

# Data pages should have chart containers
check_contains "${OUTDIR}/data/pumps.html" "pumps page" "chart-container"
check_contains "${OUTDIR}/data/cycle-times.html" "cycle-times page" "chart-container"
check_contains "${OUTDIR}/data/pump-modes.html" "pump-modes page" "chart-container"
check_contains "${OUTDIR}/data/flow-totals.html" "flow-totals page" "chart-container"
check_contains "${OUTDIR}/data/dose-zones.html" "dose-zones page" "chart-container"
check_contains "${OUTDIR}/data/environment.html" "environment page" "chart-container"

echo ""
echo "--- Layout checks ---"
check_contains "${OUTDIR}/index.html" "index.html (default layout)" 'class="hero"'
check_contains "${OUTDIR}/data/pumps.html" "pumps (data layout)" 'class="data-page"'
check_contains "${OUTDIR}/data/cycle-times.html" "cycle-times (data layout)" 'class="data-page"'
check_contains "${OUTDIR}/data/pump-modes.html" "pump-modes (data layout)" 'class="data-page"'
check_contains "${OUTDIR}/data/flow-totals.html" "flow-totals (data layout)" 'class="data-page"'
check_contains "${OUTDIR}/data/dose-zones.html" "dose-zones (data layout)" 'class="data-page"'
check_contains "${OUTDIR}/data/environment.html" "environment (data layout)" 'class="data-page"'

echo ""
echo "--- Navigation checks ---"
# Sidebar should contain links to all data pages
check_contains "${OUTDIR}/index.html" "sidebar has pumps link" 'href="/data/pumps.html"'
check_contains "${OUTDIR}/index.html" "sidebar has cycle-times link" 'href="/data/cycle-times.html"'
check_contains "${OUTDIR}/index.html" "sidebar has pump-modes link" 'href="/data/pump-modes.html"'
check_contains "${OUTDIR}/index.html" "sidebar has flow-totals link" 'href="/data/flow-totals.html"'
check_contains "${OUTDIR}/index.html" "sidebar has dose-zones link" 'href="/data/dose-zones.html"'
check_contains "${OUTDIR}/index.html" "sidebar has environment link" 'href="/data/environment.html"'

echo ""
echo "--- Data export checks ---"
# Hive-partitioned parquet under data/metrics/septic/res=<R>/year=<Y>/month=<M>/

for res in 1h 6h 1d; do
  check_has_parquet_in "${OUTDIR}/data/pumps/data/res=${res}" "data/pumps/data/res=${res}"
  check_has_parquet_in "${OUTDIR}/data/cycle-times/data/res=${res}" "data/cycle-times/data/res=${res}"
  check_has_parquet_in "${OUTDIR}/data/pump-modes/data/res=${res}" "data/pump-modes/data/res=${res}"
  check_has_parquet_in "${OUTDIR}/data/flow-totals/data/res=${res}" "data/flow-totals/data/res=${res}"
  check_has_parquet_in "${OUTDIR}/data/dose-zones/data/res=${res}" "data/dose-zones/data/res=${res}"
  check_has_parquet_in "${OUTDIR}/data/environment/data/res=${res}" "data/environment/data/res=${res}"
done

echo ""
echo "--- Asset checks ---"
check_contains "${OUTDIR}/style.css" "style.css" "sidebar-width"
check_contains "${OUTDIR}/chart.js" "chart.js" "chart-data"

# Copy output to /output if mounted
if [ -d /output ]; then
  cp -r "${OUTDIR}/"* /output/
fi

check_finish
