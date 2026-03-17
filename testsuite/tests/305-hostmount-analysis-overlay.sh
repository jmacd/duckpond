#!/bin/bash
# EXPERIMENT: End-to-end pump cycle analysis + overlay chart rendering
# EXPECTED: Synthetic well depth data flows through:
#   1. csv source -> temporal-reduce dynamic-dir at /reduced
#   2. /reduced -> analysis sql-derived-series dynamic-dir at /analysis
#      (cross-overlay reference: analysis reads from /reduced)
#   3. /analysis -> sitegen export -> HTML with overlay chart + parquet data
#
# Verifies:
#   - Cross-overlay references (analysis reads from /reduced sibling)
#   - Pump cycle detection SQL (42m threshold, cycle ID assignment)
#   - Sitegen overlay_chart shortcode renders correctly
#   - Parquet data files are exported alongside HTML
set -e
source check.sh

echo "=== Experiment: Analysis Overlay Chart Pipeline ==="

HOST_ROOT="/tmp/analysis-overlay-test"
OUTDIR="${OUTPUT:-/output}"

rm -rf "${HOST_ROOT}"
rm -rf "${OUTDIR:?}"/* 2>/dev/null || true
mkdir -p "${HOST_ROOT}" "${OUTDIR}"

# ==============================================================================
# Step 1: Create synthetic well depth CSV with known pump cycles
# ==============================================================================

echo ""
echo "--- Step 1: Create synthetic well depth data ---"

# Generate CSV with 3 clear pump cycles:
#   Cycle 1: rows 1-10  (depth drops from 44 -> 39, recovers to 44)
#   Cycle 2: rows 11-20 (same pattern)
#   Cycle 3: rows 21-30 (same pattern, different month)
# Static depth ~44m, pumping drops below 42m (threshold)

cat > "${HOST_ROOT}/well-depth.csv" << 'CSV'
timestamp,well_depth_value.avg
2024-03-01T10:00:00Z,44.0
2024-03-01T10:01:00Z,43.0
2024-03-01T10:02:00Z,41.5
2024-03-01T10:03:00Z,40.0
2024-03-01T10:04:00Z,39.5
2024-03-01T10:05:00Z,40.0
2024-03-01T10:06:00Z,41.0
2024-03-01T10:07:00Z,42.5
2024-03-01T10:08:00Z,43.5
2024-03-01T10:09:00Z,44.0
2024-03-01T12:00:00Z,44.0
2024-03-01T12:01:00Z,43.0
2024-03-01T12:02:00Z,41.0
2024-03-01T12:03:00Z,39.8
2024-03-01T12:04:00Z,39.5
2024-03-01T12:05:00Z,40.5
2024-03-01T12:06:00Z,41.5
2024-03-01T12:07:00Z,42.5
2024-03-01T12:08:00Z,43.5
2024-03-01T12:09:00Z,44.0
2024-07-15T08:00:00Z,44.0
2024-07-15T08:01:00Z,43.0
2024-07-15T08:02:00Z,41.0
2024-07-15T08:03:00Z,39.5
2024-07-15T08:04:00Z,39.0
2024-07-15T08:05:00Z,40.0
2024-07-15T08:06:00Z,41.5
2024-07-15T08:07:00Z,42.5
2024-07-15T08:08:00Z,43.5
2024-07-15T08:09:00Z,44.0
CSV

echo "Well depth CSV: $(wc -l < "${HOST_ROOT}/well-depth.csv") lines (3 pump cycles)"

# ==============================================================================
# Step 2: Create reduce dynamic-dir config (CSV -> reduced series)
# ==============================================================================

echo ""
echo "--- Step 2: Create reduce config ---"

# Simple pass-through: reads the CSV as a "reduced" series
cat > "${HOST_ROOT}/reduce.yaml" << 'YAML'
entries:
  - name: "well-depth"
    factory: "dynamic-dir"
    config:
      entries:
        - name: "data"
          factory: "dynamic-dir"
          config:
            entries:
              - name: "res=1m.series"
                factory: "sql-derived-series"
                config:
                  patterns:
                    source: "csv:///well-depth.csv"
                  query: >-
                    SELECT * FROM source ORDER BY timestamp
YAML

echo "reduce.yaml created"

# ==============================================================================
# Step 3: Create analysis dynamic-dir config (reads from /reduced)
# ==============================================================================

echo ""
echo "--- Step 3: Create analysis config (cross-overlay reference) ---"

cat > "${HOST_ROOT}/analysis.yaml" << 'YAML'
entries:
  - name: "pump-cycles"
    factory: "sql-derived-series"
    config:
      patterns:
        source: "series:///reduced/well-depth/data/res=1m.series"
      query: >-
        WITH raw AS (
          SELECT timestamp, "well_depth_value.avg" as depth
          FROM source WHERE "well_depth_value.avg" IS NOT NULL
        ),
        classified AS (
          SELECT timestamp, depth,
            CASE WHEN depth < 42.0 THEN true ELSE false END as is_draw,
            LAG(CASE WHEN depth < 42.0 THEN true ELSE false END)
              OVER (ORDER BY timestamp) as was_draw
          FROM raw
        ),
        with_pump_start AS (
          SELECT timestamp, depth, is_draw,
            CASE WHEN is_draw = true AND (was_draw = false OR was_draw IS NULL)
              THEN 1 ELSE 0
            END as pump_start
          FROM classified
        ),
        with_event_id AS (
          SELECT timestamp, depth, is_draw,
            SUM(pump_start) OVER (ORDER BY timestamp ROWS UNBOUNDED PRECEDING)
              as pump_event_id
          FROM with_pump_start
        ),
        filtered AS (
          SELECT * FROM with_event_id WHERE pump_event_id > 0
        ),
        with_meta AS (
          SELECT timestamp, depth, is_draw, pump_event_id,
            FIRST_VALUE(timestamp) OVER (
              PARTITION BY pump_event_id ORDER BY timestamp
            ) as event_start,
            FIRST_VALUE(depth) OVER (
              PARTITION BY pump_event_id ORDER BY timestamp
            ) as depth_at_start
          FROM filtered
        )
        SELECT
          timestamp,
          CAST(pump_event_id AS INT) as pump_event_id,
          CAST(EXTRACT(MONTH FROM event_start) AS INT) as month,
          EXTRACT(EPOCH FROM (timestamp - event_start)) as elapsed_s,
          depth_at_start - depth as drawdown,
          depth,
          CASE WHEN is_draw THEN 'draw' ELSE 'recovery' END as phase
        FROM with_meta
        WHERE EXTRACT(EPOCH FROM (timestamp - event_start)) <= 7200
        ORDER BY timestamp

  - name: "cycle-summary"
    factory: "sql-derived-series"
    config:
      patterns:
        source: "series:///reduced/well-depth/data/res=1m.series"
      query: >-
        WITH raw AS (
          SELECT timestamp, "well_depth_value.avg" as depth
          FROM source WHERE "well_depth_value.avg" IS NOT NULL
        ),
        classified AS (
          SELECT timestamp, depth,
            CASE WHEN depth < 42.0 THEN true ELSE false END as is_draw,
            LAG(CASE WHEN depth < 42.0 THEN true ELSE false END)
              OVER (ORDER BY timestamp) as was_draw
          FROM raw
        ),
        with_pump_start AS (
          SELECT timestamp, depth, is_draw,
            CASE WHEN is_draw = true AND (was_draw = false OR was_draw IS NULL)
              THEN 1 ELSE 0
            END as pump_start
          FROM classified
        ),
        with_event_id AS (
          SELECT timestamp, depth, is_draw,
            SUM(pump_start) OVER (ORDER BY timestamp ROWS UNBOUNDED PRECEDING)
              as pump_event_id
          FROM with_pump_start
        ),
        filtered AS (
          SELECT * FROM with_event_id WHERE pump_event_id > 0
        ),
        with_meta AS (
          SELECT timestamp, depth, is_draw, pump_event_id,
            FIRST_VALUE(timestamp) OVER (
              PARTITION BY pump_event_id ORDER BY timestamp
            ) as event_start,
            FIRST_VALUE(depth) OVER (
              PARTITION BY pump_event_id ORDER BY timestamp
            ) as depth_at_start
          FROM filtered
        ),
        capped AS (
          SELECT * FROM with_meta
          WHERE EXTRACT(EPOCH FROM (timestamp - event_start)) <= 7200
        )
        SELECT
          CAST(pump_event_id AS INT) as pump_event_id,
          MIN(timestamp) as timestamp,
          CAST(EXTRACT(MONTH FROM MIN(event_start)) AS INT) as month,
          SUM(CASE WHEN is_draw THEN 1 ELSE 0 END) * 60.0 as draw_duration_s,
          SUM(CASE WHEN NOT is_draw THEN 1 ELSE 0 END) * 60.0 as recovery_duration_s,
          COUNT(*) * 60.0 as total_duration_s,
          MAX(depth_at_start - depth) as max_drawdown,
          CAST(COUNT(*) AS INT) as num_points
        FROM capped
        GROUP BY pump_event_id
        HAVING COUNT(*) >= 3
        ORDER BY MIN(timestamp)
YAML

echo "analysis.yaml created (reads from series:///reduced/...)"

# ==============================================================================
# Step 4: Verify cross-overlay reference works
# ==============================================================================

echo ""
echo "--- Step 4: Verify cross-overlay data pipeline ---"

# First verify the reduce overlay works alone
REDUCE_OUT=$(pond cat -d "${HOST_ROOT}" \
  --hostmount "/reduced=host+dyndir:///reduce.yaml" \
  "host+file:///reduced/well-depth/data/res=1m.series" \
  --format=table --sql "SELECT COUNT(*) as cnt FROM source" 2>&1)
echo "Reduce output: ${REDUCE_OUT}"
check 'echo "${REDUCE_OUT}" | grep -q "30"' "reduce overlay has 30 rows"

# Now verify cross-overlay: analysis reads from /reduced
ANALYSIS_OUT=$(pond cat -d "${HOST_ROOT}" \
  --hostmount "/reduced=host+dyndir:///reduce.yaml" \
  --hostmount "/analysis=host+dyndir:///analysis.yaml" \
  "host+file:///analysis/pump-cycles" \
  --format=table --sql "SELECT COUNT(DISTINCT pump_event_id) as cycles FROM source" 2>&1)
echo "Analysis output: ${ANALYSIS_OUT}"
check 'echo "${ANALYSIS_OUT}" | grep -q "3"' "analysis detects 3 pump cycles"

# Verify cycle summary
SUMMARY_OUT=$(pond cat -d "${HOST_ROOT}" \
  --hostmount "/reduced=host+dyndir:///reduce.yaml" \
  --hostmount "/analysis=host+dyndir:///analysis.yaml" \
  "host+file:///analysis/cycle-summary" \
  --format=table --sql "SELECT pump_event_id, month, num_points FROM source ORDER BY pump_event_id" 2>&1)
echo "Summary output: ${SUMMARY_OUT}"
check 'echo "${SUMMARY_OUT}" | grep -q "pump_event_id"' "cycle summary has expected columns"

# Verify months: cycles 1,2 in March (3), cycle 3 in July (7)
check 'echo "${SUMMARY_OUT}" | grep -q "| 3 "' "March cycles detected (month=3)"
check 'echo "${SUMMARY_OUT}" | grep -q "| 7 "' "July cycle detected (month=7)"

# ==============================================================================
# Step 5: Verify list --long shows entry types
# ==============================================================================

echo ""
echo "--- Step 5: Verify list --long entry type display ---"

LIST_OUT=$(pond list -d "${HOST_ROOT}" \
  --hostmount "/analysis=host+dyndir:///analysis.yaml" \
  --hostmount "/reduced=host+dyndir:///reduce.yaml" \
  "host+file:///analysis/*" --long 2>&1)
echo "List output: ${LIST_OUT}"
check 'echo "${LIST_OUT}" | grep -q "table:dynamic"' "list --long shows table:dynamic"
check 'echo "${LIST_OUT}" | grep -q "pump-cycles"' "list shows pump-cycles entry"
check 'echo "${LIST_OUT}" | grep -q "cycle-summary"' "list shows cycle-summary entry"

# ==============================================================================
# Step 6: Verify pond cat --explain
# ==============================================================================

echo ""
echo "--- Step 6: Verify pond cat --explain ---"

EXPLAIN_OUT=$(pond cat -d "${HOST_ROOT}" \
  --hostmount "/reduced=host+dyndir:///reduce.yaml" \
  --hostmount "/analysis=host+dyndir:///analysis.yaml" \
  "host+file:///analysis/pump-cycles" --explain 2>&1)
echo "Explain output: ${EXPLAIN_OUT}"
check 'echo "${EXPLAIN_OUT}" | grep -q "table:dynamic"' "explain shows entry type"
check 'echo "${EXPLAIN_OUT}" | grep -q "pump_event_id"' "explain shows schema"
check 'echo "${EXPLAIN_OUT}" | grep -q "elapsed_s"' "explain shows elapsed_s field"

# ==============================================================================
# Step 7: Verify pond cat auto-format (TTY detection)
# ==============================================================================

echo ""
echo "--- Step 7: Verify auto-format behavior ---"

# When piped (not TTY), auto should produce raw parquet
RAW_OUT=$(pond cat -d "${HOST_ROOT}" \
  --hostmount "/reduced=host+dyndir:///reduce.yaml" \
  --hostmount "/analysis=host+dyndir:///analysis.yaml" \
  "host+file:///analysis/pump-cycles" \
  --sql "SELECT pump_event_id FROM source LIMIT 1" 2>/dev/null | file -)
echo "Auto-format pipe output: ${RAW_OUT}"
check 'echo "${RAW_OUT}" | grep -qi "parquet\|data"' "auto-format produces parquet when piped"

# ==============================================================================
# Step 8: Run sitegen with analysis overlay
# ==============================================================================

echo ""
echo "--- Step 8: Run sitegen with analysis overlay ---"

# Create minimal site config
mkdir -p "${HOST_ROOT}/site"

cat > "${HOST_ROOT}/site.yaml" << 'YAML'
factory: sitegen

site:
  title: "Analysis Test"
  base_url: "/"

content: []

exports:
  - name: "analysis"
    pattern: "/analysis/*"

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/site/index.md"
    routes:
      - name: "analysis"
        type: static
        slug: "analysis"
        routes:
          - name: "analysis-detail"
            type: template
            slug: "$0"
            page: "/site/analysis.md"
            export: "analysis"

partials: {}
static: []
YAML

cat > "${HOST_ROOT}/site/index.md" << 'MD'
---
title: Home
layout: default
---
Home page
MD

cat > "${HOST_ROOT}/site/analysis.md" << 'MD'
---
title: "{{ $0 }}"
layout: data
---

# {{ $0 }}

{{ overlay_chart /}}
MD

echo "Site config and templates created"

# Run sitegen
pond run -d "${HOST_ROOT}" \
  --hostmount "/reduced=host+dyndir:///reduce.yaml" \
  --hostmount "/analysis=host+dyndir:///analysis.yaml" \
  host+sitegen:///site.yaml \
  build "${OUTDIR}"

echo "sitegen complete"

# ==============================================================================
# Verification
# ==============================================================================

echo ""
echo "--- Verification ---"

# HTML pages generated
check 'test -f "${OUTDIR}/analysis/pump-cycles.html"' "pump-cycles.html exists"
check 'test -f "${OUTDIR}/analysis/cycle-summary.html"' "cycle-summary.html exists"

# Overlay JS asset
check 'test -f "${OUTDIR}/overlay.js"' "overlay.js asset generated"

# HTML contains overlay chart container
check_contains "${OUTDIR}/analysis/pump-cycles.html" \
  "pump-cycles has overlay chart container" \
  'id="overlay-chart"'

# HTML contains overlay data manifest
check_contains "${OUTDIR}/analysis/pump-cycles.html" \
  "pump-cycles has overlay data manifest" \
  'class="overlay-data"'

# HTML includes overlay.js script
check_contains "${OUTDIR}/analysis/pump-cycles.html" \
  "pump-cycles loads overlay.js" \
  'overlay.js'

# Parquet data files exported
PARQUET_COUNT=$(find "${OUTDIR}" -name "*.parquet" | wc -l)
echo "  Parquet files: ${PARQUET_COUNT}"
check '[ "${PARQUET_COUNT}" -gt 0 ]' "parquet data files exported"

# Manifest references existing parquet files
MANIFEST_FILE=$(grep -o '/data/pump-cycles/[^"]*\.parquet' \
  "${OUTDIR}/analysis/pump-cycles.html" | head -1)
echo "  First manifest file: ${MANIFEST_FILE}"
if [ -n "${MANIFEST_FILE}" ]; then
  check "test -f '${OUTDIR}${MANIFEST_FILE}'" "manifest parquet file exists on disk"
fi

echo ""
echo "Generated files:"
find "${OUTDIR}" -type f | sort | sed 's|^|  |'

check_finish
