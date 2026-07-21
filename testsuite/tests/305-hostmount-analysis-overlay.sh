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
  - name: "drawdown-by-month"
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
        draw AS (
          SELECT
            date_trunc('month', event_start) as month_start,
            CAST(EXTRACT(EPOCH FROM (timestamp - event_start)) AS BIGINT) as elapsed_s,
            depth_at_start - depth as s
          FROM with_meta
          WHERE is_draw = true
            AND EXTRACT(EPOCH FROM (timestamp - event_start)) <= 7200
        ),
        agg AS (
          SELECT month_start, elapsed_s,
            approx_percentile_cont(s, 0.1) as s_p10,
            approx_percentile_cont(s, 0.5) as s_p50,
            approx_percentile_cont(s, 0.9) as s_p90,
            COUNT(*) as n
          FROM draw
          GROUP BY month_start, elapsed_s
        )
        SELECT
          to_timestamp(EXTRACT(EPOCH FROM month_start) + elapsed_s) as timestamp,
          CAST(EXTRACT(YEAR FROM month_start) AS VARCHAR) || '-' ||
            LPAD(CAST(EXTRACT(MONTH FROM month_start) AS VARCHAR), 2, '0') as month,
          elapsed_s,
          s_p10, s_p50, s_p90,
          CAST(n AS BIGINT) as n
        FROM agg
        ORDER BY month_start, elapsed_s

  - name: "horner-by-month"
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
        rec_meta AS (
          SELECT timestamp, depth, pump_event_id,
            FIRST_VALUE(timestamp) OVER (
              PARTITION BY pump_event_id ORDER BY timestamp
            ) as event_start,
            FIRST_VALUE(timestamp) OVER (
              PARTITION BY pump_event_id ORDER BY depth ASC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as trough_ts,
            MAX(depth) OVER (PARTITION BY pump_event_id) as static_depth
          FROM filtered
        ),
        rec AS (
          SELECT
            date_trunc('month', event_start) as month_start,
            EXTRACT(EPOCH FROM (timestamp - event_start)) as tp_dt,
            EXTRACT(EPOCH FROM (timestamp - trough_ts)) as dt,
            static_depth - depth as s
          FROM rec_meta
          WHERE timestamp > trough_ts
            AND EXTRACT(EPOCH FROM (timestamp - event_start)) <= 7200
        ),
        bucketed AS (
          SELECT month_start,
            ROUND(log10(tp_dt / dt) / 0.05) * 0.05 as log_ratio,
            s
          FROM rec
          WHERE dt > 0 AND tp_dt > 0
        ),
        agg AS (
          SELECT month_start, log_ratio,
            approx_percentile_cont(s, 0.1) as s_p10,
            approx_percentile_cont(s, 0.5) as s_p50,
            approx_percentile_cont(s, 0.9) as s_p90,
            COUNT(*) as n
          FROM bucketed
          GROUP BY month_start, log_ratio
        )
        SELECT
          to_timestamp(EXTRACT(EPOCH FROM month_start)
            + CAST(log_ratio * 1000 AS BIGINT)) as timestamp,
          CAST(EXTRACT(YEAR FROM month_start) AS VARCHAR) || '-' ||
            LPAD(CAST(EXTRACT(MONTH FROM month_start) AS VARCHAR), 2, '0') as month,
          log_ratio,
          s_p10, s_p50, s_p90,
          CAST(n AS BIGINT) as n
        FROM agg
        ORDER BY month_start, log_ratio
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
  "host+file:///analysis/drawdown-by-month" \
  --format=table --sql "SELECT COUNT(DISTINCT month) as months FROM source" 2>&1)
echo "Analysis output: ${ANALYSIS_OUT}"
check 'echo "${ANALYSIS_OUT}" | grep -q "2"' "drawdown-by-month aggregates 2 calendar months"

# Verify horner recovery series
SUMMARY_OUT=$(pond cat -d "${HOST_ROOT}" \
  --hostmount "/reduced=host+dyndir:///reduce.yaml" \
  --hostmount "/analysis=host+dyndir:///analysis.yaml" \
  "host+file:///analysis/horner-by-month" \
  --format=table --sql "SELECT month, log_ratio, n FROM source ORDER BY month, log_ratio" 2>&1)
echo "Summary output: ${SUMMARY_OUT}"
check 'echo "${SUMMARY_OUT}" | grep -q "log_ratio"' "horner-by-month has expected columns"

# Verify months: two March cycles (2024-03), one July cycle (2024-07)
check 'echo "${SUMMARY_OUT}" | grep -q "2024-03"' "March cycles detected (month=2024-03)"
check 'echo "${SUMMARY_OUT}" | grep -q "2024-07"' "July cycle detected (month=2024-07)"

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
check 'echo "${LIST_OUT}" | grep -q "drawdown-by-month"' "list shows drawdown-by-month entry"
check 'echo "${LIST_OUT}" | grep -q "horner-by-month"' "list shows horner-by-month entry"

# ==============================================================================
# Step 6: Verify pond cat --explain
# ==============================================================================

echo ""
echo "--- Step 6: Verify pond cat --explain ---"

EXPLAIN_OUT=$(pond cat -d "${HOST_ROOT}" \
  --hostmount "/reduced=host+dyndir:///reduce.yaml" \
  --hostmount "/analysis=host+dyndir:///analysis.yaml" \
  "host+file:///analysis/drawdown-by-month" --explain 2>&1)
echo "Explain output: ${EXPLAIN_OUT}"
check 'echo "${EXPLAIN_OUT}" | grep -q "table:dynamic"' "explain shows entry type"
check 'echo "${EXPLAIN_OUT}" | grep -q "s_p50"' "explain shows schema"
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
  "host+file:///analysis/drawdown-by-month" \
  --sql "SELECT s_p50 FROM source LIMIT 1" 2>/dev/null | file -)
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
      - name: "explore"
        type: static
        slug: "explore"
        page: "/site/explore.md"
        export: "analysis"

explore:
  url: "explore/"
  datasets:
    - export: "analysis"
      table: "analysis"
      label: "Analysis"

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

cat > "${HOST_ROOT}/site/explore.md" << 'MD'
---
title: Explore
layout: explore
---

# Explore

{{ explore /}}
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
check 'test -f "${OUTDIR}/analysis/drawdown-by-month.html"' "drawdown-by-month.html exists"
check 'test -f "${OUTDIR}/analysis/horner-by-month.html"' "horner-by-month.html exists"

# Overlay JS asset
check 'test -f "${OUTDIR}/overlay.js"' "overlay.js asset generated"

# HTML contains overlay chart container
check_contains "${OUTDIR}/analysis/drawdown-by-month.html" \
  "drawdown-by-month has overlay chart container" \
  'id="overlay-chart"'

# HTML contains overlay data manifest
check_contains "${OUTDIR}/analysis/drawdown-by-month.html" \
  "drawdown-by-month has overlay data manifest" \
  'class="overlay-data"'

# HTML includes overlay.js script
check_contains "${OUTDIR}/analysis/drawdown-by-month.html" \
  "drawdown-by-month loads overlay.js" \
  'overlay.js'

# Explorer page generated (cross-link target)
check 'test -f "${OUTDIR}/explore/index.html"' "explore page exists"

# Overlay chart carries the explorer cross-link URL so overlay.js renders the
# "Explore this data" pivot button.
check_contains "${OUTDIR}/analysis/drawdown-by-month.html" \
  "drawdown-by-month has data-explore-url cross-link" \
  'data-explore-url'

# Parquet data files exported
PARQUET_COUNT=$(find "${OUTDIR}" -name "*.parquet" | wc -l)
echo "  Parquet files: ${PARQUET_COUNT}"
check '[ "${PARQUET_COUNT}" -gt 0 ]' "parquet data files exported"

# Manifest references existing parquet files
MANIFEST_FILE=$(grep -o '/data/drawdown-by-month/[^"]*\.parquet' \
  "${OUTDIR}/analysis/drawdown-by-month.html" | head -1)
echo "  First manifest file: ${MANIFEST_FILE}"
if [ -n "${MANIFEST_FILE}" ]; then
  check "test -f '${OUTDIR}${MANIFEST_FILE}'" "manifest parquet file exists on disk"
fi

echo ""
echo "Generated files:"
find "${OUTDIR}" -type f | sort | sed 's|^|  |'

check_finish
