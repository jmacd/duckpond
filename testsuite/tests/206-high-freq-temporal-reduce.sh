#!/bin/bash
# EXPERIMENT: High-frequency (1-second) data into temporal-reduce with count verification
# EXPECTED: 3 months of 1s data → temporal-reduce at 1h resolution produces exactly
#           3600 points per bucket. Sitegen exports partitioned parquet files. DuckDB
#           queries confirm every row has timestamp.count = 3600.
set -e

echo "=== Experiment: High-frequency temporal reduce + count verification ==="

pond init

OUTDIR=/tmp/sitegen-hf-test

# ══════════════════════════════════════════════════════════════════════════════
# Step 1: Synthetic 1-second data — 3 months (Jan 1 – Mar 31, 2025)
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 1: Create 1-second synthetic data (3 months) ---"

# 90 days × 86400 seconds/day = 7,776,000 data points
# start is inclusive, end is inclusive (while ts <= end) so we use 23:59:59
cat > /tmp/sensors.yaml << 'YAML'
entries:
  - name: "sensor_a"
    factory: "synthetic-timeseries"
    config:
      start: "2025-01-01T00:00:00Z"
      end:   "2025-03-31T23:59:59Z"
      interval: "1s"
      points:
        - name: "value"
          components:
            - type: sine
              amplitude: 10.0
              period: "24h"
              offset: 50.0
YAML

pond mknod dynamic-dir /sensors --config-path /tmp/sensors.yaml
echo "✓ Synthetic sensor created (1s interval, 3 months)"

# ══════════════════════════════════════════════════════════════════════════════
# Step 2: Temporal-reduce factory at 1h resolution with count aggregation
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 2: Create temporal-reduce with count aggregation ---"

cat > /tmp/reduce.yaml << 'YAML'
entries:
  - name: "hourly"
    factory: "temporal-reduce"
    config:
      in_pattern: "series:///sensors/*"
      out_pattern: "$0"
      time_column: "timestamp"
      resolutions: ["1h"]
      aggregations:
        - type: "count"
          columns: ["*"]
        - type: "avg"
          columns: ["*"]
        - type: "min"
          columns: ["*"]
        - type: "max"
          columns: ["*"]
YAML

pond mknod dynamic-dir /reduced --config-path /tmp/reduce.yaml
echo "✓ Temporal reduce factory created"

# ══════════════════════════════════════════════════════════════════════════════
# Step 3: Verify temporal-reduce output via pond cat
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 3: Verify temporal-reduce output ---"

# The 1h resolution should produce one row per hour.
# 90 days × 24 hours = 2160 rows.
ROW_COUNT_RAW=$(pond cat --format=table --sql "SELECT COUNT(*) AS cnt FROM source" /reduced/hourly/sensor_a/res=1h.series 2>/dev/null)
ROW_COUNT=$(echo "$ROW_COUNT_RAW" | grep -E '^\| [0-9]' | head -1 | grep -oE '[0-9]+' | head -1)
echo "Row count at 1h resolution: ${ROW_COUNT}"

# Check the count column — every row should have exactly 3600
# (3600 seconds in 1 hour, with 1-second data)
BAD_COUNTS_RAW=$(pond cat --format=table --sql "SELECT COUNT(*) AS bad FROM source WHERE \"timestamp.count\" != 3600" /reduced/hourly/sensor_a/res=1h.series 2>/dev/null)
BAD_COUNTS=$(echo "$BAD_COUNTS_RAW" | grep -E '^\| [0-9]' | head -1 | grep -oE '[0-9]+' | head -1)
echo "Rows with timestamp.count != 3600: ${BAD_COUNTS}"

# ══════════════════════════════════════════════════════════════════════════════
# Step 4: Sitegen — export with auto-partitioning
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 4: Create sitegen config ---"

pond mkdir -p /etc/site

cat > /tmp/index.md << 'MD'
---
title: "HF Test"
layout: default
---

# High-Frequency Test

Placeholder page.
MD

cat > /tmp/data.md << 'MD'
---
title: "{{ $0 }}"
layout: data
---

# {{ $0 }}

{{ chart /}}
MD

cat > /tmp/sidebar.md << 'MD'
## HF Test

### Sensors

{{ nav_list collection="sensors" base="/sensors" /}}
MD

pond copy host:///tmp/index.md /etc/site/index.md
pond copy host:///tmp/data.md /etc/site/data.md
pond copy host:///tmp/sidebar.md /etc/site/sidebar.md

cat > /tmp/site.yaml << 'YAML'
factory: sitegen

site:
  title: "HF Test"
  base_url: "/"

exports:
  - name: "sensors"
    pattern: "/reduced/hourly/*/*.series"
    target_points: 1500

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/etc/site/index.md"
  - name: "sensors"
    type: static
    slug: "sensors"
    routes:
      - name: "sensor-detail"
        type: template
        slug: "$0"
        page: "/etc/site/data.md"
        export: "sensors"

partials:
  sidebar: "/etc/site/sidebar.md"

static_assets: []
YAML

pond mknod sitegen /etc/site.yaml --config-path /tmp/site.yaml
echo "✓ Sitegen factory created"

# ══════════════════════════════════════════════════════════════════════════════
# Step 5: Generate the site
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 5: Build site ---"
pond run /etc/site.yaml build "${OUTDIR}"
echo "✓ Site generated in ${OUTDIR}"

echo ""
echo "--- Output structure ---"
find "${OUTDIR}" -type f | sort

# ══════════════════════════════════════════════════════════════════════════════
# Verification
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "=== VERIFICATION ==="

PASS=0
FAIL=0

check() {
  if [ "$1" = "true" ]; then
    echo "  ✓ $2"
    PASS=$((PASS + 1))
  else
    echo "  ✗ $2"
    FAIL=$((FAIL + 1))
  fi
}

check_file() {
  if [ -f "$1" ]; then
    echo "  ✓ $2"
    PASS=$((PASS + 1))
  else
    echo "  ✗ MISSING: $2"
    FAIL=$((FAIL + 1))
  fi
}

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

echo ""
echo "--- Structure checks ---"
check_file "${OUTDIR}/index.html" "index.html exists"
check_file "${OUTDIR}/sensors/sensor_a.html" "sensors/sensor_a.html exists"
check_file "${OUTDIR}/style.css" "style.css exists"
check_file "${OUTDIR}/chart.js" "chart.js exists"

echo ""
echo "--- Parquet export checks ---"
check_has_parquet_in "${OUTDIR}/data/hourly/sensor_a/res=1h" "data/hourly/sensor_a/res=1h"

echo ""
echo "--- Temporal-reduce row count ---"
# Should be exactly 2160 hourly buckets (90 days × 24 hours)
check "$([ "${ROW_COUNT}" = "2160" ] && echo true || echo false)" \
  "temporal-reduce produced 2160 hourly rows (got ${ROW_COUNT})"

echo ""
echo "--- Count aggregation verification ---"
# Every hourly bucket should have exactly 3600 input points
check "$([ "${BAD_COUNTS}" = "0" ] && echo true || echo false)" \
  "all hourly buckets have timestamp.count=3600 (bad rows: ${BAD_COUNTS})"

echo ""
echo "--- DuckDB verification of exported parquet ---"
# Query the exported parquet files directly with DuckDB to verify counts
PARQUET_DIR="${OUTDIR}/data/hourly/sensor_a/res=1h"
PARQUET_FILES=$(find "${PARQUET_DIR}" -name '*.parquet' -type f | sort)
PARQUET_COUNT=$(echo "${PARQUET_FILES}" | wc -l | tr -d ' ')

echo "  Found ${PARQUET_COUNT} parquet file(s) in ${PARQUET_DIR}"

for pf in ${PARQUET_FILES}; do
  RELPATH="${pf#${OUTDIR}/}"
  echo ""
  echo "  Checking: ${RELPATH}"

  # Total rows in this parquet file
  TOTAL=$(duckdb -noheader -csv -c "SELECT COUNT(*) FROM read_parquet('${pf}')" 2>/dev/null | tr -d '[:space:]')
  echo "    Total rows: ${TOTAL}"

  # Check that timestamp.count column exists and all values are 3600
  ALL_3600=$(duckdb -noheader -csv -c "
    SELECT COUNT(*) FROM read_parquet('${pf}')
    WHERE \"timestamp.count\" = 3600
  " 2>/dev/null | tr -d '[:space:]')

  NOT_3600=$(duckdb -noheader -csv -c "
    SELECT COUNT(*) FROM read_parquet('${pf}')
    WHERE \"timestamp.count\" != 3600
  " 2>/dev/null | tr -d '[:space:]')

  echo "    Rows with count=3600: ${ALL_3600}"
  echo "    Rows with count≠3600: ${NOT_3600}"

  check "$([ "${NOT_3600}" = "0" ] && echo true || echo false)" \
    "${RELPATH}: all rows have timestamp.count=3600 (${ALL_3600} rows OK)"

  # Verify total row count matches expected
  check "$([ "${TOTAL}" = "${ALL_3600}" ] && echo true || echo false)" \
    "${RELPATH}: total rows (${TOTAL}) = verified rows (${ALL_3600})"

  # Show schema for debugging
  echo "    Schema:"
  duckdb -c "DESCRIBE SELECT * FROM read_parquet('${pf}')" 2>/dev/null | sed 's/^/      /'
done

# Verify total exported rows across all parquet files = 2160
GLOBAL_TOTAL=$(duckdb -noheader -csv -c "
  SELECT COUNT(*) FROM read_parquet('${PARQUET_DIR}/**/*.parquet', hive_partitioning=true)
" 2>/dev/null | tr -d '[:space:]')
echo ""
echo "  Total rows across all parquet files: ${GLOBAL_TOTAL}"
check "$([ "${GLOBAL_TOTAL}" = "2160" ] && echo true || echo false)" \
  "total exported rows = 2160 (got ${GLOBAL_TOTAL})"

echo ""
echo "=== Results: ${PASS} passed, ${FAIL} failed ==="

if [ "${FAIL}" -gt 0 ]; then
  echo ""
  echo "FAILED"
  exit 1
fi

echo ""
echo "=== Test 206 PASSED ==="
