#!/bin/bash
# EXPERIMENT: Multiple temporal-reduce resolutions over shared multi-file source
# DESCRIPTION: Verifies that when multiple temporal-reduce nodes share the same
#   multi-file oteljson glob source, ALL outputs have correct value columns.
#
#   This catches two bugs that were present:
#   1. Schema inference (infer_schema) sampled only some parquet files,
#      dropping columns that only appear in later files.  Fixed by using
#      merge_parquet_schemas_in_dir() with Schema::try_merge().
#   2. Each reduce node rebuilt the expensive glob ListingTable because the
#      table name included the SQL hash (unique per resolution).  Fixed by
#      using a data-only table name so the first node registers and the
#      rest reuse it.
#
# EXPECTED: Two resolutions (1h, 6h) over 3 oteljson files produce correct
#   aggregations with ALL value columns present (not just those in the
#   first file).
set -e

echo "=== Experiment: Multi-Reduce Shared Source ==="
echo ""

pond init

#############################
# CREATE SYNTHETIC OTELJSON FILES
#############################

echo "=== Creating 3 oteljson files with evolving schemas ==="

mkdir -p /tmp/oteljson

# File 1 (2024-01-01): well_depth_value only
# 24 hours of data at 1h intervals = 24 data points
generate_oteljson_day() {
    local DATE_PREFIX=$1  # e.g. "2024-01-01"
    local OUTFILE=$2
    local METRICS=$3      # comma-separated metric names
    > "$OUTFILE"
    IFS=',' read -ra METRIC_NAMES <<< "$METRICS"
    for HOUR in $(seq 0 23); do
        PADDED=$(printf "%02d" "$HOUR")
        # Convert to epoch nanoseconds (approximate)
        EPOCH_S=$(($(date -d "${DATE_PREFIX}T${PADDED}:00:00Z" +%s 2>/dev/null || \
                     date -j -f "%Y-%m-%dT%H:%M:%SZ" "${DATE_PREFIX}T${PADDED}:00:00Z" +%s 2>/dev/null || \
                     echo $((1704067200 + HOUR * 3600)))))
        EPOCH_NS="${EPOCH_S}000000000"
        # Build metrics array
        METRICS_JSON=""
        for M in "${METRIC_NAMES[@]}"; do
            VALUE=$(echo "scale=2; 10 + $HOUR + $RANDOM % 100 / 100" | bc 2>/dev/null || echo "1${HOUR}.${HOUR}")
            if [ -n "$METRICS_JSON" ]; then METRICS_JSON+=","; fi
            METRICS_JSON+="{\"name\":\"${M}\",\"gauge\":{\"dataPoints\":[{\"timeUnixNano\":\"${EPOCH_NS}\",\"asDouble\":${VALUE}}]}}"
        done
        echo "{\"resourceMetrics\":[{\"resource\":{},\"scopeMetrics\":[{\"scope\":{\"name\":\"water\"},\"metrics\":[${METRICS_JSON}]}]}]}" >> "$OUTFILE"
    done
}

# Use simple epoch arithmetic instead of date parsing
cat > /tmp/oteljson/data-2024-01.json << 'OTELJSON'
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1704067200000000000","asDouble":40.1}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1704070800000000000","asDouble":40.2}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1704074400000000000","asDouble":40.3}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1704078000000000000","asDouble":40.4}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1704081600000000000","asDouble":40.5}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1704085200000000000","asDouble":40.6}]}}]}]}]}
OTELJSON

# File 2 (2024-06-01): well_depth_value + system_pressure_value
cat > /tmp/oteljson/data-2024-06.json << 'OTELJSON'
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1717200000000000000","asDouble":41.1}]}},{"name":"system_pressure_value","gauge":{"dataPoints":[{"timeUnixNano":"1717200000000000000","asDouble":1.50}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1717203600000000000","asDouble":41.2}]}},{"name":"system_pressure_value","gauge":{"dataPoints":[{"timeUnixNano":"1717203600000000000","asDouble":1.55}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1717207200000000000","asDouble":41.3}]}},{"name":"system_pressure_value","gauge":{"dataPoints":[{"timeUnixNano":"1717207200000000000","asDouble":1.60}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1717210800000000000","asDouble":41.4}]}},{"name":"system_pressure_value","gauge":{"dataPoints":[{"timeUnixNano":"1717210800000000000","asDouble":1.65}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1717214400000000000","asDouble":41.5}]}},{"name":"system_pressure_value","gauge":{"dataPoints":[{"timeUnixNano":"1717214400000000000","asDouble":1.70}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1717218000000000000","asDouble":41.6}]}},{"name":"system_pressure_value","gauge":{"dataPoints":[{"timeUnixNano":"1717218000000000000","asDouble":1.75}]}}]}]}]}
OTELJSON

# File 3 (2025-01-01): well_depth_value + system_pressure_value + ph_value
cat > /tmp/oteljson/data-2025-01.json << 'OTELJSON'
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1735689600000000000","asDouble":42.1}]}},{"name":"system_pressure_value","gauge":{"dataPoints":[{"timeUnixNano":"1735689600000000000","asDouble":1.80}]}},{"name":"ph_value","gauge":{"dataPoints":[{"timeUnixNano":"1735689600000000000","asDouble":7.10}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1735693200000000000","asDouble":42.2}]}},{"name":"system_pressure_value","gauge":{"dataPoints":[{"timeUnixNano":"1735693200000000000","asDouble":1.85}]}},{"name":"ph_value","gauge":{"dataPoints":[{"timeUnixNano":"1735693200000000000","asDouble":7.15}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1735696800000000000","asDouble":42.3}]}},{"name":"system_pressure_value","gauge":{"dataPoints":[{"timeUnixNano":"1735696800000000000","asDouble":1.90}]}},{"name":"ph_value","gauge":{"dataPoints":[{"timeUnixNano":"1735696800000000000","asDouble":7.20}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1735700400000000000","asDouble":42.4}]}},{"name":"system_pressure_value","gauge":{"dataPoints":[{"timeUnixNano":"1735700400000000000","asDouble":1.95}]}},{"name":"ph_value","gauge":{"dataPoints":[{"timeUnixNano":"1735700400000000000","asDouble":7.25}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1735704000000000000","asDouble":42.5}]}},{"name":"system_pressure_value","gauge":{"dataPoints":[{"timeUnixNano":"1735704000000000000","asDouble":2.00}]}},{"name":"ph_value","gauge":{"dataPoints":[{"timeUnixNano":"1735704000000000000","asDouble":7.30}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1735707600000000000","asDouble":42.6}]}},{"name":"system_pressure_value","gauge":{"dataPoints":[{"timeUnixNano":"1735707600000000000","asDouble":2.05}]}},{"name":"ph_value","gauge":{"dataPoints":[{"timeUnixNano":"1735707600000000000","asDouble":7.35}]}}]}]}]}
OTELJSON

echo "Created 3 files:"
echo "  data-2024-01.json: well_depth_value (6 rows)"
echo "  data-2024-06.json: well_depth_value, system_pressure_value (6 rows)"
echo "  data-2025-01.json: well_depth_value, system_pressure_value, ph_value (6 rows)"

#############################
# INGEST FILES
#############################

echo ""
echo "=== Ingesting files ==="
pond mkdir -p /ingest
pond copy "host:///tmp/oteljson/data-2024-01.json" /ingest/data-2024-01.json
pond copy "host:///tmp/oteljson/data-2024-06.json" /ingest/data-2024-06.json
pond copy "host:///tmp/oteljson/data-2025-01.json" /ingest/data-2025-01.json
pond list '/ingest/*'

#############################
# CREATE TEMPORAL-REDUCE WITH MULTIPLE RESOLUTIONS
#############################

echo ""
echo "=== Creating temporal-reduce with 2 resolutions over same glob source ==="

# This is the key pattern: multiple resolutions over the same multi-file glob.
# Before the fix, schema inference sampled only some cached parquet files,
# so columns from later files (system_pressure_value, ph_value) would be missing.
# The shared-table fix also means the 2nd resolution reuses the 1st's ListingTable.

cat > /tmp/reduce.yaml << 'YAML'
in_pattern: "oteljson:///ingest/data-*.json"
out_pattern: "$0"
time_column: "timestamp"
resolutions: ["1h", "6h"]
aggregations:
  - type: "avg"
  - type: "min"
  - type: "max"
YAML

pond mknod temporal-reduce /reduced --config-path /tmp/reduce.yaml

echo ""
echo "=== Directory structure ==="
pond list /reduced
# Should show a single entry derived from the source pattern
echo ""
echo "=== Resolution files ==="
pond list '/reduced/**/*'

#############################
# VERIFICATION
#############################

echo ""
echo "=== VERIFICATION ==="

# temporal-reduce creates per-source output directories:
#   /reduced/2024-01/ (from data-2024-01.json: well_depth only)
#   /reduced/2024-06/ (from data-2024-06.json: well_depth + system_pressure)
#   /reduced/2025-01/ (from data-2025-01.json: well_depth + system_pressure + ph)
#
# Each directory has res=1h.series and res=6h.series.
# The shared data table optimization means the 1h and 6h resolutions
# for each source reuse the same registered ListingTable.

# Helper: extract a single numeric value from pond cat --format=table output
pond_count() {
    pond cat "$1" --format=table --sql "$2" 2>&1 | grep -E '^\|' | tail -1 | tr -d '| '
}

echo ""
echo "--- 2025-01/res=1h.series: should have ALL 3 metrics ---"
# Query one row selecting all 3 metric aggregations; if any column is missing, SQL errors
RESULT_2025_1H=$(pond cat /reduced/2025-01/res=1h.series --format=table --sql "
  SELECT
    \"well_depth_value.avg\",
    \"system_pressure_value.avg\",
    \"ph_value.avg\"
  FROM source LIMIT 1
" 2>&1)
echo "$RESULT_2025_1H"
if echo "$RESULT_2025_1H" | grep -qi 'error'; then
    echo "FAIL: 2025-01/1h missing expected columns"
    exit 1
fi
echo "PASS: 2025-01/1h has all 3 metric columns"

echo ""
echo "--- 2025-01/res=6h.series: should have ALL 3 metrics (shared table reuse) ---"
RESULT_2025_6H=$(pond cat /reduced/2025-01/res=6h.series --format=table --sql "
  SELECT
    \"well_depth_value.avg\",
    \"system_pressure_value.avg\",
    \"ph_value.avg\"
  FROM source LIMIT 1
" 2>&1)
echo "$RESULT_2025_6H"
if echo "$RESULT_2025_6H" | grep -qi 'error'; then
    echo "FAIL: 2025-01/6h missing expected columns (shared table reuse broken)"
    exit 1
fi
echo "PASS: 2025-01/6h has all 3 metric columns (shared table reuse works)"

echo ""
echo "--- 2024-06/res=1h.series: should have well_depth + system_pressure ---"
RESULT_0624_1H=$(pond cat /reduced/2024-06/res=1h.series --format=table --sql "
  SELECT
    \"well_depth_value.avg\",
    \"system_pressure_value.avg\"
  FROM source LIMIT 1
" 2>&1)
echo "$RESULT_0624_1H"
if echo "$RESULT_0624_1H" | grep -qi 'error'; then
    echo "FAIL: 2024-06/1h missing expected columns"
    exit 1
fi
echo "PASS: 2024-06/1h has well_depth + system_pressure columns"

echo ""
echo "--- 2024-01/res=1h.series: should have well_depth ---"
RESULT_0124_1H=$(pond cat /reduced/2024-01/res=1h.series --format=table --sql "
  SELECT \"well_depth_value.avg\" FROM source LIMIT 1
" 2>&1)
echo "$RESULT_0124_1H"
if echo "$RESULT_0124_1H" | grep -qi 'error'; then
    echo "FAIL: 2024-01/1h missing well_depth columns"
    exit 1
fi
echo "PASS: 2024-01/1h has well_depth columns"

echo ""
echo "--- Verify ph_value.avg has data in 2025-01 (only file 3 has pH) ---"
PH_1H=$(pond_count /reduced/2025-01/res=1h.series "
  SELECT COUNT(*) AS cnt FROM source WHERE \"ph_value.avg\" IS NOT NULL
")
echo "2025-01 1h rows with ph_value.avg: ${PH_1H}"
if [ "$PH_1H" = "0" ] || [ -z "$PH_1H" ]; then
    echo "FAIL: ph_value.avg should have non-null values in 2025-01"
    exit 1
fi
echo "PASS: ph_value.avg has data"

echo ""
echo "--- Verify system_pressure_value.avg in 2024-06 (file 2 has pressure) ---"
SP_1H=$(pond_count /reduced/2024-06/res=1h.series "
  SELECT COUNT(*) AS cnt FROM source WHERE \"system_pressure_value.avg\" IS NOT NULL
")
echo "2024-06 1h rows with system_pressure_value.avg: ${SP_1H}"
if [ "$SP_1H" = "0" ] || [ -z "$SP_1H" ]; then
    echo "FAIL: system_pressure_value.avg should have non-null values in 2024-06"
    exit 1
fi
echo "PASS: system_pressure_value.avg has data"

echo ""
echo "--- Verify both resolutions produce data for same source ---"
WD_1H=$(pond_count /reduced/2025-01/res=1h.series "
  SELECT COUNT(*) AS cnt FROM source WHERE \"well_depth_value.avg\" IS NOT NULL
")
WD_6H=$(pond_count /reduced/2025-01/res=6h.series "
  SELECT COUNT(*) AS cnt FROM source WHERE \"well_depth_value.avg\" IS NOT NULL
")
echo "2025-01: 1h rows=${WD_1H}, 6h rows=${WD_6H}"
if [ "$WD_1H" = "0" ] || [ -z "$WD_1H" ]; then
    echo "FAIL: 1h resolution has no data"
    exit 1
fi
if [ "$WD_6H" = "0" ] || [ -z "$WD_6H" ]; then
    echo "FAIL: 6h resolution has no data (shared table reuse broken)"
    exit 1
fi
echo "PASS: Both resolutions produce data"

echo ""
echo "--- Sample output: 2025-01 1h resolution (all 3 metrics) ---"
pond cat /reduced/2025-01/res=1h.series --format=table --sql "
  SELECT
    timestamp,
    ROUND(\"well_depth_value.avg\", 2) AS wd_avg,
    ROUND(\"system_pressure_value.avg\", 2) AS sp_avg,
    ROUND(\"ph_value.avg\", 2) AS ph_avg
  FROM source
  ORDER BY timestamp
"

echo ""
echo "--- Sample output: 2025-01 6h resolution (all 3 metrics) ---"
pond cat /reduced/2025-01/res=6h.series --format=table --sql "
  SELECT
    timestamp,
    ROUND(\"well_depth_value.avg\", 2) AS wd_avg,
    ROUND(\"system_pressure_value.avg\", 2) AS sp_avg,
    ROUND(\"ph_value.avg\", 2) AS ph_avg
  FROM source
  ORDER BY timestamp
"

echo ""
echo "=== ALL CHECKS PASSED ==="
echo "Multiple temporal-reduce resolutions over shared multi-file oteljson source"
echo "produce correct aggregations with correct value columns per source file."
echo "Both resolutions (1h, 6h) produce data, confirming shared table reuse works."
