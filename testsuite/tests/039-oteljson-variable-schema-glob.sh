#!/bin/bash
# EXPERIMENT: Multi-file oteljson glob with variable schemas
# DESCRIPTION: Verify that sql-derived-series correctly merges multiple
#   oteljson files whose schemas evolve over time (columns appear/disappear).
#   This is the real-world pattern in Caspar Water data:
#     - 2022 files have: pid_temperature_*, well_depth_*
#     - 2024 files add: atlasph_ph, chlorine_level_value, system_pressure_value, ...
#     - 2026 files drop: pid_temperature_*, add: scrape_* metrics
#
#   The UNION must produce the superset of all columns, with NULLs where
#   a column did not exist in a particular file.
#
set -e

echo "=== Experiment: OtelJSON Variable Schema Glob ==="
echo ""

pond init

#############################
# CREATE SYNTHETIC OTELJSON FILES WITH EVOLVING SCHEMAS
#############################

echo "=== Creating synthetic oteljson files with different schemas ==="

# Simulate schema evolution like water/data/casparwater-*.json:
#   early files:  well_depth_value, pid_temperature_value
#   middle files: well_depth_value, system_pressure_value, chlorine_level_value
#   late files:   well_depth_value, system_pressure_value, atlasph_ph

mkdir -p /tmp/oteljson

# File 1: "early" schema (2 metrics)
cat > /tmp/oteljson/metrics-2022.json << 'OTELJSON'
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","unit":"ft","gauge":{"dataPoints":[{"timeUnixNano":"1664000000000000000","asDouble":42.5}]}},{"name":"pid_temperature_value","unit":"F","gauge":{"dataPoints":[{"timeUnixNano":"1664000000000000000","asDouble":63.4}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","unit":"ft","gauge":{"dataPoints":[{"timeUnixNano":"1664000060000000000","asDouble":42.3}]}},{"name":"pid_temperature_value","unit":"F","gauge":{"dataPoints":[{"timeUnixNano":"1664000060000000000","asDouble":63.5}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","unit":"ft","gauge":{"dataPoints":[{"timeUnixNano":"1664000120000000000","asDouble":42.1}]}},{"name":"pid_temperature_value","unit":"F","gauge":{"dataPoints":[{"timeUnixNano":"1664000120000000000","asDouble":63.6}]}}]}]}]}
OTELJSON

# File 2: "middle" schema (3 metrics, dropped pid_temperature, added system_pressure + chlorine)
cat > /tmp/oteljson/metrics-2024.json << 'OTELJSON'
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","unit":"ft","gauge":{"dataPoints":[{"timeUnixNano":"1719800000000000000","asDouble":43.2}]}},{"name":"system_pressure_value","unit":"psi","gauge":{"dataPoints":[{"timeUnixNano":"1719800000000000000","asDouble":1.75}]}},{"name":"chlorine_level_value","unit":"pct","gauge":{"dataPoints":[{"timeUnixNano":"1719800000000000000","asDouble":89.1}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","unit":"ft","gauge":{"dataPoints":[{"timeUnixNano":"1719800060000000000","asDouble":43.5}]}},{"name":"system_pressure_value","unit":"psi","gauge":{"dataPoints":[{"timeUnixNano":"1719800060000000000","asDouble":1.80}]}},{"name":"chlorine_level_value","unit":"pct","gauge":{"dataPoints":[{"timeUnixNano":"1719800060000000000","asDouble":88.9}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","unit":"ft","gauge":{"dataPoints":[{"timeUnixNano":"1719800120000000000","asDouble":43.8}]}},{"name":"system_pressure_value","unit":"psi","gauge":{"dataPoints":[{"timeUnixNano":"1719800120000000000","asDouble":1.72}]}},{"name":"chlorine_level_value","unit":"pct","gauge":{"dataPoints":[{"timeUnixNano":"1719800120000000000","asDouble":88.5}]}}]}]}]}
OTELJSON

# File 3: "late" schema (3 metrics, dropped chlorine, added atlasph_ph)
cat > /tmp/oteljson/metrics-2026.json << 'OTELJSON'
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","unit":"ft","gauge":{"dataPoints":[{"timeUnixNano":"1740400000000000000","asDouble":44.0}]}},{"name":"system_pressure_value","unit":"psi","gauge":{"dataPoints":[{"timeUnixNano":"1740400000000000000","asDouble":1.90}]}},{"name":"atlasph_ph","unit":"pH","gauge":{"dataPoints":[{"timeUnixNano":"1740400000000000000","asDouble":7.2}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","unit":"ft","gauge":{"dataPoints":[{"timeUnixNano":"1740400060000000000","asDouble":44.1}]}},{"name":"system_pressure_value","unit":"psi","gauge":{"dataPoints":[{"timeUnixNano":"1740400060000000000","asDouble":1.85}]}},{"name":"atlasph_ph","unit":"pH","gauge":{"dataPoints":[{"timeUnixNano":"1740400060000000000","asDouble":7.1}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","unit":"ft","gauge":{"dataPoints":[{"timeUnixNano":"1740400120000000000","asDouble":44.3}]}},{"name":"system_pressure_value","unit":"psi","gauge":{"dataPoints":[{"timeUnixNano":"1740400120000000000","asDouble":1.88}]}},{"name":"atlasph_ph","unit":"pH","gauge":{"dataPoints":[{"timeUnixNano":"1740400120000000000","asDouble":7.3}]}}]}]}]}
OTELJSON

echo "Created 3 oteljson files with evolving schemas:"
echo "  metrics-2022.json: timestamp, well_depth_value, pid_temperature_value"
echo "  metrics-2024.json: timestamp, well_depth_value, system_pressure_value, chlorine_level_value"
echo "  metrics-2026.json: timestamp, well_depth_value, system_pressure_value, atlasph_ph"

#############################
# VERIFY INDIVIDUAL FILE SCHEMAS
#############################

echo ""
echo "=== Verify individual file schemas via host+oteljson ==="

echo "--- metrics-2022 sample ---"
pond cat "host+oteljson:///tmp/oteljson/metrics-2022.json" --format=table --sql "SELECT * FROM source LIMIT 1"

echo "--- metrics-2024 sample ---"
pond cat "host+oteljson:///tmp/oteljson/metrics-2024.json" --format=table --sql "SELECT * FROM source LIMIT 1"

echo "--- metrics-2026 sample ---"
pond cat "host+oteljson:///tmp/oteljson/metrics-2026.json" --format=table --sql "SELECT * FROM source LIMIT 1"

#############################
# INGEST ALL FILES INTO POND
#############################

echo ""
echo "=== Ingesting all 3 files into pond ==="

pond mkdir -p /ingest
pond copy "host:///tmp/oteljson/metrics-2022.json" /ingest/metrics-2022.json
pond copy "host:///tmp/oteljson/metrics-2024.json" /ingest/metrics-2024.json
pond copy "host:///tmp/oteljson/metrics-2026.json" /ingest/metrics-2026.json

pond list '/ingest/*'
echo "Ingested 3 files"

#############################
# CREATE SQL-DERIVED-SERIES WITH GLOB PATTERN
#############################

echo ""
echo "=== Creating sql-derived-series over glob pattern ==="

cat > /tmp/derived.yaml << 'EOF'
patterns:
  source: "oteljson:///ingest/metrics-*.json"
query: "SELECT * FROM source ORDER BY timestamp"
EOF

pond mkdir -p /derived
pond mknod sql-derived-series /derived/all-metrics --config-path /tmp/derived.yaml
echo "Created sql-derived-series at /derived/all-metrics"

#############################
# QUERY THE UNION: VERIFY SUPERSET SCHEMA
#############################

echo ""
echo "=== Query 1: Merged schema via SELECT * LIMIT 1 ==="
pond cat /derived/all-metrics --format=table --sql "SELECT * FROM source LIMIT 1"

# The merged schema must contain ALL columns from ALL files:
#   timestamp, well_depth_value (common to all)
#   pid_temperature_value (only 2022)
#   system_pressure_value (2024 + 2026)
#   chlorine_level_value (only 2024)
#   atlasph_ph (only 2026)

echo ""
echo "=== Query 2: Total row count ==="
TOTAL=$(pond cat /derived/all-metrics --format=table --sql "SELECT COUNT(*) AS cnt FROM source" 2>&1 | grep -E '^\|' | tail -1 | tr -d '| ')
echo "Total rows: ${TOTAL}"
if [ "$TOTAL" != "9" ]; then
    echo "FAIL: Expected 9 rows (3 files x 3 rows), got ${TOTAL}"
    exit 1
fi
echo "PASS: 9 rows (3 per file)"

echo ""
echo "=== Query 3: All data with all columns (verify NULL fill) ==="
pond cat /derived/all-metrics --format=table --sql "
  SELECT
    timestamp,
    well_depth_value,
    pid_temperature_value,
    system_pressure_value,
    chlorine_level_value,
    atlasph_ph
  FROM source
  ORDER BY timestamp
"

echo ""
echo "=== Query 4: Verify early rows have NULL for new columns ==="
EARLY_NULLS=$(pond cat /derived/all-metrics --format=table --sql "
  SELECT COUNT(*) AS cnt FROM source
  WHERE well_depth_value IS NOT NULL
    AND pid_temperature_value IS NOT NULL
    AND system_pressure_value IS NULL
    AND chlorine_level_value IS NULL
    AND atlasph_ph IS NULL
" 2>&1 | grep -E '^\|' | tail -1 | tr -d '| ')
echo "Early rows (have pid_temp, missing pressure/chlorine/ph): ${EARLY_NULLS}"
if [ "$EARLY_NULLS" != "3" ]; then
    echo "FAIL: Expected 3 early rows with correct NULL pattern, got ${EARLY_NULLS}"
    exit 1
fi
echo "PASS: Early schema NULL-filled correctly"

echo ""
echo "=== Query 5: Verify middle rows have chlorine but not ph ==="
MID_NULLS=$(pond cat /derived/all-metrics --format=table --sql "
  SELECT COUNT(*) AS cnt FROM source
  WHERE chlorine_level_value IS NOT NULL
    AND atlasph_ph IS NULL
    AND pid_temperature_value IS NULL
" 2>&1 | grep -E '^\|' | tail -1 | tr -d '| ')
echo "Middle rows (have chlorine, missing ph/pid_temp): ${MID_NULLS}"
if [ "$MID_NULLS" != "3" ]; then
    echo "FAIL: Expected 3 middle rows with correct NULL pattern, got ${MID_NULLS}"
    exit 1
fi
echo "PASS: Middle schema NULL-filled correctly"

echo ""
echo "=== Query 6: Verify late rows have ph but not chlorine ==="
LATE_NULLS=$(pond cat /derived/all-metrics --format=table --sql "
  SELECT COUNT(*) AS cnt FROM source
  WHERE atlasph_ph IS NOT NULL
    AND chlorine_level_value IS NULL
    AND pid_temperature_value IS NULL
" 2>&1 | grep -E '^\|' | tail -1 | tr -d '| ')
echo "Late rows (have ph, missing chlorine/pid_temp): ${LATE_NULLS}"
if [ "$LATE_NULLS" != "3" ]; then
    echo "FAIL: Expected 3 late rows with correct NULL pattern, got ${LATE_NULLS}"
    exit 1
fi
echo "PASS: Late schema NULL-filled correctly"

echo ""
echo "=== Query 7: Verify well_depth_value is non-NULL in ALL rows ==="
WD_COUNT=$(pond cat /derived/all-metrics --format=table --sql "
  SELECT COUNT(*) AS cnt FROM source WHERE well_depth_value IS NOT NULL
" 2>&1 | grep -E '^\|' | tail -1 | tr -d '| ')
echo "Rows with well_depth_value: ${WD_COUNT}"
if [ "$WD_COUNT" != "9" ]; then
    echo "FAIL: Expected well_depth_value in all 9 rows, got ${WD_COUNT}"
    exit 1
fi
echo "PASS: Common column present in all rows"

echo ""
echo "=== Query 8: Aggregation across heterogeneous schemas ==="
pond cat /derived/all-metrics --format=table --sql "
  SELECT
    COUNT(*) AS total_rows,
    COUNT(well_depth_value) AS well_depth_count,
    COUNT(pid_temperature_value) AS pid_temp_count,
    COUNT(system_pressure_value) AS pressure_count,
    COUNT(chlorine_level_value) AS chlorine_count,
    COUNT(atlasph_ph) AS ph_count,
    AVG(well_depth_value) AS avg_well_depth
  FROM source
"

echo ""
echo "=== All Checks Passed ==="
echo "Multi-file oteljson glob with variable schemas works correctly."
echo "The UNION produces the superset schema with proper NULL-fill."
