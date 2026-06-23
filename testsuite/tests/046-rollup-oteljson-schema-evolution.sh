#!/bin/bash
# EXPERIMENT: temporal-reduce incremental rollup over oteljson sources whose
#   schema evolves across files (a metric is renamed/added over history).
# DESCRIPTION: Reproduces the watershop site-staging regression. An oteljson
#   source builds a per-file wide schema from the metric names present in that
#   file, so a metric added or renamed over history is MISSING from older files.
#
#   When several glob-matched files share ONE fixed out_pattern partition (as in
#   the real water.yaml: out_pattern "data"), schema discovery unions the columns
#   across files and finds the newer column (concrete_tank_level_value). The
#   incremental rollup then computes a per-FILE partial; the older file lacks the
#   column, so the partial SQL planned "SUM(concrete_tank_level_value)" against a
#   file that has no such field and failed:
#
#     rollup partial SQL planning failed: Schema error:
#     No field named concrete_tank_level_value.
#
#   The single-pass union path never hit this (an absent column reads as NULL),
#   so test 039/040 (which use out_pattern "$0", a 1:1 source->output mapping)
#   did not catch it. This test uses a FIXED out_pattern so multiple files feed
#   one partition, exercising the per-version partial path that regressed.
#
# EXPECTED: Reading the reduced series succeeds. It carries the newer column with
#   real values for buckets from the newer file and NULLs for buckets from the
#   older file that predates the column. Reading drives the same per-version
#   rollup partial path that the sitegen export originally failed on.
set -e
source check.sh

echo "=== Experiment: Rollup over oteljson schema evolution ==="

pond init --birthplace test-host

# ---- Setup: two oteljson files with evolving schemas ----
#
# OLDER file (2024): well_depth_value only. Predates the tank-level metric.
# NEWER file (2025): well_depth_value + concrete_tank_level_value.
#
# Two hourly buckets per file, two samples per bucket, so avg/min/max are
# non-trivial.

mkdir -p /tmp/otel-evo

cat > /tmp/otel-evo/metrics-2024.json << 'OTELJSON'
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1704067200000000000","asDouble":42.5}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1704068100000000000","asDouble":42.3}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1704070800000000000","asDouble":41.0}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1704071700000000000","asDouble":41.2}]}}]}]}]}
OTELJSON

cat > /tmp/otel-evo/metrics-2025.json << 'OTELJSON'
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1735689600000000000","asDouble":44.0}]}},{"name":"concrete_tank_level_value","gauge":{"dataPoints":[{"timeUnixNano":"1735689600000000000","asDouble":10.0}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1735690500000000000","asDouble":44.1}]}},{"name":"concrete_tank_level_value","gauge":{"dataPoints":[{"timeUnixNano":"1735690500000000000","asDouble":10.2}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1735693200000000000","asDouble":43.0}]}},{"name":"concrete_tank_level_value","gauge":{"dataPoints":[{"timeUnixNano":"1735693200000000000","asDouble":11.0}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1735694100000000000","asDouble":43.5}]}},{"name":"concrete_tank_level_value","gauge":{"dataPoints":[{"timeUnixNano":"1735694100000000000","asDouble":11.5}]}}]}]}]}
OTELJSON

echo "--- Ingest both files ---"
pond mkdir -p /ingest
pond copy "host:///tmp/otel-evo/metrics-2024.json" /ingest/metrics-2024.json
pond copy "host:///tmp/otel-evo/metrics-2025.json" /ingest/metrics-2025.json
pond list '/ingest/*'

# ---- temporal-reduce with a FIXED out_pattern (all files -> one partition) ----
#
# This mirrors water.yaml exactly: in_pattern is a multi-file oteljson glob and
# out_pattern is the constant "data", so both files feed the single "data"
# partition. The aggregations explicitly reference concrete_tank_level_value,
# which only the 2025 file provides.

cat > /tmp/reduce.yaml << 'YAML'
in_pattern: "oteljson:///ingest/metrics-*.json"
out_pattern: "data"
time_column: "timestamp"
resolutions: ["1h"]
aggregations:
  - type: "avg"
    columns: ["concrete_tank_level_value", "well_depth_value"]
  - type: "min"
    columns: ["concrete_tank_level_value", "well_depth_value"]
  - type: "max"
    columns: ["concrete_tank_level_value", "well_depth_value"]
YAML

pond mknod temporal-reduce /reduced --config-path /tmp/reduce.yaml

echo ""
echo "--- Reduced tree ---"
pond list '/reduced/**/*'

# ---- Execute: read + export, both of which drive the rollup partial path ----

echo ""
echo "--- pond cat (drives per-file rollup partials) ---"
CAT_OUT=$(pond cat /reduced/data/res=1h.series --format=table --sql "
  SELECT
    \"well_depth_value.avg\",
    \"concrete_tank_level_value.avg\",
    \"concrete_tank_level_value.min\",
    \"concrete_tank_level_value.max\"
  FROM source ORDER BY timestamp" 2>&1 || true)
echo "$CAT_OUT"

# Count buckets where the late-appearing column is non-null. Only the two 2025
# buckets have it; the two 2024 buckets must be NULL (the column did not exist).
TANK_NONNULL=$(pond cat /reduced/data/res=1h.series --format=table --sql "
  SELECT COUNT(*) AS cnt FROM source
  WHERE \"concrete_tank_level_value.avg\" IS NOT NULL" 2>&1 \
  | grep -E '^\| *[0-9]' | head -1 | grep -oE '[0-9]+' | head -1)

TOTAL_BUCKETS=$(pond cat /reduced/data/res=1h.series --format=table --sql "
  SELECT COUNT(*) AS cnt FROM source" 2>&1 \
  | grep -E '^\| *[0-9]' | head -1 | grep -oE '[0-9]+' | head -1)

WD_NONNULL=$(pond cat /reduced/data/res=1h.series --format=table --sql "
  SELECT COUNT(*) AS cnt FROM source
  WHERE \"well_depth_value.avg\" IS NOT NULL" 2>&1 \
  | grep -E '^\| *[0-9]' | head -1 | grep -oE '[0-9]+' | head -1)

# Exact reconstructed value for the first newer-file bucket: avg(10.0, 10.2).
TANK_AVG_2025=$(pond cat /reduced/data/res=1h.series --format=table --sql "
  SELECT \"concrete_tank_level_value.avg\" AS v FROM source
  WHERE \"concrete_tank_level_value.avg\" IS NOT NULL ORDER BY timestamp LIMIT 1" 2>&1 \
  | grep -E '^\| *[0-9]' | head -1 | grep -oE '[0-9]+\.[0-9]+' | head -1)

# ---- Verify ----
#
# pond cat builds the temporal-reduce table provider, which runs the same
# incremental rollup per-version partial path that the sitegen export
# (export_series_to_parquet) drove when it regressed on watershop. The regression
# was in partial SQL planning, before any export/COPY step, so reading the series
# exercises it directly.

echo ""
echo "--- Verification ---"

check '! echo "$CAT_OUT" | grep -qi "error"' \
  "reading the reduced series does not raise a schema error"

check '! echo "$CAT_OUT" | grep -q "No field named concrete_tank_level_value"' \
  "no missing-column schema error from the per-version partial path"

check '[ "${TOTAL_BUCKETS}" = "4" ]' \
  "four hourly buckets produced (two per file), got ${TOTAL_BUCKETS}"

check '[ "${WD_NONNULL}" = "4" ]' \
  "well_depth (present in all files) non-null in all 4 buckets, got ${WD_NONNULL}"

check '[ "${TANK_NONNULL}" = "2" ]' \
  "concrete_tank_level non-null only in the 2 newer-file buckets, got ${TANK_NONNULL}"

check '[ "${TANK_AVG_2025}" = "10.1" ]' \
  "newer-file bucket reconstructs avg(10.0,10.2)=10.1, got ${TANK_AVG_2025}"

check_finish
