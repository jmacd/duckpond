#!/bin/bash
# EXPERIMENT: temporal-reduce per-output-partition isolation (rollup path)
# DESCRIPTION: When out_pattern maps each globbed source file to its OWN output
#   partition, each partition's reduced series must contain ONLY that file's
#   columns and rows. The incremental rollup must scope to the partition's
#   pattern_url, never the original in_pattern glob.
#
#   Regression guard for the bug where resolve_source_files() resolved the
#   global in_pattern glob, so every partition's rollup looped over ALL sibling
#   files. Two files with DISJOINT extra columns expose the bleed in both
#   directions: the partial SQL for one partition referenced a column absent
#   from a sibling parquet ("No field named ...") and outputs mixed sibling
#   rows/columns. 040 covers growing schemas; this covers disjoint schemas and
#   strict per-partition isolation.
#
# EXPECTED: /reduced/old has well_depth + legacy_flow (NOT ph); /reduced/new has
#   well_depth + ph (NOT legacy_flow); neither partition contains the other's
#   timestamps.
source check.sh
set -e

echo "=== Experiment: temporal-reduce partition isolation ==="

pond init --birthplace test-host

#############################
# TWO SOURCE FILES, DISJOINT EXTRA COLUMNS, DISTINCT OUTPUT PARTITIONS
#############################

mkdir -p /tmp/oteljson

# data-old.json -> partition "old" (2024-01): well_depth_value + legacy_flow_value
cat > /tmp/oteljson/data-old.json << 'OTELJSON'
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1704067200000000000","asDouble":40.1}]}},{"name":"legacy_flow_value","gauge":{"dataPoints":[{"timeUnixNano":"1704067200000000000","asDouble":5.10}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1704070800000000000","asDouble":40.2}]}},{"name":"legacy_flow_value","gauge":{"dataPoints":[{"timeUnixNano":"1704070800000000000","asDouble":5.20}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1704074400000000000","asDouble":40.3}]}},{"name":"legacy_flow_value","gauge":{"dataPoints":[{"timeUnixNano":"1704074400000000000","asDouble":5.30}]}}]}]}]}
OTELJSON

# data-new.json -> partition "new" (2025-01): well_depth_value + ph_value
cat > /tmp/oteljson/data-new.json << 'OTELJSON'
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1735689600000000000","asDouble":42.1}]}},{"name":"ph_value","gauge":{"dataPoints":[{"timeUnixNano":"1735689600000000000","asDouble":7.10}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1735693200000000000","asDouble":42.2}]}},{"name":"ph_value","gauge":{"dataPoints":[{"timeUnixNano":"1735693200000000000","asDouble":7.15}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"water"},"metrics":[{"name":"well_depth_value","gauge":{"dataPoints":[{"timeUnixNano":"1735696800000000000","asDouble":42.3}]}},{"name":"ph_value","gauge":{"dataPoints":[{"timeUnixNano":"1735696800000000000","asDouble":7.20}]}}]}]}]}
OTELJSON

echo "=== Ingesting 2 files ==="
pond mkdir -p /ingest
pond copy "host:///tmp/oteljson/data-old.json" /ingest/data-old.json
pond copy "host:///tmp/oteljson/data-new.json" /ingest/data-new.json

#############################
# TEMPORAL-REDUCE: each file -> its own output partition via $0
#############################

cat > /tmp/reduce.yaml << 'YAML'
in_pattern: "oteljson:///ingest/data-*.json"
out_pattern: "$0"
time_column: "timestamp"
resolutions: ["1h", "6h"]
aggregations:
  - type: "avg"
YAML

pond mknod temporal-reduce /reduced --config-path /tmp/reduce.yaml

echo "=== Partitions ==="
pond list '/reduced/**/*'

#############################
# VERIFICATION
#############################

# Dump each partition's full schema+rows. SELECT * surfaces every output column
# and every row, so column bleed and row bleed both show up in the dump.
pond cat /reduced/new/res=1h.series --format=table --sql "SELECT * FROM source ORDER BY timestamp" > /tmp/new_1h.txt 2>&1
pond cat /reduced/old/res=1h.series --format=table --sql "SELECT * FROM source ORDER BY timestamp" > /tmp/old_1h.txt 2>&1
pond cat /reduced/new/res=6h.series --format=table --sql "SELECT * FROM source ORDER BY timestamp" > /tmp/new_6h.txt 2>&1

echo ""
echo "--- new partition (1h) ---"
cat /tmp/new_1h.txt

echo ""
echo "--- old partition (1h) ---"
cat /tmp/old_1h.txt

echo ""
echo "=== CHECKS ==="

# The read itself must succeed: before the fix the rollup partial SQL for "new"
# ran over the "old" sibling parquet and failed planning with "No field named".
check_not_contains /tmp/new_1h.txt "new partition reads without error" "Error"
check_not_contains /tmp/old_1h.txt "old partition reads without error" "Error"

# Column isolation: each partition has its own metric + the shared one, and NOT
# the sibling's exclusive metric.
check_contains     /tmp/new_1h.txt "new has its own ph_value"        "ph_value.avg"
check_contains     /tmp/new_1h.txt "new has shared well_depth_value" "well_depth_value.avg"
check_not_contains /tmp/new_1h.txt "new lacks sibling legacy_flow"   "legacy_flow_value"

check_contains     /tmp/old_1h.txt "old has its own legacy_flow"     "legacy_flow_value.avg"
check_contains     /tmp/old_1h.txt "old has shared well_depth_value" "well_depth_value.avg"
check_not_contains /tmp/old_1h.txt "old lacks sibling ph_value"      "ph_value"

# Row isolation: timestamps must come only from each partition's own file.
check_contains     /tmp/new_1h.txt "new has 2025 rows"        "2025-01-01"
check_not_contains /tmp/new_1h.txt "new has no 2024 bleed"    "2024-01-01"
check_contains     /tmp/old_1h.txt "old has 2024 rows"        "2024-01-01"
check_not_contains /tmp/old_1h.txt "old has no 2025 bleed"    "2025-01-01"

# Values must be this file's data, not a sibling-merged average.
check_contains     /tmp/new_1h.txt "new ph value is from new file"   "7.1"
check_contains     /tmp/old_1h.txt "old flow value is from old file" "5.1"

# Coarser resolution (shared finest partials) stays isolated too.
check_not_contains /tmp/new_6h.txt "new 6h reads without error"  "Error"
check_contains     /tmp/new_6h.txt "new 6h has ph_value"         "ph_value.avg"
check_not_contains /tmp/new_6h.txt "new 6h lacks legacy_flow"    "legacy_flow_value"

check_finish
