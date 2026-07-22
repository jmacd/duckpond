#!/bin/bash
# EXPERIMENT: temporal-reduce rollup vs. `maintain --collapse-versions` --
#   allowed_lateness governs whether a collapsed source tail is tolerated.
#
# DESCRIPTION:
#   Reproduces the caspar.water site-staging outage where the whole sitegen
#   build aborted for days. The water pond runs an hourly
#   `maintain --compact --collapse-versions 100`, which rewrites roughly the
#   last ~100 versions (~4 days) of each reduced series' source into a single
#   merged version. A downstream temporal-reduce rollup that has already sealed
#   buckets under the DEFAULT 1-day allowed_lateness then sees that merged tail
#   as data landing behind its sealed watermark and hard-fails:
#
#     temporal-reduce rollup: source data backfills an already-sealed bucket
#     (earliest new output bucket ... precedes the sealed watermark ...;
#     allowed_lateness = 86400s). ... Re-run the export with --rebuild ...
#
#   which aborts the entire transaction until a manual `--rebuild`. The
#   durable fix is to raise allowed_lateness so the collapse window stays
#   inside the unsealed hot window (config/water.yaml sets 14d on every
#   reduced instrument).
#
#   Both reduced nodes below read the SAME collapsed source; only their
#   allowed_lateness differs, isolating the knob under test.
#
# EXPECTED:
#   - Default (1d) lateness: after the source versions are collapsed, reading
#     the reduced series raises the sealed-bucket error.
#   - allowed_lateness=14d: the same collapse is tolerated; the reduced series
#     reads cleanly with every bucket present.
set -e
source check.sh

echo "=== Experiment: temporal-reduce collapse vs allowed_lateness ==="

export POND=/pond
pond init --birthplace test-host >/dev/null

# ---- Source: 96 hourly OTelJSON well_depth samples spanning 4 days ----------
# 2026-07-15T00:00Z (epoch 1784073600) .. 2026-07-18T23:00Z, one point/hour.
# Four days comfortably exceeds the default 1-day lateness, so a collapsed
# full-range version backfills buckets already sealed by the first read.
mkdir -p /var/log/well
awk 'BEGIN {
    start = 1784073600;
    for (i = 0; i < 96; i++) {
        e = start + i * 3600;
        v = 44.0 + (i % 5) * 0.1;
        printf "{\"resourceMetrics\":[{\"resource\":{},\"scopeMetrics\":[{\"scope\":{\"name\":\"water\"},\"metrics\":[{\"name\":\"well_depth_value\",\"gauge\":{\"dataPoints\":[{\"timeUnixNano\":\"%d000000000\",\"asDouble\":%.1f}]}}]}]}]}\n", e, v;
    }
}' > /tmp/050-all.jsonl

cat > /tmp/050-ingest.yaml << 'EOF'
archived_pattern: /var/log/well/well.json.*
active_pattern: /var/log/well/well.json
pond_path: /ingest
EOF

pond mkdir -p /system/run >/dev/null
pond mkdir -p /ingest >/dev/null
pond mknod logfile-ingest /system/run/10-well --config-path /tmp/050-ingest.yaml >/dev/null

# ---- Accumulate multiple source versions (24 samples per ingest run) --------
# Multiple append versions give `maintain --collapse-versions` something to
# merge, mirroring the many hourly ingest versions on water-staging.
: > /var/log/well/well.json
split -l 24 /tmp/050-all.jsonl /tmp/050-chunk.
for c in /tmp/050-chunk.*; do
    cat "$c" >> /var/log/well/well.json
    pond run /system/run/10-well >/dev/null 2>&1
done

ROWS=$(pond cat oteljson:///ingest/well.json --format=table \
  --sql "SELECT COUNT(*) AS c FROM source" 2>&1 \
  | grep -E '^\| *[0-9]' | head -1 | grep -oE '[0-9]+' | head -1)

# ---- Two reduced nodes over the same source, differing only in lateness -----
cat > /tmp/050-reduce-default.yaml << 'YAML'
in_pattern: "oteljson:///ingest/well.json"
out_pattern: "data"
time_column: "timestamp"
resolutions: ["1h"]
aggregations:
  - type: "avg"
    columns: ["well_depth_value"]
YAML

cat > /tmp/050-reduce-late.yaml << 'YAML'
in_pattern: "oteljson:///ingest/well.json"
out_pattern: "data"
time_column: "timestamp"
resolutions: ["1h"]
allowed_lateness: 14d
aggregations:
  - type: "avg"
    columns: ["well_depth_value"]
YAML

pond mknod temporal-reduce /reduced-default --config-path /tmp/050-reduce-default.yaml >/dev/null
pond mknod temporal-reduce /reduced-late --config-path /tmp/050-reduce-late.yaml >/dev/null

# ---- Seal both rollups by reading them BEFORE the collapse ------------------
# The first read builds the rollup cache and seals buckets older than
# newest - allowed_lateness.
echo ""
echo "--- Seal both rollups (first read) ---"
SEAL_DEFAULT=$(pond cat /reduced-default/data/res=1h.series --format=table \
  --sql "SELECT COUNT(*) AS c FROM source" 2>&1 \
  | grep -E '^\| *[0-9]' | head -1 | grep -oE '[0-9]+' | head -1)
SEAL_LATE=$(pond cat /reduced-late/data/res=1h.series --format=table \
  --sql "SELECT COUNT(*) AS c FROM source" 2>&1 \
  | grep -E '^\| *[0-9]' | head -1 | grep -oE '[0-9]+' | head -1)

# ---- Collapse the source versions (the production trigger) ------------------
echo ""
echo "--- maintain --collapse-versions 1 (rewrites the source tail) ---"
pond maintain --collapse-versions 1 2>&1 | tee /tmp/050-collapse.log | grep -i collapse

# ---- Second read: default lateness must fail, 14d lateness must succeed -----
echo ""
echo "--- Second read: default (1d) lateness ---"
DEFAULT_OUT=$(pond cat /reduced-default/data/res=1h.series --format=table \
  --sql "SELECT COUNT(*) AS c FROM source" 2>&1 || true)
echo "$DEFAULT_OUT"

echo ""
echo "--- Second read: allowed_lateness=14d ---"
LATE_OUT=$(pond cat /reduced-late/data/res=1h.series --format=table \
  --sql "SELECT COUNT(*) AS c FROM source" 2>&1 || true)
echo "$LATE_OUT"
LATE_ROWS=$(echo "$LATE_OUT" | grep -E '^\| *[0-9]' | head -1 | grep -oE '[0-9]+' | head -1)

# ---- Verify -----------------------------------------------------------------
echo ""
echo "--- Verification ---"

check '[ "${ROWS}" = "96" ]' \
  "source ingested 96 hourly points across versions, got ${ROWS}"

check 'grep -qE "collapse: [1-9][0-9]* file\(s\) collapsed" /tmp/050-collapse.log' \
  "maintain collapsed the source version tail"

check '[ "${SEAL_DEFAULT}" = "96" ] && [ "${SEAL_LATE}" = "96" ]' \
  "both rollups read all 96 buckets on the sealing pass"

check 'echo "$DEFAULT_OUT" | grep -qi "backfills an already-sealed bucket"' \
  "default 1d lateness rejects the collapsed tail (reproduces the outage)"

check '! echo "$LATE_OUT" | grep -qiE "backfills|sealed|--rebuild"' \
  "allowed_lateness=14d tolerates the collapsed tail (the fix)"

check '[ "${LATE_ROWS}" = "96" ]' \
  "14d reduced series still reads all 96 buckets after collapse, got ${LATE_ROWS}"

check_finish
