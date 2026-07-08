#!/bin/bash
# EXPERIMENT: Sitegen multi-year export -- full-rewrite fast path + cross-build
#             partition reuse (regression guard for PR #100).
#
# PR #100 replaced the sitegen export's single "COPY ... PARTITIONED BY" pass
# with a per-partition COPY (one filtered full-series rescan per Hive
# partition), causing a ~13x wall-clock regression on production builds with
# thousands of partitions. The fix restores a single partitioned COPY for the
# full-rewrite case while preserving incremental per-partition reuse on later
# builds.
#
# This exercises the sitegen export path (provider::export::
# export_table_provider_to_parquet), the ONLY caller of the reuse / manifest
# machinery. The source is an oteljson format-provider series feeding the
# temporal-reduce rollup, which is what publishes the ExportHint (digest +
# changed_since) that drives whole-series reuse -- exactly the production
# HydroVu/logingest -> reduce -> sitegen shape. (A plain series:/// source
# bypasses the rollup and would never publish a hint, so reuse could not be
# tested.)
#
# EXPECTED:
#   - A first sitegen build of a multi-year daily-reduced series produces
#     several Hive year= partitions via a single partitioned COPY, each with a
#     data.parquet, plus an export manifest carrying the reduction digest.
#   - Row counts are conserved and each year partition holds only its own year.
#   - A second identical build REUSES the historical partitions byte-for-byte
#     (unchanged mtime + inode) instead of rewriting them.
set -e

echo "=== Experiment: Sitegen multi-year export fast path + reuse ==="

# Extract the single integer value cell from a --format=table result, skipping
# the "| Int64 |" type-name header row (which also contains digits).
extract_num() {
    grep -oE '\| +[0-9]+ +\|' | head -1 | grep -oE '[0-9]+'
}

pond init --birthplace test-host

OUTDIR=/tmp/sitegen-dist

# --- Source: 800 daily OTelJSON samples spanning 2023..2025 -------------------
# One gauge point per day starting 2023-01-01 (epoch 1672531200). 800 days
# reaches into 2025, so the reduced series spans three year= partitions.
mkdir -p /var/log/well
awk 'BEGIN {
    start = 1672531200;   # 2023-01-01T00:00:00Z
    for (i = 0; i < 800; i++) {
        e = start + i * 86400;
        v = 20 + (i % 10);
        printf "{\"resourceMetrics\":[{\"resource\":{},\"scopeMetrics\":[{\"scope\":{\"name\":\"water\"},\"metrics\":[{\"name\":\"well_depth_value\",\"gauge\":{\"dataPoints\":[{\"timeUnixNano\":\"%d000000000\",\"asDouble\":%d.0}]}}]}]}]}\n", e, v;
    }
}' > /var/log/well/well.json

cat > /tmp/ingest.yaml << 'YAML'
archived_pattern: /var/log/well/well.json.*
active_pattern: /var/log/well/well.json
pond_path: /ingest
YAML
pond mkdir -p /system/run
pond mkdir -p /ingest
pond mknod logfile-ingest /system/run/10-well --config-path /tmp/ingest.yaml
pond run /system/run/10-well

# --- Reduce to 1-day resolution (oteljson rollup -> publishes ExportHint) -----
cat > /tmp/reduce.yaml << 'YAML'
in_pattern: "oteljson:///ingest/well.json"
out_pattern: "data"
time_column: "timestamp"
resolutions: ["1d"]
aggregations:
  - type: "avg"
    columns: ["well_depth_value"]
YAML
pond mknod temporal-reduce /reduced --config-path /tmp/reduce.yaml

echo ""
echo "=== 1. Reduced series row count ==="
REDUCED_ROWS=$(pond cat /reduced/data/res=1d.series --format=table \
  --sql "SELECT COUNT(*) AS cnt FROM source" | extract_num)
echo "reduced daily rows: ${REDUCED_ROWS}"
if [ "${REDUCED_ROWS}" -lt 700 ]; then
    echo "FAIL: expected >=700 daily rows across >2 years, got ${REDUCED_ROWS}"
    exit 1
fi

# --- Sitegen scaffolding ------------------------------------------------------
cat > /tmp/index.md << 'MD'
---
title: "Export Reuse Demo"
layout: default
---

# Export Reuse Demo

Multi-year rolled-up series exported with year partitioning.
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
## Export Reuse Demo

- [Home](/)
MD

pond mkdir /site
pond copy host:///tmp/index.md   /site/index.md
pond copy host:///tmp/data.md    /site/data.md
pond copy host:///tmp/sidebar.md /site/sidebar.md

cat > /tmp/site.yaml << 'YAML'
site:
  title: "Export Reuse Demo"
  base_url: "/"

exports:
  - name: "series"
    pattern: "/reduced/*/*.series"
    target_points: 4000

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/site/index.md"
  - name: "series"
    type: static
    slug: "series"
    routes:
      - name: "series-detail"
        type: template
        slug: "$0"
        page: "/site/data.md"
        export: "series"

partials:
  sidebar: "/site/sidebar.md"
YAML

pond mknod sitegen /site.yaml --config-path /tmp/site.yaml

# --- First build (full-rewrite fast path) -------------------------------------
echo ""
echo "=== 2. First sitegen build ==="
rm -rf "${OUTDIR}"
mkdir -p "${OUTDIR}"
pond run /site.yaml build "${OUTDIR}"

echo "--- exported data tree ---"
find "${OUTDIR}/data" -name '*.parquet' | sort

mapfile -t PARTS < <(find "${OUTDIR}/data" -path '*year=*' -name '*.parquet' | sort)
echo "year partition count: ${#PARTS[@]}"
if [ "${#PARTS[@]}" -lt 2 ]; then
    echo "FAIL: expected >=2 year partitions, got ${#PARTS[@]}"
    exit 1
fi

MANIFEST=$(find "${OUTDIR}/data" -name '.export-manifest.json' | head -1)
if [ -z "${MANIFEST}" ] || [ ! -f "${MANIFEST}" ]; then
    echo "FAIL: no export manifest written by the fast path"
    exit 1
fi
echo "manifest: ${MANIFEST}"
# The manifest must carry the reduction digest so the next build can reuse.
if grep -q '"digest": null' "${MANIFEST}"; then
    echo "FAIL: manifest digest is null -- reduction hint not published, reuse impossible"
    exit 1
fi

echo ""
echo "=== 3. Row conservation + per-year isolation ==="
TOTAL=0
for F in "${PARTS[@]}"; do
    Y=$(echo "$F" | grep -oE 'year=[0-9]+' | grep -oE '[0-9]+')
    N=$(pond cat "host+table://${F}" --format=table \
        --sql "SELECT COUNT(*) AS cnt FROM source" | extract_num)
    OFF=$(pond cat "host+table://${F}" --format=table \
        --sql "SELECT COUNT(*) AS bad FROM source WHERE EXTRACT(YEAR FROM timestamp) <> ${Y}" \
        | extract_num)
    echo "year=${Y}: rows=${N} off-year=${OFF}"
    if [ "${OFF}" -ne 0 ]; then
        echo "FAIL: year=${Y} partition contains ${OFF} rows from another year"
        exit 1
    fi
    TOTAL=$((TOTAL + N))
done
echo "sum of partition rows: ${TOTAL} (reduced rows: ${REDUCED_ROWS})"
if [ "${TOTAL}" -ne "${REDUCED_ROWS}" ]; then
    echo "FAIL: partition rows ${TOTAL} != reduced rows ${REDUCED_ROWS}"
    exit 1
fi

# --- Second build (must reuse historical partitions) --------------------------
echo ""
echo "=== 4. Second build into same dir -> reuse (no rewrite) ==="
HIST="${PARTS[0]}"   # earliest year partition -- can never change
MTIME_BEFORE=$(stat -c %Y "${HIST}")
INODE_BEFORE=$(stat -c %i "${HIST}")
echo "historical partition: ${HIST}"
sleep 2   # ensure a rewrite would bump mtime (1s resolution)
pond run /site.yaml build "${OUTDIR}"
MTIME_AFTER=$(stat -c %Y "${HIST}")
INODE_AFTER=$(stat -c %i "${HIST}")

echo "mtime before=${MTIME_BEFORE} after=${MTIME_AFTER}"
echo "inode before=${INODE_BEFORE} after=${INODE_AFTER}"
if [ "${MTIME_BEFORE}" != "${MTIME_AFTER}" ] || [ "${INODE_BEFORE}" != "${INODE_AFTER}" ]; then
    echo "FAIL: historical partition was rewritten on re-export (reuse broken)"
    exit 1
fi
echo "OK: historical partition reused unchanged"

# Partition set must be identical after re-export.
mapfile -t PARTS2 < <(find "${OUTDIR}/data" -path '*year=*' -name '*.parquet' | sort)
if [ "${#PARTS2[@]}" -ne "${#PARTS[@]}" ]; then
    echo "FAIL: re-export changed partition count ${#PARTS[@]} -> ${#PARTS2[@]}"
    exit 1
fi

echo ""
echo "=== VERIFICATION: PASSED ==="
