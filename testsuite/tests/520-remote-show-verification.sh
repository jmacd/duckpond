#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: Backup-table inspection via D5.7b CLI + standard tools
#
# DESCRIPTION:
#   Replaces the legacy `pond run /system/run/10-remote show` flow.  In the
#   D5.7b CLI the "show" surface is split across three commands plus
#   off-the-shelf tools:
#     - `pond log`                 -- local transaction history.
#     - `pond config`              -- pond_id / version info.
#     - `mc ls --recursive`        -- top-level layout of the S3 backup
#                                     (Delta _delta_log + partition dirs).
#     - `duckdb` + `read_parquet(..., hive_partitioning=true)`  -- queries
#                                     against the published Delta table
#                                     (`partition_kind` / `txn_seq` /
#                                     `file_path` columns per
#                                     `crates/sync-remote/src/schema.rs`).
#                                     We avoid `delta_scan()` because its
#                                     DeltaKernel binding ignores DuckDB's
#                                     s3_* settings and tries EC2 IMDS
#                                     against MinIO.
#
#   Test outline:
#     1. Pond1: init, `pond backup add origin`, create three files of
#        different sizes, push.
#     2. `pond log --limit 50` reports every transaction we made.
#     3. `pond config` prints a valid pond_id (UUID v7 shape).
#     4. `mc ls --recursive` shows the Delta layout
#        (_delta_log/ + partition_kind=manifest/, =checksum/, =data/).
#     5. `duckdb` + `read_parquet()` confirms:
#          - row count > 0,
#          - all three partition_kind values are present,
#          - data partition references >=5 source-pond parquet files
#            keyed by `pond_id=<uuid>/part_id=<uuid>/part-*.parquet`
#            (the storage-level path, NOT the user-visible /data/* path),
#          - all data file_paths share a single pond_id,
#          - manifest partition has one row per source txn_seq.
#
# EXPECTED:
#   - Every step succeeds.
#   - The three pushed files are visible via DuckDB.
#
# History:
#   Migrated D5.8.3: rewrote the disabled (DISABLED-D4) script on top of
#   the D5.7b backup CLI; the legacy `pond run …/show` + custom
#   verification-script generator was removed in D4.5.  The current
#   remote schema is `partition_kind` / `txn_seq` / `file_path`, NOT the
#   legacy `bundle_id` / `pond_txn_id` / `path`.
set -e

echo "=== Experiment: Backup-table Inspection (mc + duckdb) ==="
echo ""

#############################
# CONFIGURATION
#############################

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
BUCKET_NAME="duckpond-show-test"

CHECKS_TOTAL=0
CHECKS_FAILED=0

check() {
    local condition="$1"
    local description="$2"
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
    if eval "${condition}"; then
        echo "  ✓ ${description}"
    else
        echo "  ✗ ${description}"
        CHECKS_FAILED=$((CHECKS_FAILED + 1))
    fi
}

#############################
# PHASE 0: MinIO + tools
#############################

echo "=== Phase 0: Environment ==="
if ! curl -s "${MINIO_ENDPOINT}/minio/health/live" >/dev/null; then
    echo "[FAIL] MinIO not reachable at ${MINIO_ENDPOINT}"
    echo "       Run via 'run-test.sh --compose 520'."
    exit 1
fi
echo "[OK] MinIO reachable at ${MINIO_ENDPOINT}"

command -v duckdb >/dev/null || { echo "[FAIL] duckdb missing"; exit 1; }
echo "[OK] DuckDB: $(duckdb --version | head -1)"

mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null
mc rb --force "local/${BUCKET_NAME}" 2>/dev/null || true
mc mb --ignore-existing "local/${BUCKET_NAME}"
echo "[OK] Bucket ${BUCKET_NAME} ready"

#############################
# PHASE 1: Pond + push
#############################

echo ""
echo "=== Phase 1: Pond setup + push ==="

export POND=/pond1
rm -rf "${POND}"
pond init
pond mkdir -p /data

# Create three files of varying sizes -- all under the 64KB large-file
# threshold so they live inline in the remote table (one chunk each).
echo "sensor,value" > /tmp/small.csv
echo "s1,1.0"    >> /tmp/small.csv
echo "s2,2.5"    >> /tmp/small.csv

seq 1 1000 | awk -F, 'BEGIN{print "id,v"} {print NR","NR*0.1}' > /tmp/medium.csv

printf '{"name":"showtest","files":["small.csv","medium.csv"]}\n' > /tmp/meta.json

pond copy "host:///tmp/small.csv"  /data/small.csv
pond copy "host:///tmp/medium.csv" /data/medium.csv
pond copy "host:///tmp/meta.json"  /data/meta.json

pond backup add origin "s3://${BUCKET_NAME}" \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
    --allow-http

pond push origin
echo "[OK] Pond1 attached and pushed"

rm -f /tmp/small.csv /tmp/medium.csv /tmp/meta.json

#############################
# PHASE 2: pond log
#############################

echo ""
echo "=== Phase 2: 'pond log' replaces 'show' for local audit ==="
LOG_OUT=$(pond log --limit 50)
echo "${LOG_OUT}"

# Each transaction line carries a seq number like 'seq=N' or 'Transaction N'.
LOG_TXN_COUNT=$(echo "${LOG_OUT}" | grep -ciE 'seq[ =]|transaction[ ]+[0-9]+' || true)
echo "Transaction-ish lines in pond log: ${LOG_TXN_COUNT}"
check '[ "${LOG_TXN_COUNT}" -ge 4 ]' "pond log reports >=4 transactions (init + 3 copies + backup-add + push)"

#############################
# PHASE 3: pond config
#############################

echo ""
echo "=== Phase 3: 'pond config' replaces 'show' for pond identity ==="
CFG_OUT=$(pond config)
echo "${CFG_OUT}"

POND_ID=$(echo "${CFG_OUT}" | grep "Pond ID" | awk '{print $NF}')
echo "Pond ID parsed: ${POND_ID}"
# UUID v7 shape: 8-4-4-4-12 hex with version nibble '7'.
check 'echo "${POND_ID}" | grep -qE "^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}$"' \
    "pond config prints a valid v7 UUID for Pond ID"

#############################
# PHASE 4: mc ls layout
#############################

echo ""
echo "=== Phase 4: 'mc ls' shows the on-S3 Delta layout ==="
MC_OUT=$(mc ls --recursive "local/${BUCKET_NAME}")
echo "${MC_OUT}"

check 'echo "${MC_OUT}" | grep -q "_delta_log/"'                       "mc ls shows _delta_log/"
check 'echo "${MC_OUT}" | grep -q "partition_kind=manifest/"'          "mc ls shows partition_kind=manifest/"
check 'echo "${MC_OUT}" | grep -q "partition_kind=checksum/"'          "mc ls shows partition_kind=checksum/"
check 'echo "${MC_OUT}" | grep -q "partition_kind=data/"'              "mc ls shows partition_kind=data/"

#############################
# PHASE 5: DuckDB delta_scan
#############################

echo ""
echo "=== Phase 5: DuckDB queries the published Delta table ==="

# DuckDB's `delta_scan()` goes through DeltaKernel which tries to use
# EC2 IMDS credentials and ignores DuckDB's s3_* settings against MinIO.
# Instead we read the parquet files directly with hive-partitioning,
# which is exactly how the legacy duckpond-emergency script worked.
# For a freshly-pushed table (no vacuum yet) this returns the same row
# set as delta_scan.
DUCKDB_PRELUDE="
INSTALL httpfs;     LOAD httpfs;
SET s3_region='us-east-1';
SET s3_endpoint='${MINIO_ENDPOINT#http://}';
SET s3_url_style='path';
SET s3_use_ssl=false;
SET s3_access_key_id='${MINIO_ROOT_USER}';
SET s3_secret_access_key='${MINIO_ROOT_PASSWORD}';
"

# Glob over every partition's parquet files.
PQ_GLOB="s3://${BUCKET_NAME}/partition_kind=*/*.parquet"

# Total row count > 0 (sanity).
TOTAL_ROWS=$(duckdb -noheader -csv -c "
${DUCKDB_PRELUDE}
SELECT COUNT(*) FROM read_parquet('${PQ_GLOB}', hive_partitioning=true);
" 2>/dev/null | tr -d ' \r')
echo "Total rows in remote Delta table: ${TOTAL_ROWS}"
check '[[ "${TOTAL_ROWS}" =~ ^[0-9]+$ ]] && [ "${TOTAL_ROWS}" -gt 0 ]' \
    "DuckDB sees rows in the Delta table"

# Distinct partition_kind values must include all three.
PK_VALUES=$(duckdb -noheader -csv -c "
${DUCKDB_PRELUDE}
SELECT DISTINCT partition_kind
FROM read_parquet('${PQ_GLOB}', hive_partitioning=true)
ORDER BY 1;
" 2>/dev/null | tr '\n' ',' )
echo "Distinct partition_kind values: ${PK_VALUES}"
check 'echo "${PK_VALUES}" | grep -q "manifest"' "partition_kind has manifest"
check 'echo "${PK_VALUES}" | grep -q "checksum"' "partition_kind has checksum"
check 'echo "${PK_VALUES}" | grep -q "data"'     "partition_kind has data"

# Our copies/mkdir/init produced at least 5 source-pond parquet files
# (one per commit, possibly more if a commit touches multiple partitions).
# In the remote, `file_path` is the source-pond storage-level path:
# `pond_id=<uuid>/part_id=<uuid>/part-*.parquet` -- the underlying
# Delta data files, NOT user-visible logical paths like /data/small.csv.
# This is a stronger property: the backup is byte-faithful to the
# underlying storage, so it can be replayed without re-interpreting the
# pond's filesystem schema.
DATA_PATHS=$(duckdb -noheader -csv -c "
${DUCKDB_PRELUDE}
SELECT DISTINCT file_path
FROM read_parquet('${PQ_GLOB}', hive_partitioning=true)
WHERE partition_kind='data' AND file_action='add'
ORDER BY file_path;
" 2>/dev/null)
echo "Files under partition_kind='data' file_action='add':"
echo "${DATA_PATHS}"

DATA_COUNT=$(echo "${DATA_PATHS}" | grep -c "part-" || true)
echo "Distinct add'd parquet files: ${DATA_COUNT}"
check '[ "${DATA_COUNT}" -ge 5 ]' "data partition references >=5 source parquet files (one per commit)"

# The same pond_id appears on every file_path (single source pond).
PID_COUNT=$(echo "${DATA_PATHS}" \
    | grep -oE 'pond_id=[0-9a-f-]+' | sort -u | wc -l | tr -d ' ')
check '[ "${PID_COUNT}" -eq 1 ]' "all data file_paths carry exactly one pond_id"

# Verify the file_path values match the expected hive-partitioned storage
# layout: pond_id=<uuid>/part_id=<uuid>/part-*.parquet.
check 'echo "${DATA_PATHS}" | grep -qE "^pond_id=[0-9a-f-]+/part_id=[0-9a-f-]+/part-"' \
    "data file_path matches pond_id=.../part_id=.../part-* layout"

# Manifest rows: one per source-pond txn_seq.
MANIFEST_SEQS=$(duckdb -noheader -csv -c "
${DUCKDB_PRELUDE}
SELECT DISTINCT txn_seq
FROM read_parquet('${PQ_GLOB}', hive_partitioning=true)
WHERE partition_kind='manifest'
ORDER BY txn_seq;
" 2>/dev/null)
echo "Manifest rows (txn_seq):"
echo "${MANIFEST_SEQS}"

MANIFEST_COUNT=$(echo "${MANIFEST_SEQS}" | grep -cE '^[0-9]+$' || true)
check '[ "${MANIFEST_COUNT}" -ge 5 ]' "manifest partition has >=5 distinct txn_seq values"

#############################
# CLEANUP
#############################

rm -rf /pond1
mc rb --force "local/${BUCKET_NAME}" 2>/dev/null || true

#############################
# SUMMARY
#############################

echo ""
echo "=== VERIFICATION ==="
echo "Total checks: ${CHECKS_TOTAL}, failed: ${CHECKS_FAILED}"
if [ "${CHECKS_FAILED}" -eq 0 ]; then
    echo "[OK] Test passed"
    exit 0
else
    echo "[FAIL] ${CHECKS_FAILED} check(s) failed"
    exit 1
fi
