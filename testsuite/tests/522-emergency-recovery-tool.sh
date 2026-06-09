#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: Emergency Recovery Tool (duckpond-emergency) Verification
#
# DESCRIPTION:
#   End-to-end coverage of the `duckpond-emergency` disaster-recovery
#   script on top of the D5.7b backup CLI + the D5.x Delta-Lake-native
#   remote schema (partition_kind in {manifest, checksum, data}, with
#   chunk_data BLOBs per file).
#
#   The emergency script lets a user recover the *underlying storage*
#   of a source pond using only `duckdb` + `b3sum` + `xxd` -- no pond
#   binary required.  This test proves that path works:
#
#     Phase 1.  Pond1: init + `pond backup add origin s3://BUCKET`
#               + write some files into /data + push.
#     Phase 2.  `duckpond-emergency s3://BUCKET info` reports the
#               expected sections (snapshot summary, per-partition
#               counts, manifest commits).  We cross-check the
#               substantive counts (live_files >= 3, manifest writes
#               >= 4) with direct DuckDB queries.
#     Phase 3.  `duckpond-emergency s3://BUCKET list` lists the
#               pond_id=*/part_id=*/part-*.parquet source-pond
#               storage paths that make up the current snapshot.
#               (Note: the source pond's _delta_log/ is NOT replicated
#               to the remote -- only the parquet files are -- so the
#               remote is enough to reconstruct the source pond's data
#               but not its on-disk Delta protocol log verbatim.)
#     Phase 4.  Pick one specific storage parquet and run
#               `duckpond-emergency ... extract <path>` to recover its
#               bytes.  Confirm PAR1 magic + DuckDB readability.
#     Phase 5.  `duckpond-emergency s3://BUCKET verify` re-extracts
#               every snapshot file and confirms b3sum matches the
#               stored file_blake3.  This is the keystone check:
#               every byte round-trips through DuckDB + xxd correctly.
#     Phase 6.  `duckpond-emergency s3://BUCKET export-all DIR`
#               recovers every snapshot file; spot-check that one is
#               a valid parquet.
#
# EXPECTED:
#   - All 5 emergency-tool subcommands work without the pond binary.
#   - Every BLAKE3 round-trip matches (proving byte-faithful backup).
#   - extract/export-all produce one file per source-pond storage path.
#
# REQUIREMENTS (installed in container):
#   - duckdb, b3sum, xxd, jq, mc, pond
#
# History:
#   Migrated D5.8.4: rewritten on top of the D5.7b backup CLI
#   (`pond backup add`, `pond push`) and the rewritten `duckpond-
#   emergency` script (v2.0.0) targeting the current Delta-Lake-
#   native remote schema.  The legacy version pre-dated D4.5 and
#   queried a long-removed chunked-parquet schema (bundle_id, path,
#   root_hash, pond_txn_id) via the equally-removed
#   `pond run /system/run/10-remote` flow.
set -e

echo "=== Experiment: Emergency Recovery Tool Verification ==="
echo ""

#############################
# CONFIGURATION
#############################

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
BUCKET_NAME="duckpond-emergency-test"
BUCKET_URL="s3://${BUCKET_NAME}"

CHECKS_TOTAL=0
CHECKS_FAILED=0

check() {
    local condition="$1"
    local description="$2"
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
    if eval "${condition}"; then
        echo "  [OK] ${description}"
    else
        echo "  [FAIL] ${description}"
        CHECKS_FAILED=$((CHECKS_FAILED + 1))
    fi
}

check_eq() {
    local description="$1"
    local actual="$2"
    local expected="$3"
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
    if [[ "${actual}" == "${expected}" ]]; then
        echo "  [OK] ${description} (${actual})"
    else
        echo "  [FAIL] ${description}: got ${actual}, expected ${expected}"
        CHECKS_FAILED=$((CHECKS_FAILED + 1))
    fi
}

check_ge() {
    local description="$1"
    local actual="$2"
    local lower="$3"
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
    if [[ "${actual}" -ge "${lower}" ]]; then
        echo "  [OK] ${description} (${actual} >= ${lower})"
    else
        echo "  [FAIL] ${description}: got ${actual}, expected >= ${lower}"
        CHECKS_FAILED=$((CHECKS_FAILED + 1))
    fi
}

#############################
# CHECK PREREQUISITES
#############################

echo "=== Checking prerequisites ==="
for tool in duckdb b3sum xxd jq mc pond duckpond-emergency; do
    if command -v "${tool}" >/dev/null 2>&1; then
        echo "[OK] ${tool} available"
    else
        echo "[FAIL] ${tool} required but not installed"
        exit 1
    fi
done

if ! curl -s "${MINIO_ENDPOINT}/minio/health/live" >/dev/null; then
    echo "[FAIL] MinIO not reachable at ${MINIO_ENDPOINT}"
    echo "       This test must run via 'run-test.sh --compose 522'."
    exit 1
fi
echo "[OK] MinIO reachable at ${MINIO_ENDPOINT}"

mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null
mc rb --force "local/${BUCKET_NAME}" 2>/dev/null || true
mc mb --ignore-existing "local/${BUCKET_NAME}"
echo "[OK] Bucket ${BUCKET_NAME} ready"

# Configure environment for duckpond-emergency S3 access
export AWS_ENDPOINT_URL="${MINIO_ENDPOINT}"
export AWS_REGION="us-east-1"
export AWS_ACCESS_KEY_ID="${MINIO_ROOT_USER}"
export AWS_SECRET_ACCESS_KEY="${MINIO_ROOT_PASSWORD}"

#############################
# PHASE 1: POND1 ATTACH + PUSH
#############################

echo ""
echo "=== Phase 1: Pond1 setup + push ==="

export POND=/pond1
rm -rf "${POND}"
pond init

pond backup add origin "${BUCKET_URL}" \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
    --allow-http
echo "[OK] origin attached"

# Write three small files with known content so we can sanity-check
# byte-faithful recovery.
pond mkdir -p /data
echo "hello from pond1 (file 1)" > /tmp/file1.txt
printf "line A\nline B\nline C\n"  > /tmp/file2.txt
head -c 4096 /dev/urandom | base64 > /tmp/file3.txt   # ~5.5 KB
pond copy "host:///tmp/file1.txt" /data/file1.txt
pond copy "host:///tmp/file2.txt" /data/file2.txt
pond copy "host:///tmp/file3.txt" /data/file3.txt
rm -f /tmp/file1.txt /tmp/file2.txt /tmp/file3.txt

pond push origin
echo "[OK] Pond1 push complete"

# Quick sanity: bucket has the three partitions + delta log.
DELTA_LOG_COUNT=$(mc ls --recursive "local/${BUCKET_NAME}" 2>/dev/null | grep -c "_delta_log/" || true)
check_ge "bucket has Delta log entries"               "${DELTA_LOG_COUNT}" 1

#############################
# PHASE 2: emergency info
#############################

echo ""
echo "=== Phase 2: duckpond-emergency info ==="

set +e
INFO_OUT=$(duckpond-emergency "${BUCKET_URL}" info 2>&1)
INFO_RC=$?
set -e
echo "${INFO_OUT}"
echo "(exit code: ${INFO_RC})"

check_eq "info exits 0"                "${INFO_RC}" 0
echo "${INFO_OUT}" | grep -q "Current-snapshot summary" \
    && check "true"  "info prints current-snapshot summary section" \
    || check "false" "info prints current-snapshot summary section"
echo "${INFO_OUT}" | grep -q "Per-partition row counts" \
    && check "true"  "info prints per-partition row counts section" \
    || check "false" "info prints per-partition row counts section"
echo "${INFO_OUT}" | grep -q "manifest" \
    && check "true"  "info lists manifest partition" \
    || check "false" "info lists manifest partition"
echo "${INFO_OUT}" | grep -q "checksum" \
    && check "true"  "info lists checksum partition" \
    || check "false" "info lists checksum partition"
echo "${INFO_OUT}" | grep -E "^.{0,5}data " >/dev/null \
    && check "true"  "info lists data partition" \
    || check "false" "info lists data partition"

# Use a direct DuckDB query for the substantive count check (table-box
# parsing across DuckDB versions is fragile; the script delegates the
# same SQL anyway).
DUCKDB_PRELUDE="
INSTALL httpfs; LOAD httpfs;
SET s3_region='us-east-1';
SET s3_endpoint='${MINIO_ENDPOINT#http://}';
SET s3_url_style='path';
SET s3_use_ssl=false;
SET s3_access_key_id='${MINIO_ROOT_USER}';
SET s3_secret_access_key='${MINIO_ROOT_PASSWORD}';"

LIVE_FILES=$(duckdb -noheader -csv -c "
${DUCKDB_PRELUDE}
WITH per_file AS (
    SELECT file_path, file_action, txn_seq,
           ROW_NUMBER() OVER (PARTITION BY file_path ORDER BY txn_seq DESC) AS rn
    FROM read_parquet('${BUCKET_URL}/partition_kind=data/*.parquet', hive_partitioning=true)
    WHERE chunk_id = 0
)
SELECT COUNT(*) FROM per_file WHERE rn = 1 AND file_action = 'add';
" 2>/dev/null | tr -d ' \r\n')
check_ge "live snapshot has >= 3 files" "${LIVE_FILES:-0}" 3

MANIFEST_COMMITS=$(duckdb -noheader -csv -c "
${DUCKDB_PRELUDE}
SELECT COUNT(*) FROM read_parquet('${BUCKET_URL}/partition_kind=manifest/*.parquet', hive_partitioning=true)
WHERE commit_kind = 'write';
" 2>/dev/null | tr -d ' \r\n')
check_ge "manifest partition has >= 4 write commits" "${MANIFEST_COMMITS:-0}" 4

#############################
# PHASE 3: emergency list
#############################

echo ""
echo "=== Phase 3: duckpond-emergency list ==="

set +e
LIST_OUT=$(duckpond-emergency "${BUCKET_URL}" list 2>&1)
LIST_RC=$?
set -e
echo "${LIST_OUT}" | head -20

check_eq "list exits 0" "${LIST_RC}" 0

LIST_PART=$(echo "${LIST_OUT}" | grep -E "pond_id=.*part_id=.*part-" | wc -l | tr -d ' ')
check_ge "list shows >= 3 source-pond storage parquet files" "${LIST_PART}" 3

#############################
# PHASE 4: emergency extract (one pond_id storage parquet)
#############################

echo ""
echo "=== Phase 4: duckpond-emergency extract one source-pond parquet ==="

EXTRACT_DIR=/tmp/em-extract
rm -rf "${EXTRACT_DIR}"
mkdir -p "${EXTRACT_DIR}"

# Pick one specific source-pond parquet path to extract.
PICK_PATH=$(duckdb -noheader -csv -c "
${DUCKDB_PRELUDE}
WITH per_file AS (
    SELECT file_path, file_action, file_size, txn_seq,
           ROW_NUMBER() OVER (PARTITION BY file_path ORDER BY txn_seq DESC) AS rn
    FROM read_parquet('${BUCKET_URL}/partition_kind=data/*.parquet', hive_partitioning=true)
    WHERE chunk_id = 0
)
SELECT file_path
FROM per_file
WHERE rn = 1 AND file_action = 'add' AND file_path LIKE 'pond_id=%part-%'
ORDER BY file_path
LIMIT 1;
" 2>/dev/null | tr -d '\r\n')
echo "picking: ${PICK_PATH}"
check "[ -n \"${PICK_PATH}\" ]" "found a source-pond storage parquet to extract"

# SQL LIKE: escape % in pattern? Path uses '=', not '%'.  Pass file_path as-is.
duckpond-emergency "${BUCKET_URL}" extract "${PICK_PATH}" "${EXTRACT_DIR}"

EXTRACT_COUNT=$(ls -1 "${EXTRACT_DIR}/" | wc -l | tr -d ' ')
check_eq "extract produced exactly 1 file" "${EXTRACT_COUNT}" 1

# Validate the extracted file is a valid Parquet (PAR1 magic).
FIRST_PART=$(ls "${EXTRACT_DIR}"/* | head -n 1)
HEAD_MAGIC=$(head -c 4 "${FIRST_PART}")
TAIL_MAGIC=$(tail -c 4 "${FIRST_PART}")
check_eq "extracted file has PAR1 head magic"   "${HEAD_MAGIC}" "PAR1"
check_eq "extracted file has PAR1 tail magic"   "${TAIL_MAGIC}" "PAR1"
duckdb -c "SELECT COUNT(*) FROM read_parquet('${FIRST_PART}');" >/dev/null 2>&1 \
    && check "true"  "extracted source-pond parquet is readable by DuckDB" \
    || check "false" "extracted source-pond parquet is readable by DuckDB"

#############################
# PHASE 5: emergency verify
#############################

echo ""
echo "=== Phase 5: duckpond-emergency verify ==="

set +e
VERIFY_OUT=$(duckpond-emergency "${BUCKET_URL}" verify 2>&1)
VERIFY_RC=$?
set -e
echo "${VERIFY_OUT}" | tail -20

check_eq "verify exits 0 (all snapshot files match file_blake3)" "${VERIFY_RC}" 0

MISMATCH_COUNT=$(echo "${VERIFY_OUT}" | grep -E "^Mismatched:" | awk '{print $2}')
check_eq "verify summary reports 0 mismatched" "${MISMATCH_COUNT:-X}" "0"

VERIFIED_COUNT=$(echo "${VERIFY_OUT}" | grep -E "^Verified:" | awk '{print $2}')
check_eq "verify summary verified == live file count" "${VERIFIED_COUNT:-X}" "${LIVE_FILES}"

#############################
# PHASE 6: emergency export-all
#############################

echo ""
echo "=== Phase 6: duckpond-emergency export-all ==="

EXPORT_DIR=/tmp/em-exportall
rm -rf "${EXPORT_DIR}"
mkdir -p "${EXPORT_DIR}"

duckpond-emergency "${BUCKET_URL}" export-all "${EXPORT_DIR}"

EXPORT_COUNT=$(ls -1 "${EXPORT_DIR}/" | wc -l | tr -d ' ')
check_eq "export-all produced live_files count" "${EXPORT_COUNT}" "${LIVE_FILES}"

# Spot-check: pick any extracted file and validate it's a real parquet.
FIRST_PART=$(ls "${EXPORT_DIR}"/pond_id_*part-* 2>/dev/null | head -n 1)
if [ -n "${FIRST_PART}" ]; then
    duckdb -c "SELECT COUNT(*) FROM read_parquet('${FIRST_PART}');" >/dev/null 2>&1 \
        && check "true"  "spot-check: export-all parquet is readable" \
        || check "false" "spot-check: export-all parquet is readable"
fi

#############################
# CLEANUP
#############################

rm -rf /pond1 "${EXTRACT_DIR}" "${EXPORT_DIR}"
mc rb --force "local/${BUCKET_NAME}" 2>/dev/null || true

#############################
# VERIFICATION
#############################

echo ""
echo "=== VERIFICATION ==="
echo "Total checks: ${CHECKS_TOTAL}, failed: ${CHECKS_FAILED}"

if [[ "${CHECKS_FAILED}" -gt 0 ]]; then
    echo "[FAIL] ${CHECKS_FAILED}/${CHECKS_TOTAL} checks failed"
    exit 1
fi

echo "=== PASSED ${CHECKS_TOTAL}/${CHECKS_TOTAL} checks ==="
