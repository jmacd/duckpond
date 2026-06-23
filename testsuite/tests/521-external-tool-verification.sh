#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: External-tool extraction + BLAKE3 round-trip on backup
#
# DESCRIPTION:
#   Replaces the legacy `pond run /system/run/10-remote show --script`
#   verification flow.  In the D5.7b CLI the backup is a native Delta
#   Lake table with columns `partition_kind`, `txn_seq`, `file_path`,
#   `file_action`, `file_size`, `file_blake3`, `chunk_id`, `chunk_count`,
#   `chunk_data`, `chunk_blake3` (see crates/sync-remote/src/schema.rs).
#
#   This test exercises the full "extract using only standard tools"
#   path that 520 stopped just short of:
#
#     1. Pond1: init + `pond backup add origin` + write a small text
#        file (guaranteed single-chunk) into /data/hello.txt + push.
#     2. Use DuckDB to read the remote and pick one data-add row whose
#        chunk_count = 1 (single-chunk Add).
#     3. Use DuckDB `hex(chunk_data)` + `xxd -r -p` to extract that
#        single chunk byte-for-byte.
#     4. b3sum the extracted bytes and verify it matches the stored
#        chunk_blake3 (hex equality).
#     5. For a single-chunk file, also verify file_blake3 ==
#        chunk_blake3 == b3sum(extracted bytes).
#
#   This proves that even without the `pond` binary, an operator with
#   only `duckdb` + `b3sum` can extract source-pond data files and
#   verify their integrity end-to-end.
#
# EXPECTED:
#   - DuckDB sees the remote and reports >= one data-add row.
#   - Extracted bytes have the correct length (matches file_size when
#     chunk_count = 1).
#   - b3sum(extracted) == chunk_blake3 (hex).
#   - b3sum(extracted) == file_blake3 (hex) for a single-chunk file.
#
# History:
#   Migrated D5.8.3: rewrote on top of the D5.7b backup CLI; legacy
#   `pond run …/show --script` generator was removed in D4.5.  Schema
#   changed from chunked-parquet (path/root_hash/total_size/pond_txn_id)
#   to native Delta (file_path/file_blake3/file_size/txn_seq).
set -e

echo "=== Experiment: DuckDB BLAKE3 Round-Trip on Remote Backup ==="
echo ""

#############################
# CONFIGURATION
#############################

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
BUCKET_NAME="duckpond-extract-test"

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

check_eq() {
    local description="$1"
    local actual="$2"
    local expected="$3"
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
    if [[ "${actual}" == "${expected}" ]]; then
        echo "  ✓ ${description} (${actual})"
    else
        echo "  ✗ ${description}: got '${actual}', expected '${expected}'"
        CHECKS_FAILED=$((CHECKS_FAILED + 1))
    fi
}

#############################
# PHASE 0: Environment
#############################

echo "=== Phase 0: Environment ==="
if ! curl -s "${MINIO_ENDPOINT}/minio/health/live" >/dev/null; then
    echo "[FAIL] MinIO not reachable at ${MINIO_ENDPOINT}"
    echo "       Run via 'run-test.sh --compose 521'."
    exit 1
fi
echo "[OK] MinIO reachable"

command -v duckdb >/dev/null || { echo "[FAIL] duckdb missing"; exit 1; }
command -v b3sum  >/dev/null || { echo "[FAIL] b3sum missing"; exit 1; }
echo "[OK] DuckDB + b3sum present"

mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null
mc rb --force "local/${BUCKET_NAME}" 2>/dev/null || true
mc mb --ignore-existing "local/${BUCKET_NAME}"

#############################
# PHASE 1: Pond + push
#############################

echo ""
echo "=== Phase 1: Pond setup + push ==="

export POND=/pond1
rm -rf "${POND}"
pond init --birthplace test-host
pond mkdir -p /data

# A small text file -- guarantees one source parquet whose chunk_count = 1.
echo "hello duckpond" > /tmp/hello.txt
pond copy "host:///tmp/hello.txt" /data/hello.txt

pond backup add origin "s3://${BUCKET_NAME}" \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
    --allow-http

pond push origin
echo "[OK] Pond1 pushed"

rm -f /tmp/hello.txt

#############################
# PHASE 2: Pick a single-chunk Add row via DuckDB
#############################

echo ""
echo "=== Phase 2: Pick a single-chunk Add row ==="

DUCKDB_PRELUDE="
INSTALL httpfs;     LOAD httpfs;
SET s3_region='us-east-1';
SET s3_endpoint='${MINIO_ENDPOINT#http://}';
SET s3_url_style='path';
SET s3_use_ssl=false;
SET s3_access_key_id='${MINIO_ROOT_USER}';
SET s3_secret_access_key='${MINIO_ROOT_PASSWORD}';
"

PQ_GLOB="s3://${BUCKET_NAME}/partition_kind=*/*.parquet"

# Count how many distinct files have chunk_count = 1.
SINGLE_CHUNK_COUNT=$(duckdb -noheader -csv -c "
${DUCKDB_PRELUDE}
SELECT COUNT(DISTINCT file_path)
FROM read_parquet('${PQ_GLOB}', hive_partitioning=true)
WHERE partition_kind='data' AND file_action='add' AND chunk_count=1;
" 2>/dev/null | tr -d ' \r')
echo "Distinct single-chunk Add files: ${SINGLE_CHUNK_COUNT}"
check '[[ "${SINGLE_CHUNK_COUNT}" =~ ^[0-9]+$ ]] && [ "${SINGLE_CHUNK_COUNT}" -ge 1 ]' \
    "DuckDB sees at least one single-chunk Add row"

# Pick one: smallest file_size, deterministic ordering on file_path.
PICK=$(duckdb -noheader -csv -c "
${DUCKDB_PRELUDE}
SELECT file_path, txn_seq, file_size,
       hex(file_blake3) AS file_blake3_hex,
       hex(chunk_blake3) AS chunk_blake3_hex
FROM read_parquet('${PQ_GLOB}', hive_partitioning=true)
WHERE partition_kind='data' AND file_action='add' AND chunk_count=1
ORDER BY file_size, file_path
LIMIT 1;
" 2>/dev/null)
echo "Picked row: ${PICK}"

PICK_PATH=$(echo "${PICK}"   | cut -d',' -f1)
PICK_TXN=$(echo  "${PICK}"   | cut -d',' -f2)
PICK_SIZE=$(echo "${PICK}"   | cut -d',' -f3)
PICK_FBLAKE=$(echo "${PICK}" | cut -d',' -f4 | tr 'A-Z' 'a-z')
PICK_CBLAKE=$(echo "${PICK}" | cut -d',' -f5 | tr 'A-Z' 'a-z')

echo "  file_path:    ${PICK_PATH}"
echo "  txn_seq:      ${PICK_TXN}"
echo "  file_size:    ${PICK_SIZE}"
echo "  file_blake3:  ${PICK_FBLAKE}"
echo "  chunk_blake3: ${PICK_CBLAKE}"

# For a single-chunk Add, file_blake3 == chunk_blake3 by definition
# (BLAKE3 of "the file" == BLAKE3 of "its only chunk").
check_eq "single-chunk: file_blake3 == chunk_blake3" "${PICK_FBLAKE}" "${PICK_CBLAKE}"

#############################
# PHASE 3: Extract chunk_data via DuckDB COPY (FORMAT BLOB)
#############################

echo ""
echo "=== Phase 3: Extract chunk bytes via DuckDB ==="

EXTRACT_PATH="/tmp/extracted.bin"
rm -f "${EXTRACT_PATH}"

# DuckDB doesn't have a portable raw-binary COPY across versions, so we
# hex-encode the BLOB and pipe through xxd to reconstruct the bytes
# exactly.  This is the same trick crates/cmd/scripts/duckpond-emergency
# uses (base64 there; hex is even simpler and avoids newline issues).
HEX_OUT=$(duckdb -noheader -csv -c "
${DUCKDB_PRELUDE}
SELECT hex(chunk_data)
FROM read_parquet('${PQ_GLOB}', hive_partitioning=true)
WHERE partition_kind='data' AND file_action='add'
  AND file_path = '${PICK_PATH}'
  AND txn_seq   = ${PICK_TXN}
  AND chunk_id  = 0;
" 2>/dev/null | tr -d ' \r\n')

printf "%s" "${HEX_OUT}" | xxd -r -p > "${EXTRACT_PATH}"

EXTRACT_SIZE=$(wc -c < "${EXTRACT_PATH}" | tr -d ' ')
echo "Extracted bytes: ${EXTRACT_SIZE}"
check_eq "extracted byte count matches file_size" "${EXTRACT_SIZE}" "${PICK_SIZE}"

#############################
# PHASE 4: BLAKE3 round-trip
#############################

echo ""
echo "=== Phase 4: BLAKE3 round-trip ==="

ACTUAL_B3=$(b3sum --no-names "${EXTRACT_PATH}" | tr -d ' \n' | tr 'A-Z' 'a-z')
echo "b3sum(extracted): ${ACTUAL_B3}"
echo "stored chunk_blake3:  ${PICK_CBLAKE}"
echo "stored file_blake3:   ${PICK_FBLAKE}"

check_eq "b3sum(extracted) == chunk_blake3" "${ACTUAL_B3}" "${PICK_CBLAKE}"
check_eq "b3sum(extracted) == file_blake3"  "${ACTUAL_B3}" "${PICK_FBLAKE}"

#############################
# CLEANUP
#############################

rm -rf /pond1 "${EXTRACT_PATH}"
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
