#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: Cross-pond import via MinIO (D5.7b.2 foundation)
#
# DESCRIPTION:
#   Two ponds against the same MinIO bucket exercise the cross-pond
#   import path end-to-end:
#
#     Pond A (producer):
#       1. init (gets pond_id_A).
#       2. write a mixed tree under /data:
#            /data/sensors.csv         (data, ~200 bytes)
#            /data/sites.csv           (data, multi-add bundling)
#            /data/nested/readme.txt   (data, in subdirectory)
#            /data/big.bin             (data, 80 KiB -> large-file path)
#       3. pond backup add origin URL
#       4. pond push origin
#
#     Pond B (consumer):
#       5. init (gets pond_id_B != pond_id_A).
#       6. write a LOCAL file at /local/note.txt (must coexist with mount).
#       7. pond remote add upstream URL /imports/A
#          (cross-pond import; path != "/", foreign store_id required).
#       8. pond pull upstream
#       9. read every file via /imports/A/<...>, byte-compare against A.
#      10. confirm /local/note.txt is still readable.
#      11. confirm the mount entry's pond_id matches Pond A's pond_id
#          (provenance preserved across the boundary).
#      12. confirm a second `pond pull upstream` is idempotent.
#
# EXPECTED:
#   - All three files in /imports/A/* round-trip bit-for-bit.
#   - Pond B's local /local/note.txt is unaffected by the import.
#   - `pond config` on B shows the producer's pond_id as the foreign
#     store_id behind the mount.
#   - Second pull does not error and does not duplicate the mount.
#
# History:
#   Revived D5.8.5 against the D5.7b.2 cross-pond CLI surface
#   (`pond backup add`, `pond remote add NAME URL /imports/X`,
#   `pond push`, `pond pull`).  The legacy script wired up the deleted
#   `remote` factory via mknod + per-config-file YAML; that surface is
#   gone.
set -e

echo "=== Experiment: Cross-Pond Import via MinIO ==="
echo ""

#############################
# CONFIGURATION
#############################

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
BUCKET_NAME="cross-pond-530"

CHECKS_TOTAL=0
CHECKS_FAILED=0

check() {
    local description="$1"
    local cmd="$2"
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
    if eval "${cmd}" >/dev/null 2>&1; then
        echo "  ✓ ${description}"
    else
        echo "  ✗ ${description} (cmd: ${cmd})"
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
# MINIO READINESS
#############################

echo "=== Checking MinIO availability ==="
if ! curl -s "${MINIO_ENDPOINT}/minio/health/live" >/dev/null; then
    echo "[FAIL] MinIO not reachable at ${MINIO_ENDPOINT}"
    exit 1
fi
echo "MinIO is reachable at ${MINIO_ENDPOINT}"

mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null 2>&1
mc rb --force "local/${BUCKET_NAME}" >/dev/null 2>&1 || true
mc mb "local/${BUCKET_NAME}" >/dev/null 2>&1

#############################
# POND A — PRODUCER
#############################

echo ""
echo "=== Phase 1: Pond A (producer) — init and populate ==="

export POND=/tmp/pond-a
rm -rf "${POND}"
pond init >/dev/null

pond mkdir /data >/dev/null
pond mkdir /data/nested >/dev/null

cat > /tmp/sensors.csv << 'EOF'
timestamp,sensor_id,temperature,humidity
2024-01-01T00:00:00Z,sensor-001,22.5,45
2024-01-01T01:00:00Z,sensor-001,23.1,44
2024-01-01T02:00:00Z,sensor-001,22.8,46
2024-01-01T03:00:00Z,sensor-002,21.0,50
2024-01-01T04:00:00Z,sensor-002,20.5,52
EOF
pond copy host:///tmp/sensors.csv /data/sensors.csv >/dev/null

# A second small file in the same partition to exercise multi-add
# bundling on a single push.
printf 'lat,lon\n39.4283,-123.8053\n39.4290,-123.8061\n' > /tmp/sites.csv
pond copy host:///tmp/sites.csv /data/sites.csv >/dev/null

# Files in a subdirectory to exercise nested path resolution through
# the foreign mount.
echo "Hello from upstream pond A." > /tmp/readme.txt
pond copy host:///tmp/readme.txt /data/nested/readme.txt >/dev/null

# 80 KiB > LARGE_FILE_THRESHOLD (64 KiB).  Exercises the large-file
# replication path through cross-pond import: the body is externalized
# to `<pond>/data/_large_files/blake3_16=…/blake3=….parquet` on A, and
# `sync_remote::push` must include those external blobs in the bundle
# alongside the Delta partition parquet so B can satisfy reads through
# the mount (P1-BUG-LF-REPLICATION, fixed in D5.9).
python3 -c "import sys; sys.stdout.buffer.write(bytes((i * 31) & 0xff for i in range(80 * 1024)))" > /tmp/big.bin
pond copy host:///tmp/big.bin /data/big.bin >/dev/null

# Capture A's pond_id (uuid hex with dashes) for later provenance check.
POND_ID_A=$(pond config 2>/dev/null | awk '/^Pond ID:/ {print $NF; exit}')
echo "Pond A pond_id: ${POND_ID_A}"

if [[ -z "${POND_ID_A}" ]]; then
    echo "[FAIL] could not extract pond_id from \`pond config\` on A"
    pond config
    exit 1
fi

echo ""
echo "=== Phase 2: Pond A — attach backup and push ==="
pond backup add origin "s3://${BUCKET_NAME}" \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
    --allow-http \
    --overwrite >/dev/null
pond push origin >/dev/null
echo "Push complete."

# Capture A's view of each file's bytes for later comparison.
SENSORS_HASH_A=$(pond cat /data/sensors.csv 2>/dev/null | md5sum | cut -d' ' -f1)
SITES_HASH_A=$(pond cat /data/sites.csv 2>/dev/null | md5sum | cut -d' ' -f1)
README_HASH_A=$(pond cat /data/nested/readme.txt 2>/dev/null | md5sum | cut -d' ' -f1)
BIG_HASH_A=$(pond cat /data/big.bin 2>/dev/null | md5sum | cut -d' ' -f1)
echo "Pond A hashes:"
echo "  sensors.csv:        ${SENSORS_HASH_A}"
echo "  sites.csv:          ${SITES_HASH_A}"
echo "  nested/readme.txt:  ${README_HASH_A}"
echo "  big.bin (80 KiB):   ${BIG_HASH_A}"

#############################
# POND B — CONSUMER
#############################

echo ""
echo "=== Phase 3: Pond B (consumer) — init and write local data ==="

export POND=/tmp/pond-b
rm -rf "${POND}"
pond init >/dev/null

POND_ID_B=$(pond config 2>/dev/null | awk '/^Pond ID:/ {print $NF; exit}')
echo "Pond B pond_id: ${POND_ID_B}"

check "Pond A and Pond B have distinct pond_ids" \
    "[ \"${POND_ID_A}\" != \"${POND_ID_B}\" ]"

# A local file that lives entirely in B's own pond_id partition.
pond mkdir /local >/dev/null
echo "Local note in pond B." > /tmp/local-note.txt
pond copy host:///tmp/local-note.txt /local/note.txt >/dev/null
LOCAL_HASH_B=$(pond cat /local/note.txt 2>/dev/null | md5sum | cut -d' ' -f1)
echo "Pond B local /local/note.txt hash: ${LOCAL_HASH_B}"

echo ""
echo "=== Phase 4: Pond B — cross-pond attach + pull ==="
pond remote add upstream "s3://${BUCKET_NAME}" /imports/A \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
    --allow-http \
    --overwrite >/dev/null
pond pull upstream >/dev/null
echo "Pull complete."

#############################
# VERIFICATION
#############################

echo ""
echo "=== Phase 5: Verify imported files are readable on B ==="

# `pond list PATH` returns the entry itself; `pond list PATH/` returns
# the directory's children.
pond list /imports/A/ > /tmp/b-list-mount.txt 2>&1 || true
cat /tmp/b-list-mount.txt
check "imports/A listing mentions 'data'" \
    "grep -q 'data' /tmp/b-list-mount.txt"

pond list /imports/A/data/ > /tmp/b-list-data.txt 2>&1 || true
cat /tmp/b-list-data.txt
check "imports/A/data listing mentions sensors.csv" \
    "grep -q 'sensors.csv' /tmp/b-list-data.txt"
check "imports/A/data listing mentions sites.csv" \
    "grep -q 'sites.csv' /tmp/b-list-data.txt"
check "imports/A/data listing mentions nested" \
    "grep -q 'nested' /tmp/b-list-data.txt"
check "imports/A/data listing mentions big.bin" \
    "grep -q 'big.bin' /tmp/b-list-data.txt"

echo ""
echo "=== Phase 6: Byte-faithful import (md5 round-trip) ==="
SENSORS_HASH_B=$(pond cat /imports/A/data/sensors.csv 2>/dev/null | md5sum | cut -d' ' -f1)
SITES_HASH_B=$(pond cat /imports/A/data/sites.csv 2>/dev/null | md5sum | cut -d' ' -f1)
README_HASH_B=$(pond cat /imports/A/data/nested/readme.txt 2>/dev/null | md5sum | cut -d' ' -f1)
BIG_HASH_B=$(pond cat /imports/A/data/big.bin 2>/dev/null | md5sum | cut -d' ' -f1)
check_eq "sensors.csv survives cross-pond import" "${SENSORS_HASH_B}" "${SENSORS_HASH_A}"
check_eq "sites.csv (multi-add bundle) survives cross-pond import" "${SITES_HASH_B}" "${SITES_HASH_A}"
check_eq "nested/readme.txt survives cross-pond import" "${README_HASH_B}" "${README_HASH_A}"
check_eq "big.bin (>64KiB large-file) survives cross-pond import" "${BIG_HASH_B}" "${BIG_HASH_A}"

echo ""
echo "=== Phase 7: Local data on B is unaffected by the import ==="
LOCAL_HASH_B_AFTER=$(pond cat /local/note.txt 2>/dev/null | md5sum | cut -d' ' -f1)
check_eq "Pond B /local/note.txt unchanged by import" "${LOCAL_HASH_B_AFTER}" "${LOCAL_HASH_B}"

echo ""
echo "=== Phase 8: Mount provenance (foreign pond_id tail surfaces) ==="
# `pond list` includes a `[<last-12-hex>]` tag on every entry that is
# the trailing 12 hex chars of the owning pond_id.  The mount entry
# itself should carry Pond A's pond_id, not Pond B's.
POND_ID_A_TAIL="${POND_ID_A##*-}"   # everything after the last '-'
POND_ID_B_TAIL="${POND_ID_B##*-}"
pond list /imports/ > /tmp/b-list-imports.txt 2>&1 || true
cat /tmp/b-list-imports.txt
check "Pond A's pond_id tail visible on /imports/A entry" \
    "grep -q '\[${POND_ID_A_TAIL}\]' /tmp/b-list-imports.txt"
check "Pond B's pond_id tail does NOT appear on /imports/A entry" \
    "! grep -q '\[${POND_ID_B_TAIL}\].*/imports/A$' /tmp/b-list-imports.txt"

echo ""
echo "=== Phase 9: Second pull is idempotent ==="
pond pull upstream >/dev/null
SENSORS_HASH_B2=$(pond cat /imports/A/data/sensors.csv 2>/dev/null | md5sum | cut -d' ' -f1)
check_eq "sensors.csv still correct after second pull" "${SENSORS_HASH_B2}" "${SENSORS_HASH_A}"

# /imports/ should still have exactly one child entry (A).
pond list /imports/ > /tmp/b-list-imports2.txt 2>&1 || true
IMPORTS_COUNT=$(grep -cE '/imports/A$' /tmp/b-list-imports2.txt || true)
check_eq "/imports/ has exactly one entry 'A' after second pull" \
    "${IMPORTS_COUNT}" "1"

#############################
# RESULTS
#############################

echo ""
echo "=== Results ==="
echo "Checks: ${CHECKS_TOTAL}, Failed: ${CHECKS_FAILED}"
if [[ "${CHECKS_FAILED}" -ne 0 ]]; then
    echo "[FAIL] one or more checks failed"
    exit 1
fi
echo "[OK] all checks passed"
