#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: Refuse attach when bucket contains a different pond
# DESCRIPTION:
#   - Create Pond1, attach to S3 bucket via `pond remote add` (auto-inits
#     the bucket as a Delta table with Pond1's pond_id), then push.
#   - Create Pond2 (different pond_id) and try to attach the SAME bucket
#     in push mode.
#   - Verify the second `pond remote add` is rejected with a pond_id
#     mismatch error (the operator finds out at attach time, not later
#     on first push).
#
# EXPECTED:
#   - First `pond remote add` + `pond push` succeed
#   - Second `pond remote add` (different pond_id) fails with clear error
#   - Error message mentions both pond IDs
#
# Migrated D4.6: rewritten on top of the D4 CLI verbs
# (`pond remote add` + `pond push`) after the legacy
# chunked-parquet `remote` factory was removed in D4.5.
# Updated D4.8: `pond remote add` now auto-initializes the
# remote Delta table, which means foreign-pond mismatch is
# detected at attach time (was at first push).
set -e

echo "=== Experiment: Pond ID Mismatch Detection ==="
echo ""

# MinIO configuration
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
BUCKET_NAME="pond-mismatch-test"

# Start MinIO if binary is available (standalone runs)
if command -v minio &>/dev/null; then
    MINIO_DIR=$(mktemp -d)
    MINIO_ROOT_USER="${MINIO_ROOT_USER}" MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD}" \
        minio server "${MINIO_DIR}" --address ":9000" &
    MINIO_PID=$!
    sleep 2
    cleanup() { kill $MINIO_PID 2>/dev/null; rm -rf "${MINIO_DIR}"; }
    trap cleanup EXIT
fi

# Create bucket
mc alias set testminio "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null || true
mc mb --ignore-existing "testminio/${BUCKET_NAME}"

# Helper: attach the remote to the current $POND using D4 CLI verbs.
attach_remote() {
    pond remote add origin "s3://${BUCKET_NAME}" \
        --mode push \
        --region us-east-1 \
        --endpoint "${MINIO_ENDPOINT}" \
        --access-key-id "${MINIO_ROOT_USER}" \
        --secret-access-key "${MINIO_ROOT_PASSWORD}" \
        --allow-http
}

#############################
# STEP 1: Create Pond1 and push
#############################
echo "--- Step 1: Create first pond, attach, and push ---"

POND1_DIR=$(mktemp -d)
export POND="${POND1_DIR}"

pond init

# First attach auto-initializes the bucket as a fresh Delta table
# with Pond1's pond_id as the store_id.
attach_remote
echo "[OK] First attach succeeded (bucket auto-initialized with Pond1's pond_id)"

# Create some data
pond mkdir -p /data
TMPFILE=$(mktemp)
echo "hello from pond1" > "${TMPFILE}"
pond copy "host:///${TMPFILE}" /data/file1.txt
rm -f "${TMPFILE}"

# Push should succeed
pond push origin
echo "[OK] First pond push succeeded"

POND1_ID=$(pond config 2>/dev/null | grep "Pond ID" | awk '{print $NF}')
echo "Pond1 ID: ${POND1_ID}"

#############################
# STEP 2: Create Pond2 and try to attach to same bucket
#############################
echo ""
echo "--- Step 2: Create second pond, try to attach to same bucket ---"

POND2_DIR=$(mktemp -d)
export POND="${POND2_DIR}"

pond init

POND2_ID=$(pond config 2>/dev/null | grep "Pond ID" | awk '{print $NF}')
echo "Pond2 ID: ${POND2_ID}"

# Verify they are different ponds
if [ "${POND1_ID}" = "${POND2_ID}" ]; then
    echo "[FAIL] Both ponds have the same ID -- this shouldn't happen"
    exit 1
fi
echo "[OK] Pond IDs differ: ${POND1_ID} vs ${POND2_ID}"

# Attach should FAIL with mismatch error: the bucket's Delta table
# already has Pond1's store_id, and Pond2's pond_id does not match.
echo ""
echo "--- Attempting attach (should fail) ---"
OUTPUT=$(attach_remote 2>&1 || true)
echo "${OUTPUT}"

if echo "${OUTPUT}" | grep -qi "does not match" \
   && echo "${OUTPUT}" | grep -qi "foreign pond"; then
    echo "[OK] Attach correctly refused with mismatch error"
else
    echo "[FAIL] Expected store_id-mismatch / foreign-pond error"
    exit 1
fi

#############################
# VERIFICATION
#############################
echo ""
echo "=== VERIFICATION ==="
echo "[OK] Pond ID mismatch correctly detected at attach time"
echo "[OK] Attach refused with clear error message"
echo "[OK] Test passed"

# Cleanup
rm -rf "${POND1_DIR}" "${POND2_DIR}"
