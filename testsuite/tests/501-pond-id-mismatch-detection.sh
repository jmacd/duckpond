#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: Refuse push when bucket contains different pond
# DESCRIPTION:
#   - Create Pond1, push to S3 bucket via `pond remote add` + `pond push`
#   - Create Pond2 (new pond_id) targeting the same bucket
#   - Try to push Pond2 to the same bucket
#   - Verify the second push is rejected with a pond_id mismatch error
#
# EXPECTED:
#   - First push succeeds
#   - Second push (different pond_id) fails with clear error
#   - Error message mentions both pond IDs
#
# Migrated D4.6: rewritten on top of the D4 CLI verbs
# (`pond remote add` + `pond push`) after the legacy
# chunked-parquet `remote` factory was removed in D4.5.
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
echo "--- Step 1: Create first pond and push ---"

POND1_DIR=$(mktemp -d)
export POND="${POND1_DIR}"

pond init

attach_remote

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
# STEP 2: Create Pond2 and try to push to same bucket
#############################
echo ""
echo "--- Step 2: Create second pond and push to same bucket ---"

POND2_DIR=$(mktemp -d)
export POND="${POND2_DIR}"

pond init

attach_remote

# Create different data
pond mkdir -p /data
TMPFILE=$(mktemp)
echo "hello from pond2" > "${TMPFILE}"
pond copy "host:///${TMPFILE}" /data/file2.txt
rm -f "${TMPFILE}"

POND2_ID=$(pond config 2>/dev/null | grep "Pond ID" | awk '{print $NF}')
echo "Pond2 ID: ${POND2_ID}"

# Verify they are different ponds
if [ "${POND1_ID}" = "${POND2_ID}" ]; then
    echo "[FAIL] Both ponds have the same ID -- this shouldn't happen"
    exit 1
fi
echo "[OK] Pond IDs differ: ${POND1_ID} vs ${POND2_ID}"

# Push should FAIL with mismatch error
echo ""
echo "--- Attempting push (should fail) ---"
OUTPUT=$(pond push origin 2>&1 || true)
echo "${OUTPUT}"

if echo "${OUTPUT}" | grep -qi "mismatch"; then
    echo "[OK] Push correctly refused with mismatch error"
else
    echo "[FAIL] Expected mismatch error"
    exit 1
fi

#############################
# VERIFICATION
#############################
echo ""
echo "=== VERIFICATION ==="
echo "[OK] Pond ID mismatch correctly detected"
echo "[OK] Push refused with clear error message"
echo "[OK] Test passed"

# Cleanup
rm -rf "${POND1_DIR}" "${POND2_DIR}"
