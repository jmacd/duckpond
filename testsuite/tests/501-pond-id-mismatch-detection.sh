#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: Refuse push when bucket contains different pond
# DESCRIPTION:
#   - Create Pond1, back up to S3 bucket
#   - Delete Pond1, create Pond2 (new pond_id)
#   - Try to push Pond2 to the same bucket
#   - Verify it fails with a pond_id mismatch error
#
# EXPECTED:
#   - First push succeeds
#   - Second push (different pond_id) fails with clear error
#   - Error message mentions both pond IDs
#
set -e

echo "=== Experiment: Pond ID Mismatch Detection ==="
echo ""

# MinIO configuration
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
BUCKET_NAME="pond-mismatch-test"

# Start MinIO if binary is available
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

# Remote config template
make_remote_config() {
    cat <<EOF
region: "us-east-1"
url: "s3://${BUCKET_NAME}"
endpoint: "${MINIO_ENDPOINT}"
access_key: "${MINIO_ROOT_USER}"
secret_key: "${MINIO_ROOT_PASSWORD}"
allow_http: true
EOF
}

#############################
# STEP 1: Create Pond1 and push
#############################
echo "--- Step 1: Create first pond and push ---"

POND1_DIR=$(mktemp -d)
export POND="${POND1_DIR}"

pond init
pond mkdir -p /system/run

REMOTE_CFG=$(mktemp)
make_remote_config > "${REMOTE_CFG}"
pond mknod remote /system/run/1-backup --config-path "${REMOTE_CFG}"

# Create some data
pond mkdir -p /data
TMPFILE=$(mktemp)
echo "hello from pond1" > "${TMPFILE}"
pond copy "host:///${TMPFILE}" /data/file1.txt
rm -f "${TMPFILE}"

# Push should succeed
pond run /system/run/1-backup push
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
pond mkdir -p /system/run

REMOTE_CFG2=$(mktemp)
make_remote_config > "${REMOTE_CFG2}"
pond mknod remote /system/run/1-backup --config-path "${REMOTE_CFG2}"

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
OUTPUT=$(pond run /system/run/1-backup push 2>&1 || true)
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
rm -f "${REMOTE_CFG}" "${REMOTE_CFG2}"
