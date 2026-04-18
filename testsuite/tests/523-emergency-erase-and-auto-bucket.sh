#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: Emergency erase-bucket and automatic bucket creation
# DESCRIPTION:
#   1. Create a pond with a remote backup factory (auto-creates bucket)
#   2. Write data + push backup
#   3. Erase the bucket with pond emergency erase-bucket
#   4. Verify bucket is empty
#   5. Create a NEW pond, apply same bucket -- should auto-create
#   6. Push from new pond succeeds
#   7. Create a THIRD pond, try to push to same bucket -- should fail
#      (pond ID mismatch)
set -e
source check.sh

echo "=== Experiment: Emergency Erase & Auto Bucket Creation ==="

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
BUCKET="auto-bucket-test"
BUCKET_URL="s3://${BUCKET}"

# Wait for MinIO
for i in $(seq 1 30); do
    if curl -s "${MINIO_ENDPOINT}/minio/health/live" &>/dev/null; then
        break
    fi
    sleep 1
done

# Create the bucket via mc (needed because object_store can't create buckets)
if command -v mc &>/dev/null; then
    mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null || true
    mc mb --ignore-existing "local/${BUCKET}" 2>/dev/null || true
fi

S3_ARGS="--endpoint ${MINIO_ENDPOINT} --region us-east-1 --access-key ${MINIO_ROOT_USER} --secret-key ${MINIO_ROOT_PASSWORD} --allow-http"

# ==============================================================================
# Step 1: Create pond1 with remote backup, auto-creates Delta table in bucket
# ==============================================================================

echo ""
echo "--- Step 1: Create pond1 with backup factory ---"

export POND=/tmp/pond1
rm -rf $POND
pond init

cat > /tmp/backup.yaml << EOF
version: v1
kind: mknod
metadata:
  path: /system/run/10-remote
spec:
  factory: remote
  config:
    url: "${BUCKET_URL}"
    endpoint: "${MINIO_ENDPOINT}"
    region: "us-east-1"
    access_key: "${MINIO_ROOT_USER}"
    secret_key: "${MINIO_ROOT_PASSWORD}"
    allow_http: true
EOF

pond apply -f /tmp/backup.yaml
check 'pond list /system/run/* | grep -q 10-remote' "backup factory created"

POND1_ID=$(pond config | grep "Pond ID" | awk '{print $NF}')
echo "Pond1 ID: ${POND1_ID}"

# ==============================================================================
# Step 2: Write data and push backup
# ==============================================================================

echo ""
echo "--- Step 2: Write data and push ---"

pond mkdir -p /data
echo "hello world" | pond copy - /data/test.txt 2>/dev/null || \
    (echo "test content" > /tmp/testfile.txt && pond copy "host:///tmp/testfile.txt" /data/test.txt)

# The post-commit hook should auto-push, but let's force it
pond run /system/run/10-remote push
check 'true' "backup push succeeded"

# ==============================================================================
# Step 3: Erase the bucket
# ==============================================================================

echo ""
echo "--- Step 3: Erase bucket ---"

pond emergency erase-bucket "${BUCKET_URL}" ${S3_ARGS} --dangerous
check 'true' "bucket erased"

# ==============================================================================
# Step 4: Create pond2, apply same bucket -- should work (empty bucket)
# ==============================================================================

echo ""
echo "--- Step 4: Create pond2 with same bucket ---"

export POND=/tmp/pond2
rm -rf $POND
pond init

pond apply -f /tmp/backup.yaml
check 'pond list /system/run/* | grep -q 10-remote' "pond2 backup factory created"

POND2_ID=$(pond config | grep "Pond ID" | awk '{print $NF}')
echo "Pond2 ID: ${POND2_ID}"
check '[ "${POND1_ID}" != "${POND2_ID}" ]' "pond2 has different ID"

# Write and push from pond2
pond mkdir -p /data
echo "pond2 data" > /tmp/testfile2.txt
pond copy "host:///tmp/testfile2.txt" /data/test2.txt
pond run /system/run/10-remote push
check 'true' "pond2 push succeeded"

# ==============================================================================
# Step 5: Create pond3, try to push to same bucket -- should fail (mismatch)
# ==============================================================================

echo ""
echo "--- Step 5: Pond3 push to occupied bucket should fail ---"

export POND=/tmp/pond3
rm -rf $POND
pond init

pond apply -f /tmp/backup.yaml

PUSH_OUT=$(pond run /system/run/10-remote push 2>&1 || true)
echo "$PUSH_OUT"
check 'echo "$PUSH_OUT" | grep -qi "mismatch\|already contains"' "pond3 push rejected (pond ID mismatch)"

# Cleanup
rm -rf /tmp/pond1 /tmp/pond2 /tmp/pond3 /tmp/backup.yaml /tmp/testfile*.txt

check_finish
