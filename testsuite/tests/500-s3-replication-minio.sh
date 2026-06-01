#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: S3 Push to MinIO (single pond)
# DESCRIPTION:
#   - Start MinIO as local S3 server
#   - Initialize Pond1 with some CSV data
#   - Attach an `origin` remote pointing at MinIO via `pond remote add`
#   - Push the pond and verify the bucket received the Delta files
#
# EXPECTED:
#   - `pond push origin` uploads `_delta_log/*.json` and parquet data
#   - `mc ls` shows the expected Delta layout in the bucket
#
# Migrated D4.6: rewritten on top of the D4 CLI verbs
# (`pond remote add` + `pond push`) after the legacy chunked-parquet
# `remote` factory was removed in D4.5.  The replica pull side of the
# old test (Pond2 pulls from S3) is deferred until `Remote::restart_from_compact`
# is generic over `RemoteSteward` so the destination pond can bootstrap
# from a real first pull instead of the in-Rust seed helper.
set -e

echo "=== Experiment: S3 Push with MinIO ==="
echo ""

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
BUCKET_NAME="duckpond-backup"

#############################
# START MINIO (if available)
#############################

echo "=== Checking MinIO availability ==="

if curl -s "${MINIO_ENDPOINT}/minio/health/live" &>/dev/null; then
    echo "[OK] MinIO already running at ${MINIO_ENDPOINT}"
elif command -v minio &>/dev/null; then
    echo "Starting MinIO server..."
    mkdir -p /tmp/minio-data
    MINIO_ROOT_USER=${MINIO_ROOT_USER} \
    MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD} \
    minio server /tmp/minio-data --console-address ":9001" &
    MINIO_PID=$!
    for i in $(seq 1 30); do
        if curl -s "${MINIO_ENDPOINT}/minio/health/live" &>/dev/null; then
            echo "[OK] MinIO ready after ${i}s"
            break
        fi
        sleep 1
    done
else
    echo "[FAIL] MinIO not available -- this test requires MinIO"
    exit 1
fi

mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null || true
mc mb --ignore-existing "local/${BUCKET_NAME}"
echo "[OK] MinIO configured"

#############################
# SETUP POND - PRIMARY
#############################

echo ""
echo "=== Setting up Pond (Primary) ==="

export POND=/pond1
pond init
echo "[OK] Pond initialized"

pond mkdir /data

cat > /tmp/measurements.csv << 'EOF'
timestamp,sensor_id,temperature,humidity
2024-01-01T00:00:00Z,sensor-001,22.5,45
2024-01-01T01:00:00Z,sensor-001,23.1,44
2024-01-01T02:00:00Z,sensor-001,22.8,46
2024-01-01T03:00:00Z,sensor-002,21.0,50
2024-01-01T04:00:00Z,sensor-002,20.5,52
EOF
pond copy host:///tmp/measurements.csv /data/measurements.csv
echo "[OK] Test data copied into pond"

#############################
# ATTACH REMOTE
#############################

echo ""
echo "=== Attaching origin backup (push) ==="

pond backup add origin "s3://${BUCKET_NAME}" \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key "${MINIO_ROOT_PASSWORD}" \
    --allow-http

echo "[OK] origin attached"
pond backup list

#############################
# PUSH
#############################

echo ""
echo "=== Pushing pond to origin ==="

pond push origin
echo "[OK] Push complete"

#############################
# VERIFICATION
#############################

echo ""
echo "=== VERIFICATION ==="
echo ""
echo "--- Bucket layout ---"
mc ls --recursive "local/${BUCKET_NAME}/" | tee /tmp/mc-list.txt

# We expect Delta-style files: _delta_log/*.json and at least one parquet
DELTA_LOG_COUNT=$(grep -c "_delta_log/" /tmp/mc-list.txt || true)
PARQUET_COUNT=$(grep -c "\.parquet" /tmp/mc-list.txt || true)

if [ "${DELTA_LOG_COUNT}" -lt 1 ]; then
    echo "[FAIL] Expected at least one _delta_log/*.json file in the bucket"
    exit 1
fi
if [ "${PARQUET_COUNT}" -lt 1 ]; then
    echo "[FAIL] Expected at least one .parquet file in the bucket"
    exit 1
fi

echo ""
echo "[OK] Bucket contains ${DELTA_LOG_COUNT} delta-log entries and ${PARQUET_COUNT} parquet file(s)"

#############################
# CLEANUP
#############################

if [[ -n "${MINIO_PID}" ]]; then
    kill ${MINIO_PID} 2>/dev/null || true
fi

echo ""
echo "=== Experiment Complete ==="
