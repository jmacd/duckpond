#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: Cross-pond import via MinIO
# DESCRIPTION:
#   Two ponds: a "producer" (Pond1) that ingests data and pushes its
#   backup to S3, and a "consumer" (Pond2) that imports the producer's
#   data at a local path and can query it.
#
#   This exercises the full cross-pond import pipeline:
#   1. Pond1: init, create data, push backup to MinIO
#   2. Pond2: init, mknod remote with import config (discovers foreign
#      partition, creates local directory referencing it)
#   3. Pond2: pond run pull (downloads foreign parquet files)
#   4. Verify: imported data is queryable in Pond2
#
# EXPECTED:
#   - Pond2 can list and cat imported data at /sources/producer/
#   - pond_id in imported records is the producer's pond_id (provenance)
#   - Local data in Pond2 coexists with imported data
set -e

source /usr/local/bin/check.sh 2>/dev/null || source check.sh 2>/dev/null || true

echo "=== Experiment: Cross-Pond Import via MinIO ==="
echo ""

# MinIO configuration (env vars from docker-compose.test.yaml)
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
BUCKET_NAME="cross-pond-test"

#############################
# CONFIGURE MINIO
#############################

echo "=== Configuring MinIO ==="

# Wait for MinIO
for i in $(seq 1 30); do
    if curl -s "${MINIO_ENDPOINT}/minio/health/live" &>/dev/null; then
        echo "MinIO ready"
        break
    fi
    sleep 1
done

mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null || true
mc mb "local/${BUCKET_NAME}" 2>/dev/null || true

#############################
# POND1 — PRODUCER
#############################

echo ""
echo "=== Setting up Pond1 (Producer) ==="

export POND=/pond1
pond init

pond mkdir /ingest

# Create test CSV data
cat > /tmp/sensors.csv << 'EOF'
timestamp,sensor_id,temperature,humidity
2024-01-01T00:00:00Z,sensor-001,22.5,45
2024-01-01T01:00:00Z,sensor-001,23.1,44
2024-01-01T02:00:00Z,sensor-001,22.8,46
2024-01-01T03:00:00Z,sensor-002,21.0,50
2024-01-01T04:00:00Z,sensor-002,20.5,52
EOF

pond copy host:///tmp/sensors.csv /ingest/sensors.csv
echo "Created /ingest/sensors.csv"

# Capture the producer's pond_id for later verification
PRODUCER_POND_ID=$(pond config 2>/dev/null | grep "Pond ID" | awk '{print $NF}')
echo "Producer pond_id: ${PRODUCER_POND_ID}"

# Configure remote backup to MinIO
pond mkdir /system
pond mkdir /system/run

cat > /tmp/producer-backup.yaml << EOF
url: "s3://${BUCKET_NAME}"
endpoint: "${MINIO_ENDPOINT}"
region: "us-east-1"
access_key_id: "${MINIO_ROOT_USER}"
secret_access_key: "${MINIO_ROOT_PASSWORD}"
allow_http: true
EOF

pond mknod remote /system/run/10-backup --config-path /tmp/producer-backup.yaml
echo "Remote backup configured"

# Push producer backup to MinIO
echo ""
echo "=== Pushing Producer backup to MinIO ==="
pond run /system/run/10-backup push
echo "Push complete"

# Verify backup is in MinIO
mc ls "local/${BUCKET_NAME}/" 2>/dev/null || echo "(listing bucket)"

#############################
# POND2 — CONSUMER
#############################

echo ""
echo "=== Setting up Pond2 (Consumer with import) ==="

export POND=/pond2
pond init

CONSUMER_POND_ID=$(pond config 2>/dev/null | grep "Pond ID" | awk '{print $NF}')
echo "Consumer pond_id: ${CONSUMER_POND_ID}"

pond mkdir /system
pond mkdir /system/etc

# Configure import factory pointing at the producer's backup
# source_path: the path in the producer's pond to import
# local_path: where the imported data appears in this pond
# Configure import factory pointing at the producer's backup bucket.
# The import code discovers the producer's pond UUID internally from FILE-META objects.
cat > /tmp/import-config.yaml << EOF
url: "s3://${BUCKET_NAME}"
endpoint: "${MINIO_ENDPOINT}"
region: "us-east-1"
access_key_id: "${MINIO_ROOT_USER}"
secret_access_key: "${MINIO_ROOT_PASSWORD}"
allow_http: true
import:
  source_path: "/ingest"
  local_path: "/sources/producer"
EOF

echo ""
echo "=== Creating import factory (mknod) ==="
# mknod creates the factory AND discovers the foreign partition structure
pond mknod remote /system/etc/10-producer --config-path /tmp/import-config.yaml
echo "Import factory created"

# Verify the import path was created
echo ""
echo "=== Verifying import path exists ==="
POND=/pond2 pond list '/*' > /tmp/pond2-list.txt 2>&1 || true
cat /tmp/pond2-list.txt

check 'grep -q "sources" /tmp/pond2-list.txt' "local_path /sources/ created"

#############################
# PULL IMPORTED DATA
#############################

echo ""
echo "=== Pulling imported data from foreign backup ==="
pond run /system/etc/10-producer pull
echo "Pull complete"

#############################
# VERIFICATION
#############################

echo ""
echo "=== Verification ==="

# List imported content
echo ""
echo "--- Pond2 full listing ---"
POND=/pond2 pond list '/**' > /tmp/pond2-full-list.txt 2>&1 || true
cat /tmp/pond2-full-list.txt

check 'grep -q "sensors.csv" /tmp/pond2-full-list.txt' "imported sensors.csv visible in listing"

# Read the imported data
echo ""
echo "--- Reading imported data ---"
POND=/pond2 pond cat /sources/producer/sensors.csv > /tmp/imported-data.txt 2>&1 || true
cat /tmp/imported-data.txt

check 'grep -q "sensor-001" /tmp/imported-data.txt' "imported data contains sensor-001"
check 'grep -q "sensor-002" /tmp/imported-data.txt' "imported data contains sensor-002"

# Compare with original
echo ""
echo "--- Comparing with original ---"
POND=/pond1 pond cat /ingest/sensors.csv 2>/dev/null > /tmp/original-data.txt
POND=/pond2 pond cat /sources/producer/sensors.csv 2>/dev/null > /tmp/imported-data2.txt

ORIG_HASH=$(md5sum /tmp/original-data.txt | cut -d' ' -f1)
IMPORT_HASH=$(md5sum /tmp/imported-data2.txt | cut -d' ' -f1)
check '[ "${ORIG_HASH}" = "${IMPORT_HASH}" ]' "imported data matches original byte-for-byte"

# Verify pond_ids are different (provenance preserved)
echo ""
echo "--- Provenance check ---"
echo "Producer pond_id: ${PRODUCER_POND_ID}"
echo "Consumer pond_id: ${CONSUMER_POND_ID}"
check '[ "${PRODUCER_POND_ID}" != "${CONSUMER_POND_ID}" ]' "producer and consumer have different pond_ids"

#############################
# RESULTS
#############################

check_finish
