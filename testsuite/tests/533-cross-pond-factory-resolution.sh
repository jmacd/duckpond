#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: Cross-pond import with dynamic factory resolution
# DESCRIPTION:
#   Tests that factories using absolute paths work correctly after
#   cross-pond import. The producer pond has a dynamic-dir with
#   synthetic-timeseries entries and a timeseries-join that references
#   them via absolute paths (/sensors/station_a, /sensors/station_b).
#
#   The consumer imports the producer's entire tree. When the consumer
#   reads /imports/producer/sensors/combined, the timeseries-join
#   factory must resolve its input patterns within the imported foreign
#   tree -- not the consumer's global root.
#
#   This validates that factories use context.root() (which respects
#   the effective_root / pond boundary) rather than fs.root().
#
# EXPECTED:
#   - Producer: timeseries-join produces combined output with scoped columns
#   - Consumer: after import, same path produces identical output
#   - The factory's absolute paths resolve within the foreign tree
set -e

source /usr/local/bin/check.sh 2>/dev/null || source check.sh 2>/dev/null || true

echo "=== Experiment: Cross-Pond Dynamic Factory Resolution ==="
echo ""

# MinIO configuration
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
BUCKET_NAME="factory-import-test"

#############################
# CONFIGURE MINIO
#############################

echo "=== Configuring MinIO ==="
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
# POND1 -- PRODUCER
# Has synthetic sensors + timeseries-join
#############################

echo ""
echo "=== Setting up Pond1 (Producer with dynamic factories) ==="

export POND=/pond1
pond init

PRODUCER_POND_ID=$(pond config 2>/dev/null | grep "Pond ID" | awk '{print $NF}')
echo "Producer pond_id: ${PRODUCER_POND_ID}"

# Create a dynamic-dir with two synthetic timeseries and a join.
# The timeseries-join uses absolute paths: /sensors/station_a, /sensors/station_b
cat > /tmp/sensors.yaml << 'YAML'
entries:
  - name: "station_a"
    factory: "synthetic-timeseries"
    config:
      start: "2024-01-01T00:00:00Z"
      end:   "2024-01-10T00:00:00Z"
      interval: "1h"
      points:
        - name: "temperature"
          components:
            - type: sine
              amplitude: 5.0
              period: "24h"
              offset: 20.0
        - name: "pressure"
          components:
            - type: sine
              amplitude: 3.0
              period: "12h"
              offset: 1013.0

  - name: "station_b"
    factory: "synthetic-timeseries"
    config:
      start: "2024-01-05T00:00:00Z"
      end:   "2024-01-15T00:00:00Z"
      interval: "1h"
      points:
        - name: "temperature"
          components:
            - type: triangle
              amplitude: 8.0
              period: "24h"
              offset: 30.0
        - name: "pressure"
          components:
            - type: square
              amplitude: 4.0
              period: "6h"
              offset: 1010.0

  - name: "combined"
    factory: "timeseries-join"
    config:
      inputs:
        - pattern: "/sensors/station_a"
          scope: StationA
        - pattern: "/sensors/station_b"
          scope: StationB
YAML

pond mknod dynamic-dir /sensors --config-path /tmp/sensors.yaml
echo "Producer dynamic-dir created"

# Query the combined output from the producer for reference
echo ""
echo "=== Producer: query combined join output ==="
POND=/pond1 pond cat /sensors/combined --format=table --sql "
  SELECT COUNT(*) AS rows,
    MIN(timestamp) AS first_ts,
    MAX(timestamp) AS last_ts
  FROM source
" > /tmp/producer-summary.txt 2>&1
cat /tmp/producer-summary.txt

# Get the actual data for comparison
POND=/pond1 pond cat /sensors/combined --sql "
  SELECT timestamp,
    \"StationA.temperature\",
    \"StationA.pressure\",
    \"StationB.temperature\",
    \"StationB.pressure\"
  FROM source
  ORDER BY timestamp
" > /tmp/producer-data.parquet 2>/dev/null

POND=/pond1 pond cat /sensors/combined --format=table --sql "
  SELECT
    COUNT(*) AS total_rows,
    COUNT(\"StationA.temperature\") AS a_rows,
    COUNT(\"StationB.temperature\") AS b_rows
  FROM source
" > /tmp/producer-counts.txt 2>&1
cat /tmp/producer-counts.txt

check 'grep -q "rows" /tmp/producer-summary.txt' "producer combined has rows"

#############################
# PUSH BACKUP
#############################

echo ""
echo "=== Pushing producer backup to MinIO ==="

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
pond run /system/run/10-backup push
echo "Push complete"

#############################
# POND2 -- CONSUMER
# Imports the producer by root
#############################

echo ""
echo "=== Setting up Pond2 (Consumer with import) ==="

export POND=/pond2
pond init

CONSUMER_POND_ID=$(pond config 2>/dev/null | grep "Pond ID" | awk '{print $NF}')
echo "Consumer pond_id: ${CONSUMER_POND_ID}"

pond mkdir /system
pond mkdir /system/etc

# Import the producer's entire tree
cat > /tmp/import-config.yaml << EOF
url: "s3://${BUCKET_NAME}/pond-${PRODUCER_POND_ID}"
endpoint: "${MINIO_ENDPOINT}"
region: "us-east-1"
access_key_id: "${MINIO_ROOT_USER}"
secret_access_key: "${MINIO_ROOT_PASSWORD}"
allow_http: true
import:
  source_path: "/**"
  local_path: "/imports/producer"
EOF

pond mknod remote /system/etc/10-producer --config-path /tmp/import-config.yaml
echo "Import factory created"

pond run /system/etc/10-producer pull
echo "Import pull complete"

#############################
# VERIFY IMPORTED STRUCTURE
#############################

echo ""
echo "=== Verify imported directory structure ==="
POND=/pond2 pond list '/imports/producer/*' > /tmp/consumer-listing.txt 2>&1 || true
cat /tmp/consumer-listing.txt

check 'grep -q "sensors" /tmp/consumer-listing.txt' "imported /sensors visible"

# List inside the imported sensors directory
POND=/pond2 pond list '/imports/producer/sensors/*' > /tmp/consumer-sensors.txt 2>&1 || true
cat /tmp/consumer-sensors.txt

check 'grep -q "station_a" /tmp/consumer-sensors.txt' "imported station_a visible"
check 'grep -q "station_b" /tmp/consumer-sensors.txt' "imported station_b visible"
check 'grep -q "combined" /tmp/consumer-sensors.txt' "imported combined visible"

#############################
# THE KEY TEST: Factory output matches
#############################

echo ""
echo "=== Key test: query combined via imported path ==="

# Query the combined output through the import mount.
# The timeseries-join factory must resolve /sensors/station_a and
# /sensors/station_b within the foreign tree, not the consumer's root.
if ! POND=/pond2 pond cat /imports/producer/sensors/combined --format=table --sql "
  SELECT COUNT(*) AS rows,
    MIN(timestamp) AS first_ts,
    MAX(timestamp) AS last_ts
  FROM source
" > /tmp/consumer-summary.txt 2>&1; then
  echo "pond cat failed:"
  cat /tmp/consumer-summary.txt
  exit 1
fi
cat /tmp/consumer-summary.txt

check 'grep -q "rows" /tmp/consumer-summary.txt' "consumer combined has rows"

# Compare row counts
POND=/pond2 pond cat /imports/producer/sensors/combined --format=table --sql "
  SELECT
    COUNT(*) AS total_rows,
    COUNT(\"StationA.temperature\") AS a_rows,
    COUNT(\"StationB.temperature\") AS b_rows
  FROM source
" > /tmp/consumer-counts.txt 2>&1
cat /tmp/consumer-counts.txt

# Extract just the data lines for comparison
PRODUCER_TOTAL=$(grep -o '[0-9]\+' /tmp/producer-counts.txt | head -1)
CONSUMER_TOTAL=$(grep -o '[0-9]\+' /tmp/consumer-counts.txt | head -1)
echo "Producer total rows: ${PRODUCER_TOTAL}"
echo "Consumer total rows: ${CONSUMER_TOTAL}"
check '[ "${PRODUCER_TOTAL}" = "${CONSUMER_TOTAL}" ]' "row counts match between producer and consumer"

# Compare actual data (parquet byte comparison)
POND=/pond2 pond cat /imports/producer/sensors/combined --sql "
  SELECT timestamp,
    \"StationA.temperature\",
    \"StationA.pressure\",
    \"StationB.temperature\",
    \"StationB.pressure\"
  FROM source
  ORDER BY timestamp
" > /tmp/consumer-data.parquet 2>/dev/null

PROD_HASH=$(md5sum /tmp/producer-data.parquet | cut -d' ' -f1)
CONS_HASH=$(md5sum /tmp/consumer-data.parquet | cut -d' ' -f1)
echo "Producer data hash: ${PROD_HASH}"
echo "Consumer data hash: ${CONS_HASH}"
check '[ "${PROD_HASH}" = "${CONS_HASH}" ]' "imported factory output matches producer byte-for-byte"

#############################
# INDIVIDUAL SERIES MATCH
#############################

echo ""
echo "=== Verify individual series accessible via import ==="

# station_a through import should produce the same data as from producer
POND=/pond1 pond cat /sensors/station_a --format=table --sql "SELECT COUNT(*) AS cnt FROM source" > /tmp/prod-a.txt 2>&1
POND=/pond2 pond cat /imports/producer/sensors/station_a --format=table --sql "SELECT COUNT(*) AS cnt FROM source" > /tmp/cons-a.txt 2>&1

PROD_A=$(grep -o '[0-9]\+' /tmp/prod-a.txt | head -1)
CONS_A=$(grep -o '[0-9]\+' /tmp/cons-a.txt | head -1)
check '[ "${PROD_A}" = "${CONS_A}" ]' "station_a row count matches (${PROD_A} vs ${CONS_A})"

POND=/pond1 pond cat /sensors/station_b --format=table --sql "SELECT COUNT(*) AS cnt FROM source" > /tmp/prod-b.txt 2>&1
POND=/pond2 pond cat /imports/producer/sensors/station_b --format=table --sql "SELECT COUNT(*) AS cnt FROM source" > /tmp/cons-b.txt 2>&1

PROD_B=$(grep -o '[0-9]\+' /tmp/prod-b.txt | head -1)
CONS_B=$(grep -o '[0-9]\+' /tmp/cons-b.txt | head -1)
check '[ "${PROD_B}" = "${CONS_B}" ]' "station_b row count matches (${PROD_B} vs ${CONS_B})"

#############################
# PROVENANCE
#############################

echo ""
echo "=== Provenance check ==="
echo "Producer pond_id: ${PRODUCER_POND_ID}"
echo "Consumer pond_id: ${CONSUMER_POND_ID}"
check '[ "${PRODUCER_POND_ID}" != "${CONSUMER_POND_ID}" ]' "producer and consumer have different pond_ids"

#############################
# RESULTS
#############################

check_finish
