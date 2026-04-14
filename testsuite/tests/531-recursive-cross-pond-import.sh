#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: Recursive cross-pond import via MinIO
# DESCRIPTION:
#   Tests the /** recursive import pattern. The producer pond has nested
#   subdirectories (/data/sensors/ and /data/logs/) each in their own
#   partition. The consumer imports /data/** which should pull all
#   descendant partitions.
#
#   Also tests the error message when using flat import on a directory
#   with children (should suggest /** pattern).
#
# EXPECTED:
#   - Flat import of /data shows clear error for child partitions
#   - Recursive import of /data/** imports all subdirectories
#   - Data in nested dirs is queryable in the consumer pond
set -e

source /usr/local/bin/check.sh 2>/dev/null || source check.sh 2>/dev/null || true

echo "=== Experiment: Recursive Cross-Pond Import ==="
echo ""

# MinIO configuration
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
BUCKET_NAME="recursive-import-test"

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
# POND1 — PRODUCER with nested dirs
#############################

echo ""
echo "=== Setting up Pond1 (Producer with nested directories) ==="

export POND=/pond1
pond init

# Create nested directory structure: /data/sensors/ and /data/logs/
pond mkdir /data
pond mkdir /data/sensors
pond mkdir /data/logs

# Put files in each subdirectory
cat > /tmp/temps.csv << 'EOF'
timestamp,location,temperature
2024-01-01T00:00:00Z,roof,5.2
2024-01-01T01:00:00Z,roof,4.8
2024-01-01T02:00:00Z,basement,18.1
EOF

cat > /tmp/events.csv << 'EOF'
timestamp,level,message
2024-01-01T00:00:00Z,INFO,system started
2024-01-01T01:00:00Z,WARN,disk usage 80%
2024-01-01T02:00:00Z,ERROR,connection lost
EOF

pond copy host:///tmp/temps.csv /data/sensors/temps.csv
pond copy host:///tmp/events.csv /data/logs/events.csv
echo "Created nested data structure"

# Push to MinIO
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
echo "Backup configured"

echo ""
echo "=== Pushing to MinIO ==="
pond run /system/run/10-backup push
echo "Push complete"

#############################
# POND2 — CONSUMER: flat import (should fail on child dirs)
#############################

echo ""
echo "=== Test 1: Flat import shows clear error for child partitions ==="

export POND=/pond2
pond init
pond mkdir /system
pond mkdir /system/etc

# Flat import — no /** suffix
cat > /tmp/flat-import.yaml << EOF
url: "s3://${BUCKET_NAME}"
endpoint: "${MINIO_ENDPOINT}"
region: "us-east-1"
access_key_id: "${MINIO_ROOT_USER}"
secret_access_key: "${MINIO_ROOT_PASSWORD}"
allow_http: true
import:
  source_path: "/data"
  local_path: "/imported/flat"
EOF

pond mknod remote /system/etc/10-flat --config-path /tmp/flat-import.yaml
pond run 10-flat pull

# Try to list imported content — child dirs should produce clear error
FLAT_ERR=$(POND=/pond2 pond list '/imported/flat/**' 2>&1 || true)
echo "$FLAT_ERR"

check 'echo "$FLAT_ERR" | grep -q "partition"' "flat import error mentions partition"
check 'echo "$FLAT_ERR" | grep -q "/\*\*"' "flat import error suggests /** pattern"

#############################
# POND3 — CONSUMER: recursive import
#############################

echo ""
echo "=== Test 2: Recursive import with /** ==="

export POND=/pond3
pond init
pond mkdir /system
pond mkdir /system/etc

# Recursive import — with /** suffix
cat > /tmp/recursive-import.yaml << EOF
url: "s3://${BUCKET_NAME}"
endpoint: "${MINIO_ENDPOINT}"
region: "us-east-1"
access_key_id: "${MINIO_ROOT_USER}"
secret_access_key: "${MINIO_ROOT_PASSWORD}"
allow_http: true
import:
  source_path: "/data/**"
  local_path: "/imported/recursive"
EOF

pond mknod remote /system/etc/10-recursive --config-path /tmp/recursive-import.yaml
echo "Recursive import factory created"

# Pull data
echo ""
echo "=== Pulling recursive import ==="
pond run 10-recursive pull
echo "Pull complete"

# Verify directory structure after pull
POND=/pond3 pond list '/imported/recursive/*' > /tmp/recursive-dirs.txt 2>&1 || true
cat /tmp/recursive-dirs.txt
check 'grep -q "sensors" /tmp/recursive-dirs.txt' "child dir 'sensors' visible after pull"
check 'grep -q "logs" /tmp/recursive-dirs.txt' "child dir 'logs' visible after pull"

# Verify nested data is accessible
echo ""
echo "=== Verifying imported data ==="

POND=/pond3 pond list '/imported/recursive/**' > /tmp/recursive-full.txt 2>&1 || true
cat /tmp/recursive-full.txt
check 'grep -q "temps.csv" /tmp/recursive-full.txt' "sensors/temps.csv visible"
check 'grep -q "events.csv" /tmp/recursive-full.txt' "logs/events.csv visible"

# Read the actual data
POND=/pond3 pond cat /imported/recursive/sensors/temps.csv 2>/dev/null > /tmp/imported-temps.txt || true
POND=/pond3 pond cat /imported/recursive/logs/events.csv 2>/dev/null > /tmp/imported-events.txt || true

check 'grep -q "roof" /tmp/imported-temps.txt' "sensor data contains 'roof'"
check 'grep -q "basement" /tmp/imported-temps.txt' "sensor data contains 'basement'"
check 'grep -q "connection lost" /tmp/imported-events.txt' "log data contains 'connection lost'"

# Compare with originals
POND=/pond1 pond cat /data/sensors/temps.csv 2>/dev/null > /tmp/orig-temps.txt
POND=/pond1 pond cat /data/logs/events.csv 2>/dev/null > /tmp/orig-events.txt

ORIG_T=$(md5sum /tmp/orig-temps.txt | cut -d' ' -f1)
IMP_T=$(md5sum /tmp/imported-temps.txt | cut -d' ' -f1)
ORIG_E=$(md5sum /tmp/orig-events.txt | cut -d' ' -f1)
IMP_E=$(md5sum /tmp/imported-events.txt | cut -d' ' -f1)

check '[ "$ORIG_T" = "$IMP_T" ]' "sensor data matches original"
check '[ "$ORIG_E" = "$IMP_E" ]' "log data matches original"

#############################
# RESULTS
#############################

check_finish
