#!/bin/bash
# EXPERIMENT: S3 Replication with MinIO
# DESCRIPTION:
#   - Start MinIO as local S3 server
#   - Configure Pond1 as primary with S3 backup
#   - Configure Pond2 as replica pulling from S3
#   - Write files to Pond1, verify they appear in Pond2
#
# EXPECTED:
#   - MinIO serves as S3-compatible storage
#   - pond run /etc/system.d/10-remote push uploads to S3
#   - pond run /etc/system.d/10-remote pull downloads to replica
#
# NOTE: This experiment requires the MinIO container or binary
#       If running standalone, MinIO must be started separately
#
set -e

echo "=== Experiment: S3 Replication with MinIO ==="
echo ""

# MinIO configuration
MINIO_ROOT_USER="minioadmin"
MINIO_ROOT_PASSWORD="minioadmin"
MINIO_ENDPOINT="http://localhost:9000"
BUCKET_NAME="duckpond-backup"

#############################
# START MINIO (if available)
#############################

echo "=== Checking MinIO availability ==="

# Check if MinIO is already running
if curl -s "${MINIO_ENDPOINT}/minio/health/live" &>/dev/null; then
    echo "✓ MinIO already running at ${MINIO_ENDPOINT}"
elif command -v minio &>/dev/null; then
    echo "Starting MinIO server..."
    mkdir -p /data/minio
    MINIO_ROOT_USER=${MINIO_ROOT_USER} \
    MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD} \
    minio server /data/minio --console-address ":9001" &
    MINIO_PID=$!
    echo "MinIO started with PID ${MINIO_PID}"
    
    # Wait for MinIO to be ready
    for i in $(seq 1 30); do
        if curl -s "${MINIO_ENDPOINT}/minio/health/live" &>/dev/null; then
            echo "✓ MinIO ready after ${i}s"
            break
        fi
        sleep 1
    done
else
    echo "⚠ MinIO not available"
    echo "This experiment requires MinIO. Options:"
    echo "  1. Use docker-compose with MinIO service"
    echo "  2. Install MinIO: wget https://dl.min.io/server/minio/release/linux-amd64/minio"
    echo ""
    echo "Continuing with file:// backup for demonstration..."
    MINIO_ENDPOINT=""
fi

#############################
# CONFIGURE MINIO CLIENT
#############################

if [[ -n "${MINIO_ENDPOINT}" ]]; then
    echo ""
    echo "=== Configuring MinIO client ==="
    
    mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null || true
    mc mb "local/${BUCKET_NAME}" 2>/dev/null || echo "Bucket may already exist"
    echo "✓ MinIO configured"
fi

#############################
# SETUP POND1 - PRIMARY
#############################

echo ""
echo "=== Setting up Pond1 (Primary) ==="

export POND=/pond1
pond init
echo "✓ Pond1 initialized"

pond mkdir /data
pond mkdir /etc
pond mkdir /etc/system.d

# Create test data
echo "Creating test data..."
cat > /tmp/measurements.csv << 'EOF'
timestamp,sensor_id,temperature,humidity
2024-01-01T00:00:00Z,sensor-001,22.5,45
2024-01-01T01:00:00Z,sensor-001,23.1,44
2024-01-01T02:00:00Z,sensor-001,22.8,46
2024-01-01T03:00:00Z,sensor-002,21.0,50
2024-01-01T04:00:00Z,sensor-002,20.5,52
EOF

pond copy /tmp/measurements.csv /data/measurements.csv
echo "✓ Test data created"

# Configure remote backup
echo "Configuring remote backup..."

if [[ -n "${MINIO_ENDPOINT}" ]]; then
    cat > /tmp/remote-config.yaml << EOF
url: "s3://${BUCKET_NAME}"
endpoint: "${MINIO_ENDPOINT}"
region: "us-east-1"
access_key_id: "${MINIO_ROOT_USER}"
secret_access_key: "${MINIO_ROOT_PASSWORD}"
allow_http: true
EOF
else
    # Fallback to file:// for local testing
    mkdir -p /data/s3-backup
    cat > /tmp/remote-config.yaml << 'EOF'
url: "file:///data/s3-backup"
compression_level: 3
EOF
fi

pond mknod remote /etc/system.d/10-remote --config-path /tmp/remote-config.yaml
echo "✓ Remote backup configured"

#############################
# PUSH TO REMOTE
#############################

echo ""
echo "=== Pushing Pond1 to remote ==="

pond run /etc/system.d/10-remote push
echo "✓ Push complete"

# Verify backup exists
echo ""
echo "=== Verifying backup ==="
if [[ -n "${MINIO_ENDPOINT}" ]]; then
    mc ls "local/${BUCKET_NAME}/" 2>/dev/null || echo "Listing bucket contents..."
else
    echo "Local backup contents:"
    ls -la /data/s3-backup/ 2>/dev/null || echo "Backup directory contents not visible yet"
fi

#############################
# SETUP POND2 - REPLICA
#############################

echo ""
echo "=== Setting up Pond2 (Replica) ==="

export POND=/pond2
pond init
echo "✓ Pond2 initialized"

pond mkdir /etc
pond mkdir /etc/system.d

# Configure Pond2 to pull from same remote
pond mknod remote /etc/system.d/10-remote --config-path /tmp/remote-config.yaml
echo "✓ Remote configured for Pond2"

#############################
# PULL TO REPLICA
#############################

echo ""
echo "=== Pulling to Pond2 (Replica) ==="

pond run /etc/system.d/10-remote pull
echo "✓ Pull complete"

#############################
# VERIFY REPLICATION
#############################

echo ""
echo "=== Verification ==="

echo ""
echo "--- Pond1 (Primary) Structure ---"
POND=/pond1 pond list '/*'

echo ""
echo "--- Pond1 Data ---"
POND=/pond1 pond cat /data/measurements.csv

echo ""
echo "--- Pond2 (Replica) Structure ---"
POND=/pond2 pond list '/*'

echo ""
echo "--- Pond2 Data (should match Pond1) ---"
POND=/pond2 pond cat /data/measurements.csv 2>/dev/null || echo "(Data may be at different path after replication)"

# Compare data
echo ""
echo "--- Comparison ---"
POND1_HASH=$(POND=/pond1 pond cat /data/measurements.csv 2>/dev/null | md5sum | cut -d' ' -f1)
POND2_HASH=$(POND=/pond2 pond cat /data/measurements.csv 2>/dev/null | md5sum | cut -d' ' -f1) || POND2_HASH="N/A"

if [[ "${POND1_HASH}" == "${POND2_HASH}" ]]; then
    echo "✓ Data matches! Replication successful."
else
    echo "⚠ Data mismatch or replication not complete"
    echo "  Pond1 hash: ${POND1_HASH}"
    echo "  Pond2 hash: ${POND2_HASH}"
fi

#############################
# CLEANUP
#############################

if [[ -n "${MINIO_PID}" ]]; then
    echo ""
    echo "=== Cleanup ==="
    kill ${MINIO_PID} 2>/dev/null || true
    echo "✓ MinIO stopped"
fi

echo ""
echo "=== Experiment Complete ==="
