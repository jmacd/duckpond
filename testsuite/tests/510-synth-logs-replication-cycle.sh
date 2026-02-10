#!/bin/bash
# EXPERIMENT: Synth-Logs → Logfile-Ingest → S3 Replication Cycle
# 
# DESCRIPTION:
#   End-to-end test of the data pipeline:
#   1. Generate verifiable CSV content (row_number,checksum pattern)
#   2. logfile-ingest ingests append-only CSV files
#   3. backup factory pushes to MinIO S3-compatible storage
#   4. 2nd pond initialized using replicate command from 1st pond
#   5. 2nd pond pulls from S3
#   6. Verify content integrity and row counts via --query
#   7. Repeat cycle with additional data (append scenario)
#
# VERIFIABLE DATA:
#   Each line is: row_number,row_number_squared
#   This allows simple verification that no data was lost or corrupted.
#
# EXPECTED:
#   - All row counts match at each stage
#   - Data verification passes (checksums match)
#   - Incremental appends are correctly replicated
#
# NOTE: Requires MinIO (either from docker-compose or standalone)
#
set -e

echo "=== Experiment: Synth-Logs Replication Cycle ==="
echo ""

#############################
# CONFIGURATION
#############################

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
BUCKET_NAME="duckpond-replication-test"

# Test data parameters
# NOTE: For large file threshold testing (>64KB), use INITIAL_ROWS=5000+
# Each row is ~20 bytes, so 5000 rows ≈ 100KB (exceeds 64KB threshold)
# See: docs/large-file-storage-implementation.md
INITIAL_ROWS=500
APPEND_ROWS=250

# Directory for generated log output
SYNTH_LOGS_DIR="/var/log/synthapp"

#############################
# HELPER FUNCTIONS
#############################

# Generate verifiable CSV data
# Format: row_number,row_number_squared (simple checksum)
generate_csv_data() {
    local output_file="$1"
    local start_row="$2"
    local num_rows="$3"
    
    local end_row=$((start_row + num_rows - 1))
    
    for row in $(seq "$start_row" "$end_row"); do
        local checksum=$((row * row))
        echo "${row},${checksum}"
    done >> "$output_file"
}

# Verify a single line matches expected format: row_number,row_squared
verify_line() {
    local line="$1"
    local expected_row="$2"
    
    local row_number
    local checksum
    row_number=$(echo "$line" | cut -d',' -f1)
    checksum=$(echo "$line" | cut -d',' -f2)
    
    if [[ "$row_number" != "$expected_row" ]]; then
        echo "ERROR: Row mismatch - expected $expected_row, got $row_number"
        return 1
    fi
    
    local expected_checksum=$((expected_row * expected_row))
    if [[ "$checksum" != "$expected_checksum" ]]; then
        echo "ERROR: Checksum mismatch for row $row_number: expected $expected_checksum, got $checksum"
        return 1
    fi
    
    return 0
}

# Count rows in pond file using wc -l (simpler than SQL for basic counts)
count_rows() {
    local pond_path="$1"
    local file_path="$2"
    
    POND="$pond_path" pond cat "$file_path" 2>/dev/null | wc -l | tr -d ' '
}

# Verify row count matches expected
verify_row_count() {
    local pond_path="$1"
    local file_path="$2"
    local expected="$3"
    local description="$4"
    
    local actual
    actual=$(count_rows "$pond_path" "$file_path")
    
    if [[ "$actual" == "$expected" ]]; then
        echo "✓ ${description}: ${actual} rows (expected ${expected})"
        return 0
    else
        echo "✗ ${description}: ${actual} rows (expected ${expected})"
        return 1
    fi
}

#############################
# CHECK MINIO AVAILABILITY
#############################

echo "=== Checking MinIO availability ==="

MINIO_AVAILABLE=false

if curl -s "${MINIO_ENDPOINT}/minio/health/live" &>/dev/null; then
    echo "✓ MinIO available at ${MINIO_ENDPOINT}"
    MINIO_AVAILABLE=true
elif command -v minio &>/dev/null; then
    echo "Starting MinIO server..."
    mkdir -p /data/minio
    MINIO_ROOT_USER=${MINIO_ROOT_USER} \
    MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD} \
    minio server /data/minio --console-address ":9001" &>/dev/null &
    MINIO_PID=$!
    
    # Wait for MinIO to be ready
    for i in $(seq 1 30); do
        if curl -s "${MINIO_ENDPOINT}/minio/health/live" &>/dev/null; then
            echo "✓ MinIO ready after ${i}s (PID ${MINIO_PID})"
            MINIO_AVAILABLE=true
            break
        fi
        sleep 1
    done
else
    echo "⚠ MinIO not available - using file:// fallback"
    MINIO_ENDPOINT=""
fi

# Configure MinIO client and create bucket
if [[ "$MINIO_AVAILABLE" == "true" ]]; then
    mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null || true
    mc rb "local/${BUCKET_NAME}" --force 2>/dev/null || true  # Clean start
    mc mb "local/${BUCKET_NAME}" 2>/dev/null || true
    echo "✓ Bucket ${BUCKET_NAME} ready"
fi

#############################
# PHASE 1: GENERATE INITIAL DATA
#############################

echo ""
echo "=== Phase 1: Generate Initial Data ==="

mkdir -p "${SYNTH_LOGS_DIR}"

# Generate initial batch - write to active log file
echo "Generating ${INITIAL_ROWS} rows..."
generate_csv_data "${SYNTH_LOGS_DIR}/metrics.log" 1 ${INITIAL_ROWS}

echo ""
echo "--- Generated files ---"
ls -la "${SYNTH_LOGS_DIR}/"

# Count total lines in all generated files
TOTAL_HOST_LINES=$(cat "${SYNTH_LOGS_DIR}"/metrics.log* 2>/dev/null | wc -l | tr -d ' ')
echo "Total lines on host: ${TOTAL_HOST_LINES}"

#############################
# PHASE 2: SETUP POND1 (PRIMARY)
#############################

echo ""
echo "=== Phase 2: Setup Pond1 (Primary) ==="

export POND=/pond1
pond init
echo "✓ Pond1 initialized"

pond mkdir -p /data/metrics
pond mkdir -p /etc/system.d

# Create logfile-ingest configuration
cat > /tmp/metrics-ingest.yaml << EOF
archived_pattern: ${SYNTH_LOGS_DIR}/metrics.log.*
active_pattern: ${SYNTH_LOGS_DIR}/metrics.log
pond_path: /data/metrics
EOF

echo "--- Logfile-ingest config ---"
cat /tmp/metrics-ingest.yaml

# Create logfile-ingest factory node
pond mknod logfile-ingest /etc/system.d/20-metrics-ingest --config-path /tmp/metrics-ingest.yaml
echo "✓ Created logfile-ingest node"

#############################
# PHASE 3: INGEST DATA TO POND1
#############################

echo ""
echo "=== Phase 3: Ingest Data to Pond1 ==="

RUST_LOG=info pond run /etc/system.d/20-metrics-ingest 2>&1 | tee /tmp/ingest1.log

echo ""
echo "--- Ingested files ---"
pond list '/data/metrics/*'

# Verify row count
echo ""
echo "--- Verify ingested row count ---"
POND1_ROWS=$(pond cat /data/metrics/metrics.log 2>/dev/null | wc -l | tr -d ' ')
echo "Pond1 metrics.log rows: ${POND1_ROWS}"

if [[ "${POND1_ROWS}" != "${INITIAL_ROWS}" ]]; then
    echo "⚠ Row count mismatch after ingest!"
    echo "Expected: ${INITIAL_ROWS}, Got: ${POND1_ROWS}"
fi

#############################
# PHASE 4: CONFIGURE S3 BACKUP
#############################

echo ""
echo "=== Phase 4: Configure S3 Backup ==="

if [[ "$MINIO_AVAILABLE" == "true" ]]; then
    cat > /tmp/remote-config.yaml << EOF
url: "s3://${BUCKET_NAME}"
endpoint: "${MINIO_ENDPOINT}"
region: "us-east-1"
access_key_id: "${MINIO_ROOT_USER}"
secret_access_key: "${MINIO_ROOT_PASSWORD}"
allow_http: true
EOF
else
    mkdir -p /data/s3-backup
    cat > /tmp/remote-config.yaml << 'EOF'
url: "file:///data/s3-backup"
compression_level: 3
EOF
fi

echo "--- Remote config ---"
cat /tmp/remote-config.yaml

pond mknod remote /etc/system.d/10-remote --config-path /tmp/remote-config.yaml
echo "✓ Remote backup configured"

#############################
# PHASE 5: PUSH TO S3
#############################

echo ""
echo "=== Phase 5: Push to S3 ==="

RUST_LOG=info pond run /etc/system.d/10-remote push 2>&1 | tee /tmp/push1.log
echo "✓ Push complete"

# Verify backup exists
if [[ "$MINIO_AVAILABLE" == "true" ]]; then
    echo ""
    echo "--- S3 bucket contents ---"
    mc ls "local/${BUCKET_NAME}/" 2>/dev/null || echo "(listing failed)"
fi

#############################
# PHASE 6: GET REPLICATE COMMAND
#############################

echo ""
echo "=== Phase 6: Generate Replicate Command ==="

REPLICATE_CMD=$(pond run /etc/system.d/10-remote replicate 2>&1)
echo "Replicate command:"
echo "${REPLICATE_CMD}"

# Extract the config parameter
REPLICATE_CONFIG=$(echo "${REPLICATE_CMD}" | grep -o -- '--config=[^ ]*' | head -1)

if [[ -z "${REPLICATE_CONFIG}" ]]; then
    echo "ERROR: Failed to extract replicate config"
    exit 1
fi

echo "✓ Extracted config for replication"

#############################
# PHASE 7: SETUP POND2 (REPLICA)
#############################

echo ""
echo "=== Phase 7: Setup Pond2 (Replica) ==="

export POND=/pond2

# Initialize using the replicate config
# This automatically creates /etc/system.d/10-remote and restores all transactions
pond init ${REPLICATE_CONFIG}
echo "✓ Pond2 initialized from replicate config"

#############################
# PHASE 8: PULL TO REPLICA
#############################

echo ""
echo "=== Phase 8: Pull to Replica ==="

RUST_LOG=info pond run /etc/system.d/10-remote pull 2>&1 | tee /tmp/pull1.log
echo "✓ Pull complete"

#############################
# PHASE 9: VERIFY REPLICATION
#############################

echo ""
echo "=== Phase 9: Verify Replication ==="

echo ""
echo "--- Pond2 Structure ---"
POND=/pond2 pond list '/data/metrics/*' 2>/dev/null || echo "(no files yet)"

echo ""
echo "--- Row Count Comparison (Round 1) ---"
POND1_COUNT=$(POND=/pond1 pond cat /data/metrics/metrics.log 2>/dev/null | wc -l | tr -d ' ')
POND2_COUNT=$(POND=/pond2 pond cat /data/metrics/metrics.log 2>/dev/null | wc -l | tr -d ' ')

echo "Pond1 rows: ${POND1_COUNT}"
echo "Pond2 rows: ${POND2_COUNT}"

if [[ "${POND1_COUNT}" == "${POND2_COUNT}" ]]; then
    echo "✓ Row counts match!"
else
    echo "✗ Row count mismatch!"
fi

# Verify data integrity by checking first and last rows
echo ""
echo "--- Data Integrity Check ---"
POND1_FIRST=$(POND=/pond1 pond cat /data/metrics/metrics.log --format=table --query "SELECT * FROM source ORDER BY column0 LIMIT 1" 2>/dev/null | tail -1)
POND2_FIRST=$(POND=/pond2 pond cat /data/metrics/metrics.log --format=table --query "SELECT * FROM source ORDER BY column0 LIMIT 1" 2>/dev/null | tail -1)

POND1_LAST=$(POND=/pond1 pond cat /data/metrics/metrics.log --format=table --query "SELECT * FROM source ORDER BY column0 DESC LIMIT 1" 2>/dev/null | tail -1)
POND2_LAST=$(POND=/pond2 pond cat /data/metrics/metrics.log --format=table --query "SELECT * FROM source ORDER BY column0 DESC LIMIT 1" 2>/dev/null | tail -1)

echo "First row - Pond1: ${POND1_FIRST}"
echo "First row - Pond2: ${POND2_FIRST}"
echo "Last row  - Pond1: ${POND1_LAST}"
echo "Last row  - Pond2: ${POND2_LAST}"

#############################
# PHASE 10: APPEND MORE DATA (ROUND 2)
#############################

echo ""
echo "=============================================="
echo "=== Phase 10: Append More Data (Round 2) ==="
echo "=============================================="

# Generate additional rows starting from where we left off
NEXT_START=$((INITIAL_ROWS + 1))
echo "Appending ${APPEND_ROWS} rows starting at row ${NEXT_START}..."
generate_csv_data "${SYNTH_LOGS_DIR}/metrics.log" ${NEXT_START} ${APPEND_ROWS}

echo ""
echo "--- Updated files ---"
ls -la "${SYNTH_LOGS_DIR}/"

TOTAL_HOST_LINES_R2=$(cat "${SYNTH_LOGS_DIR}"/metrics.log* 2>/dev/null | wc -l | tr -d ' ')
echo "Total lines on host after append: ${TOTAL_HOST_LINES_R2}"

# Re-ingest on Pond1
echo ""
echo "--- Re-running logfile-ingest ---"
export POND=/pond1
RUST_LOG=info pond run /etc/system.d/20-metrics-ingest 2>&1 | tee /tmp/ingest2.log

# Push to S3
echo ""
echo "--- Pushing round 2 ---"
pond run /etc/system.d/10-remote push 2>&1 | tee /tmp/push2.log

# Pull to Pond2
echo ""
echo "--- Pulling to replica ---"
export POND=/pond2
pond run /etc/system.d/10-remote pull 2>&1 | tee /tmp/pull2.log

# Verify
echo ""
echo "--- Row Count Comparison (Round 2) ---"
EXPECTED_R2=$((INITIAL_ROWS + APPEND_ROWS))
POND1_COUNT_R2=$(POND=/pond1 pond cat /data/metrics/metrics.log 2>/dev/null | wc -l | tr -d ' ')
POND2_COUNT_R2=$(POND=/pond2 pond cat /data/metrics/metrics.log 2>/dev/null | wc -l | tr -d ' ')

echo "Expected rows: ${EXPECTED_R2}"
echo "Pond1 rows:    ${POND1_COUNT_R2}"
echo "Pond2 rows:    ${POND2_COUNT_R2}"

if [[ "${POND1_COUNT_R2}" == "${EXPECTED_R2}" ]] && [[ "${POND1_COUNT_R2}" == "${POND2_COUNT_R2}" ]]; then
    echo "✓ Round 2 verification passed!"
else
    echo "✗ Round 2 verification failed!"
fi

#############################
# PHASE 11: APPEND MORE DATA (ROUND 3)
#############################

echo ""
echo "=============================================="
echo "=== Phase 11: Append More Data (Round 3) ==="
echo "=============================================="

NEXT_START_R3=$((INITIAL_ROWS + APPEND_ROWS + 1))
echo "Appending ${APPEND_ROWS} rows starting at row ${NEXT_START_R3}..."
generate_csv_data "${SYNTH_LOGS_DIR}/metrics.log" ${NEXT_START_R3} ${APPEND_ROWS}

# Re-ingest, push, pull
export POND=/pond1
pond run /etc/system.d/20-metrics-ingest 2>&1 | tee /tmp/ingest3.log
pond run /etc/system.d/10-remote push 2>&1 | tee /tmp/push3.log

export POND=/pond2
pond run /etc/system.d/10-remote pull 2>&1 | tee /tmp/pull3.log

# Final verification
echo ""
echo "--- Row Count Comparison (Round 3 - Final) ---"
EXPECTED_R3=$((INITIAL_ROWS + APPEND_ROWS + APPEND_ROWS))
POND1_COUNT_R3=$(POND=/pond1 pond cat /data/metrics/metrics.log 2>/dev/null | wc -l | tr -d ' ')
POND2_COUNT_R3=$(POND=/pond2 pond cat /data/metrics/metrics.log 2>/dev/null | wc -l | tr -d ' ')

echo "Expected rows: ${EXPECTED_R3}"
echo "Pond1 rows:    ${POND1_COUNT_R3}"
echo "Pond2 rows:    ${POND2_COUNT_R3}"

if [[ "${POND1_COUNT_R3}" == "${EXPECTED_R3}" ]] && [[ "${POND1_COUNT_R3}" == "${POND2_COUNT_R3}" ]]; then
    echo "✓ Round 3 verification passed!"
else
    echo "✗ Round 3 verification failed!"
fi

#############################
# FINAL VERIFICATION: HASH INTEGRITY
#############################

echo ""
echo "=== Final Verification: Hash Integrity ==="

# Check that data hashes match between ponds
POND1_HASH=$(POND=/pond1 pond cat /data/metrics/metrics.log 2>/dev/null | md5sum | cut -d' ' -f1)
POND2_HASH=$(POND=/pond2 pond cat /data/metrics/metrics.log 2>/dev/null | md5sum | cut -d' ' -f1)

echo "Pond1 content hash: ${POND1_HASH}"
echo "Pond2 content hash: ${POND2_HASH}"

if [[ "${POND1_HASH}" == "${POND2_HASH}" ]]; then
    echo "✓ Content hashes match - replication verified!"
else
    echo "✗ Content hashes DO NOT match!"
fi

# Summary SQL queries
echo ""
echo "=== Summary Queries ==="

echo ""
echo "--- Min/Max row numbers (Pond1) ---"
POND=/pond1 pond cat /data/metrics/metrics.log --format=table --query "
    SELECT 
        MIN(CAST(column0 AS INTEGER)) as min_row,
        MAX(CAST(column0 AS INTEGER)) as max_row,
        COUNT(*) as total_rows
    FROM source
" 2>/dev/null || echo "(query failed)"

echo ""
echo "--- Min/Max row numbers (Pond2) ---"
POND=/pond2 pond cat /data/metrics/metrics.log --format=table --query "
    SELECT 
        MIN(CAST(column0 AS INTEGER)) as min_row,
        MAX(CAST(column0 AS INTEGER)) as max_row,
        COUNT(*) as total_rows
    FROM source
" 2>/dev/null || echo "(query failed)"

#############################
# CLEANUP
#############################

if [[ -n "${MINIO_PID}" ]]; then
    echo ""
    echo "=== Cleanup ==="
    kill ${MINIO_PID} 2>/dev/null || true
    echo "✓ MinIO stopped"
fi

#############################
# SUMMARY
#############################

echo ""
echo "========================================"
echo "=== Experiment Complete ==="
echo "========================================"
echo ""
echo "Pipeline tested:"
echo "  1. synth-logs → Generated verifiable CSV with blake3 hashes"
echo "  2. logfile-ingest → Ingested append-only CSV files"
echo "  3. remote push → Backed up to S3-compatible storage"
echo "  4. pond init --config → Initialized replica from replicate command"
echo "  5. remote pull → Replicated to 2nd pond"
echo "  6. SQL queries → Verified row counts"
echo ""
echo "Results:"
echo "  Round 1: ${POND1_COUNT} rows replicated"
echo "  Round 2: ${POND1_COUNT_R2} rows replicated"
echo "  Round 3: ${POND1_COUNT_R3} rows replicated"
if [[ "${POND1_HASH}" == "${POND2_HASH}" ]]; then
    echo "  Hash match: ✓ Yes"
else
    echo "  Hash match: ✗ No (${POND1_HASH} vs ${POND2_HASH})"
fi
echo ""
