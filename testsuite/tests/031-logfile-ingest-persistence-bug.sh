#!/bin/bash
# EXPERIMENT: Logfile Ingest - State Persistence Bug
# DESCRIPTION: Reproduce the "was 0" bug where second run doesn't see first run's state
# EXPECTED: Second run should show "was N, now N" (unchanged), not "was 0, now N"
#
set -e

echo "=== Experiment: Logfile Ingest State Persistence Bug ==="
echo ""

#############################
# SETUP
#############################

export POND=/pond
LOGDIR=/var/log/testapp

mkdir -p "${LOGDIR}"

# Create test log file
cat > "${LOGDIR}/app.log" << 'EOF'
{"ts":"2024-01-01T00:00:00Z","level":"INFO","msg":"Application started"}
{"ts":"2024-01-01T00:00:01Z","level":"INFO","msg":"Connecting to database"}
{"ts":"2024-01-01T00:00:02Z","level":"INFO","msg":"Database connected"}
{"ts":"2024-01-01T00:00:03Z","level":"INFO","msg":"Loading configuration"}
{"ts":"2024-01-01T00:00:04Z","level":"INFO","msg":"Server ready"}
EOF

HOST_SIZE=$(wc -c < "${LOGDIR}/app.log" | tr -d ' ')
echo "Host log file: ${HOST_SIZE} bytes"

# Create ingest config
cat > /tmp/ingest.yaml << EOF
archived_pattern: ${LOGDIR}/app.log.*
active_pattern: ${LOGDIR}/app.log
pond_path: /logs/app
EOF

echo ""
echo "=== Config ==="
cat /tmp/ingest.yaml

#############################
# INITIALIZE POND
#############################

echo ""
echo "=== Initializing pond ==="
pond init

#############################
# CREATE FACTORY NODE
#############################

echo ""
echo "=== Creating logfile-ingest factory node ==="
pond mkdir -p /etc/system.d
pond mkdir -p /logs/app
pond mknod logfile-ingest /etc/system.d/10-logs --config-path /tmp/ingest.yaml

echo ""
echo "=== Pond structure after mknod ==="
pond list '/*'

#############################
# FIRST RUN
#############################

echo ""
echo "=============================================="
echo "=== FIRST RUN ==="
echo "=============================================="

RUST_LOG=provider::factory::logfile_ingest=debug,info pond run /etc/system.d/10-logs 2>&1 | tee /tmp/run1.log

echo ""
echo "--- First run: key messages ---"
grep -E "(grew by|Ingesting|was [0-9]+|unchanged|cumulative_size)" /tmp/run1.log || echo "(none)"

#############################
# VERIFY AFTER FIRST RUN
#############################

echo ""
echo "=== Pond state after first run ==="
pond list '/logs/app/*' 2>&1 || echo "(no files)"

POND_SIZE=$(pond cat /logs/app/app.log 2>/dev/null | wc -c | tr -d ' ')
echo "Pond file size: ${POND_SIZE} bytes (host: ${HOST_SIZE} bytes)"

if [ "${POND_SIZE}" != "${HOST_SIZE}" ]; then
    echo "🐛 SIZE MISMATCH: Pond has ${POND_SIZE} bytes but host has ${HOST_SIZE}"
    exit 1
fi

echo ""
echo "=== Delta table contents (checking bao_outboard is persisted) ==="
# This shows the raw Delta table to verify bao_outboard was written
pond show --sql "SELECT part_id, node_id, version, size, blake3 IS NOT NULL as has_blake3, bao_outboard IS NOT NULL as has_bao FROM delta_table WHERE file_type = 'FilePhysicalSeries' ORDER BY timestamp DESC LIMIT 5" 2>&1 || echo "(SQL failed)"

#############################
# SECOND RUN - THE BUG CHECK
#############################

echo ""
echo "=============================================="
echo "=== SECOND RUN ==="
echo "=== Expected: 'unchanged (${HOST_SIZE} bytes)' ==="
echo "=== Bug symptom: 'was 0, now ${HOST_SIZE}' ==="
echo "=============================================="

RUST_LOG=provider::factory::logfile_ingest=debug,info pond run /etc/system.d/10-logs 2>&1 | tee /tmp/run2.log

echo ""
echo "--- Second run: key messages ---"
grep -E "(grew by|Ingesting|was [0-9]+|unchanged|Found.*files in pond)" /tmp/run2.log || echo "(none)"

#############################
# BUG DETECTION
#############################

echo ""
echo "=== BUG ANALYSIS ==="

# Check for the specific bug: "was 0" means pond_state.cumulative_size was 0
if grep -q "was 0, now" /tmp/run2.log; then
    echo ""
    echo "🐛 BUG CONFIRMED: Second run shows 'was 0' - pond file size not read correctly!"
    echo ""
    echo "Expected: 'unchanged (${HOST_SIZE} bytes)' or no change message"
    echo "Got: 'was 0, now ${HOST_SIZE}' - the pond state was not persisted or read back"
    echo ""
    echo "--- Second run log ---"
    cat /tmp/run2.log
    exit 1
fi

# Check that second run shows "unchanged" with correct byte count
if grep -q "unchanged (${HOST_SIZE} bytes)" /tmp/run2.log; then
    echo "✅ Second run correctly detected file unchanged at ${HOST_SIZE} bytes"
else
    # If no unchanged message, check if it re-ingested (also a bug)
    if grep -q "Ingesting" /tmp/run2.log; then
        echo ""
        echo "🐛 BUG: Second run re-ingested file that should already exist!"
        cat /tmp/run2.log
        exit 1
    fi
    # If no unchanged and no ingesting, something else is wrong
    echo "⚠️  No 'unchanged' message found. Checking logs..."
    grep -E "Active file" /tmp/run2.log || echo "(no Active file messages)"
fi

echo ""
echo "=== Experiment Complete ==="
