#!/bin/bash
# EXPERIMENT: Logfile Ingest - Large File (>64KB) Persistence
# DESCRIPTION: Test that large files correctly persist bao_outboard cumulative_size
# BUG REPRODUCED: Files >64KB use HybridWriter's external storage path, which was
#                 not computing bao_outboard correctly (cumulative_size was 0)
#
# The threshold is 64KB (LARGE_FILE_THRESHOLD in large_files.rs)
#
set -e

echo "=== Experiment: Logfile Ingest Large File Persistence ==="
echo ""

# Use a fixed pond location to preserve state between runs
export POND=/pond

pond init

#############################
# SETUP - Create 100KB log file (above 64KB threshold)
#############################

echo "=== Setting up host log directory with LARGE file ==="

mkdir -p /var/log/testapp

# Generate a 100KB log file (well above 64KB threshold)
# Each line is ~100 bytes, so 1000 lines = ~100KB
echo "Generating 100KB log file..."
for i in $(seq 1 1000); do
    printf '{"ts":"2024-01-01T00:%02d:%02dZ","level":"INFO","msg":"Log entry number %04d with padding to make it longer"}\n' $((i/60)) $((i%60)) $i
done > /var/log/testapp/app.log

HOST_SIZE=$(wc -c < /var/log/testapp/app.log | tr -d ' ')
echo "Host log file: ${HOST_SIZE} bytes"

if [ "${HOST_SIZE}" -lt 65536 ]; then
    echo "ERROR: File must be >64KB to test large file path!"
    exit 1
fi
echo "✓ File size ${HOST_SIZE} > 65536 (64KB threshold) - will use large file path"

#############################
# CREATE INGEST CONFIG
#############################

echo ""
echo "=== Creating logfile-ingest configuration ==="

cat > /tmp/ingest.yaml << 'EOF'
archived_pattern: /var/log/testapp/app.log.*
active_pattern: /var/log/testapp/app.log
pond_path: /logs/app
EOF

cat /tmp/ingest.yaml

#############################
# CREATE FACTORY NODE
#############################

echo ""
echo "=== Creating logfile-ingest factory node ==="

pond mkdir -p /etc/system.d
pond mkdir -p /logs/app

pond mknod logfile-ingest /etc/system.d/10-logs --config-path /tmp/ingest.yaml
echo "✓ Created logfile-ingest node"

#############################
# FIRST RUN
#############################

echo ""
echo "=============================================="
echo "=== FIRST RUN ==="
echo "=============================================="

RUST_LOG=provider::factory::logfile_ingest=debug,tlogfs::file=debug,info pond run /etc/system.d/10-logs 2>&1 | tee /tmp/run1.log

echo ""
echo "--- First run: key messages ---"
grep -E "(bao_total_size|Ingesting|is_large|cumulative_size)" /tmp/run1.log | head -10 || echo "(none)"

#############################
# VERIFY AFTER FIRST RUN
#############################

echo ""
echo "=== Pond state after first run ==="
pond list '/logs/app/*' 2>&1 || echo "(no files)"

# Verify large file was created
if ls /pond/data/_large_files/*.parquet 2>/dev/null | head -1; then
    echo "✓ Large file parquet created in _large_files/"
else
    echo "⚠ No large file parquet found (might be inline)"
fi

POND_SIZE=$(pond cat /logs/app/app.log 2>/dev/null | wc -c | tr -d ' ')
echo "Pond file size: ${POND_SIZE} bytes (host: ${HOST_SIZE} bytes)"

if [ "${POND_SIZE}" != "${HOST_SIZE}" ]; then
    echo "🐛 SIZE MISMATCH: Pond has ${POND_SIZE} bytes but host has ${HOST_SIZE}"
    exit 1
fi

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
grep -E "(grew by|Ingesting|was [0-9]+|unchanged|cumulative_size)" /tmp/run2.log || echo "(none)"

#############################
# BUG DETECTION
#############################

echo ""
echo "=== BUG ANALYSIS ==="

# Check for the specific bug: "was 0" means bao_outboard.cumulative_size was 0
if grep -q "was 0, now" /tmp/run2.log; then
    echo ""
    echo "🐛 BUG CONFIRMED: Second run shows 'was 0' - bao_outboard not computed correctly!"
    echo ""
    echo "The HybridWriter large file path did not compute cumulative_size."
    echo "Expected: bao_state.total_size = ${HOST_SIZE}"
    echo ""
    echo "--- Second run log ---"
    cat /tmp/run2.log
    exit 1
fi

# Check that second run shows "unchanged" with correct byte count
if grep -q "unchanged (${HOST_SIZE} bytes)" /tmp/run2.log; then
    echo "✅ Second run correctly detected file unchanged at ${HOST_SIZE} bytes"
elif grep -q "Ingesting" /tmp/run2.log; then
    echo ""
    echo "🐛 BUG: Second run re-ingested file that should already exist!"
    cat /tmp/run2.log
    exit 1
else
    echo "⚠ No 'unchanged' message found. Checking for other activity..."
    grep -E "Active file|Found.*files" /tmp/run2.log || echo "(no key messages)"
fi

echo ""
echo "=== Experiment Complete - Large File Path Works ==="
