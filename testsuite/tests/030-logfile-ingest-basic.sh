#!/bin/bash
# EXPERIMENT: Logfile Ingest - State Persistence Bug Hunt
# DESCRIPTION: Simulate real cron-like invocations where each run is a separate process
# BUG: Second run should NOT re-ingest same bytes - it should detect file already exists
#
# Real-world symptom observed:
#   First run:  "grew by N bytes (was 0, now N)"
#   Second run: "grew by N bytes (was 0, now N)"  <-- BUG! Should be (was N, now N)
#
set -e

echo "=== Experiment: Logfile Ingest State Persistence ==="
echo ""

# Use a fixed pond location to preserve state between runs
export POND=/pond

pond init

#############################
# SETUP HOST LOG FILES
#############################

echo "=== Setting up host log directory ==="

mkdir -p /var/log/testapp

# Create a reasonably sized log file (more realistic)
cat > /var/log/testapp/app.log << 'EOF'
{"ts":"2024-01-01T00:00:00Z","level":"INFO","msg":"Application started","component":"main"}
{"ts":"2024-01-01T00:00:01Z","level":"INFO","msg":"Connecting to database","component":"db"}
{"ts":"2024-01-01T00:00:02Z","level":"INFO","msg":"Database connected","component":"db"}
{"ts":"2024-01-01T00:00:03Z","level":"INFO","msg":"Loading configuration","component":"config"}
{"ts":"2024-01-01T00:00:04Z","level":"INFO","msg":"Starting HTTP server on :8080","component":"http"}
{"ts":"2024-01-01T00:00:05Z","level":"INFO","msg":"Server ready to accept connections","component":"http"}
EOF

HOST_SIZE=$(wc -c < /var/log/testapp/app.log | tr -d ' ')
echo "✓ Created /var/log/testapp/app.log (${HOST_SIZE} bytes)"

#############################
# CREATE INGEST CONFIG
#############################

echo ""
echo "=== Creating logfile-ingest configuration ==="

cat > /tmp/ingest.yaml << 'EOF'
archived_pattern: /var/log/testapp/app.log.*
active_pattern: /var/log/testapp/app.log
pond_path: /logs/testapp
EOF

cat /tmp/ingest.yaml

#############################
# CREATE FACTORY NODE
#############################

echo ""
echo "=== Creating logfile-ingest factory node ==="

pond mkdir -p /etc/system.d
pond mkdir -p /logs/testapp

# Use mode=pull to DISABLE auto-execution on commit
# This simulates cron-triggered runs more accurately
pond mknod logfile-ingest /etc/system.d/10-logs --config-path /tmp/ingest.yaml
echo "✓ Created logfile-ingest node (mode=pull, no auto-execution)"

#############################
# FIRST RUN - Simulate first cron invocation
#############################

echo ""
echo "=== FIRST RUN: Simulating first cron invocation ==="

RUST_LOG=info pond run /etc/system.d/10-logs 2>&1 | tee /tmp/run1.log

echo ""
echo "--- First run key messages ---"
grep -E "(grew by|Ingesting|Found.*files|was [0-9]+, now)" /tmp/run1.log || echo "(no key messages)"

#############################
# VERIFY POND STATE AFTER FIRST RUN
#############################

echo ""
echo "=== Verifying pond state after first run ==="

echo ""
echo "--- Files in pond ---"
pond list '/logs/testapp/*' 2>/dev/null || echo "ERROR: No files in pond!"

echo ""
echo "--- File content verification ---"
POND_SIZE=$(pond cat /logs/testapp/app.log 2>/dev/null | wc -c | tr -d ' ')
echo "Pond file size: ${POND_SIZE} bytes (host: ${HOST_SIZE} bytes)"

if [ "${POND_SIZE}" != "${HOST_SIZE}" ]; then
    echo "🐛 SIZE MISMATCH after first run!"
    exit 1
fi

#############################
# SECOND RUN - Simulate second cron invocation
# This is where the bug manifests
#############################

echo ""
echo "=============================================="
echo "=== SECOND RUN: Simulating second cron invocation ==="
echo "=============================================="
echo "(This is where the bug shows: 'was 0' instead of 'was ${HOST_SIZE}')"
echo ""

RUST_LOG=info pond run /etc/system.d/10-logs 2>&1 | tee /tmp/run2.log

echo ""
echo "--- Second run key messages ---"
grep -E "(grew by|Ingesting|Found.*files|was [0-9]+, now|unchanged)" /tmp/run2.log || echo "(no key messages)"

#############################
# BUG DETECTION
#############################

echo ""
echo "=== BUG ANALYSIS ==="

# Check if second run shows "was 0" - that's the bug!
if grep -q "was 0, now" /tmp/run2.log; then
    echo ""
    echo "🐛 BUG CONFIRMED: Second run shows 'was 0' - state not persisted!"
    echo ""
    echo "The factory thinks the pond file has 0 bytes, but it should have ${HOST_SIZE} bytes."
    echo ""
    echo "Debugging info:"
    echo "--- First run log ---"
    cat /tmp/run1.log
    echo ""
    echo "--- Second run log ---"
    cat /tmp/run2.log
    exit 1
fi

# Check if second run re-ingested when it shouldn't have
RUN2_INGEST=$(grep -c "Ingesting" /tmp/run2.log 2>/dev/null | tr -d '\n' || echo "0")
if [ "${RUN2_INGEST:-0}" -gt 0 ]; then
    echo ""
    echo "🐛 BUG: Second run re-ingested file that should already exist!"
    exit 1
fi

# Verify file is readable
if pond cat /logs/testapp/app.log > /dev/null 2>&1; then
    echo "✓ File exists in pond and is readable"
    echo "✓ Second run correctly detected no changes needed"
else
    echo "🐛 BUG: File was not properly ingested!"
    exit 1
fi

echo ""
echo "=== Experiment Complete - No Bug Detected ==="
