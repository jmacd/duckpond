#!/bin/bash
# TEST: Logfile Ingest - Truncated-and-rewritten file (no archive)
# DESCRIPTION: When a logfile is truncated and rewritten with different
#   content (no archived copy), the ingest must fail with a clear error
#   rather than silently ingesting wrong data.
#
# This is NOT a rotation — no archived file exists. The tracked prefix
# was destroyed. The ingest should abort the transaction.
#
set -e
source check.sh

echo "=== Test: Truncated File Without Archive ==="
echo ""

export POND=/pond
pond init

#############################
# SETUP
#############################

mkdir -p /var/log/testapp

cat > /tmp/ingest.yaml << 'EOF'
archived_pattern: /var/log/testapp/app.log.*
active_pattern: /var/log/testapp/app.log
pond_path: /logs/app
EOF

pond mkdir -p /system/run
pond mkdir -p /logs/app
pond mknod logfile-ingest /system/run/10-logs --config-path /tmp/ingest.yaml

#############################
# PHASE 1: Ingest a file
#############################

echo "=== Phase 1: Initial ingest ==="

for i in $(seq 1 200); do
    printf '{"ts":"2024-01-01T%02d:%02d:%02dZ","level":"INFO","msg":"Original entry %04d padding-xxxxxxxxx"}\n' $((i/3600%24)) $((i/60%60)) $((i%60)) $i
done > /var/log/testapp/app.log

SIZE1=$(wc -c < /var/log/testapp/app.log | tr -d ' ')
echo "  File size: ${SIZE1} bytes"

pond run /system/run/10-logs 2>/tmp/run1.log

check "pond cat /logs/app/app.log > /dev/null 2>&1" \
    "phase 1: file ingested"

#############################
# PHASE 2: Truncate and rewrite with different content, larger than before
# No archived copy — the old content is gone.
#############################

echo ""
echo "=== Phase 2: Truncate and rewrite (no archive) ==="

for i in $(seq 1 300); do
    printf '{"ts":"2024-06-01T%02d:%02d:%02dZ","level":"ERROR","msg":"Completely different content %04d yyyyyyy"}\n' $((i/3600%24)) $((i/60%60)) $((i%60)) $i
done > /var/log/testapp/app.log

SIZE2=$(wc -c < /var/log/testapp/app.log | tr -d ' ')
echo "  New file size: ${SIZE2} bytes (old was ${SIZE1})"
echo "  No archived copy exists — old prefix is destroyed"

# The ingest should fail because the prefix changed and there's
# no archived file to match against.
set +e
pond run /system/run/10-logs 2>/tmp/run2.log
RUN2_EXIT=$?
set -e

check "[ ${RUN2_EXIT} -ne 0 ]" \
    "phase 2: ingest correctly failed (exit ${RUN2_EXIT})"

check "grep -q 'Prefix verification failed' /tmp/run2.log" \
    "phase 2: error message mentions prefix verification"

# The pond should still have the ORIGINAL content (transaction rolled back)
pond cat /logs/app/app.log > /tmp/pond2.out 2>/dev/null
POND_SIZE2=$(wc -c < /tmp/pond2.out | tr -d ' ')
check "[ '${POND_SIZE2}' = '${SIZE1}' ]" \
    "phase 2: pond still has original content (${POND_SIZE2} bytes, not ${SIZE2})"

echo ""
echo "=== All phases complete ==="
check_finish
