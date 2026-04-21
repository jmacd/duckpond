#!/bin/bash
# TEST: Logfile Ingest - Rotation where new file grew past old size
# DESCRIPTION: When a logfile is rotated and the new file grows larger than
#   the previously tracked cumulative_size before the next ingestion run,
#   the ingest must detect the rotation (prefix mismatch) rather than
#   treating it as a normal append.
#
# This is the scenario observed on watershop staging: the active file
# was rotated, a new file with different content appeared under the same
# name, and by the time ingestion ran the new file was already larger
# than what the pond had tracked.  The ingest must:
#   1. Detect that the prefix no longer matches (content changed)
#   2. Find the matching archived file
#   3. Rename the pond entry to the archived name
#   4. Ingest the new active file from scratch
#
set -e
source /test/../helpers/check.sh 2>/dev/null || source "$(dirname "$0")/../helpers/check.sh"

echo "=== Test: Rotation Where New File Grew Past Old Size ==="
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
# PHASE 1: Create and ingest initial logfile (~20KB)
#############################

echo "=== Phase 1: Initial ingest ==="

for i in $(seq 1 200); do
    printf '{"ts":"2024-01-01T%02d:%02d:%02dZ","level":"INFO","msg":"Original entry %04d padding-xxxxxxxxx"}\n' $((i/3600%24)) $((i/60%60)) $((i%60)) $i
done > /var/log/testapp/app.log

SIZE1=$(wc -c < /var/log/testapp/app.log | tr -d ' ')
echo "  Active file: ${SIZE1} bytes"

pond run /system/run/10-logs 2>/tmp/run1.log

POND_HASH1=$(pond run /system/run/10-logs b3sum 2>/dev/null | grep 'app.log' | awk '{print $1}')
HOST_HASH1=$(b3sum /var/log/testapp/app.log | awk '{print $1}')

check "[ '${POND_HASH1}' = '${HOST_HASH1}' ]" \
    "phase 1: initial ingest blake3 correct (${SIZE1} bytes)"

#############################
# PHASE 2: Rotate and grow past old size BEFORE next ingest
#
# This is the critical scenario:
#   - app.log (20KB, tracked) → app.log.1 (archived)
#   - new app.log appears with DIFFERENT content
#   - new app.log grows to 30KB (> the 20KB we tracked)
#   - next ingest sees size grew: "must be an append" — WRONG
#   - prefix verification catches the mismatch
#############################

echo ""
echo "=== Phase 2: Rotate, new file grows past old tracked size ==="

# Rotate: rename active → archived
mv /var/log/testapp/app.log /var/log/testapp/app.log.1
echo "  Rotated app.log → app.log.1"

# Create new active file with DIFFERENT content, LARGER than old size
for i in $(seq 1 300); do
    printf '{"ts":"2024-02-01T%02d:%02d:%02dZ","level":"WARN","msg":"New-generation entry %04d completely-different-content-yyy"}\n' $((i/3600%24)) $((i/60%60)) $((i%60)) $i
done > /var/log/testapp/app.log

SIZE2=$(wc -c < /var/log/testapp/app.log | tr -d ' ')
echo "  New active file: ${SIZE2} bytes (old tracked: ${SIZE1} bytes)"
echo "  New file is LARGER than old tracked size → looks like append"

# This is where the bug would manifest:
# The ingest sees host.size (30KB) > pond.cumulative_size (20KB) and
# assumes it's a normal append.  But the prefix is different content.
pond run /system/run/10-logs 2>/tmp/run2.log

# The ingest must NOT fail
check "[ $? -eq 0 ]" \
    "phase 2: ingestion succeeded (did not crash)"

# It must detect rotation
check "grep -q 'Rotation detected\|rotation\|Prefix verification' /tmp/run2.log" \
    "phase 2: rotation or prefix mismatch detected"

# It must NOT report a spurious error
check "! grep -q 'Transaction aborted' /tmp/run2.log" \
    "phase 2: no transaction abort"

# The archived file must exist in the pond
pond cat /logs/app/app.log.1 > /tmp/archived.out 2>/dev/null
check "diff /var/log/testapp/app.log.1 /tmp/archived.out" \
    "phase 2: archived file content preserved in pond"

# The new active must be in the pond with correct content
pond cat /logs/app/app.log > /tmp/active.out 2>/dev/null
check "diff /var/log/testapp/app.log /tmp/active.out" \
    "phase 2: new active file ingested correctly"

# blake3 for both files
POND_HASH_ARCHIVED=$(pond run /system/run/10-logs b3sum 2>/dev/null | grep 'app.log.1' | awk '{print $1}')
HOST_HASH_ARCHIVED=$(b3sum /var/log/testapp/app.log.1 | awk '{print $1}')
check "[ '${POND_HASH_ARCHIVED}' = '${HOST_HASH_ARCHIVED}' ]" \
    "phase 2: archived file blake3 correct"

POND_HASH_NEW=$(pond run /system/run/10-logs b3sum 2>/dev/null | grep -w 'app.log$' | awk '{print $1}')
HOST_HASH_NEW=$(b3sum /var/log/testapp/app.log | awk '{print $1}')
check "[ '${POND_HASH_NEW}' = '${HOST_HASH_NEW}' ]" \
    "phase 2: new active file blake3 correct"

#############################
# PHASE 3: Verify normal append still works after rotation
#############################

echo ""
echo "=== Phase 3: Normal append after rotation ==="

for i in $(seq 301 400); do
    printf '{"ts":"2024-02-02T%02d:%02d:%02dZ","level":"INFO","msg":"Post-rotation entry %04d"}\n' $((i/3600%24)) $((i/60%60)) $((i%60)) $i
done >> /var/log/testapp/app.log

SIZE3=$(wc -c < /var/log/testapp/app.log | tr -d ' ')
echo "  Active file: ${SIZE3} bytes (grew from ${SIZE2})"

pond run /system/run/10-logs 2>/tmp/run3.log

check "! grep -q 'Prefix verification failed' /tmp/run3.log" \
    "phase 3: no prefix verification failure on normal append"

pond cat /logs/app/app.log > /tmp/pond3.out
check "diff /var/log/testapp/app.log /tmp/pond3.out" \
    "phase 3: content matches after post-rotation append"

POND_HASH3=$(pond run /system/run/10-logs b3sum 2>/dev/null | grep -w 'app.log$' | awk '{print $1}')
HOST_HASH3=$(b3sum /var/log/testapp/app.log | awk '{print $1}')
check "[ '${POND_HASH3}' = '${HOST_HASH3}' ]" \
    "phase 3: blake3 correct after post-rotation append"

echo ""
echo "=== All phases complete ==="
check_finish
