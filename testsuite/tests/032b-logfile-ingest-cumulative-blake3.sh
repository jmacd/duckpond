#!/bin/bash
# TEST: Logfile Ingest - Cumulative BLAKE3 correctness across appends
# DESCRIPTION: Verifies that cumulative_blake3 stays correct as a logfile
#   grows through multiple ingestion cycles, including across the 64KB
#   large-file threshold.
#
# REGRESSION: Previously, HybridWriter was not resumed from the previous
#   bao-tree frontier, causing incorrect cumulative_blake3 for files with
#   externally stored content (>64KB) or cumulative size >10MB. This made
#   logfile-ingest prefix verification fail with spurious "File may have
#   been rotated" errors.
#
set -e
source check.sh

echo "=== Test: Logfile Ingest Cumulative BLAKE3 ==="
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
# PHASE 1: Initial 40KB file (below 64KB large-file threshold)
#############################

echo "=== Phase 1: Initial ingest (40KB, inline) ==="

# Generate ~40KB of log content
for i in $(seq 1 400); do
    printf '{"ts":"2024-01-01T00:%02d:%02dZ","level":"INFO","msg":"Entry %04d padding-to-fill-line-length-past-one-hundred-bytes-xxxxxxxx"}\n' $((i/60)) $((i%60)) $i
done > /var/log/testapp/app.log

SIZE1=$(wc -c < /var/log/testapp/app.log | tr -d ' ')
echo "  File size: ${SIZE1} bytes"

pond run /system/run/10-logs 2>/tmp/run1.log

# Verify content matches
pond cat /logs/app/app.log > /tmp/pond1.out
check "diff /var/log/testapp/app.log /tmp/pond1.out" \
    "phase 1: pond content matches host (${SIZE1} bytes)"

# Get cumulative blake3 from pond via b3sum command
POND_HASH1=$(pond run /system/run/10-logs b3sum 2>/dev/null | grep 'app.log' | awk '{print $1}')

# Compute expected blake3 of host file using b3sum
HOST_HASH1=$(b3sum /var/log/testapp/app.log | awk '{print $1}')

check "[ '${POND_HASH1}' = '${HOST_HASH1}' ]" \
    "phase 1: cumulative blake3 matches (${HOST_HASH1:0:16}...)"

#############################
# PHASE 2: Append to cross 64KB threshold (large-file path)
#############################

echo ""
echo "=== Phase 2: Append past 64KB threshold ==="

# Append ~40KB more, crossing the 64KB threshold for new version content
for i in $(seq 401 800); do
    printf '{"ts":"2024-01-02T00:%02d:%02dZ","level":"WARN","msg":"Entry %04d padding-to-fill-line-length-past-one-hundred-bytes-xxxxxxxx"}\n' $((i/60)) $((i%60)) $i
done >> /var/log/testapp/app.log

SIZE2=$(wc -c < /var/log/testapp/app.log | tr -d ' ')
echo "  File size: ${SIZE2} bytes (grew from ${SIZE1})"

pond run /system/run/10-logs 2>/tmp/run2.log

# Verify content
pond cat /logs/app/app.log > /tmp/pond2.out
check "diff /var/log/testapp/app.log /tmp/pond2.out" \
    "phase 2: pond content matches host (${SIZE2} bytes)"

# Verify no re-ingestion of full file
check "grep -q 'Appending to app.log' /tmp/run2.log" \
    "phase 2: detected as append (not re-ingest)"

# Verify cumulative blake3
POND_HASH2=$(pond run /system/run/10-logs b3sum 2>/dev/null | grep 'app.log' | awk '{print $1}')
HOST_HASH2=$(b3sum /var/log/testapp/app.log | awk '{print $1}')

check "[ '${POND_HASH2}' = '${HOST_HASH2}' ]" \
    "phase 2: cumulative blake3 correct after crossing 64KB (${HOST_HASH2:0:16}...)"

#############################
# PHASE 3: Small append (exercises pending-byte resumption)
#############################

echo ""
echo "=== Phase 3: Small append (pending byte edge case) ==="

# Append just a few lines
for i in $(seq 801 810); do
    printf '{"ts":"2024-01-03T00:%02d:%02dZ","level":"ERROR","msg":"Entry %04d"}\n' $((i/60)) $((i%60)) $i
done >> /var/log/testapp/app.log

SIZE3=$(wc -c < /var/log/testapp/app.log | tr -d ' ')
echo "  File size: ${SIZE3} bytes (grew from ${SIZE2})"

pond run /system/run/10-logs 2>/tmp/run3.log

pond cat /logs/app/app.log > /tmp/pond3.out
check "diff /var/log/testapp/app.log /tmp/pond3.out" \
    "phase 3: pond content matches host (${SIZE3} bytes)"

POND_HASH3=$(pond run /system/run/10-logs b3sum 2>/dev/null | grep 'app.log' | awk '{print $1}')
HOST_HASH3=$(b3sum /var/log/testapp/app.log | awk '{print $1}')

check "[ '${POND_HASH3}' = '${HOST_HASH3}' ]" \
    "phase 3: cumulative blake3 correct after small append (${HOST_HASH3:0:16}...)"

#############################
# PHASE 4: Idempotency — re-run with no change
#############################

echo ""
echo "=== Phase 4: Re-run with no change ==="

pond run /system/run/10-logs 2>/tmp/run4.log

check "grep -q 'no changes' /tmp/run4.log" \
    "phase 4: no-change run detected correctly"

# blake3 unchanged
POND_HASH4=$(pond run /system/run/10-logs b3sum 2>/dev/null | grep 'app.log' | awk '{print $1}')
check "[ '${POND_HASH4}' = '${HOST_HASH3}' ]" \
    "phase 4: blake3 unchanged after no-op run"

#############################
# PHASE 5: Grow past 10MB cumulative (the production failure size)
#############################

echo ""
echo "=== Phase 5: Grow file to >10MB cumulative ==="

# Append ~10MB in one shot — each line ~130 bytes, need ~80000 lines
for i in $(seq 811 81000); do
    printf '{"ts":"2024-02-%02dT%02d:%02d:%02dZ","level":"INFO","msg":"Bulk entry %06d padding-to-fill-line-to-approximately-one-hundred-and-thirty-bytes-xxxxxxxxxxxxxxxxxxxx"}\n' $((i%28+1)) $((i/3600%24)) $((i/60%60)) $((i%60)) $i
done >> /var/log/testapp/app.log

SIZE5=$(wc -c < /var/log/testapp/app.log | tr -d ' ')
echo "  File size: ${SIZE5} bytes (~$((SIZE5/1024/1024))MB)"

pond run /system/run/10-logs 2>/tmp/run5.log

check "! grep -q 'Prefix verification failed' /tmp/run5.log" \
    "phase 5: no prefix verification failure"

check "! grep -q 'rotated during ingestion' /tmp/run5.log" \
    "phase 5: no spurious rotation error"

POND_HASH5=$(pond run /system/run/10-logs b3sum 2>/dev/null | grep 'app.log' | awk '{print $1}')
HOST_HASH5=$(b3sum /var/log/testapp/app.log | awk '{print $1}')

check "[ '${POND_HASH5}' = '${HOST_HASH5}' ]" \
    "phase 5: cumulative blake3 correct at ~$((SIZE5/1024/1024))MB (${HOST_HASH5:0:16}...)"

#############################
# PHASE 6: One more append after 10MB — prefix verification must still pass
#############################

echo ""
echo "=== Phase 6: Append after 10MB — prefix verification ==="

for i in $(seq 81001 81100); do
    printf '{"ts":"2024-03-01T00:%02d:%02dZ","level":"WARN","msg":"Post-10MB entry %06d"}\n' $((i/60%60)) $((i%60)) $i
done >> /var/log/testapp/app.log

SIZE6=$(wc -c < /var/log/testapp/app.log | tr -d ' ')
echo "  File size: ${SIZE6} bytes"

pond run /system/run/10-logs 2>/tmp/run6.log

check "! grep -q 'Prefix verification failed' /tmp/run6.log" \
    "phase 6: no prefix verification failure after >10MB"

check "! grep -q 'rotated during ingestion' /tmp/run6.log" \
    "phase 6: no spurious rotation error"

pond cat /logs/app/app.log > /tmp/pond6.out
check "diff /var/log/testapp/app.log /tmp/pond6.out" \
    "phase 6: pond content matches host (${SIZE6} bytes)"

POND_HASH6=$(pond run /system/run/10-logs b3sum 2>/dev/null | grep 'app.log' | awk '{print $1}')
HOST_HASH6=$(b3sum /var/log/testapp/app.log | awk '{print $1}')

check "[ '${POND_HASH6}' = '${HOST_HASH6}' ]" \
    "phase 6: final cumulative blake3 correct (${HOST_HASH6:0:16}...)"

echo ""
echo "=== All phases complete ==="
check_finish
