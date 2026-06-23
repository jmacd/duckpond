#!/bin/bash
# EXPERIMENT: Multi-version data:series collapse via `pond maintain`
#
# DESCRIPTION:
#   A FilePhysicalSeries file (data:series) gains one OplogEntry version per
#   ingest. This test grows a host log and re-runs logfile-ingest several
#   times to accumulate multiple versions of one series file, then collapses
#   them with `pond maintain --collapse-versions 1`. Option B stores a single
#   `collapsed_through` sentinel on the merged version; series reads then skip
#   every superseded version.
#
# EXPECTED:
#   - Repeated ingest produces a multi-version data:series file.
#   - `pond maintain --collapse-versions 1` reports >=1 file collapsed.
#   - File content is byte-identical before and after the collapse.
#   - A re-run with no qualifying files reports 0 collapsed.
#
# History:
#   Added on jmacd/56 alongside Ship::collapse_versions / the
#   --collapse-versions flag, which had no testsuite coverage.
set -e
source check.sh

echo "=== Experiment: data:series version collapse ==="

export POND=/pond
pond init --birthplace test-host >/dev/null

mkdir -p /var/log/app
cat > /tmp/716-ingest.yaml << 'EOF'
archived_pattern: /var/log/app/events.log.*
active_pattern: /var/log/app/events.log
pond_path: /logs/app
EOF
pond mkdir -p /system/run >/dev/null 2>&1
pond mkdir -p /logs/app >/dev/null 2>&1
pond mknod logfile-ingest /system/run/10-events --config-path /tmp/716-ingest.yaml >/dev/null 2>&1

echo "--- Step 1: accumulate versions by growing the active log ---"
: > /var/log/app/events.log
for i in 1 2 3 4 5; do
    printf 'event-%d at line %d\n' "$i" "$i" >> /var/log/app/events.log
    pond run /system/run/10-events >/dev/null 2>&1
done

POND_SIZE=$(pond cat /logs/app/events.log 2>/dev/null | wc -c | tr -d ' ')
HOST_SIZE=$(wc -c < /var/log/app/events.log | tr -d ' ')
check '[ "'"${POND_SIZE}"'" = "'"${HOST_SIZE}"'" ]' "ingested series matches host log (${POND_SIZE} bytes)"

BEFORE_MD5=$(pond cat /logs/app/events.log 2>/dev/null | md5sum | awk '{print $1}')
check '[ -n "'"${BEFORE_MD5}"'" ]' "pre-collapse content md5 computed"

echo "--- Step 2: collapse versions ---"
pond maintain --collapse-versions 1 > /tmp/716-collapse.log 2>&1
cat /tmp/716-collapse.log
check 'grep -qE "collapse: [1-9][0-9]* file\(s\) collapsed" /tmp/716-collapse.log' "maintain collapsed >=1 file"

echo "--- Step 3: content is unchanged after collapse ---"
AFTER_MD5=$(pond cat /logs/app/events.log 2>/dev/null | md5sum | awk '{print $1}')
check '[ "'"${AFTER_MD5}"'" = "'"${BEFORE_MD5}"'" ]' "content byte-identical after collapse"
check 'pond cat /logs/app/events.log | grep -q "event-1 at line 1"' "first version survives collapse"
check 'pond cat /logs/app/events.log | grep -q "event-5 at line 5"' "last version survives collapse"

echo "--- Step 4: second collapse is a no-op ---"
pond maintain --collapse-versions 1 > /tmp/716-collapse2.log 2>&1
cat /tmp/716-collapse2.log
check 'grep -qE "collapse: 0 file\(s\) collapsed" /tmp/716-collapse2.log' "re-collapse finds nothing to do"

echo "--- Step 5: append after collapse still reads correctly ---"
printf 'event-6 at line 6\n' >> /var/log/app/events.log
pond run /system/run/10-events >/dev/null 2>&1
EXPECT_MD5=$(md5sum < /var/log/app/events.log | awk '{print $1}')
APPEND_MD5=$(pond cat /logs/app/events.log 2>/dev/null | md5sum | awk '{print $1}')
check '[ "'"${APPEND_MD5}"'" = "'"${EXPECT_MD5}"'" ]' "post-collapse append reads merged + appended content"

check_finish
