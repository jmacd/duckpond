#!/bin/bash
# EXPERIMENT: data:series version collapse survives push/pull replication
#
# DESCRIPTION:
#   A producer accumulates many versions of one data:series file, collapses
#   them with `pond maintain --collapse-versions`, and pushes to a file://
#   remote. A fresh consumer cross-pond imports and pulls the whole history,
#   including the collapse commit, and must read byte-identical content. This
#   proves the Option B merged row + `collapsed_through` sentinel replicate
#   correctly and that a consumer replaying pre-collapse commits plus the
#   collapse commit converges on the same content as the producer.
#
# EXPECTED:
#   - Producer collapses >=1 file.
#   - Push uploads the collapse commit; verify stays clean.
#   - Consumer reproduces the producer's merged content exactly (md5).
#
# History:
#   Added on jmacd/56 to cover collapse x push/pull replication, a gap left by
#   716 (single-pond collapse) and 712 (compact push).
set -e
source check.sh

echo "=== Experiment: collapse + push/pull replication (file://) ==="

P1=/tmp/717-p1
P2=/tmp/717-p2
REMOTE=/tmp/717-remote
rm -rf "$P1" "$P2" "$REMOTE"
mkdir -p "$REMOTE"

echo "--- Step 1: producer accumulates a multi-version series file ---"
export POND="$P1"
pond init --birthplace test-host >/dev/null

mkdir -p /var/log/app717
cat > /tmp/717-ingest.yaml << 'EOF'
archived_pattern: /var/log/app717/events.log.*
active_pattern: /var/log/app717/events.log
pond_path: /logs/app
EOF
pond mkdir -p /system/run >/dev/null 2>&1
pond mkdir -p /logs/app >/dev/null 2>&1
pond mknod logfile-ingest /system/run/10-events --config-path /tmp/717-ingest.yaml >/dev/null 2>&1

: > /var/log/app717/events.log
for i in 1 2 3 4 5; do
    printf 'event-%d at line %d\n' "$i" "$i" >> /var/log/app717/events.log
    pond run /system/run/10-events >/dev/null 2>&1
done

SRC_MD5=$(pond cat /logs/app/events.log 2>/dev/null | md5sum | awk '{print $1}')
check '[ -n "'"${SRC_MD5}"'" ]' "producer series md5 computed"

echo "--- Step 2: backup add (pushes existing history) ---"
pond backup add origin "file://${REMOTE}" > /tmp/717-backup.log 2>&1
check 'grep -q "added remote origin" /tmp/717-backup.log' "backup add origin succeeded"

echo "--- Step 3: collapse versions ---"
pond maintain --collapse-versions 1 > /tmp/717-collapse.log 2>&1
cat /tmp/717-collapse.log
check 'grep -qE "collapse: [1-9][0-9]* file\(s\) collapsed" /tmp/717-collapse.log' "producer collapsed >=1 file"

echo "--- Step 4: collapse commit replicated to remote ---"
check 'grep -qE "post-commit auto-push: origin done \(objects_pushed=[1-9]" /tmp/717-collapse.log' "collapse commit auto-pushed"

echo "--- Step 5: verify clean after collapse ---"
pond verify origin > /tmp/717-verify.log 2>&1
check_contains /tmp/717-verify.log "verify clean after collapse" "live data matches remote"

echo "--- Step 6: fresh consumer pulls and reproduces content ---"
export POND="$P2"
pond init --birthplace test-host >/dev/null
pond remote add upstream "file://${REMOTE}" /imports/up >/dev/null 2>&1
pond pull upstream > /tmp/717-pull.log 2>&1
check 'grep -q "pull upstream complete" /tmp/717-pull.log' "consumer completed cross-pond import"

IMPORTED_MD5=$(pond cat /imports/up/logs/app/events.log 2>/dev/null | md5sum | awk '{print $1}')
check '[ "'"${IMPORTED_MD5}"'" = "'"${SRC_MD5}"'" ]' "consumer content matches producer post-collapse"
check 'pond cat /imports/up/logs/app/events.log | grep -q "event-1 at line 1"' "first version survives replication"
check 'pond cat /imports/up/logs/app/events.log | grep -q "event-5 at line 5"' "last version survives replication"

check_finish
