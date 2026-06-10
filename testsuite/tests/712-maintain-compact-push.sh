#!/bin/bash
# EXPERIMENT: Producer-side compaction + Compact-bundle push (D7)
#
# DESCRIPTION:
#   Exercises `pond maintain --compact` and confirms it produces a pushable
#   Compact baseline bundle on a file:// remote (self-contained; no compose):
#     1. Producer P1: ingest several files, `pond backup add origin`
#        (auto-pushes each write).
#     2. `pond maintain --compact` records the data-table optimize as a
#        CommitKind::Compact transaction (D7); the log advises `pond push`.
#     3. `pond push origin` emits exactly the Compact bundle.
#     4. `pond verify origin` is clean AFTER compaction (per-partition
#        checksums are invariant across the optimize).
#     5. A fresh consumer P2 cross-pond imports and pulls -- including the
#        compact baseline -- and sees identical file content (md5).
#     6. `pond status` reflects identity + remote watermark post-compact.
#
# EXPECTED:
#   - maintain --compact reports a compaction commit.
#   - The follow-up push uploads the Compact bundle.
#   - verify stays clean; the consumer reproduces producer content exactly.
#
# History:
#   Added on jmacd/52 to cover D7 (`Ship::compact` / `pond maintain --compact`),
#   which had no testsuite coverage.  file:// remote -> runs without compose.
set -e
source check.sh

echo "=== Experiment: maintain --compact + Compact-bundle push (file://) ==="

P1=/tmp/712-p1
P2=/tmp/712-p2
REMOTE=/tmp/712-remote
rm -rf "$P1" "$P2" "$REMOTE"
mkdir -p "$REMOTE"

echo "--- Step 1: producer ingest + backup add ---"
export POND="$P1"
pond init >/dev/null
pond mkdir -p /data >/dev/null 2>&1
for i in 1 2 3 4; do
    printf 'compact-row-%d\n' "$i" > /tmp/712-f$i.txt
    pond copy host:///tmp/712-f$i.txt /data/f$i.txt >/dev/null 2>&1
done
pond backup add origin "file://${REMOTE}" >/dev/null 2>&1
SRC_MD5=$(pond cat /data/f3.txt 2>/dev/null | md5sum | awk '{print $1}')
check '[ -n "'"${SRC_MD5}"'" ]' "producer f3 md5 computed"

echo "--- Step 2: maintain --compact ---"
pond maintain --compact > /tmp/712-compact.log 2>&1
check_contains /tmp/712-compact.log "compaction recorded as a transaction" "Compaction committed"
check_contains /tmp/712-compact.log "log advises pushing the compact bundle" "pond push"

echo "--- Step 3: push the Compact bundle ---"
pond push origin > /tmp/712-push.log 2>&1
check 'grep -qE "push origin complete \(pushed=[1-9]" /tmp/712-push.log' "compact push uploaded >=1 bundle"

echo "--- Step 4: verify clean after compaction ---"
pond verify origin > /tmp/712-verify.log 2>&1
check_contains /tmp/712-verify.log "verify clean after compact" "live data matches remote"

echo "--- Step 5: fresh consumer pulls (incl. compact baseline) ---"
export POND="$P2"
pond init >/dev/null
pond remote add upstream "file://${REMOTE}" /imports/up >/dev/null 2>&1
pond pull upstream > /tmp/712-pull.log 2>&1
check 'grep -qE "applied [1-9][0-9]* bundle" /tmp/712-pull.log' "consumer applied bundles"
IMPORTED_MD5=$(pond cat /imports/up/data/f3.txt 2>/dev/null | md5sum | awk '{print $1}')
check '[ "'"${IMPORTED_MD5}"'" = "'"${SRC_MD5}"'" ]' "consumer f3 content matches producer post-compact"
check 'pond cat /imports/up/data/f1.txt | grep -q compact-row-1' "first file survives compaction round-trip"
check 'pond cat /imports/up/data/f4.txt | grep -q compact-row-4' "last file survives compaction round-trip"

echo "--- Step 6: producer status ---"
export POND="$P1"
pond status > /tmp/712-status.log 2>&1
check_contains /tmp/712-status.log "status prints identity" "Pond ID:"
check_contains /tmp/712-status.log "status lists the backup remote" "origin"

check_finish
