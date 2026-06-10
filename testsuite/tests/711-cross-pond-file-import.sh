#!/bin/bash
# EXPERIMENT: Cross-pond import over a file:// remote (no S3/MinIO)
#
# DESCRIPTION:
#   Exercises the D5.7b pull-side cross-pond import model against a local
#   `file:///` remote (self-contained; no docker-compose):
#     1. Producer pond P1: ingest two verifiable files, `pond backup add`
#        (auto-push) so the foreign pond's bundles land in the remote.
#     2. Consumer pond P2: `pond remote add upstream URL /imports/up`
#        (cross-pond mount; foreign store_id must differ), then `pond pull`.
#     3. Verify the imported file CONTENT (md5) matches the producer exactly
#        and that `pond remote list` LAST_PULLED_SEQ advanced.
#     4. Pull again with no upstream change -> "applied 0 bundle(s)" and the
#        watermark is UNCHANGED (bandwidth-bug regression guard).
#     5. Producer adds a third file + push; consumer pull picks up exactly
#        the new file; LAST_PULLED_SEQ advances.
#     6. `pond remote remove upstream` (detach) -> imported data STILL
#        readable via the mount.
#     7. Re-add + `pond remote remove --purge` -> mount entry gone, the
#        imported path is no longer resolvable.
#     8. Negative path: a non-root mount of one's OWN remote is refused
#        (cross-pond requires a foreign store_id).
#
# EXPECTED:
#   - Content md5 matches across the import boundary.
#   - Watermark advances only when there are new upstream bundles.
#   - Detach preserves data; --purge removes the mount.
#
# History:
#   Added on jmacd/52.  Equivalent to the MinIO 530/540 cross-pond tests but
#   uses a file:// remote so it runs in the base container without compose,
#   and adds explicit detach-vs-purge and self-mount-refusal coverage.
set -e
source check.sh

echo "=== Experiment: cross-pond import (file://) ==="

P1=/tmp/711-p1
P2=/tmp/711-p2
REMOTE=/tmp/711-remote
rm -rf "$P1" "$P2" "$REMOTE"
mkdir -p "$REMOTE"

remote_last_pulled() {  # name
    POND="$P2" pond remote list 2>/dev/null | awk -v n="$1" '$1==n {print $NF; exit}'
}

echo "--- Step 1: producer ingest + backup add (auto-push) ---"
export POND="$P1"
pond init >/dev/null
pond mkdir -p /data >/dev/null 2>&1
printf 'p1-one\np1-two\n'   > /tmp/711-f1.txt
printf 'p1-three\np1-four\n' > /tmp/711-f2.txt
pond copy host:///tmp/711-f1.txt /data/f1.txt >/dev/null 2>&1
pond copy host:///tmp/711-f2.txt /data/f2.txt >/dev/null 2>&1
pond backup add origin "file://${REMOTE}" >/dev/null 2>&1
F1_MD5=$(pond cat /data/f1.txt 2>/dev/null | md5sum | awk '{print $1}')
check '[ -n "'"${F1_MD5}"'" ]' "producer f1 md5 computed"

echo "--- Step 2: consumer cross-pond import + pull ---"
export POND="$P2"
pond init >/dev/null
pond remote add upstream "file://${REMOTE}" /imports/up > /tmp/711-add.log 2>&1
check_contains /tmp/711-add.log "remote add reports pull mount" "mode=pull, mount=/imports/up"
pond pull upstream > /tmp/711-pull1.log 2>&1
check 'grep -qE "applied [1-9][0-9]* bundle" /tmp/711-pull1.log' "pull #1 applied >0 bundles"

echo "--- Step 3: content + watermark ---"
IMPORTED_MD5=$(pond cat /imports/up/data/f1.txt 2>/dev/null | md5sum | awk '{print $1}')
check '[ "'"${IMPORTED_MD5}"'" = "'"${F1_MD5}"'" ]' "imported f1 content matches producer (md5)"
check 'pond cat /imports/up/data/f2.txt | grep -q p1-four' "imported f2 content present"
PULLED1=$(remote_last_pulled upstream)
check '[ -n "'"${PULLED1}"'" ] && [ "'"${PULLED1}"'" != "-" ]' "LAST_PULLED_SEQ advanced after pull #1"

echo "--- Step 4: idempotent pull (watermark guard) ---"
pond pull upstream > /tmp/711-pull2.log 2>&1
check 'grep -q "applied 0 bundle" /tmp/711-pull2.log' "pull #2 applied 0 bundles"
PULLED2=$(remote_last_pulled upstream)
check '[ "'"${PULLED2}"'" = "'"${PULLED1}"'" ]' "LAST_PULLED_SEQ unchanged on empty pull"

echo "--- Step 5: incremental upstream change propagates ---"
export POND="$P1"
printf 'p1-five\n' > /tmp/711-f3.txt
pond copy host:///tmp/711-f3.txt /data/f3.txt >/dev/null 2>&1   # auto-pushed
export POND="$P2"
pond pull upstream > /tmp/711-pull3.log 2>&1
check 'grep -qE "applied [1-9][0-9]* bundle" /tmp/711-pull3.log' "pull #3 applied new bundle(s)"
check 'pond cat /imports/up/data/f3.txt | grep -q p1-five'      "new file visible after pull #3"
PULLED3=$(remote_last_pulled upstream)
check '[ "'"${PULLED3}"'" -gt "'"${PULLED2}"'" ]' "LAST_PULLED_SEQ advanced on pull #3"

echo "--- Step 6: detach preserves data ---"
pond remote remove upstream > /tmp/711-detach.log 2>&1
check_contains /tmp/711-detach.log "remove reports detach" "detached"
check '! pond remote list | grep -q "^upstream "'      "upstream no longer attached"
check 'pond cat /imports/up/data/f1.txt | grep -q p1-one' "imported data readable after detach"

echo "--- Step 7: re-add + purge removes the mount ---"
pond remote add upstream "file://${REMOTE}" /imports/up >/dev/null 2>&1
pond remote remove upstream --purge > /tmp/711-purge.log 2>&1
check_contains /tmp/711-purge.log "purge reports mount removal" "purged mount entry"
check '! pond cat /imports/up/data/f1.txt' "imported path not resolvable after purge"

echo "--- Step 8: self-mount under non-root path is refused ---"
export POND="$P1"
check '! pond remote add self "file://'"${REMOTE}"'" /imports/self' \
    "cross-pond mount of own remote refused"

check_finish
