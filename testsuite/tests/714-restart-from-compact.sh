#!/bin/bash
# EXPERIMENT: restart-from-compact for a cross-pond consumer (D6.4)
#
# DESCRIPTION:
#   `pond restart-from-compact <name>` re-bootstraps an attached consumer from
#   the remote's oldest surviving compact baseline.  For a CROSS-POND import
#   (foreign store_id != local pond_id) it drops only the foreign pond's
#   footprint and rebuilds it, leaving the consumer's own data untouched.
#   This test (self-contained; no compose):
#     1. Producer P1: ingest files, `pond backup add`, `pond maintain --compact`
#        + push so the remote carries a Compact baseline.
#     2. Consumer P2: own local file + cross-pond import of P1, pull.
#     3. `pond restart-from-compact upstream` re-bootstraps the foreign
#        footprint from the compact baseline and catches up.
#     4. After restart: imported content is intact (md5 matches producer) AND
#        the consumer's OWN unrelated file is untouched.
#     5. Negative path: restart-from-compact of an unknown remote fails.
#
# EXPECTED:
#   - restart-from-compact reports a cross-pond rebuild + catch-up.
#   - Imported files reproduce producer content exactly post-restart.
#   - The consumer's own data survives the foreign-only rebuild.
#
# History:
#   Added on jmacd/52 to cover D6.4 (`pond restart-from-compact`), which had no
#   testsuite coverage.  Uses the cross-pond path (CLI-drivable end-to-end);
#   the mirror-restart-from-fresh-dir path remains Rust-only per remote-redesign.md.
set -e
source check.sh

echo "=== Experiment: restart-from-compact (cross-pond, file://) ==="

P1=/tmp/714-p1
P2=/tmp/714-p2
REMOTE=/tmp/714-remote
rm -rf "$P1" "$P2" "$REMOTE"
mkdir -p "$REMOTE"

echo "--- Step 1: producer + compact baseline ---"
export POND="$P1"
pond init >/dev/null
pond mkdir -p /data >/dev/null 2>&1
for i in 1 2 3; do
    printf 'prod-row-%d\n' "$i" > /tmp/714-f$i.txt
    pond copy host:///tmp/714-f$i.txt /data/f$i.txt >/dev/null 2>&1
done
pond backup add origin "file://${REMOTE}" >/dev/null 2>&1
pond maintain --compact >/dev/null 2>&1
pond push origin >/dev/null 2>&1
SRC_MD5=$(pond cat /data/f2.txt 2>/dev/null | md5sum | awk '{print $1}')
check '[ -n "'"${SRC_MD5}"'" ]' "producer f2 md5 computed"

echo "--- Step 2: consumer own data + cross-pond import ---"
export POND="$P2"
pond init >/dev/null
printf 'consumer-private\n' > /tmp/714-own.txt
pond mkdir -p /local >/dev/null 2>&1
pond copy host:///tmp/714-own.txt /local/own.txt >/dev/null 2>&1
OWN_MD5=$(pond cat /local/own.txt 2>/dev/null | md5sum | awk '{print $1}')
pond remote add upstream "file://${REMOTE}" /imports/up >/dev/null 2>&1
pond pull upstream > /tmp/714-pull.log 2>&1
check 'grep -qE "applied [1-9][0-9]* bundle" /tmp/714-pull.log' "consumer pulled bundles"
check 'pond cat /imports/up/data/f1.txt | grep -q prod-row-1'   "imported content present before restart"

echo "--- Step 3: restart-from-compact ---"
pond restart-from-compact upstream > /tmp/714-restart.log 2>&1
check_contains /tmp/714-restart.log "restart reports cross-pond rebuild" "cross-pond restart"
check_contains /tmp/714-restart.log "restart reports catch-up" "caught up to latest"

echo "--- Step 4: integrity after restart ---"
IMPORTED_MD5=$(pond cat /imports/up/data/f2.txt 2>/dev/null | md5sum | awk '{print $1}')
check '[ "'"${IMPORTED_MD5}"'" = "'"${SRC_MD5}"'" ]' "imported f2 matches producer after restart"
check 'pond cat /imports/up/data/f3.txt | grep -q prod-row-3'  "all imported files present after restart"
OWN_AFTER=$(pond cat /local/own.txt 2>/dev/null | md5sum | awk '{print $1}')
check '[ "'"${OWN_AFTER}"'" = "'"${OWN_MD5}"'" ]' "consumer's own data untouched by foreign-only rebuild"

echo "--- Step 5: negative path ---"
check '! pond restart-from-compact nope' "restart-from-compact of unknown remote fails"

check_finish
