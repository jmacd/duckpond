#!/bin/bash
# EXPERIMENT: Two foreign producers at divergent txn_seq feed one consumer;
#             the SLOW producer's new low-seq txn must still import even
#             though the FAST producer's frontier is far ahead.
#
# DESCRIPTION:
#   Regression guard for the May-10 2026 production outage (cross-pond
#   import staleness).  Originally diagnosed in a root-level `may-10-bugs.md`
#   (now removed); the resolution is recorded in docs/remote-redesign.md
#   under "Resolved production bugs".  Bug 1 was: the OLD `crates/remote`
#   import factory used a single GLOBAL max() watermark across all imported
#   partitions, so once a FAST producer pushed the consumer's watermark up
#   to its frontier F, any SLOWER producer whose new transactions landed at
#   a txn_seq < F was masked FOREVER -- the symptom that froze caspar.water's
#   water data for ~6 days.
#
#   The post-D5 model attaches each producer as its OWN remote with its OWN
#   `last_pulled_seq:<url>` watermark, so divergent producer frontiers can
#   never mask one another.  This test reproduces the exact masking
#   condition and asserts the slow producer's data still flows.
#
#   Topology (all over self-contained file:// remotes; no S3/compose):
#     - P_SLOW: init + 1 file  -> low frontier (~2)
#     - P_FAST: init + many files -> high frontier (F, ~13+)
#     - P_C consumer: `remote add` BOTH at separate mounts, pull both.
#       Consumer's LAST_PULLED_SEQ(fast) == F  >>  LAST_PULLED_SEQ(slow).
#     - P_SLOW writes a NEW file -> its producer txn_seq S2 is STILL < F.
#     - P_C pulls the slow remote -> the new file MUST appear, and
#       LAST_PULLED_SEQ(slow) advances to S2 (with S2 < F: the precise
#       condition the old global-max code dropped).
#
# EXPECTED:
#   - Both producers' data imports across the boundary (md5 match).
#   - Consumer's slow/fast watermarks are independent and divergent.
#   - A slow-producer txn at seq S2 < F (the fast frontier) still imports.
#   - Pulling the slow remote does not touch the fast remote's watermark.
#
# History:
#   Added on jmacd/52 to close the point-3 coverage gap from the
#   remote-redesign review: 710-714 covered single-producer import but not
#   the multi-producer divergent-frontier failure mode that motivated the
#   whole cross-pond redesign (docs/remote-redesign.md A1).
set -e
source check.sh

echo "=== Experiment: cross-pond divergent-frontier masking (file://) ==="

SLOW=/tmp/715-slow
FAST=/tmp/715-fast
CONS=/tmp/715-cons
R_SLOW=/tmp/715-remote-slow
R_FAST=/tmp/715-remote-fast
rm -rf "$SLOW" "$FAST" "$CONS" "$R_SLOW" "$R_FAST"
mkdir -p "$R_SLOW" "$R_FAST"

cons_pulled() {  # remote-name -> LAST_PULLED_SEQ field
    POND="$CONS" pond remote list 2>/dev/null | awk -v n="$1" '$1==n {print $NF; exit}'
}

echo "--- Step 1: SLOW producer (low frontier) ---"
export POND="$SLOW"
pond init --birthplace test-host >/dev/null
pond mkdir -p /data >/dev/null 2>&1
pond backup add origin "file://${R_SLOW}" >/dev/null 2>&1
printf 'slow-one\n' > /tmp/715-slow1.txt
pond copy host:///tmp/715-slow1.txt /data/s1.txt >/dev/null 2>&1   # auto-pushed
SLOW1_MD5=$(pond cat /data/s1.txt 2>/dev/null | md5sum | awk '{print $1}')
check '[ -n "'"${SLOW1_MD5}"'" ]' "slow producer s1 md5 computed"

echo "--- Step 2: FAST producer (high frontier) ---"
export POND="$FAST"
pond init --birthplace test-host >/dev/null
pond mkdir -p /data >/dev/null 2>&1
pond backup add origin "file://${R_FAST}" >/dev/null 2>&1
# Push the frontier well past the slow producer's: each copy is one
# auto-pushed transaction.
for i in $(seq 1 12); do
    printf 'fast-%02d\n' "$i" > "/tmp/715-fast${i}.txt"
    pond copy "host:///tmp/715-fast${i}.txt" "/data/f${i}.txt" >/dev/null 2>&1
done
FAST1_MD5=$(pond cat /data/f1.txt 2>/dev/null | md5sum | awk '{print $1}')
check '[ -n "'"${FAST1_MD5}"'" ]' "fast producer f1 md5 computed"

echo "--- Step 3: consumer imports BOTH producers ---"
export POND="$CONS"
pond init --birthplace test-host >/dev/null
pond remote add slow "file://${R_SLOW}" /imports/slow >/dev/null 2>&1
pond remote add fast "file://${R_FAST}" /imports/fast >/dev/null 2>&1
pond pull slow > /tmp/715-pull-slow1.log 2>&1
pond pull fast > /tmp/715-pull-fast1.log 2>&1
check 'grep -qE "applied [1-9][0-9]* bundle" /tmp/715-pull-slow1.log' "pull slow #1 applied >0 bundles"
check 'grep -qE "applied [1-9][0-9]* bundle" /tmp/715-pull-fast1.log' "pull fast #1 applied >0 bundles"

echo "--- Step 4: both producers' data present; frontiers diverge ---"
IMP_SLOW_MD5=$(pond cat /imports/slow/data/s1.txt 2>/dev/null | md5sum | awk '{print $1}')
IMP_FAST_MD5=$(pond cat /imports/fast/data/f1.txt 2>/dev/null | md5sum | awk '{print $1}')
check '[ "'"${IMP_SLOW_MD5}"'" = "'"${SLOW1_MD5}"'" ]' "imported slow s1 matches producer (md5)"
check '[ "'"${IMP_FAST_MD5}"'" = "'"${FAST1_MD5}"'" ]' "imported fast f1 matches producer (md5)"
PULLED_SLOW1=$(cons_pulled slow)
PULLED_FAST=$(cons_pulled fast)
check '[ -n "'"${PULLED_SLOW1}"'" ] && [ "'"${PULLED_SLOW1}"'" != "-" ]' "slow watermark set"
check '[ -n "'"${PULLED_FAST}"'" ] && [ "'"${PULLED_FAST}"'" != "-" ]' "fast watermark set"
# The precondition for the old bug: the fast producer's frontier is well
# above the slow producer's.  Under the old GLOBAL max() this F would have
# become the single watermark applied to BOTH.
check '[ "'"${PULLED_FAST}"'" -gt "'"${PULLED_SLOW1}"'" ]' \
    "fast frontier ($PULLED_FAST) > slow frontier ($PULLED_SLOW1) [divergence established]"

echo "--- Step 5: SLOW producer emits a NEW txn BELOW the fast frontier ---"
export POND="$SLOW"
printf 'slow-two\n' > /tmp/715-slow2.txt
pond copy host:///tmp/715-slow2.txt /data/s2.txt >/dev/null 2>&1   # auto-pushed
SLOW2_MD5=$(pond cat /data/s2.txt 2>/dev/null | md5sum | awk '{print $1}')

echo "--- Step 6: consumer pulls slow -> new low-seq file MUST import ---"
export POND="$CONS"
pond pull slow > /tmp/715-pull-slow2.log 2>&1
check 'grep -qE "applied [1-9][0-9]* bundle" /tmp/715-pull-slow2.log' \
    "pull slow #2 applied the new bundle (NOT masked by fast frontier)"
IMP_SLOW2_MD5=$(pond cat /imports/slow/data/s2.txt 2>/dev/null | md5sum | awk '{print $1}')
check '[ "'"${IMP_SLOW2_MD5}"'" = "'"${SLOW2_MD5}"'" ]' \
    "new slow file s2 imported and content matches (the May-10 regression)"
PULLED_SLOW2=$(cons_pulled slow)
check '[ "'"${PULLED_SLOW2}"'" -gt "'"${PULLED_SLOW1}"'" ]' \
    "slow watermark advanced ($PULLED_SLOW1 -> $PULLED_SLOW2)"
# The crux: the newly-imported slow txn sits BELOW the fast frontier the
# consumer already holds.  A global max() watermark would have filtered it.
check '[ "'"${PULLED_SLOW2}"'" -lt "'"${PULLED_FAST}"'" ]' \
    "new slow seq ($PULLED_SLOW2) is BELOW fast frontier ($PULLED_FAST) [exact masking condition]"

echo "--- Step 7: pulling slow did not disturb the fast remote ---"
pond pull fast > /tmp/715-pull-fast2.log 2>&1
check 'grep -q "applied 0 bundle" /tmp/715-pull-fast2.log' "fast pull is a no-op (independent watermark)"
PULLED_FAST2=$(cons_pulled fast)
check '[ "'"${PULLED_FAST2}"'" = "'"${PULLED_FAST}"'" ]' "fast watermark unchanged by slow-side activity"
check 'pond cat /imports/fast/data/f1.txt | grep -q fast-01' "fast imported data still intact"

check_finish
