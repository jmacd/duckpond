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
#       Consumer's PULLED_TIP(fast) and PULLED_TIP(slow) are independent hashes.
#     - P_SLOW writes a NEW file -> its producer txn_seq S2 is STILL < F.
#     - P_C pulls the slow remote -> the new file MUST appear, and
#       PULLED_TIP(slow) advances independently of the fast remote's tip
#       (the low-activity update the old global-max seq code dropped).
#
# EXPECTED:
#   - Both producers' data imports across the boundary (md5 match).
#   - Consumer's slow/fast tip frontiers are independent and divergent.
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

cons_pulled() {  # remote-name -> PULLED_TIP field
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
check 'grep -q "pull slow complete" /tmp/715-pull-slow1.log' "pull slow #1 completed import"
check 'grep -q "pull fast complete" /tmp/715-pull-fast1.log' "pull fast #1 completed import"

echo "--- Step 4: both producers' data present; frontiers diverge ---"
IMP_SLOW_MD5=$(pond cat /imports/slow/data/s1.txt 2>/dev/null | md5sum | awk '{print $1}')
IMP_FAST_MD5=$(pond cat /imports/fast/data/f1.txt 2>/dev/null | md5sum | awk '{print $1}')
check '[ "'"${IMP_SLOW_MD5}"'" = "'"${SLOW1_MD5}"'" ]' "imported slow s1 matches producer (md5)"
check '[ "'"${IMP_FAST_MD5}"'" = "'"${FAST1_MD5}"'" ]' "imported fast f1 matches producer (md5)"
PULLED_SLOW1=$(cons_pulled slow)
PULLED_FAST=$(cons_pulled fast)
check '[ -n "'"${PULLED_SLOW1}"'" ] && [ "'"${PULLED_SLOW1}"'" != "-" ]' "slow tip set"
check '[ -n "'"${PULLED_FAST}"'" ] && [ "'"${PULLED_FAST}"'" != "-" ]' "fast tip set"
# The precondition for the old bug: the two producers have divergent
# frontiers.  Under the old GLOBAL max() seq watermark a single frontier
# was applied to BOTH; now each remote tracks its OWN tip commit hash, so
# the two frontiers are simply different, independent hashes.
check '[ "'"${PULLED_FAST}"'" != "'"${PULLED_SLOW1}"'" ]' \
    "fast and slow frontiers are independent, divergent tips"

echo "--- Step 5: SLOW producer emits a NEW txn BELOW the fast frontier ---"
export POND="$SLOW"
printf 'slow-two\n' > /tmp/715-slow2.txt
pond copy host:///tmp/715-slow2.txt /data/s2.txt >/dev/null 2>&1   # auto-pushed
SLOW2_MD5=$(pond cat /data/s2.txt 2>/dev/null | md5sum | awk '{print $1}')

echo "--- Step 6: consumer pulls slow -> new low-seq file MUST import ---"
export POND="$CONS"
pond pull slow > /tmp/715-pull-slow2.log 2>&1
check 'grep -qE "files: [1-9]" /tmp/715-pull-slow2.log' \
    "pull slow #2 imported the new file (NOT masked by fast frontier)"
IMP_SLOW2_MD5=$(pond cat /imports/slow/data/s2.txt 2>/dev/null | md5sum | awk '{print $1}')
check '[ "'"${IMP_SLOW2_MD5}"'" = "'"${SLOW2_MD5}"'" ]' \
    "new slow file s2 imported and content matches (the May-10 regression)"
PULLED_SLOW2=$(cons_pulled slow)
check '[ -n "'"${PULLED_SLOW2}"'" ] && [ "'"${PULLED_SLOW2}"'" != "'"${PULLED_SLOW1}"'" ]' \
    "slow tip advanced ($PULLED_SLOW1 -> $PULLED_SLOW2)"
# The crux: importing the slow producer's new commit did NOT require the
# fast remote's independent frontier to move.  A global max() watermark
# would have masked this low-activity slow update; independent per-remote
# tips cannot.  Step 7 confirms the fast tip is untouched.
check '[ "'"${PULLED_SLOW2}"'" != "'"${PULLED_FAST}"'" ]' \
    "slow tip is independent of the fast frontier tip"

echo "--- Step 7: pulling slow did not disturb the fast remote ---"
pond pull fast > /tmp/715-pull-fast2.log 2>&1
check 'grep -q "already up to date" /tmp/715-pull-fast2.log' "fast pull is a no-op (independent tip)"
PULLED_FAST2=$(cons_pulled fast)
check '[ "'"${PULLED_FAST2}"'" = "'"${PULLED_FAST}"'" ]' "fast tip unchanged by slow-side activity"
check 'pond cat /imports/fast/data/f1.txt | grep -q fast-01' "fast imported data still intact"

check_finish
