#!/bin/bash
# EXPERIMENT: `pond tlog` history growth, mixed commit types, and maintenance.
#
# DESCRIPTION:
#   Complements 719 (healthy/tamper/heal) with the log's lifecycle properties:
#     1. A pond with NO commits has no transparency log; `tlog show` and
#        `tlog verify` both report that gracefully and exit 0.
#     2. Every write transaction -- not just `copy`, but `mkdir` too -- appends
#        exactly one leaf.  The checkpoint history grows monotonically (1,2,3,4)
#        and `tlog verify` passes all checks.
#     3. `pond maintain --compact` IS a spine-bearing commit: it stamps a
#        `DataCommitted(Compact)` on the content-graph chain (so a
#        post-compaction `pond push` can reach the rewrite) and therefore
#        appends exactly one transparency-log leaf.  The log still verifies
#        clean afterward (the published leaves stay faithful to the
#        control-table commit spine).
#
# EXPECTED:
#   - Empty pond: no {POND}/tlog, graceful show/verify, exit 0.
#   - mkdir and copy each add one leaf; history is [1,2,3,4]; verify passes.
#   - Compaction adds exactly one leaf (history [1..5]) and verify still passes.
#
# History:
#   Added with `pond tlog` to cover the log's growth and maintenance behavior.
#   Self-contained, runs in the base container.
set -e
source check.sh

echo "=== Experiment: pond tlog history + maintain ==="

export POND=/tmp/720-pond
rm -rf "$POND"

pond init --birthplace test-host >/dev/null

echo "--- Step 1: empty pond has no transparency log ---"
check "[ ! -d \"$POND/tlog\" ]" "no {POND}/tlog before any commit"
pond tlog show > /tmp/720-empty-show.txt 2>/dev/null
check_contains /tmp/720-empty-show.txt "empty show is graceful" "no checkpoint published yet"
pond tlog verify > /tmp/720-empty-verify.txt 2>/dev/null
check "[ $? -eq 0 ]" "empty verify exits 0"
check_contains /tmp/720-empty-verify.txt "empty verify is graceful" "nothing to verify"

echo "--- Step 2: mixed commit types each add one leaf ---"
pond mkdir /d >/dev/null 2>&1                 # leaf 1
pond mkdir /d/e >/dev/null 2>&1               # leaf 2
echo "row a" > /tmp/720-a.txt
pond copy host:///tmp/720-a.txt /d/a.txt >/dev/null 2>&1   # leaf 3
echo "row b" > /tmp/720-b.txt
pond copy host:///tmp/720-b.txt /d/e/b.txt >/dev/null 2>&1 # leaf 4

pond tlog show > /tmp/720-show.txt 2>/dev/null
cat /tmp/720-show.txt
check_contains /tmp/720-show.txt "four leaves after 2 mkdir + 2 copy" "Tree size:  4"
check "[ \$(grep -c '^  size=' /tmp/720-show.txt) -eq 4 ]" "history has four checkpoints"
# History sizes must be strictly 1,2,3,4 in order.
SIZES=$(grep -oE 'size=[0-9]+' /tmp/720-show.txt | grep -oE '[0-9]+' | tr '\n' ' ')
check "[ \"$SIZES\" = \"1 2 3 4 \" ]" "history sizes are monotonic 1..4 (got: $SIZES)"

pond tlog verify > /tmp/720-verify.txt 2>/dev/null
check "[ $? -eq 0 ]" "verify passes on the grown log"
check "[ \$(grep -c '\[PASS\]' /tmp/720-verify.txt) -eq 4 ]" "all four checks PASS"

echo "--- Step 3: maintain --compact is spine-bearing and adds one leaf ---"
pond maintain --compact > /tmp/720-maintain.txt 2>&1 || true
pond tlog show > /tmp/720-show2.txt 2>/dev/null
check_contains /tmp/720-show2.txt "compaction adds one leaf (size 5)" "Tree size:  5"
check "[ \$(grep -c '^  size=' /tmp/720-show2.txt) -eq 5 ]" "history grew to five checkpoints after compaction"
# The compaction leaf extends the monotonic history to [1..5].
SIZES2=$(grep -oE 'size=[0-9]+' /tmp/720-show2.txt | grep -oE '[0-9]+' | tr '\n' ' ')
check "[ \"$SIZES2\" = \"1 2 3 4 5 \" ]" "history sizes are monotonic 1..5 (got: $SIZES2)"

pond tlog verify > /tmp/720-verify2.txt 2>/dev/null
check "[ $? -eq 0 ]" "verify still passes after compaction"
check_contains /tmp/720-verify2.txt "still faithful after compaction" "all checks passed"

check_finish
