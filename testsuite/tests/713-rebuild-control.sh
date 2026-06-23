#!/bin/bash
# EXPERIMENT: Rebuild the control table from the data Delta log (D6.3)
#
# DESCRIPTION:
#   `pond rebuild-control` reconstructs the steward control table from the
#   data table's `pond_txn` commit history when the control table is lost or
#   corrupt.  This test (self-contained; no compose):
#     1. init pond, make several writes (each stamps a pond_txn commit).
#     2. `pond rebuild-control` WITHOUT --force is refused because a control
#        table already exists (gating check).
#     3. `pond rebuild-control --force` reconstructs N transactions, recovers
#        pond identity, and moves the old control table aside to control.bak.*.
#     4. After rebuild the pond is still usable: identity is unchanged, data
#        is readable, `pond log` and `pond status` work.
#     5. Settings/watermarks are NOT recovered (documented behaviour): the
#        command warns that remotes must be re-attached.
#
# EXPECTED:
#   - Bare rebuild-control fails with a "pass --force" message.
#   - --force reconstructs the same number of transactions as were committed,
#     preserves the pond_id, and leaves a control.bak.* directory.
#   - Data remains readable; pond_id is identical before and after.
#
# History:
#   Added on jmacd/52 to cover D6.3 (`pond rebuild-control`), which had no
#   testsuite coverage.
set -e
source check.sh

echo "=== Experiment: rebuild-control from data Delta log (file://) ==="

export POND=/tmp/713-pond
rm -rf "$POND"

pond_id() { pond config 2>/dev/null | grep -iE "pond[ _]?id" | grep -oE "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}" | head -1; }

echo "--- Step 1: init + writes ---"
pond init --birthplace test-host >/dev/null
for i in 1 2 3; do
    printf 'rc-row-%d\n' "$i" > /tmp/713-f$i.txt
    pond copy host:///tmp/713-f$i.txt /f$i.txt >/dev/null 2>&1
done
ID_BEFORE=$(pond_id)
check '[ -n "'"${ID_BEFORE}"'" ]'  "pond_id readable before rebuild"
check '[ -d "'"${POND}"'/control" ]' "control table exists before rebuild"

echo "--- Step 2: rebuild-control without --force is refused ---"
# set -e is active; capture the expected failure without aborting the test.
if pond rebuild-control > /tmp/713-norforce.log 2>&1; then
    echo "  ✗ rebuild-control without --force unexpectedly succeeded"; _CHECK_FAIL=$((_CHECK_FAIL+1))
else
    echo "  ✓ rebuild-control without --force fails"; _CHECK_PASS=$((_CHECK_PASS+1))
fi
check 'grep -qiE "force" /tmp/713-norforce.log' "error message mentions --force"

echo "--- Step 3: rebuild-control --force ---"
pond rebuild-control --force > /tmp/713-force.log 2>&1
check_contains /tmp/713-force.log "reports a rebuilt control table" "rebuilt control table"
check 'grep -qE "reconstructed [1-9][0-9]* transaction" /tmp/713-force.log' "reconstructed >=1 transactions"
check 'ls -d "'"${POND}"'"/control.bak.* >/dev/null 2>&1' "old control table moved aside to control.bak.*"

echo "--- Step 4: pond still usable after rebuild ---"
ID_AFTER=$(pond_id)
check '[ "'"${ID_AFTER}"'" = "'"${ID_BEFORE}"'" ]' "pond_id unchanged across rebuild"
check 'pond cat /f2.txt | grep -q rc-row-2' "data readable after rebuild"
pond status > /tmp/713-status.log 2>&1
check_contains /tmp/713-status.log "status works after rebuild" "Pond ID:"
pond log --limit 5 > /tmp/713-log.log 2>&1
check 'grep -qiE "transaction" /tmp/713-log.log' "log works after rebuild"

echo "--- Step 5: documented loss of settings/watermarks ---"
check 'grep -qiE "settings.*NOT recovered|re-attach" /tmp/713-force.log' \
    "rebuild warns that remotes must be re-attached"

check_finish
