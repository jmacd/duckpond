#!/bin/bash
# EXPERIMENT: `pond tlog show` / `pond tlog verify` -- the key-free half of the
# D5 transparency log.
#
# DESCRIPTION:
#   Every write transaction appends one leaf (its commit object) to the pond's
#   RFC 6962 SHA-256 transparency log, published as C2SP tlog-tiles under
#   {POND}/tlog with a checkpoint and an append-only checkpoint history.  These
#   properties are verifiable WITHOUT any signing key (signing is deferred):
#     - inclusion: every published leaf sits in the checkpoint's tree;
#     - append-only: every checkpoint ever published is a prefix of the current
#       log (consistency proof against the recorded history);
#     - faithfulness: the published leaves match the control-table commit spine.
#
#   Steps:
#     1. Build a pond with three commits; `tlog show` reports size 3 and three
#        history checkpoints; `tlog verify` passes all four checks (exit 0).
#     2. Tamper with a level-0 tile -> `tlog verify` FAILS (exit non-zero) with
#        [FAIL] lines: corruption is detected.
#     3. Delete the entire {POND}/tlog export, then make one more commit: the
#        export self-heals (design D5 reconciliation), and `tlog verify` passes
#        again at size 4.
#
# EXPECTED:
#   - A fresh pond publishes one leaf per commit with a growing history.
#   - Verification passes on an untampered log and fails on a tampered one.
#   - A dropped export is fully rebuilt from the control table on the next
#     commit and re-verifies clean.
#
# History:
#   Added alongside `pond tlog` (D5 key-free verification: checkpoint history +
#   verification CLI).  Self-contained, runs in the base container.
set -e
source check.sh

echo "=== Experiment: pond tlog verify ==="

export POND=/tmp/719-pond
rm -rf "$POND"

pond init --birthplace test-host >/dev/null

for i in 1 2 3; do
  echo "row $i" > /tmp/719-f$i.txt
  pond copy host:///tmp/719-f$i.txt /f$i.txt >/dev/null 2>&1
done

echo "--- Step 1: show + verify a healthy log ---"
pond tlog show > /tmp/719-show.txt 2>/dev/null
cat /tmp/719-show.txt
check_contains /tmp/719-show.txt "show reports three leaves" "Tree size:  3"
check "[ \$(grep -c '^  size=' /tmp/719-show.txt) -eq 3 ]" "three checkpoints in history"

pond tlog verify > /tmp/719-verify.txt 2>/dev/null
VERIFY_RC=$?
cat /tmp/719-verify.txt
check "[ $VERIFY_RC -eq 0 ]" "verify exits 0 on a healthy log"
check_contains /tmp/719-verify.txt "verify passes all checks" "all checks passed"
check "[ \$(grep -c '\[PASS\]' /tmp/719-verify.txt) -eq 4 ]" "all four checks PASS"
check_not_contains /tmp/719-verify.txt "no failures reported" "[FAIL]"

echo "--- Step 2: tamper detection ---"
TILE=$(find "$POND/tlog/tile/0" -type f | head -1)
echo "tampering with tile: $TILE"
printf '\x00%.0s' $(seq 1 96) > "$TILE"

set +e
pond tlog verify > /tmp/719-tamper.txt 2>/dev/null
TAMPER_RC=$?
set -e
cat /tmp/719-tamper.txt
check "[ $TAMPER_RC -ne 0 ]" "verify exits non-zero on a tampered log"
check_contains /tmp/719-tamper.txt "tamper produces a FAIL" "[FAIL]"

echo "--- Step 3: dropped export self-heals on next commit ---"
rm -rf "$POND/tlog"
echo "row 4" > /tmp/719-f4.txt
pond copy host:///tmp/719-f4.txt /f4.txt >/dev/null 2>&1

pond tlog verify > /tmp/719-heal.txt 2>/dev/null
HEAL_RC=$?
cat /tmp/719-heal.txt
check "[ $HEAL_RC -eq 0 ]" "verify exits 0 after the export self-heals"
check_contains /tmp/719-heal.txt "healed log has four leaves" "4 leaf/leaves, all checks passed"

check_finish
