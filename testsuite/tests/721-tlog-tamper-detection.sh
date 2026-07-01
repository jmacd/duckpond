#!/bin/bash
# EXPERIMENT: `pond tlog verify` detects tampering of the checkpoint and the
# checkpoint history, and localizes which property broke.
#
# DESCRIPTION:
#   719 corrupts a level-0 tile; this test corrupts the two other published
#   artifacts and shows verify pinpoints the failure:
#     1. Flip the ROOT in {POND}/tlog/checkpoint -> the tiles no longer
#        reproduce the checkpoint, so the reproduce/inclusion/consistency checks
#        FAIL, but faithfulness to the control-table commit spine still PASSES
#        (the authoritative log is untouched).  Exit non-zero.
#     2. Flip an old ROOT in {POND}/tlog/checkpoints (the append-only history)
#        -> only the append-only consistency check FAILS; the live checkpoint
#        still reproduces and every leaf still includes.  Exit non-zero.
#
# EXPECTED:
#   - Checkpoint tamper: [FAIL] on reproduce/inclusion/consistency, [PASS] on
#     faithfulness, non-zero exit.
#   - History tamper: [FAIL] only on append-only consistency, [PASS] on the
#     checkpoint reproduce check, non-zero exit.
#
# History:
#   Added with `pond tlog` to cover artifact-specific tamper detection.
#   Self-contained, runs in the base container (uses base64/awk, no Python).
set -e
source check.sh

echo "=== Experiment: pond tlog tamper detection ==="

build_pond() {  # DIR
  local P="$1"
  rm -rf "$P"
  POND="$P" pond init --birthplace test-host >/dev/null
  local i
  for i in 1 2 3; do
    echo "row $i" > /tmp/721-f$i.txt
    POND="$P" pond copy host:///tmp/721-f$i.txt /f$i.txt >/dev/null 2>&1
  done
}

# A valid-but-wrong 32-byte root (all zeros), so Checkpoint parsing still
# succeeds and the semantic checks -- not a parse error -- are what fail.
WRONG_ROOT=$(head -c 32 /dev/zero | base64)

echo "--- Step 1: tamper the checkpoint root ---"
P1=/tmp/721-p1
build_pond "$P1"
CP="$P1/tlog/checkpoint"
{ sed -n '1,2p' "$CP"; echo "$WRONG_ROOT"; } > "$CP.tmp" && mv "$CP.tmp" "$CP"

set +e
POND="$P1" pond tlog verify > /tmp/721-cp.txt 2>/dev/null
CP_RC=$?
set -e
cat /tmp/721-cp.txt
check "[ $CP_RC -ne 0 ]" "checkpoint tamper -> non-zero exit"
check_contains /tmp/721-cp.txt "reproduce check fails" "[FAIL] checkpoint root"
check_contains /tmp/721-cp.txt "inclusion check fails" "[FAIL] inclusion"
check_contains /tmp/721-cp.txt "faithfulness still holds" "[PASS] faithful to control-table"

echo "--- Step 2: tamper an old history root ---"
P2=/tmp/721-p2
build_pond "$P2"
H="$P2/tlog/checkpoints"
awk -v nr="$WRONG_ROOT" 'NR==1{$3=nr} {print}' "$H" > "$H.tmp" && mv "$H.tmp" "$H"

set +e
POND="$P2" pond tlog verify > /tmp/721-hist.txt 2>/dev/null
H_RC=$?
set -e
cat /tmp/721-hist.txt
check "[ $H_RC -ne 0 ]" "history tamper -> non-zero exit"
check_contains /tmp/721-hist.txt "consistency check fails" "[FAIL] append-only consistency"
check_contains /tmp/721-hist.txt "checkpoint still reproduces" "[PASS] checkpoint root"
check_contains /tmp/721-hist.txt "inclusion still holds" "[PASS] inclusion"

check_finish
