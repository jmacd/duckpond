#!/bin/bash
# EXPERIMENT: billing crate -- fixing operator mistakes
# DESCRIPTION: Demonstrate the workflow rules for correcting mistakes:
#   bills reverse + reissue when an expense is added late, payment void,
#   tenancy edit refused after issue, customer merge with split-not-mutate
#   semantics. Verify is clean throughout (and catches the mid-flight
#   stale-totals state).
# EXPECTED: Each fix reaches a verify-clean state. The journal preserves
#   both the original txn and the reversal txn (audit trail).
set -e
source check.sh

echo "=== Experiment: billing -- fixing operator mistakes ==="

pond init >/dev/null

# ---- Setup ----

cat > /tmp/accounts.yaml << 'YAML'
version: v1
kind: mkdir
metadata:
  path: /system/etc
---
version: v1
kind: mknod
metadata:
  path: /system/etc/accounts
spec:
  factory: accounts
  config: {}
YAML
pond apply -f /tmp/accounts.yaml >/dev/null 2>&1

cat > /tmp/policy.yaml << 'PY'
margin: 0.2
weights:
  commercial: 2.0
  residential: 1.0
PY

pond run /system/etc/accounts -- customer add --name=Alice --billing=addr1 >/dev/null 2>&1
pond run /system/etc/accounts -- customer add --name=Bob --billing=addr2 >/dev/null 2>&1
pond run /system/etc/accounts -- connection add --name=CommCtr --service="100 Main" --first-active=2010-01-01 --commercial >/dev/null 2>&1
pond run /system/etc/accounts -- connection add --name="200 Main" --service="200 Main" --first-active=2010-01-01 >/dev/null 2>&1
pond run /system/etc/accounts -- tenancy start --connection=CommCtr --customer=Alice --start=2020-01-01 >/dev/null 2>&1
pond run /system/etc/accounts -- tenancy start --connection="200 Main" --customer=Bob --start=2020-01-01 >/dev/null 2>&1
pond run /system/etc/accounts -- policy add --name=p1 --kind=share-by-weight --config-path=/tmp/policy.yaml >/dev/null 2>&1
pond run /system/etc/accounts -- cycle add --name=2024H1 --start=2024-04-01 --bill-date=2024-10-01 --policy=p1 >/dev/null 2>&1
pond run /system/etc/accounts -- expense add --date=2024-05-01 --account=5100 --amount='$200.00' --cycle=1 >/dev/null 2>&1
pond run /system/etc/accounts -- expense add --date=2024-06-01 --account=5200 --amount='$100.00' --cycle=1 >/dev/null 2>&1
pond run /system/etc/accounts -- cycle totals-refresh >/dev/null 2>&1
pond run /system/etc/accounts -- bills issue --cycle=2024H1 >/dev/null 2>&1

echo "Initial state: cycle 2024H1 issued"
VERIFY1=$(pond run /system/etc/accounts -- verify 2>&1)
check 'echo "$VERIFY1" | grep -q "all invariants hold"'   "verify clean after initial issue"

# ==============================================================================
# Mistake 1: forgot an expense -> reverse + reissue
# ==============================================================================

echo ""
echo "--- Mistake 1: forgot an expense ---"

# Tenancy edit while a cycle is issued is refused
TEDIT_OUT=$(pond run /system/etc/accounts -- tenancy edit --tenancy=1 --notes="should refuse" 2>&1 || true)
check 'echo "$TEDIT_OUT" | grep -q "issued cycle"'         "tenancy edit refused on issued cycle"

# Add the missed expense -- this leaves the cycle in a stale state
pond run /system/etc/accounts -- expense add --date=2024-07-01 --account=5100 --amount='$30.00' --cycle=1 >/dev/null 2>&1

# Trying to reissue without reversing should fail (cycle already issued)
REISSUE_OUT=$(pond run /system/etc/accounts -- bills issue --cycle=2024H1 2>&1 || true)
check 'echo "$REISSUE_OUT" | grep -q "already issued"'    "bills issue refuses if already issued"

# verify catches the stale totals (via EXPENSE_CYCLE_TOTALS, before BILL_DRIFT)
VSTALE=$(pond run /system/etc/accounts -- verify 2>&1 || true)
check 'echo "$VSTALE" | grep -q "EXPENSE_CYCLE_TOTALS"'    "verify reports stale cycle_totals"
check 'echo "$VSTALE" | grep -q "skipping BILL_DRIFT"'     "verify skips BILL_DRIFT when totals stale"

# Operator workflow: reverse, refresh, reissue
pond run /system/etc/accounts -- bills reverse --cycle=2024H1 --reason="missed expense" >/dev/null 2>&1
pond run /system/etc/accounts -- cycle totals-refresh >/dev/null 2>&1
pond run /system/etc/accounts -- bills issue --cycle=2024H1 >/dev/null 2>&1

VERIFY2=$(pond run /system/etc/accounts -- verify 2>&1)
check 'echo "$VERIFY2" | grep -q "all invariants hold"'   "verify clean after reverse+reissue"

# Both the original AND reversal txns are still in the journal (audit trail)
JOURNAL_OUT=$(pond run /system/etc/accounts -- journal-show --cycle=1 2>&1)
check 'echo "$JOURNAL_OUT" | grep -q "bill"'              "original bill txn preserved"
check 'echo "$JOURNAL_OUT" | grep -q "reversal"'          "reversal txn preserved"

# Cycle revenue is now (200 + 100 + 30) * 1.2 = $396
NEW_BAL=$(pond run /system/etc/accounts -- pnl 2>&1)
check 'echo "$NEW_BAL" | grep -q "Revenue.*\$396.00"'     "P&L revenue updated to \$396"

# ==============================================================================
# Mistake 2: customer merge with split-not-mutate
# ==============================================================================

echo ""
echo "--- Mistake 2: discover Alice and Bob are the same person; merge ---"

# Alice has unpaid AR from the issue. Record a payment, then later
# we'll discover the merge guard prevents voiding it after merge.
pond run /system/etc/accounts -- payment add --date=2024-10-15 --customer=Alice --amount='$50.00' >/dev/null 2>&1
ALICE_PAY_ID=1

# Snapshot tenancy IDs before merge
T_BEFORE=$(pond run /system/etc/accounts -- tenancy list 2>&1 | grep "INFO" | grep -c "[0-9]" || true)

# Merge Alice -> Bob. Alice's open tenancy (CommCtr) gets split: closed
# at merge_date - 1 (history under Alice), new tenancy starts at
# merge_date for Bob. Alice's $X AR balance is transferred to Bob via
# DR 1100/Bob; CR 1100/Alice.
MERGE_OUT=$(pond run /system/etc/accounts -- customer merge --from=Alice --into=Bob 2>&1)
check 'echo "$MERGE_OUT" | grep -q "customer merge"'      "customer merge logs OK line"
check 'echo "$MERGE_OUT" | grep -q "split"'               "merge splits open tenancies"
check 'echo "$MERGE_OUT" | grep -q "transfer balance"'    "merge posts transfer txn"

# Verify Alice is marked merged_into=Bob
ALICE_SHOW=$(pond run /system/etc/accounts -- customer show Alice 2>&1)
check 'echo "$ALICE_SHOW" | grep -q "merged into"'        "Alice shows merged_into"
check 'echo "$ALICE_SHOW" | grep -q "active.*: false"'    "Alice is now inactive"

# Tenancy count went UP by 1 (Alice's open tenancy split into 2 rows)
T_AFTER=$(pond run /system/etc/accounts -- tenancy list 2>&1 | grep "INFO" | grep -c "[0-9]" || true)
check '[ "$T_AFTER" -gt "$T_BEFORE" ]'                     "tenancy count increased after split"

VERIFY3=$(pond run /system/etc/accounts -- verify 2>&1)
check 'echo "$VERIFY3" | grep -q "all invariants hold"'   "verify clean after merge"

# Voiding Alice's pre-merge payment is now refused (would reopen AR on
# the merged-from customer).
VOID_MERGED=$(pond run /system/etc/accounts -- payment void --id=$ALICE_PAY_ID --memo="too late" 2>&1 || true)
check 'echo "$VOID_MERGED" | grep -q "merged into"'        "payment void refused on merged customer"

check_finish
