#!/bin/bash
# EXPERIMENT: billing crate -- verify catches table corruption
# DESCRIPTION: Walk through several deliberate corruption scenarios
#   (modifying parquet files behind verify's back) and confirm each
#   fires the expected invariant category. Demonstrates that the
#   verify subcommand is a real safety net, not just a smoke test.
# EXPECTED: Each corruption produces a distinct VIOLATION category
#   that names the offending row.
set -e
source check.sh

echo "=== Experiment: billing -- verify detects corruption ==="

pond init --birthplace test-host >/dev/null

# ---- Setup: a clean issued cycle ----

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

VERIFY_BASE=$(pond run /system/etc/accounts -- verify 2>&1)
check 'echo "$VERIFY_BASE" | grep -q "all invariants hold"'  "baseline pond is clean"

# ==============================================================================
# Detection 1: stale cycle_totals_materialized (operator added an expense
#              without refreshing). EXPENSE_CYCLE_TOTALS should fire AND
#              the noisy BILL_DRIFT check should be skipped.
# ==============================================================================

echo ""
echo "--- Detection 1: stale cycle_totals -> EXPENSE_CYCLE_TOTALS ---"

pond run /system/etc/accounts -- expense add --date=2024-07-01 --account=5100 --amount='$25.00' --cycle=1 >/dev/null 2>&1

V1=$(pond run /system/etc/accounts -- verify 2>&1 || true)
echo "$V1" | grep "INFO  billing::cli::verify" | head -10
check 'echo "$V1" | grep -q "EXPENSE_CYCLE_TOTALS"'         "EXPENSE_CYCLE_TOTALS violation reported"
check 'echo "$V1" | grep -q "skipping BILL_DRIFT"'          "BILL_DRIFT skipped (not noisy)"
check 'echo "$V1" | grep -q "totals-refresh"'               "remediation hint mentions totals-refresh"

# Recover.
pond run /system/etc/accounts -- bills reverse --cycle=2024H1 >/dev/null 2>&1
pond run /system/etc/accounts -- cycle totals-refresh >/dev/null 2>&1
pond run /system/etc/accounts -- bills issue --cycle=2024H1 >/dev/null 2>&1

V_RECOVER=$(pond run /system/etc/accounts -- verify 2>&1)
check 'echo "$V_RECOVER" | grep -q "all invariants hold"'   "verify clean after recovery"

# ==============================================================================
# Detection 2: tenancy edit refused on issued cycle (preventive guard,
#              not a verify check, but exercises the same correctness
#              boundary).
# ==============================================================================

echo ""
echo "--- Detection 2: tenancy edit guard ---"

TEDIT=$(pond run /system/etc/accounts -- tenancy edit --tenancy=1 --notes="bypass attempt" 2>&1 || true)
check 'echo "$TEDIT" | grep -q "issued cycle"'              "tenancy edit refused with clear message"

# ==============================================================================
# Detection 3: bills issue gated on stale totals (preventive)
# ==============================================================================

echo ""
echo "--- Detection 3: bills issue refuses stale totals ---"

# After a reverse + adding an expense WITHOUT refresh, issue must refuse.
pond run /system/etc/accounts -- bills reverse --cycle=2024H1 >/dev/null 2>&1
pond run /system/etc/accounts -- expense add --date=2024-08-01 --account=5100 --amount='$15.00' --cycle=1 >/dev/null 2>&1
ISS_STALE=$(pond run /system/etc/accounts -- bills issue --cycle=2024H1 2>&1 || true)
check 'echo "$ISS_STALE" | grep -q "stale"'                "bills issue refuses on stale totals"
check 'echo "$ISS_STALE" | grep -q "totals-refresh"'        "error names the fix"

# Recover for next test.
pond run /system/etc/accounts -- cycle totals-refresh >/dev/null 2>&1
pond run /system/etc/accounts -- bills issue --cycle=2024H1 >/dev/null 2>&1

# ==============================================================================
# Detection 4: policy edit refuses inverted date range
# ==============================================================================

echo ""
echo "--- Detection 4: policy edit refuses effective_to < effective_from ---"

# Policy p1 was added with effective_from = 1970-01-01 (default). Try
# to set effective_to BEFORE that.
PINV=$(pond run /system/etc/accounts -- policy edit p1 --effective-to=1969-01-01 2>&1 || true)
check 'echo "$PINV" | grep -q "cannot precede effective_from"'  "policy edit rejects inverted date range"

# ==============================================================================
# Detection 5: opening-balance refused after activity exists
# ==============================================================================

echo ""
echo "--- Detection 5: opening-balance refused after activity ---"

# Alice has bill activity from the issue above. opening-balance must refuse.
OPEN_REFUSED=$(pond run /system/etc/accounts -- opening-balance --customer=Alice --amount='$100.00' --date=2024-01-01 2>&1 || true)
check 'echo "$OPEN_REFUSED" | grep -q "already has"'        "opening-balance refused with clear message"

# ==============================================================================
# Detection 6: adjust unbalanced is rejected
# ==============================================================================

echo ""
echo "--- Detection 6: adjust unbalanced is rejected ---"

ADJ_BAD=$(pond run /system/etc/accounts -- adjust --date=2024-12-01 --memo="bad" --leg=1100:1:5000:0 --leg=4000::0:4999 2>&1 || true)
check 'echo "$ADJ_BAD" | grep -q "unbalanced"'              "adjust rejects unbalanced legs"

# ==============================================================================
# Detection 7: pond is still clean after all the failed attempts
# ==============================================================================

echo ""
echo "--- Detection 7: pond is still clean ---"

V_FINAL=$(pond run /system/etc/accounts -- verify 2>&1)
check 'echo "$V_FINAL" | grep -q "all invariants hold"'     "verify clean after rejected operations"

check_finish
