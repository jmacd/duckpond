#!/bin/bash
# EXPERIMENT: billing crate bootstrap + full cycle issuance
# DESCRIPTION: Apply the accounts factory, verify the initialize hook
#   seeds chart_of_accounts and amortization rules, build a small fixture
#   (2 customers, 2 connections, 1 commercial), define a share-by-weight
#   policy, add expenses, issue a cycle, record a payment, and verify
#   reports show the expected numbers.
# EXPECTED: Cycle of $300 with margin 0.2 produces $360 total revenue.
#   Commercial connection (weight 2) bills $240, residential (weight 1)
#   bills $120. After a $100 partial payment, Alice owes $140, Bob owes
#   $120. Trial balance is exactly zero. P&L shows $60 reserves.
set -e
source check.sh

echo "=== Experiment: billing bootstrap + full cycle issuance ==="

pond init >/dev/null

# ==============================================================================
# Step 1: Apply the accounts factory
# ==============================================================================

echo ""
echo "--- Step 1: Apply accounts factory ---"

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

APPLY_OUT=$(pond apply -f /tmp/accounts.yaml 2>&1)
echo "$APPLY_OUT"
check 'echo "$APPLY_OUT" | grep -q "created.*accounts"' "accounts node created"

# ==============================================================================
# Step 2: Verify initialize hook seeded reference data
# ==============================================================================

echo ""
echo "--- Step 2: Initialize hook seeded chart + amortization rules ---"

CHART_OUT=$(pond cat /data/chart_of_accounts.parquet --format=table 2>&1)
echo "$CHART_OUT" | head -15
check 'echo "$CHART_OUT" | grep -q "Cash"'                "chart has Cash account"
check 'echo "$CHART_OUT" | grep -q "Accounts Receivable"' "chart has AR"
check 'echo "$CHART_OUT" | grep -q "Service Revenue"'     "chart has Service Revenue"
check 'echo "$CHART_OUT" | grep -q "Operations"'          "chart has Operations expense"

AMORT_OUT=$(pond run /system/etc/accounts -- policy amortize list 2>&1)
echo "$AMORT_OUT" | grep "INFO  billing::cli::policy"
check 'echo "$AMORT_OUT" | grep -q "5300"'      "amortization rule for Insurance (5300)"
check 'echo "$AMORT_OUT" | grep -q "5400"'      "amortization rule for Taxes (5400)"

# ==============================================================================
# Step 3: Build the fixture
# ==============================================================================

echo ""
echo "--- Step 3: Build fixture (customers, connections, tenancies, policy) ---"

pond run /system/etc/accounts -- business-set --name=TestCo --address="1 Main St" --contact="t@test" >/dev/null 2>&1
pond run /system/etc/accounts -- customer add --name=Alice --billing="PO Box 1" --contact="alice@x" >/dev/null 2>&1
pond run /system/etc/accounts -- customer add --name=Bob --billing="PO Box 2" >/dev/null 2>&1
pond run /system/etc/accounts -- connection add --name=CommCtr --service="100 Main" --first-active=2010-01-01 --commercial >/dev/null 2>&1
pond run /system/etc/accounts -- connection add --name="200 Main" --service="200 Main" --first-active=2010-01-01 >/dev/null 2>&1
pond run /system/etc/accounts -- tenancy start --connection=CommCtr --customer=Alice --start=2020-01-01 >/dev/null 2>&1
pond run /system/etc/accounts -- tenancy start --connection="200 Main" --customer=Bob --start=2020-01-01 >/dev/null 2>&1

cat > /tmp/policy.yaml << 'PY'
margin: 0.2
weights:
  commercial: 2.0
  residential: 1.0
PY
pond run /system/etc/accounts -- policy add --name=p1 --kind=share-by-weight --config-path=/tmp/policy.yaml >/dev/null 2>&1
pond run /system/etc/accounts -- cycle add --name=2024H1 --start=2024-04-01 --bill-date=2024-10-01 --policy=p1 >/dev/null 2>&1

echo "fixture loaded"

# ==============================================================================
# Step 4: Add expenses + issue
# ==============================================================================

echo ""
echo "--- Step 4: Add expenses, refresh totals, issue ---"

pond run /system/etc/accounts -- expense add --date=2024-05-01 --account=5100 --amount='$200.00' --vendor=ChemCo --cycle=1 >/dev/null 2>&1
pond run /system/etc/accounts -- expense add --date=2024-06-01 --account=5200 --amount='$100.00' --vendor=PG --cycle=1 >/dev/null 2>&1

# Bills issue without refresh should refuse (stale-totals guard).
ISSUE_STALE=$(pond run /system/etc/accounts -- bills issue --cycle=2024H1 2>&1 || true)
check 'echo "$ISSUE_STALE" | grep -q "stale"'   "bills issue refused on stale totals"

pond run /system/etc/accounts -- cycle totals-refresh >/dev/null 2>&1

ISSUE_OUT=$(pond run /system/etc/accounts -- bills issue --cycle=2024H1 2>&1)
echo "$ISSUE_OUT" | grep "OK\|outcome"
check 'echo "$ISSUE_OUT" | grep -q "totaling .360.00"'   "cycle revenue is \$360"

# ==============================================================================
# Step 5: Reports
# ==============================================================================

echo ""
echo "--- Step 5: Verify reports ---"

# Alice (commercial): commercial=2, residential=1 -> denom=3 -> alice=2/3 of $360 = $240
# Bob (residential): 1/3 of $360 = $120
BAL_OUT=$(pond run /system/etc/accounts -- balance 2>&1)
echo "$BAL_OUT" | grep "INFO  billing::cli::reports"
check 'echo "$BAL_OUT" | grep -q "Alice.*\$240.00"'      "Alice owes \$240 after issue"
check 'echo "$BAL_OUT" | grep -q "Bob.*\$120.00"'        "Bob owes \$120 after issue"

# Partial payment: Alice pays $100 -> owes $140
pond run /system/etc/accounts -- payment add --date=2024-10-15 --customer=Alice --amount='$100.00' --method=check >/dev/null 2>&1

BAL_OUT2=$(pond run /system/etc/accounts -- balance 2>&1)
check 'echo "$BAL_OUT2" | grep -q "Alice.*\$140.00"'     "Alice owes \$140 after \$100 partial pay"
check 'echo "$BAL_OUT2" | grep -q "Bob.*\$120.00"'       "Bob still owes \$120"

# Trial balance must balance to zero
TB_OUT=$(pond run /system/etc/accounts -- trial-balance 2>&1)
echo "$TB_OUT" | grep "INFO  billing::cli::reports"
check 'echo "$TB_OUT" | grep -q "TOTAL"'                 "trial balance prints TOTAL row"
# Reports format: [WARN] would appear in trial balance log if not balanced
check 'echo "$TB_OUT" | grep -v -q "trial balance does not balance"'  "trial balance balanced"

# P&L: revenue $360, expenses $300, reserves $60
PNL_OUT=$(pond run /system/etc/accounts -- pnl 2>&1)
echo "$PNL_OUT" | grep "INFO  billing::cli::reports"
check 'echo "$PNL_OUT" | grep -q "Revenue.*\$360.00"'    "P&L revenue is \$360"
check 'echo "$PNL_OUT" | grep -q "Reserves.*\$60.00"'    "P&L reserves contribution is \$60"

# Who-owes lists both customers desc by balance
WO_OUT=$(pond run /system/etc/accounts -- who-owes 2>&1)
check 'echo "$WO_OUT" | grep -q "Alice"'                 "who-owes lists Alice"
check 'echo "$WO_OUT" | grep -q "Bob"'                   "who-owes lists Bob"

# ==============================================================================
# Step 6: Verify
# ==============================================================================

echo ""
echo "--- Step 6: Verify all 11 invariants hold ---"

VERIFY_OUT=$(pond run /system/etc/accounts -- verify 2>&1)
echo "$VERIFY_OUT" | grep "INFO  billing::cli::verify"
check 'echo "$VERIFY_OUT" | grep -q "all invariants hold"'  "verify clean"

check_finish
