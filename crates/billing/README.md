<!--
SPDX-FileCopyrightText: 2026 Caspar Water Company

SPDX-License-Identifier: Apache-2.0
-->

# `billing` — Caspar Water bookkeeping (v2)

A tiny, transactional, double-entry bookkeeping system implemented as a
Watertown executable factory. Replaces the legacy CSV-driven Go program
with an append-only journal, pluggable billing policies, and operator
CLI commands that maintain audit-correctness automatically.

See [`docs/billing-v2.md`](../../docs/billing-v2.md) for the design
document this implementation follows.

## What this crate provides

- The **`accounts` executable factory** registered in the watertown
  factory registry. Mounted via `pond apply` (conventionally at
  `/system/etc/accounts`) and invoked via `pond run`.
- ~30 operator subcommands grouped under nouns:
  `business`, `customer`, `connection`, `tenancy`, `cycle`, `expense`,
  `payment`, `policy`, `bills`, plus standalone `opening-balance`,
  `adjust`, `journal-show`, `balance`, `who-owes`, `aging`,
  `trial-balance`, `pnl`, `verify`.
- A `BillingPolicy` trait + linkme registry; one strategy ships day-1
  (`share-by-weight`).
- A `verify` subcommand running 11 invariant checks for end-to-end
  audit-correctness.

All money is stored as integer cents. All dates parse/print as
`YYYY-MM-DD`. All journal transactions are balanced (`Σ debit_cents
== Σ credit_cents`) and audited.

## Quickstart

### One-time bootstrap (tech contact)

```bash
# Choose where the books live and stand them up.
export POND=$HOME/caspar-books-pond
pond init

# Mount the accounts factory. The initialize hook seeds the chart of
# accounts and default amortization rules (5300 Insurance + 5400 Taxes
# split 50/50). The mount path is your choice; /system/etc/accounts is
# the convention.
cat > /tmp/accounts.yaml <<'EOF'
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
EOF
pond apply -f /tmp/accounts.yaml

# (Optional) Mount the trial-balance view.
pond apply -f /path/to/billing/views/view-trial-balance.yaml

# Set the company identity.
pond run /system/etc/accounts -- business-set \
  --name='Caspar Water Company, LLC' \
  --address='200 Example Lane; Anytown, CA 99999' \
  --contact='p: 555-555-5555; e: ops@example.com'

# Register a billing policy.
cat > /tmp/policy.yaml <<'EOF'
margin: 0.2
weights:
  commercial: 2.0
  residential: 1.0
EOF
pond run /system/etc/accounts -- policy add \
  --name=share-by-weight-2024 \
  --kind=share-by-weight \
  --config-path=/tmp/policy.yaml \
  --description='Standard 20% margin'
```

The operator now has a working bookkeeping pond. They add connections,
customers, tenancies, expenses, payments, and cycles via the
subcommands below. The journal records everything.

### Daily operator workflow

Add new customer + connection + tenancy:

```bash
pond run /system/etc/accounts -- customer add \
  --name='Alex Carver' --billing='123 Maple St'
pond run /system/etc/accounts -- connection add \
  --name='100 Maple' --service='100 Maple St' \
  --first-active=2024-04-01
pond run /system/etc/accounts -- tenancy start \
  --connection='100 Maple' --customer='Alex Carver' \
  --start=2024-04-01
```

Record an expense:

```bash
pond run /system/etc/accounts -- expense add \
  --date=2024-05-15 --account=5100 --amount='$1,234.56' \
  --vendor=ChemCo --cycle=1
```

Receive a payment:

```bash
pond run /system/etc/accounts -- payment add \
  --date=2024-05-20 --customer='Alex Carver' \
  --amount='$50.00' --method=check
```

Bill a cycle (preview, then issue):

```bash
pond run /system/etc/accounts -- bills preview --cycle=2024H1
pond run /system/etc/accounts -- cycle totals-refresh   # required before issue
pond run /system/etc/accounts -- bills issue --cycle=2024H1
```

If you need to fix something on an issued cycle:

```bash
pond run /system/etc/accounts -- bills reverse --cycle=2024H1 \
  --reason='added missed expense'
# ... edit reference data / add expenses / etc ...
pond run /system/etc/accounts -- cycle totals-refresh
pond run /system/etc/accounts -- bills issue --cycle=2024H1
```

End-of-day health check:

```bash
pond run /system/etc/accounts -- verify
```

## Operator workflow rules

The system enforces these invariants automatically; they're documented
here for the operator's mental model.

### Connections are write-once for billing fields

`connection edit` only allows `--name` and `--notes`. Billing-affecting
fields (`service`, `commercial`, `weight-class`, `first-active`) are
write-once. To fix a mistake, retire the connection (`connection
retire --as-of=...`) and add a fresh one.

### Tenancies are immutable for issued cycles

If a tenancy covers the `bill_date` of an already-issued cycle,
`tenancy edit` is refused. The operator must `bills reverse` first,
edit, then re-issue.

### Customer merge is split-not-mutate

`customer merge --from=A --into=B` does NOT re-point A's open
tenancies in place. Instead it CLOSES each open tenancy at
`merge_date - 1` (preserving history under A) and creates a new
tenancy starting `merge_date` for B. The historical responsible
customer for any pre-merge bill is forever A. Refused if any open
tenancy on A covers an issued cycle's bill_date after merge_date.

### Always reverse + reissue when changing inputs to an issued cycle

If you add/void an expense for a cycle that was already issued, OR
correct a tenancy that covered that cycle's bill_date, you must
`bills reverse <cycle>` and `bills issue` again. Otherwise `verify`
will report `BILL_DRIFT` (the strategy's current output disagrees with
the recorded `bill_breakdowns`).

### Customer reverse with `payment void`, not `payment add` of a negative

Each payment is its own row. To undo a mistakenly-recorded payment,
use `payment void --id=N`. The system writes a paired reversing
journal transaction (`source = 'reversal'`, `source_ref =
'reversal-of:payment=N'`) so the audit trail explains the
correction.

## Subcommand reference (one-line each)

### Setup / identity

- `business-set --name --address --contact [--ein]` — set the company row.

### Connections (the houses)

- `connection add --name --service --first-active --commercial=true|false [--weight-class] [--notes]`
- `connection edit ID|NAME [--name] [--notes]` — only soft fields editable
- `connection retire ID|NAME --as-of=DATE` — refused with open tenancy
- `connection list [--active] [--commercial]`
- `connection show ID|NAME` — full detail incl. current customer

### Customers (the paying parties)

- `customer add --name --billing [--contact] [--notes]`
- `customer edit ID|NAME [--name] [--billing] [--contact] [--active]`
- `customer merge --from=ID|NAME --into=ID|NAME` — split-not-mutate
- `customer list [--active] [--with-balance]`
- `customer show ID|NAME` — incl. AR balance + last 5 payments
- `customer find QUERY` — fuzzy substring lookup

### Tenancies (who is responsible right now)

- `tenancy start --connection=ID|NAME --customer=ID|NAME --start=DATE [--notes]` — auto-closes prior open tenancy on that connection
- `tenancy end {--connection=ID|NAME | --tenancy=ID} --end=DATE [--notes]`
- `tenancy edit --tenancy=ID [--start] [--end] [--customer] [--notes]` — refused if covers issued cycle
- `tenancy list [--connection] [--customer] [--as-of=DATE]`

### Cycles (six-month billing periods)

- `cycle add --name --start=DATE --bill-date=DATE --policy=ID|NAME [--inactive=ID,ID] [--notes] [--allow-irregular]` — period_end auto-computed (start + 6mo - 1d); warns on non-Apr-1 / Oct-1 starts unless `--allow-irregular`
- `cycle edit ID|NAME [--bill-date] [--inactive] [--notes]` — refused if issued
- `cycle list` / `cycle show ID|NAME`
- `cycle totals [--cycle=ID|NAME]` — read materialized totals
- `cycle totals-refresh` — recompute materialized cycle_totals from expenses + amortization rules; idempotent

### Expenses (line-item costs)

- `expense add --date=DATE --account=CODE --amount=$... [--vendor] [--memo] [--cycle=ID|NAME]` — DR account; CR 1000 (Cash)
- `expense void --id=N [--memo]` — reversing entry
- `expense list [--cycle] [--account] [--from] [--to]`

### Payments (customer payments received)

- `payment add --date=DATE --customer=ID|NAME --amount=$... [--method=check] [--memo]` — DR 1000 (Cash); CR 1100 (AR) tagged with customer
- `payment void --id=N [--memo]` — reversing entry; refused if customer is merged
- `payment list [--customer] [--cycle] [--from] [--to]`

### Billing policies

- `policy kinds` — enumerate registered strategy kinds
- `policy add --name --kind --config-path=PATH|- [--effective-from=DATE] [--description]` — YAML validated against the kind's schema
- `policy list [--kind] [--effective-as-of=DATE]`
- `policy show ID|NAME`
- `policy edit ID|NAME [--description] [--effective-to=DATE]` — refused if shrinking would invalidate any issued cycle
- `policy amortize add --account=CODE --this --next --effective-from=DATE [--description]` — append-only; auto-supersedes prior open rule on same account
- `policy amortize list [--effective-as-of=DATE]`

### Bills

- `bills preview --cycle=ID|NAME` — runs strategy, prints proposed rows. No writes. Warns about active connections without covering tenancies.
- `bills issue --cycle=ID|NAME` — single transaction: 2N journal legs + N bill_breakdowns rows + cycle update (`issued=true`, `bill_txn_id=N`). Refused if any active connection lacks a covering tenancy.
- `bills reverse --cycle=ID|NAME [--reason]` — single transaction: posts swapped legs, deletes cycle's bill_breakdowns, clears `bill_txn_id` and `issued`.

### Adjustments + opening balances

- `opening-balance --customer=ID|NAME --amount=$... [--date] [--memo]` — one-time per customer; refused if any non-opening activity exists
- `adjust --date --memo --leg=ACCT[:CUST]:DR_CENTS:CR_CENTS [--leg=...]...` — hand-written balanced transaction

### Reports

All reports support optional `--as-of=DATE` (or `--from`/`--to`).

- `balance [--customer=ID|NAME] [--as-of]` — per-customer AR balance
- `who-owes [--as-of] [--min=$0]` — positive balances, sorted desc, with oldest unpaid bill date and current connections
- `aging [--customer] [--as-of]` — FIFO-bucketed unpaid amounts (0-30/31-60/61-90/90+)
- `trial-balance [--as-of]` — per-account net DR/CR; warns if not balanced
- `pnl [--cycle | --from --to]` — revenue, expenses, reserves contribution
- `journal-show [--cycle] [--customer] [--connection] [--account] [--from] [--to] [--limit=200]` — raw journal slice

### Health

- `verify` — run all 11 invariant checks; exit non-zero on any violation. See [Verify checks](#verify-checks).

## Verify checks

| Category | What it checks |
|----------|----------------|
| TXN_BALANCE | Every txn_id has `Σ debit == Σ credit` |
| CUSTOMER_AR | Every AR (1100) leg references an existing customer |
| TENANCY_REFS | Tenancy IDs reference real customers + connections |
| TENANCY_NO_OVERLAP | Pairwise check per connection |
| CYCLE_POLICY | Cycle's policy_id resolves; effective dates honored at issue |
| CYCLE_ISSUED_TXN | `bill_txn_id IS NOT NULL ↔ issued = true`; txn must exist |
| CYCLE_TENANCY_COVERAGE | Every active connection on issued cycle's bill_date has a covering tenancy |
| BILL_BREAKDOWN_JOURNAL | Journal `DR 1100 = breakdown.base + breakdown.margin` |
| BILL_DRIFT | Re-running the strategy reproduces every bill_breakdowns row exactly |
| CUSTOMER_MERGE | `merged_into_customer_id` references existing/active/non-merged customer |
| EXPENSE_CYCLE_TOTALS | Materialized cycle_totals matches a fresh recompute |

`BILL_DRIFT` and `EXPENSE_CYCLE_TOTALS` are the most commonly-tripped
checks; both indicate that the operator made a change that requires
either `cycle totals-refresh` or `bills reverse + bills issue`.

## Pond layout

After `pond apply` and the first few subcommand invocations, the books
pond contains:

```
$POND/
  data/
    business.parquet                # one-row company identity
    chart_of_accounts.parquet       # seeded by initialize hook
    billing_policies.parquet        # operator-registered policies
    amortization_rules.parquet      # seeded for codes 5300, 5400
    connections.parquet
    customers.parquet
    tenancies.parquet
    cycles.parquet                  # incl. issued + bill_txn_id
    bill_breakdowns.parquet         # per-cycle strategy outputs
    cycle_totals_materialized.parquet  # output of cycle totals-refresh
    expenses.series                 # append-only
    payments.series                 # append-only
    journal.series                  # append-only -- the ledger
  system/etc/
    accounts                        # the executable factory
  views/                            # operator-applied SQL views (e.g. trial-balance)
```

## Backups

Use the standard watertown `remote` factory. The books pond is just a
pond.

## Out of scope (future phases)

- **PDF rendering / `statement` subcommand.** The design doc envisions
  a `statement --connection=... --cycle=...` subcommand whose JSON output
  feeds the existing Go `cmd/billing` PDF generator. This crate ships
  the bookkeeping layer; the PDF bridge has not been built. To produce
  printable statements today, query the journal via `journal-show
  --cycle=N` or read `/data/bill_breakdowns.parquet`.
- **Most `/views/*` SQL views.** The design doc lists nine views (bills,
  balances, who_owes, ar_aging, statements, pnl, cycle_totals,
  active_tenancies, trial_balance, bill_preview). Only
  `view-trial-balance.yaml` ships today. The CLI report subcommands
  (`balance`, `who-owes`, `aging`, `trial-balance`, `pnl`) cover the
  same data without the multi-version-row concern that bites
  `sql-derived-table` over reference tables.
- **`expense import`.** Bulk CSV import is not implemented; use repeated
  `expense add`.
- **`customer list --with-balance`.** The flag is accepted but ignored;
  use `balance` or `who-owes` instead.
- **`--format=csv|json`.** Report subcommands accept the flag for API
  compatibility but always emit table output today.
- **Email / SMS bill delivery, payment portal, multi-currency,
  depreciation, multi-tenant ponds.**
- **Meter readings + usage-based billing policies.** The
  `BillingPolicy` trait abstraction is in place; new kinds register
  the same way as `share-by-weight`.

### Migration / legacy data caveats

- **Default amortization rules.** The `initialize` hook seeds 50/50
  amortization for accounts 5300 (Insurance) and 5400 (Taxes) starting
  from 1970-01-01. If you're migrating from a legacy bookkeeping system
  that used different amortization (or none), `policy amortize add`
  with a specific `--effective-from` will supersede the default for
  cycles billed after that date — but the default will still apply to
  any historical cycles billed in v2 with a `bill_date` before the
  superseding rule. To represent fully-non-amortized historical
  expenses, add such expenses to a cycle whose `bill_date` precedes
  any rule, or use `adjust` to post non-amortized journal entries
  directly.
- **Opening balances.** Use `opening-balance --customer=NAME --amount=$N
  --date=DATE` exactly once per customer at migration time. The system
  refuses to opening-balance a customer who already has any
  non-`opening` activity.

## Architecture summary

```
src/
  factory.rs              the `accounts` executable factory; clap parser; dispatcher
  schema.rs               Arrow schemas + serde models for every table
  store.rs                read_table / write_table / read_series / write_series
  money.rs                Cents (i64) wrapper + parse/display/split/scale
  ids.rs                  (n/a; max+1 allocator lives in store.rs)
  dates.rs                YYYY-MM-DD parse/format
  lookup.rs               name-or-id resolver with ambiguity errors
  chart.rs                default chart of accounts seed
  cli/                    one module per subcommand group
    business.rs / customer.rs / connection.rs / tenancy.rs
    cycle.rs / expense.rs / payment.rs / policy.rs
    journal.rs            JournalWriter::write_transaction
    bills.rs              preview / issue / reverse
    opening.rs / adjust.rs
    reports.rs            balance / who-owes / aging / trial-balance / pnl
    verify.rs             11 invariant checks
  domain/
    tenancy_resolver.rs   responsible_customer_map(wd, on_date)
    active_connections.rs active_for_cycle(connections, cycle)
    amortization.rs       compute_totals(expenses, cycles, rules)
    cycle_totals.rs       refresh / read materialized totals
  policy/
    mod.rs                BillingPolicy trait + linkme registry
    shuffle.rs            SplitMix64 deterministic shuffle
    share_by_weight.rs    day-1 strategy
views/
  view-trial-balance.yaml example sql-derived-series view
```

## Running tests

```bash
cargo test -p billing
cargo clippy -p billing --all-features --tests -- -D warnings
cargo fmt --all
```
