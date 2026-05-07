<!--
SPDX-FileCopyrightText: 2026 Caspar Water Company

SPDX-License-Identifier: Apache-2.0
-->

# Caspar Water Bookkeeping v2 — Design & Implementation Plan

> **Status:** design complete, no code written yet.
> **Goal:** replace the Go `cmd/billing` data plane with a tiny double-entry
> bookkeeping system on DuckPond (Rust). Operator-facing CLI is the
> primary interface. PDF rendering stays in the existing Go program for
> now and consumes data from a `statement` subcommand of the new system.
> **Operator guide:** [`cmd/billing/new-guide.md`](../cmd/billing/new-guide.md)
> (the customer-facing "what to type" manual; this document is the
> developer-facing "what to build").

---

## Table of contents

1. [Problem and goals](#1-problem-and-goals)
2. [Mental model](#2-mental-model)
3. [Pond layout](#3-pond-layout)
4. [Schemas](#4-schemas)
5. [Standard journal entries](#5-standard-journal-entries)
6. [Billing policies](#6-billing-policies)
7. [Factories](#7-factories)
8. [CLI surface](#8-cli-surface)
9. [Dynamic-factory views](#9-dynamic-factory-views)
10. [Crate layout and code organization](#10-crate-layout-and-code-organization)
11. [Implementation phases](#11-implementation-phases)
12. [Testing strategy](#12-testing-strategy)
13. [Bootstrap recipe (operator setup)](#13-bootstrap-recipe-operator-setup)
14. [Open questions](#14-open-questions)
15. [Out of scope](#15-out-of-scope)
16. [Reference material](#16-reference-material)

---

## 1. Problem and goals

The Go `cmd/billing` program reads four CSV files (`users`, `business`,
`cycles`, `payments`) and produces PDF statements. Every six months the
operator updates the CSVs by hand from an external accounting system,
runs the program, and mails the PDFs.

Pain points addressed by v2:

- **No persistent ledger.** Each run starts from CSVs; there's no
  immutable audit trail of historical edits.
- **No double-entry.** Adjustments and reversals can't be expressed
  cleanly; the books can drift out of balance silently.
- **One conflated table for "users".** A row in `users.csv` is both a
  *connection* (the house) and a *customer* (the person). When a tenant
  moves out, today there's no way to keep the balance attached to the
  person rather than the house.
- **Billing policy hard-coded.** The `commercial`-doubles-weight rule
  and the 14-connection denominator are baked into Go code. Future
  policy variants (per-meter usage billing) will need plumbing.
- **No CLI for routine tasks.** Recording a new payment means editing
  a CSV by hand.

v2 design goals:

- Persistent, append-only journal stored in DuckPond (Delta Lake +
  Parquet) with proper double-entry semantics.
- Three normalized tables: connections, customers, tenancies. Balances
  follow customers, not connections.
- Pluggable, versioned billing policies — each cycle pins exactly one
  policy. Day 1 ships one policy *kind* (`share-by-weight`)
  configurable enough to express both the historical and current
  Caspar Water billing models.
- A single executable factory `accounts` providing every operator
  command, including friendly report subcommands. Operators never need
  to write SQL or know a `/views/...` path.
- Reuse of existing duckpond infrastructure: `pond apply` for
  bootstrap, `pond run` for invocation, `sql-derived-table` /
  `sql-derived-series` factories for views, the `remote` factory for
  S3 backup.

Non-goals (for this phase): PDF rendering (the existing Go program
keeps doing that, fed by the `statement` subcommand's structured
output), email/print delivery, multi-currency, depreciation,
usage-based metering.

## 2. Mental model

### Three concepts kept separate

- **Connection** — a physical service connection at a fixed address.
  Stable; changes only when service is permanently retired. Owns the
  `commercial` flag (a property of the meter/building, not the
  occupant).
- **Customer** — a person, family, or organization that pays bills.
  Survives moves between connections; carries balances long after a
  tenancy ends. One customer may be responsible for several
  connections at once (a landlord who owns four houses is one
  customer).
- **Tenancy** — `(connection_id, customer_id, start_date, end_date)`
  recording who is responsible for which connection over time. The
  customer responsible for a given cycle is the one whose tenancy
  covers the cycle's `bill_date`. **No pro-rating across mid-cycle
  changes** — kept simple deliberately. Operators can post a manual
  `adjust` transaction to split a cycle if they really need to.

Bills are *issued against connections* (each connection pays its share
of the cycle's allocated cost), but the AR debit lands on the
*customer* identified through the active tenancy. Payments are
recorded against customers and reduce their AR. "Who owes me?"
aggregates per customer across all of that customer's connections.

### Double-entry, single source of truth

Every financial fact lives as one or more balanced legs in
`journal.series`. The expenses and payments tables are operator-facing
input records; they post into the journal automatically. Customer
balances, AR aging, trial balance, P&L, and cycle totals are all
derived from the journal via DataFusion views.

### Integer IDs, named lookups

Every row in every table has a monotonically-increasing `Int32` id
assigned by the system (no operator-chosen slugs, no UUIDs). Reference
tables (connections, customers, cycles, billing_policies) also carry a
`name` Utf8 column that's unique-per-table. CLI flags accept either the
id or the name; ambiguous name lookups fail with a list of candidates.

Allocator strategy: `max(id)+1` read inside the same transaction. The
volumes here (low hundreds of customers/connections lifetime, ~30
expenses per six-month cycle) make any cleverer scheme unnecessary.
See [open question #6](#14-open-questions).

### Money in cents, USD only

All monetary fields are `Int64` cents. Display formatting and parsing
of dollar strings (`$1,234.56`) happens at the CLI boundary; the
storage layer never sees floats or strings for amounts. This matches
the existing Go program's `currency.Amount.units` convention — by
keeping the same int64 cent representation we get bit-identical
arithmetic.

### Books-only pond

The bookkeeping system lives in its own DuckPond at `$BOOKS` (separate
from the existing operations pond at `$POND`). The operator typically
sets `POND=$BOOKS` for a session. Backup is via the standard `remote`
factory.

## 3. Pond layout

```
$BOOKS/
  data/
    business.parquet                # entry type: table     -- one row, company identity
    chart_of_accounts.parquet       # table                 -- account codes
    billing_policies.parquet        # table                 -- named, versioned policies
    connections.parquet             # table                 -- physical service connections (stable)
    customers.parquet               # table                 -- paying parties (people, may move)
    tenancies.parquet               # table                 -- (connection, customer, start, end)
    cycles.parquet                  # table                 -- six-month billing periods; each pins one policy
    expenses.series                 # table:series          -- append-only line-item expenses
    payments.series                 # table:series          -- append-only customer payments
    journal.series                  # table:series          -- the financial journal (every leg)
    -- future, when meter reading begins:
    -- meter_readings.series        # table:series          -- (connection, read_date, gallons)
  system/
    etc/
      accounts.yaml                 # config for the `accounts` executable factory
      bill-allocator.yaml           # config for the `bill-allocator` dynamic factory
      view-balances.yaml            # sql-derived-table  -> /views/balances
      view-who-owes.yaml            # sql-derived-table  -> /views/who_owes
      view-trial-balance.yaml       # sql-derived-table  -> /views/trial_balance
      view-cycle-totals.yaml        # sql-derived-table  -> /views/cycle_totals
      view-active-tenancies.yaml    # sql-derived-table  -> /views/active_tenancies
      view-statements.yaml          # sql-derived-series -> /views/statements
      view-pnl.yaml                 # sql-derived-series -> /views/pnl
      view-ar-aging.yaml            # sql-derived-table  -> /views/ar_aging
      view-bill-preview.yaml        # sql-derived-series -> /views/bill_preview
```

Reference tables (`*.parquet`, entry type `table`) are rewritten in
place when edited — their full contents fit easily in memory and edits
are infrequent. Append-only series files (`*.series`, entry type
`table:series`) get a new version per write; corrections happen via
reversing entries, never in-place mutation.

`accounts` is filed under `/system/etc/` (not `/system/run/`) — it is
manually triggered, not auto-run after every commit.

## 4. Schemas

### `business.parquet` (one row)

| col | type | notes |
|---|---|---|
| name | Utf8 | company display name |
| address | Utf8 | mailing address (multi-line) |
| contact | Utf8 | free text (phone, email) |
| ein | Utf8 (nullable) | tax id |

### `chart_of_accounts.parquet`

| col | type | notes |
|---|---|---|
| code | Int32 (PK) | e.g. 1100 |
| name | Utf8 | e.g. "Accounts Receivable" |
| kind | Utf8 | one of `asset` `liability` `equity` `income` `expense` |
| normal_side | Utf8 | `debit` or `credit` (derived from kind, stored for clarity) |
| description | Utf8 (nullable) | |

Default seed populated by `accounts init`:

```
1000  Cash                       asset      debit
1100  Accounts Receivable        asset      debit
2000  Customer Deposits          liability  credit   (future use, e.g. overpayments)
3000  Owner's Equity             equity     credit
3500  Reserves (Margin)          equity     credit
4000  Service Revenue            income     credit
5100  Operations                 expense    debit
5200  Utilities                  expense    debit
5300  Insurance                  expense    debit
5400  Taxes                      expense    debit
5900  Other Expenses             expense    debit
9000  Suspense                   asset      debit    (parking lot for bad data)
```

### `billing_policies.parquet`

Reference table of named policies. Reusable across many cycles. Old
rows are never deleted (already-issued cycles still need them).

| col | type | notes |
|---|---|---|
| policy_id | Int32 (PK) | auto-assigned |
| name | Utf8 (unique) | operator-friendly label, e.g. `share-by-weight-2024` |
| kind | Utf8 | discriminator for a Rust strategy. Day 1: `share-by-weight` |
| config_yaml | Utf8 | YAML text whose shape depends on `kind`; validated at `policy add` time |
| effective_from | Date32 | first cycle this policy can be assigned to |
| effective_to | Date32 (nullable) | last cycle (null = ongoing) |
| description | Utf8 (nullable) | human-readable notes |

`policy edit` is restricted to non-effective fields (`description`,
`effective_to`). The `config_yaml` of a policy used by any issued
cycle is immutable — to change billing parameters, register a new
policy and assign it to the next cycle.

### `connections.parquet`

| col | type | notes |
|---|---|---|
| connection_id | Int32 (PK) | auto-assigned |
| name | Utf8 (unique) | short label (e.g. "Community Center", "100 Example St") |
| service_address | Utf8 | full physical address |
| first_active | Date32 | when service first started |
| last_active | Date32 (nullable) | when service was retired; null = still active |
| commercial | Boolean | real-world property of the meter; **input** to billing policies |
| weight_class | Utf8 (nullable) | optional finer classification used by future policies |
| notes | Utf8 (nullable) | |

### `customers.parquet`

| col | type | notes |
|---|---|---|
| customer_id | Int32 (PK) | auto-assigned |
| name | Utf8 (unique) | full name as printed on bills |
| billing_address | Utf8 | mailing address (free text; "E-mail" / "Hand delivery" are valid) |
| contact | Utf8 (nullable) | phone / email |
| active | Boolean | inactive customers may still have balances; default `true` |
| notes | Utf8 (nullable) | |

### `tenancies.parquet`

| col | type | notes |
|---|---|---|
| tenancy_id | Int32 (PK) | auto-assigned |
| connection_id | Int32 | FK to connections |
| customer_id | Int32 | FK to customers |
| start_date | Date32 | inclusive |
| end_date | Date32 (nullable) | exclusive; null = still current |
| notes | Utf8 (nullable) | |

`verify` invariants: at most one open tenancy per connection; no
overlapping tenancies for the same connection; every issued cycle has
exactly one responsible tenancy per active connection on `bill_date`.

### `cycles.parquet`

| col | type | notes |
|---|---|---|
| cycle_id | Int32 (PK) | auto-assigned |
| name | Utf8 (unique) | e.g. `2026H1` (Apr/Oct = H1/H2) |
| period_start | Date32 | must be Apr 1 or Oct 1 (constraint may relax under future policies) |
| period_end | Date32 | derived: start + 6 months − 1 day |
| bill_date | Date32 | when statements will be / were issued |
| policy_id | Int32 | FK to billing_policies; **immutable** once `issued=true` |
| inactive | List\<Int32\> | connection_ids excluded for this cycle (vacant, broken meter, etc.) |
| notes | Utf8 (nullable) | |
| issued | Boolean | true once bills have been written to the journal |

### `expenses.series` (table:series, time column = `paid_date`)

| col | type | notes |
|---|---|---|
| expense_id | Int32 (PK) | auto-assigned |
| paid_date | Timestamp(us, UTC) | from outside accounting program |
| account_code | Int32 | from chart_of_accounts (must be `expense` kind) |
| vendor | Utf8 (nullable) | |
| amount_cents | Int64 | always positive; reversals use `void_of` |
| memo | Utf8 (nullable) | |
| cycle_id | Int32 (nullable) | optional explicit assignment; otherwise inferred from paid_date |
| void_of | Int32 (nullable) | expense_id this entry voids |

### `payments.series` (table:series, time column = `received_date`)

| col | type | notes |
|---|---|---|
| payment_id | Int32 (PK) | auto-assigned |
| received_date | Timestamp(us, UTC) | |
| customer_id | Int32 | |
| amount_cents | Int64 | positive |
| method | Utf8 (nullable) | check / ach / cash / etc. |
| memo | Utf8 (nullable) | |
| void_of | Int32 (nullable) | payment_id this entry voids |

### `journal.series` (table:series, time column = `txn_date`)

The source of truth. Every domain command writes one balanced
transaction; legs sharing a `txn_id` must satisfy
`SUM(debit_cents) = SUM(credit_cents)`.

| col | type | notes |
|---|---|---|
| txn_id | Int32 | auto-assigned per transaction; groups legs together |
| txn_date | Timestamp(us, UTC) | effective date (== paid_date / received_date / bill_date) |
| recorded_at | Timestamp(us, UTC) | when the leg was recorded (audit trail) |
| leg_seq | Int32 | 0,1,…; primary key with txn_id |
| account_code | Int32 | from chart_of_accounts |
| customer_id | Int32 (nullable) | sub-ledger key for AR (account 1100) |
| connection_id | Int32 (nullable) | which connection produced the bill leg (audit / per-connection rollup) |
| cycle_id | Int32 (nullable) | links bill/expense legs to a cycle |
| debit_cents | Int64 | exactly one of debit/credit is non-zero |
| credit_cents | Int64 | |
| source | Utf8 | `bill` / `payment` / `expense` / `adjustment` / `opening` / `reversal` |
| source_ref | Utf8 (nullable) | e.g. `payment:42`, `expense:71`, `bill:cycle=12,connection=5`, `reversal-of:txn=87` |
| memo | Utf8 (nullable) | |

## 5. Standard journal entries

| Event | Legs |
|---|---|
| **Bill issued** (per connection per cycle) | `DR 1100 X` (customer_id resolved via tenancy on bill_date, also tagged with connection_id+cycle_id); `CR 4000 X` |
| **Payment received** | `DR 1000 X`; `CR 1100 X` (customer_id) |
| **Expense paid** | `DR 5xxx X`; `CR 1000 X` |
| **Reverse** (bill / payment / expense) | swap debit↔credit on every leg of the source txn; `source=reversal`, `source_ref=reversal-of:txn=<original>` |
| **Manual adjustment** | user-supplied legs; system enforces `Σdebit = Σcredit` and at least 2 legs |
| **Opening balance** | one-time per customer: `DR 1100 X` (or `CR` if a credit balance), `CR 3000 X` (or `DR`); `source=opening` |

The margin contribution to reserves is *not* automatically split out —
the cycle total credits Service Revenue (4000), and a P&L view
computes the reserves contribution as `SUM(4000 credits) − SUM(5xxx
debits)`. If the company ever wants margin to land in a separate
account at issue time, that's a one-line strategy change in
`share-by-weight`.

## 6. Billing policies

How a cycle's total cost gets divided among connections is encapsulated
in a **billing policy**. Two layers:

1. **`billing_policies.parquet`** stores the operator-visible config:
   `name`, `kind`, `config_yaml`, effective dates.
2. **A `BillingPolicy` Rust trait** with one implementation per
   `kind`. Strategies live under `crates/accounts/src/policy/`.
   Registry is built at start-up and discovered via `linkme` (matching
   the existing `register_dynamic_factory!` / `register_executable_factory!`
   pattern in duckpond).

### Trait sketch

```rust
// crates/accounts/src/policy/mod.rs
pub trait BillingPolicy: Send + Sync {
    /// Stable kind identifier (e.g. "share-by-weight").
    fn kind(&self) -> &'static str;

    /// Parse and validate YAML config. Called by `policy add` and at
    /// allocator dispatch time. Returns a typed config or a clear
    /// error pointing at the offending field.
    fn parse_config(&self, yaml: &str) -> anyhow::Result<Box<dyn PolicyConfig>>;

    /// Compute per-connection bill rows for the given cycle.
    /// Inputs are pre-loaded RecordBatches; the strategy is pure.
    fn allocate(
        &self,
        ctx: &AllocateContext,
        config: &dyn PolicyConfig,
    ) -> anyhow::Result<Vec<BillRow>>;
}

pub struct AllocateContext<'a> {
    pub cycle: &'a CycleRow,
    pub active_connections: &'a [ConnectionRow],   // already filtered by cycle.inactive and date
    pub responsible_customer_for: &'a dyn Fn(ConnectionId) -> Option<CustomerId>,
    pub cycle_cost_cents: i64,                     // pre-margin total from cost_view
    pub policy_inputs: &'a HashMap<String, String>,
}

pub struct BillRow {
    pub connection_id: i32,
    pub customer_id: Option<i32>,
    pub total_cents: i64,
    pub base_cents: i64,
    pub margin_cents: i64,
    pub weight: f64,
    pub share_fraction: f64,
}
```

### Day-1 policy kind: `share-by-weight`

The only policy kind shipped initially. Covers everything Caspar Water
has ever billed (the historical equal-share introductory cycles and
the current 1×/2× weight model).

**Required YAML schema** (validated at `policy add` time):

```yaml
margin: 0.2          # float; cycle total = expenses * (1 + margin)
weights:
  commercial: 2.0    # float; weight applied when connection.commercial = true
  residential: 1.0   # float; weight applied otherwise
```

**Optional fields** (omit to take defaults):

```yaml
denominator: 14      # int|float; the divisor for the share split.
                     #   Omit -> defaults to the sum of weights across the
                     #   cycle's active connections (auto-grows when a new
                     #   connection joins or one is marked inactive).
                     #   Provide explicitly to pin a baked-in number for
                     #   historical reasons (today's 14 is one such case).
rounding: deterministic-shuffle   # only choice today; matches the Go
                                  # program's seeded rounding behavior.
cost_view: cycle_totals           # the view that supplies pre-margin
                                  # cycle costs; default /views/cycle_totals.
```

The early "introductory" period is the same kind with `margin: 0.0`
and both weights set to `1.0` (denominator omitted, so the divisor
naturally equals the count of active connections).

**Algorithm** (matches Go `cmd/internal/billing/logic`):

1. `total = cycle_cost_cents * (1 + margin)`, integer-rounded.
2. `weight[i]` = `commercial_weight` if connection is commercial, else
   `residential_weight`.
3. `denominator` = configured value, or `Σ weight[i]` over active
   connections if absent.
4. `raw_share[i] = total * weight[i] / denominator`, integer-rounded.
5. **Deterministic-shuffle rounding**: distribute `total - Σraw_share`
   pennies across connections in an order seeded by the cycle close
   date (matches the Go `rand.New(rand.NewSource(cycle.PeriodStart.
   Closing().Date().UnixNano()))` — exact same seed and shuffle
   algorithm so v2 produces bit-identical bills to the Go program for
   the same inputs).
6. `base_cents[i] = raw_share[i] / (1 + margin)`,
   `margin_cents[i] = total[i] - base_cents[i]`.

### Reserved future kinds (not in day-1 scope)

`metered-flat-rate`, `tiered-metered`. These will register the same
way and consume a `meter_readings.series` input. No change to
`billing_policies` or `cycles` is required to add them — only new Rust
under `crates/accounts/src/policy/`. See [§15](#15-out-of-scope).

### Switching policies

Each cycle pins exactly one `policy_id`. Policy edits are forbidden
once any issued cycle references that policy. To change the math:
`policy add` a new policy, then `cycle add --policy=NEW` for the next
cycle. Already-issued cycles keep their original policy forever.

## 7. Factories

### `accounts` — executable factory

The single entry point for every operator command. Created by
`pond apply` from `system/etc/accounts.yaml`. Invoked as:

```
POND=$BOOKS pond run /accounts <subcommand> [flags]
```

Modeled after the existing `hydrovu` and `sitegen` executable
factories (see `duckpond/crates/hydrovu/src/factory.rs` and
`duckpond/crates/sitegen/src/factory.rs`):

- Implements `provider::registry::FactoryCommand` with `clap` `Parser`
  / `Subcommand` derives.
- All write subcommands declare
  `ExecutionMode::PondReadWriter`; pure read subcommands declare
  `ExecutionMode::PondReader`.
- Registered via `provider::register_executable_factory!`.
- Lives at `/system/etc/accounts` (manual trigger, not auto-run).

The factory's YAML config is empty for now (everything is in the
subcommand args). If needed later, the config can hold defaults like
"prompt-on-destructive" or output-format preferences.

### `bill-allocator` — dynamic factory

A policy-dispatching `table:dynamic` factory. Created by `pond apply`
from `system/etc/bill-allocator.yaml`. Used by `bills preview`,
`bills issue`, and the `view-bill-preview` view.

**Inputs** (passed from the YAML config):
- `cycles_path`, `billing_policies_path`, `connections_path`,
  `tenancies_path` (data tables it reads).
- `cost_view_default: /views/cycle_totals` (overridable per-policy
  via the `cost_view` config field).
- A required `cycle_id` (Int32) or cycle name selector at query time
  (the factory exposes a parameter; `bills preview` passes it).

**Output** — one row per (cycle_id, connection_id):

| col | type | notes |
|---|---|---|
| cycle_id | Int32 | |
| connection_id | Int32 | |
| customer_id | Int32 (nullable) | resolved from tenancy on `bill_date`; NULL → no covering tenancy |
| total_cents | Int64 | the headline number on the bill |
| base_cents | Int64 | cost share before margin |
| margin_cents | Int64 | margin contribution |
| weight | Float64 | this connection's weight under `share-by-weight` |
| share_fraction | Float64 | weight ÷ denominator |
| estimated | Boolean | true if `bill_date < period_end` (early estimate) |
| policy_kind | Utf8 | which strategy produced the row |

This typed schema fits the day-1 `share-by-weight` kind. When a second
kind ships, decide between widening the schema with new nullable
columns or adding a single `breakdown_yaml` Utf8 column ([open
question #7](#14-open-questions)).

The factory's responsibilities:

1. Look up the cycle row by id/name.
2. Look up the policy row by `cycle.policy_id`.
3. Find the matching strategy in the `BillingPolicyRegistry` by
   `policy.kind`.
4. Parse `policy.config_yaml` via the strategy's `parse_config`.
5. Build `AllocateContext` (active connections after filtering by
   `cycle.inactive` and date; tenancy resolver function; cycle cost
   from `cost_view`).
6. Invoke `strategy.allocate(...)` and return a `RecordBatch`.

### Re-used existing factories

- `sql-derived-table` for views with no time dimension (balances,
  trial_balance, ar_aging, who_owes, active_tenancies, cycle_totals).
- `sql-derived-series` for views with a per-cycle dimension that
  benefits from versioned semantics (statements, pnl, bill_preview).
- `dynamic-dir` if we want a `/views/` namespace directory rather than
  applying each view at its own path.
- `remote` for S3 backup of the entire books pond (see operator
  guide §8).

## 8. CLI surface

Every subcommand is invoked as `pond run /accounts <verb> [args]`.

### Bootstrap and reference data

| Subcommand | Purpose |
|---|---|
| `init` | seed chart_of_accounts and create empty tables (idempotent) |
| `business set --name=… --address=… --contact=… [--ein=…]` | set/update the company info row |

### Billing policies

| Subcommand | Purpose |
|---|---|
| `policy kinds` | list available strategy kinds, one-line each, with each kind's required YAML fields |
| `policy add --name=… --kind=… --config-path=PATH [--effective-from=YYYY-MM-DD] [--description=…]` | register a policy. `--config-path=-` reads from stdin. YAML validated against the kind's schema |
| `policy list [--kind=…] [--effective-as-of=YYYY-MM-DD]` | enumerate policies |
| `policy show ID|NAME` | full details including parsed config |
| `policy edit ID|NAME [--description=…] [--effective-to=YYYY-MM-DD]` | non-effective metadata only; `config_yaml` is immutable once any cycle has used it |

### Connections

| Subcommand | Purpose |
|---|---|
| `connection add --name=… --service=… --first-active=YYYY-MM-DD [--commercial] [--weight-class=…] [--notes=…]` | prints the new `connection_id` |
| `connection edit ID|NAME [--name=…] [--service=…] [--commercial=true|false] [--weight-class=…] [--notes=…]` | |
| `connection retire ID|NAME --as-of=YYYY-MM-DD` | sets `last_active`; refused if any open tenancy |
| `connection list [--active] [--commercial]` | |
| `connection show ID|NAME` | service address, status, all tenancies, all bills, current responsible customer |

### Customers

| Subcommand | Purpose |
|---|---|
| `customer add --name=… --billing=… [--contact=…] [--notes=…]` | prints the new `customer_id` |
| `customer edit ID|NAME [--name=…] [--billing=…] [--contact=…] [--active=true|false]` | |
| `customer merge --from=ID|NAME --into=ID|NAME` | re-points tenancies and journal AR legs from `from` to `into`, deactivates `from` |
| `customer list [--active] [--with-balance]` | `--with-balance` orders by AR balance desc and prints contact info |
| `customer show ID|NAME` | display info, all tenancies, AR balance, last 10 payments, oldest unpaid bill |
| `customer find QUERY` | fuzzy lookup by partial name |

### Tenancies

| Subcommand | Purpose |
|---|---|
| `tenancy start --connection=ID|NAME --customer=ID|NAME --start=YYYY-MM-DD [--notes=…]` | auto-closes any prior open tenancy on that connection at `start_date`; prints the new `tenancy_id` |
| `tenancy end --connection=ID|NAME --end=YYYY-MM-DD [--notes=…]` (or `--tenancy=ID`) | |
| `tenancy edit --tenancy=ID [--start=…] [--end=…] [--customer=ID|NAME] [--notes=…]` | |
| `tenancy list [--connection=…] [--customer=…] [--as-of=YYYY-MM-DD]` | defaults to `--as-of=today`; the operator's "who lives where" report |

### Cycles

| Subcommand | Purpose |
|---|---|
| `cycle add --name=… --start=YYYY-MM-DD --bill-date=YYYY-MM-DD --policy=ID|NAME [--inactive=ID,ID] [--notes=…]` | prints the new `cycle_id` |
| `cycle edit ID|NAME [--bill-date=…] [--inactive=…] [--notes=…]` | refused once `issued=true`; `policy_id` immutable on a saved cycle |
| `cycle list` / `cycle show ID|NAME` | |
| `cycle totals [--cycle=ID|NAME]` | per-cycle expense rollup (operations / utilities / insurance / taxes); omit `--cycle` to see all |

### Expenses

| Subcommand | Purpose |
|---|---|
| `expense add --date=YYYY-MM-DD --account=5xxx --amount=$… [--vendor=…] [--memo=…] [--cycle=ID|NAME]` | prints the new `expense_id` |
| `expense void --id=N [--memo=…]` | writes a reversing entry |
| `expense import host:///path/expenses.csv [--cycle=ID|NAME]` | bulk-import per-cycle line items; columns: date, account, vendor, amount, [memo], [cycle]; duplicates detected by date+amount+vendor |
| `expense list [--cycle=…] [--account=…] [--from=…] [--to=…]` | |

### Payments

| Subcommand | Purpose |
|---|---|
| `payment add --date=YYYY-MM-DD --customer=ID|NAME --amount=$… [--method=check] [--memo=…]` | prints the new `payment_id` |
| `payment void --id=N [--memo=…]` | |
| `payment list [--customer=…] [--cycle=…] [--from=…] [--to=…]` | |

### Bills

| Subcommand | Purpose |
|---|---|
| `bills preview --cycle=ID|NAME` | uses `bill-allocator`; no writes; warns if any active connection has no covering tenancy |
| `bills issue --cycle=ID|NAME` | writes bill legs to journal, sets `cycles.issued=true`; refuses if any connection lacks a covering tenancy |
| `bills reverse --cycle=ID|NAME [--reason=…]` | writes reversing legs, clears `issued` |
| `statement --connection=ID|NAME --cycle=ID|NAME` | structured per-connection statement data the PDF generator consumes |

### Adjustments and opening balances

| Subcommand | Purpose |
|---|---|
| `opening-balance --customer=ID|NAME --amount=$… [--date=YYYY-MM-DD] [--memo=…]` | one-time roll-forward; posts `DR 1100/customer; CR 3000` (use negative `--amount` for a credit balance). Refused once any non-`opening` activity exists for that customer |
| `adjust --date=YYYY-MM-DD --memo=… --leg=ACCT[:CUSTOMER_ID]:DEBIT_CENTS:CREDIT_CENTS [--leg=…]…` | hand-written transaction; ≥2 legs; sums must balance |

### Reports

| Subcommand | Purpose |
|---|---|
| `balance [--customer=ID|NAME] [--as-of=YYYY-MM-DD]` | one customer's AR balance, or everyone (including zeros) |
| `who-owes [--as-of=YYYY-MM-DD] [--min=$0]` | customers with positive balance, sorted desc, with contact info and oldest unpaid bill |
| `aging [--customer=ID|NAME] [--as-of=YYYY-MM-DD]` | AR aging report bucketed 0–30 / 31–60 / 61–90 / 90+ days from bill date |
| `trial-balance [--as-of=YYYY-MM-DD]` | sum(debit), sum(credit), net per account_code |
| `pnl [--cycle=…|--from=…--to=…]` | revenue (4000), expenses (5xxx), reserves contribution |
| `journal show [--cycle=…] [--customer=…] [--connection=…] [--account=…] [--from=…] [--to=…] [--limit=N]` | raw journal slice |

### Health

| Subcommand | Purpose |
|---|---|
| `verify` | invariant checks: every txn balances; AR per customer matches sub-ledger; expense rows sum to cycle totals; no orphan customer/connection ids; no overlapping tenancies; every issued cycle has a tenancy for every active connection on bill_date; every issued cycle's `policy_id` resolves and was within `effective_from`/`effective_to` at `bill_date` |

### CLI design principles

- **Operators never write SQL.** Every common report has a dedicated
  subcommand. The `/views/...` paths exist for the tech contact and
  the PDF generator only.
- **All ids are integers; names are interchangeable.** Anywhere an
  `ID|NAME` flag appears, either form works. Ambiguous names fail
  with a candidate list, never silently pick the wrong row.
- **Common report flags.** Every report subcommand (and every `list`
  subcommand) accepts `--format=table|csv|json`. Default is
  `table` for terminal, `csv` for piping to a file or spreadsheet.
- **Mutating commands print what they did.** `add` commands print the
  assigned id; `edit`/`void`/`issue`/`reverse` print a one-line
  summary including the journal `txn_id` they posted.
- **Refusal is loud.** Bad input doesn't get a default — the command
  exits non-zero with a message naming the field and the constraint.

## 9. Dynamic-factory views

All YAMLs live under `/system/etc/`, materialized at `/views/<name>`.
Each is `pond apply`-able and can be queried with
`pond cat /views/X --sql ...` (or via the friendly subcommand).

| Path | Factory | What it computes |
|---|---|---|
| `/views/cycle_totals` | `sql-derived-table` | per-cycle sum of expenses by category (operations / utilities / insurance / taxes); annual taxes/insurance amortized like Go's `expense.SplitAnnual` |
| `/views/trial_balance` | `sql-derived-table` | sum(debit), sum(credit), net per account_code; total at bottom should be 0 |
| `/views/balances` | `sql-derived-table` | per-customer current AR balance (sum of journal legs against 1100); zero balances included |
| `/views/who_owes` | `sql-derived-table` | balance > 0, sorted desc, with display_name, billing_address, contact, last_payment_date, oldest_unpaid_bill_date, current connections list |
| `/views/active_tenancies` | `sql-derived-table` | one row per currently-occupied connection: connection_id, customer_id, name, service_address, since_date, commercial |
| `/views/ar_aging` | `sql-derived-table` | unpaid amounts bucketed 0–30 / 31–60 / 61–90 / 90+ days from bill_date, per customer |
| `/views/statements` | `sql-derived-series` | one row per (cycle_id, connection_id) with prior balance, charge, payments since last cycle, total due, last-payment date — the rendering input for the PDF generator |
| `/views/pnl` | `sql-derived-series` | per-cycle revenue (4000), expense (5xxx), reserves contribution; computed from journal |
| `/views/bill_preview` | `sql-derived-series` | wraps `bill-allocator` output joined with customer/cycle metadata |

## 10. Crate layout and code organization

### New crate: `duckpond/crates/accounts/`

```
duckpond/crates/accounts/
  Cargo.toml
  src/
    lib.rs                 -- factory registration; re-exports the public API
    factory.rs             -- the `accounts` executable factory + clap parser
    schema.rs              -- Arrow schema definitions for every table
    money.rs               -- Cents (i64) wrapper, parse "$1,234.56", display
    ids.rs                 -- IdAllocator (max+1 within a transaction)
    chart.rs               -- chart_of_accounts seed and lookup
    business.rs            -- business set
    connection.rs          -- connection add/edit/retire/list/show
    customer.rs            -- customer add/edit/merge/list/show/find
    tenancy.rs             -- tenancy start/end/edit/list + tenancy resolver
    cycle.rs               -- cycle add/edit/list/show + cycle totals
    expense.rs             -- expense add/void/import/list
    payment.rs             -- payment add/void/list
    journal.rs             -- write_transaction(legs) + journal show
    opening.rs             -- opening-balance
    adjust.rs              -- adjust
    bills/
      mod.rs               -- bills preview/issue/reverse, statement
      allocator_factory.rs -- the `bill-allocator` dynamic factory
    policy/
      mod.rs               -- BillingPolicy trait, registry, AllocateContext
      share_by_weight.rs   -- the day-1 strategy
    reports/
      mod.rs               -- balance, who-owes, aging, trial-balance, pnl
    verify.rs              -- the verify subcommand
    formatting.rs          -- table/csv/json output helpers
  tests/
    golden/                -- captured fixtures + expected outputs
    e2e_share_by_weight.rs -- end-to-end against synthetic data
    e2e_lifecycle.rs       -- tenant move-in/out, balance follows customer
    parity_with_go.rs      -- bill-byte-identical to the Go program for known inputs
    verify_invariants.rs
```

`Cargo.toml` workspace member added to `duckpond/Cargo.toml`. The
crate is wired into the `pond` binary via the `provider::register_*`
linkme pattern (mirroring how `hydrovu` and `sitegen` are pulled in by
`use hydrovu as _;` in `crates/cmd/src/main.rs`).

### Why a new crate (and not extending `provider`)

- `provider` is already heavy; bookkeeping logic doesn't belong there.
- A standalone crate keeps the testing surface small and lets the
  parity-with-Go tests live next to the strategy code.
- Future deletion / reorg is easier when the boundary is explicit.

### Dependencies

- `arrow`, `parquet`, `datafusion` — standard, already in workspace.
- `tinyfs`, `tlogfs`, `provider`, `steward` — duckpond core.
- `clap` (with `derive`) — matches existing factory style.
- `serde`, `serde_yaml` — for policy config parsing.
- `chrono` — dates.
- `linkme` — strategy registration.
- `thiserror`, `anyhow` — error types (pattern matches existing code).
- `csv` — for `expense import`.
- `regex` — money parsing.

## 11. Implementation phases

Each phase is a self-contained PR-sized chunk that leaves the system
in a working state. Phases 1–6 are required before the first cycle can
be billed; phases 7–9 are operator-quality-of-life.

### Phase 1: Crate scaffold and schema (~1 day)

- Create `duckpond/crates/accounts/` with `Cargo.toml`, `lib.rs`,
  `factory.rs` (empty clap subcommand stubs that just print
  "unimplemented"), and `schema.rs` (every Arrow schema fully written
  out, with unit tests that round-trip a sample RecordBatch).
- `accounts.yaml` factory config (empty `spec`).
- Wire the crate into the workspace and into `crates/cmd/src/main.rs`.
- Verify: `pond apply system/etc/accounts.yaml` followed by
  `pond run /accounts --help` lists the subcommands.

### Phase 2: `init`, `business`, `customer`, `connection`, `tenancy` (~2 days)

- `IdAllocator` (read max(id)+1 inside the transaction).
- Reference-table CRUD subcommands: `business set`, `customer
  add/edit/list/show/find/merge`, `connection
  add/edit/retire/list/show`, `tenancy start/end/edit/list`.
- Default chart-of-accounts seed in `init`.
- Tenancy resolver function (used later by bill issuance):
  `responsible_customer(connection_id, on_date) -> Option<customer_id>`.
- Tests: round-trip CRUD for each table; tenancy auto-close on overlap;
  customer merge re-points tenancies; lookup by id-or-name with
  ambiguity error.

### Phase 3: `expense`, `payment`, journal writes (~2 days)

- Money parsing/formatting (`Cents` wrapper).
- Atomic `write_transaction(legs: &[JournalLeg])` helper that
  validates `Σdebit = Σcredit` and assigns one `txn_id`.
- `expense add/void/import/list` — adds an expense row and posts
  `DR 5xxx; CR 1000`. `void` posts the reverse.
- `payment add/void/list` — adds a payment row and posts
  `DR 1000; CR 1100/customer`.
- `opening-balance` — refused if any non-`opening` activity exists for
  the customer.
- `adjust` — parses `--leg=ACCT[:CUSTOMER]:DEBIT:CREDIT`, validates
  balance, posts.
- `journal show` with all the filter flags.
- Tests: every command produces exactly the expected legs; voids
  reverse cleanly; running totals against a known dataset.

### Phase 4: Cycles and `cycle_totals` view (~1.5 days)

- `cycle add/edit/list/show`.
- `view-cycle-totals.yaml` — `sql-derived-table` over expenses
  grouped by `cycle_id` and `account_code` (with the `SplitAnnual`
  rule for taxes/insurance — mirror the Go logic in SQL or in a small
  pre-aggregation step inside the strategy).
- `cycle totals [--cycle=…]` subcommand.
- Tests: tax/insurance amortization matches Go; cycle date
  constraints (Apr 1 / Oct 1).

### Phase 5: Billing policies + `share-by-weight` strategy (~2 days)

- `BillingPolicy` trait and `BillingPolicyRegistry` (linkme-backed).
- `share-by-weight` strategy: parse YAML, allocate.
- The deterministic-shuffle rounding ported byte-identically from Go:
  `rand.New(rand.NewSource(period_close_unix_nanos)).Shuffle(...)`.
  Use the `rand` crate's `StdRng` seeded with the same i64 *and*
  reproduce Go's Fisher-Yates exactly. **This deserves its own focused
  test** (see [§12](#12-testing-strategy)).
- `policy add/list/show/edit/kinds` subcommands. YAML validated by
  the matching strategy's `parse_config`.
- Tests: parse rejects unknown fields and bad types; allocate matches
  hand-computed totals for a small synthetic cycle.

### Phase 6: `bill-allocator` factory + `bills preview/issue/reverse` (~2 days)

- `bill-allocator` dynamic factory: looks up cycle → policy → strategy,
  builds `AllocateContext`, calls `allocate`, returns RecordBatch with
  the schema in [§7](#7-factories).
- `view-bill-preview.yaml` — joins allocator output with customer and
  cycle metadata.
- `bills preview --cycle=…` — runs the allocator, prints the result,
  warns about NULL `customer_id` rows.
- `bills issue --cycle=…` — refuses if any allocator row has a NULL
  `customer_id`; otherwise wraps everything in one journal transaction
  posting `DR 1100/customer; CR 4000` per row, then sets
  `cycles.issued = true`.
- `bills reverse --cycle=…` — for each leg with `cycle_id=N` and
  `source=bill`, post the swap; clear `issued`.
- `statement --connection=… --cycle=…` — single-connection statement
  data for the PDF generator.
- Tests: end-to-end "create cycle, issue bills, customer balance moves
  by the right amount, reverse, balance moves back". **Parity test**:
  feed the existing `users.csv` / `cycles.csv` shape into v2 (via the
  CLI subcommands) and confirm `bills preview` produces the exact
  same per-customer totals as the Go program.

### Phase 7: Reporting subcommands and views (~1.5 days)

- `view-balances`, `view-who-owes`, `view-trial-balance`,
  `view-active-tenancies`, `view-ar-aging`, `view-pnl`,
  `view-statements` — write the SQL.
- Friendly subcommands wrapping each: `balance`, `who-owes`, `aging`,
  `trial-balance`, `pnl`. Each accepts `--format=table|csv|json` and
  the standard filters from §8.
- `customer show` and `connection show` populated with their full
  joined picture.
- Tests: each view's SQL against a synthetic journal produces the
  expected rollups.

### Phase 8: `verify` (~1 day)

- All invariants from §8. Each check returns a `Vec<Violation>`;
  `verify` prints them grouped by category and exits non-zero if any
  are present.
- Tests: hand-craft each violation type and confirm `verify` flags it.

### Phase 9: Operator polish + documentation (~1 day)

- Pretty-print money in CLI output (cents → `$1,234.56`).
- Confirm-on-destructive prompts for `bills issue`, `bills reverse`,
  `customer merge`, `connection retire`. (A `--yes` flag bypasses.)
- Re-read the operator guide with the working CLI in hand and adjust
  any wording that doesn't match reality.
- Add a `pond apply system/etc/accounts/*.yaml` recipe under
  `cmd/billing/` (or the books-pond bootstrap script — see §13).

### After Phase 9 (separate work)

- PDF generator in the existing `cmd/billing/main.go` switches input
  source from CSV to `pond run /accounts statement` JSON output.
  Schema documented in §9 (`/views/statements`).
- Decommission CSV input path once a full cycle has been billed
  through v2 successfully.

## 12. Testing strategy

### Unit tests (per phase)

Per the phase list above. Each phase lands with its own `tests/` files
and passes `cargo test -p accounts` before merging.

### Parity test against the Go program

Critical for confidence. Captured as a single integration test:

1. `tests/golden/v1_inputs/` holds the four legacy CSVs at known
   contents (a synthetic dataset, not real customer data).
2. The test boots a books pond, runs the equivalent `accounts add`
   sequence to populate connections / customers / tenancies / cycles
   / expenses / payments matching the CSVs.
3. For every cycle, runs `bills preview` and compares per-customer
   totals against a captured snapshot from running the Go program on
   the same inputs.
4. Bill amounts must match **to the cent** — that's what the
   deterministic-shuffle rounding test guarantees.

This is the test that keeps v2 honest while the operator transitions
between systems. Once we cut over, the test continues to guard
against drift.

### Property tests

Two worth writing:

- `verify` is idempotent and returns `OK` after any sequence of valid
  CRUD commands (use the existing duckpond sandbox property-test
  harness pattern: see `duckpond/sandbox/tests/tests/properties.rs`).
- For any `share-by-weight` config and any active-connection set, the
  sum of per-connection bills equals the configured cycle total to
  the cent (no money lost to rounding).

### Manual smoke test before each release

Following the operator guide step by step against a fresh books pond
populated with synthetic data. Catches CLI-ergonomic regressions that
unit tests miss.

## 13. Bootstrap recipe (operator setup)

This is the script the tech contact runs once to stand up a books
pond. Operators should never see this — but it's documented here so
the next session can replay it.

```bash
# 1. Choose where the books live
export BOOKS=$HOME/caspar-books-pond
export POND=$BOOKS

# 2. Initialize the pond
pond init

# 3. Apply all the factory configs in one go
pond apply -f /path/to/system/etc/accounts.yaml
pond apply -f /path/to/system/etc/bill-allocator.yaml
pond apply -f /path/to/system/etc/view-balances.yaml
pond apply -f /path/to/system/etc/view-who-owes.yaml
pond apply -f /path/to/system/etc/view-trial-balance.yaml
pond apply -f /path/to/system/etc/view-cycle-totals.yaml
pond apply -f /path/to/system/etc/view-active-tenancies.yaml
pond apply -f /path/to/system/etc/view-statements.yaml
pond apply -f /path/to/system/etc/view-pnl.yaml
pond apply -f /path/to/system/etc/view-ar-aging.yaml
pond apply -f /path/to/system/etc/view-bill-preview.yaml

# 4. Initialize the data tables (chart of accounts seed, empty other tables)
pond run /accounts init

# 5. Set company info
pond run /accounts business set \
  --name='Caspar Water Company, LLC' \
  --address='200 Example Lane; Anytown, CA 99999' \
  --contact='p: 555-555-5555; e: ops@example.com'

# 6. Register the policies you'll use
pond run /accounts policy add \
  --name=share-by-weight-2024 \
  --kind=share-by-weight \
  --effective-from=2023-10-01 \
  --description='Standard 20% margin, commercial counts as 2, denominator 14' \
  --config-path=- <<'EOF'
margin: 0.2
denominator: 14
weights:
  commercial: 2.0
  residential: 1.0
EOF

# 7. (For the operator) Add connections, customers, tenancies one by
#    one with `connection add`, `customer add`, `tenancy start`. Each
#    `add` prints the assigned id; the operator records them.

# 8. Record any opening AR balances per customer with `opening-balance`.

# 9. Hand over to the operator. They follow new-guide.md from here.
```

The recipe is best captured as a small shell script under
`cmd/billing/bootstrap-books.sh` (or similar) that the tech contact
runs once. Steps 7 and 8 are interactive — they could be driven from
a one-time YAML manifest, but with only ~14 connections and ~14
customers it's not worth the plumbing.

## 14. Open questions

These need answers before / during implementation. Defaults are
proposed where reasonable.

1. **Tax/insurance amortization view.** Today the Go program splits
   the annual taxes/insurance line across the two cycles in a year
   (`SplitAnnual`). Option (a): bake the same rule into
   `view-cycle-totals` so any policy reading from it sees amortized
   numbers. Option (b): expose two views (`cycle_totals_amortized`,
   `cycle_totals_paid`) and let each policy choose. **Default: (a)**
   because today's only policy wants amortized; revisit when a second
   policy disagrees.

2. **Partial / overpayments.** When a customer pays an amount that
   doesn't match an outstanding bill exactly, do we (a) apply to
   oldest open bill (FIFO), (b) apply to newest, (c) leave the credit
   un-applied until an explicit `apply-payments` run? **Default: (c)**
   — payments hit AR as a credit on the customer; reports show the
   net balance. The operator can cut a refund or apply to next cycle
   manually. Simpler and matches today's mental model.

3. **Policy clone.** When the policy changes year-to-year by a single
   numeric value (margin, denominator), do we add a `policy clone
   --from=NAME --name=NEW [--set FIELD=VAL]` shortcut? **Default:
   no** for v1; if operators ask, add later.

4. **Where does `pond apply` create the `data/` tree?** Either (a)
   `pond apply` for `accounts.yaml` does it as a side-effect, (b)
   `pond run /accounts init` does it explicitly. **Default: (b)** —
   apply registers the factory, init seeds the data. Keeps the two
   stages separable.

5. **`accounts` crate placement.** New crate
   `duckpond/crates/accounts/` (matches `hydrovu` and `sitegen`
   pattern). **Confirmed.**

6. **Integer ID allocator.** (a) `max(id) + 1` inside the transaction
   (simple, fine for our volumes), (b) `id_counters.parquet`
   reference table. **Default: (a).**

7. **Bill output schema for future kinds.** When a non-`share-by-
   weight` kind ships, do we widen `bill-allocator` output with new
   nullable columns, or add `breakdown_yaml` Utf8? **Defer until
   there's a concrete second kind.**

8. **Confirm-on-destructive prompts.** Should `bills issue`, `bills
   reverse`, `customer merge`, `connection retire` prompt before
   acting (with `--yes` to bypass)? **Default: yes**, for the
   operator's sake. None of these are recoverable without doing
   another inverse command.

9. **Where do bootstrap YAMLs live in the source tree?** Probably
   `cmd/billing/bootstrap/` alongside the operator guide, since
   `cmd/billing` is the existing billing-program directory. Or
   `duckpond/crates/accounts/bootstrap/`. **Default: the former** so
   they live with the operator guide.

10. **Handling cycle inactives that weren't really inactive.** Today
    Caspar Water has a string list `Inactive` on `cycles.csv`. If the
    operator forgot to mark a connection inactive and bills get
    issued with it included, the recovery path is `bills reverse` →
    `cycle edit --inactive=…` → `bills issue`. Worth a paragraph in
    the operator guide §7 (already there as troubleshooting case 3).

## 15. Out of scope (next phase)

Tracked here so we don't lose them.

- **PDF rendering / mail-merge.** Stays in the existing Go program;
  switches input from CSV to `pond run /accounts statement` JSON.
- **Sitegen integration.** Publishing balance / aging summaries to
  a private operator page on the existing static site. Trivial once
  v2 is up — just a sitegen shortcode that calls the report
  subcommands.
- **Remote backup config.** Use the existing `remote` factory exactly
  like other ponds; not v2-specific.
- **Multi-currency, taxes-collected-on-revenue, depreciation.** Not
  needed for a small water utility.
- **Meter readings + usage-based policies.** The policy abstraction is
  in place. When meter reading actually starts:
  - Add `meter_readings.series` schema and an ingest CLI.
  - Implement `metered-flat-rate` and (later) `tiered-metered`
    strategies under `crates/accounts/src/policy/`.
  - Decide bill-allocator output schema widening
    ([open question #7](#14-open-questions)).
- **Email / SMS bill delivery, payment portal.** Not in the medium-
  term roadmap.
- **Multi-tenant / multi-business** (one books pond serving more than
  one company). Trivially blocked by the `business` table being one
  row; would require a `business_id` foreign key on every other
  table. Not anticipated.

## 16. Reference material

### Existing code to study before implementing

- **Go billing program (the model to replicate functionally):**
  - `cmd/billing/main.go`
  - `cmd/internal/billing/logic/logic.go` — the cycle billing
    algorithm; mirror for `share-by-weight`.
  - `cmd/internal/billing/expense/expense.go` — `SplitAnnual` rule
    for taxes/insurance amortization.
  - `cmd/internal/billing/account/account.go` — current account
    abstraction (will be replaced by proper double-entry).
  - `cmd/internal/billing/currency/currency.go` — money parsing/
    formatting; the new `accounts::money::Cents` mirrors this.

- **DuckPond factory examples to copy from:**
  - `duckpond/crates/hydrovu/src/factory.rs` — executable factory
    skeleton with clap, multi-mode subcommands, registry hookup.
  - `duckpond/crates/sitegen/src/factory.rs` — second executable
    factory; richer config parsing.
  - `duckpond/crates/provider/src/factory/temporal_reduce.rs`,
    `synthetic_timeseries.rs`, `timeseries_join.rs` — dynamic factory
    examples; `bill-allocator` follows the same pattern.
  - `duckpond/crates/cmd/src/commands/apply.rs` — how `pond apply`
    parses k8s-style YAML manifests; relevant for writing the
    bootstrap configs in §13.

- **DuckPond docs (read these):**
  - `duckpond/.github/copilot-instructions.md` — system mental model,
    transaction model, anti-patterns, presubmit checks.
  - `duckpond/docs/cli-reference.md` — factory types, URL schemes,
    `pond apply` / `pond run` / `pond cat` semantics.

- **DuckPond conventions to follow:**
  - ASCII-only in `.rs` files (use `[OK]`, `[ERR]` tags).
  - Pre-merge: `cargo fmt --all`, `cargo clippy --workspace
    --all-features -- -D warnings`, `cargo test --workspace`.
  - No `unwrap_used`, `print_stdout`, `print_stderr` (clippy-denied
    in workspace lints). Use `log::info!` etc.
  - All `#[derive(Deserialize)]` config structs need
    `#[serde(deny_unknown_fields)]`.
  - One `TransactionGuard` per CLI invocation; never call
    `OpLogPersistence::open()` in helpers — pass `&mut tx` instead.

### Inputs from the previous design conversation

The seven design rounds that produced this plan, summarized for
context (no need to re-litigate):

1. **Initial design** — agreed on double-entry, line-item expenses,
   single executable factory `accounts`, books-only pond.
2. **Connections vs customers vs tenancies** split out; `customer
   merge` for consolidating multi-house owners.
3. **Operator guide v1** written; PII scrubbed.
4. **No legacy CSV import**, manual close of old books, integer ids
   with name columns; added `opening-balance` and `customer find`.
5. **Pluggable billing policies** introduced as the abstraction;
   `cycles.parquet` shed `method`/`margin`/`effective_connections`
   and gained `policy_id`.
6. **YAML config (not JSON)** for policies; day-1 ships only
   `share-by-weight`; introductory equal-share is just that kind with
   different config.
7. **`denominator` made optional** with sum-of-weights default.
8. **Operator guide rewrite** — task-first, no setup; friendly report
   subcommands (`who-owes`, `aging`, `cycle totals`, `trial-balance`,
   `pnl`) instead of raw `pond cat /views/...` calls.

The operator guide at `cmd/billing/new-guide.md` reflects the
end-state; this implementation plan reflects everything the developer
needs to build it.
