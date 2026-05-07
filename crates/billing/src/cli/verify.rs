// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `accounts verify` -- run all invariant checks; exit non-zero on any
//! violation.
//!
//! Each check function takes a `&mut Vec<Violation>` and pushes findings.
//! All checks always run (no fail-fast), so the operator sees the full
//! picture in one pass.
//!
//! Invariants:
//!
//! 1. **TXN_BALANCE** — for each `txn_id`, `Σ debit_cents == Σ credit_cents`.
//! 2. **CUSTOMER_AR** — every journal AR (account 1100) leg references a
//!    customer that exists in `customers.parquet`.
//! 3. **TENANCY_REFS** — every tenancy references a connection and customer
//!    that exist.
//! 4. **TENANCY_NO_OVERLAP** — no two tenancies overlap on the same
//!    connection.
//! 5. **CYCLE_POLICY** — every cycle's `policy_id` resolves; for issued
//!    cycles, the policy was effective at `bill_date`.
//! 6. **CYCLE_ISSUED_TXN** — `cycles.bill_txn_id IS NOT NULL` iff
//!    `cycles.issued = true`; the txn must exist in the journal.
//! 7. **CYCLE_TENANCY_COVERAGE** — every issued cycle's active connections
//!    have a covering tenancy on `bill_date`.
//! 8. **BILL_BREAKDOWN_JOURNAL** — for each `bill_breakdowns` row, the
//!    journal has a matching `DR 1100/customer` leg with `debit_cents ==
//!    base_cents + margin_cents`.
//! 9. **BILL_DRIFT** — re-running the strategy against current immutable
//!    inputs reproduces every issued cycle's `bill_breakdowns` rows
//!    exactly. Any divergence indicates strategy code drift or unauthorized
//!    table edits.
//! 10. **CUSTOMER_MERGE** — `merged_into_customer_id` references a customer
//!     that exists, is active, and is not itself merged.
//! 11. **EXPENSE_CYCLE_TOTALS** — the materialized `cycle_totals` reflects
//!     the current expenses + amortization rules. (Stale materialization
//!     warning, not corruption.)

use crate::cli::{CliError, Result};
use crate::domain::{active_connections, amortization, tenancy_resolver};
use crate::money::Cents;
use crate::schema::{
    AMORTIZATION_RULES_PATH, AmortizationRuleRow, BILL_BREAKDOWNS_PATH, BILLING_POLICIES_PATH,
    BillBreakdownRow, BillingPolicyRow, CONNECTIONS_PATH, CUSTOMERS_PATH,
    CYCLE_TOTALS_MATERIALIZED_PATH, CYCLES_PATH, ConnectionRow, CustomerRow, CycleRow,
    CycleTotalRow, EXPENSES_PATH, ExpenseRow, JOURNAL_PATH, JournalLegRow, TENANCIES_PATH,
    TenancyRow,
};
use crate::store;
use chrono::NaiveDate;
use std::collections::{HashMap, HashSet};
use tinyfs::WD;

#[derive(Debug, Clone)]
pub struct Violation {
    pub category: &'static str,
    pub message: String,
}

pub async fn run(wd: &WD) -> Result<()> {
    let mut violations: Vec<Violation> = Vec::new();

    // Load all the data once.
    let customers: Vec<CustomerRow> = store::read_table(wd, CUSTOMERS_PATH).await?;
    let connections: Vec<ConnectionRow> = store::read_table(wd, CONNECTIONS_PATH).await?;
    let tenancies: Vec<TenancyRow> = store::read_table(wd, TENANCIES_PATH).await?;
    let cycles: Vec<CycleRow> = store::read_table(wd, CYCLES_PATH).await?;
    let policies: Vec<BillingPolicyRow> = store::read_table(wd, BILLING_POLICIES_PATH).await?;
    let breakdowns: Vec<BillBreakdownRow> = store::read_table(wd, BILL_BREAKDOWNS_PATH).await?;
    let journal: Vec<JournalLegRow> = store::read_series(wd, JOURNAL_PATH).await?;
    let expenses: Vec<ExpenseRow> = store::read_series(wd, EXPENSES_PATH).await?;
    let rules: Vec<AmortizationRuleRow> = store::read_table(wd, AMORTIZATION_RULES_PATH).await?;
    let materialized: Vec<CycleTotalRow> =
        store::read_table(wd, CYCLE_TOTALS_MATERIALIZED_PATH).await?;

    check_txn_balance(&journal, &mut violations);
    check_customer_ar(&journal, &customers, &mut violations);
    check_tenancy_refs(&tenancies, &customers, &connections, &mut violations);
    check_tenancy_no_overlap(&tenancies, &mut violations);
    check_cycle_policy(&cycles, &policies, &mut violations);
    check_cycle_issued_txn(&cycles, &journal, &mut violations);
    check_cycle_tenancy_coverage(&cycles, &connections, wd, &mut violations).await?;
    check_bill_breakdown_journal(&breakdowns, &journal, &cycles, &mut violations);
    // EXPENSE_CYCLE_TOTALS must run BEFORE BILL_DRIFT. If totals are stale,
    // the strategy will produce incorrect numbers, generating spurious
    // BILL_DRIFT noise that obscures the real (totals-stale) problem.
    let totals_stale =
        check_cycle_totals_fresh(&expenses, &cycles, &rules, &materialized, &mut violations);
    if !totals_stale {
        check_bill_drift(&breakdowns, &cycles, wd, &mut violations).await?;
    } else {
        log::info!(
            "[verify] skipping BILL_DRIFT check: cycle_totals_materialized is stale; \
             run `cycle totals-refresh` then re-run `verify`"
        );
    }
    check_customer_merge(&customers, &mut violations);

    print_summary(&violations);
    if !violations.is_empty() {
        return Err(CliError::Invalid(format!(
            "{} invariant violation(s); see log above",
            violations.len()
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Individual checks
// ---------------------------------------------------------------------------

fn check_txn_balance(journal: &[JournalLegRow], v: &mut Vec<Violation>) {
    let mut by_txn: HashMap<i32, (i64, i64)> = HashMap::new();
    for l in journal {
        let entry = by_txn.entry(l.txn_id).or_insert((0, 0));
        entry.0 += l.debit_cents;
        entry.1 += l.credit_cents;
    }
    for (txn_id, (dr, cr)) in by_txn {
        if dr != cr {
            v.push(Violation {
                category: "TXN_BALANCE",
                message: format!(
                    "txn #{txn_id}: sum(debit)={} != sum(credit)={} (drift={})",
                    Cents::from_units(dr).display(),
                    Cents::from_units(cr).display(),
                    Cents::from_units(dr - cr).display()
                ),
            });
        }
    }
}

fn check_customer_ar(journal: &[JournalLegRow], customers: &[CustomerRow], v: &mut Vec<Violation>) {
    let valid: HashSet<i32> = customers.iter().map(|c| c.customer_id).collect();
    for l in journal {
        if l.account_code == 1100
            && let Some(id) = l.customer_id
            && !valid.contains(&id)
        {
            v.push(Violation {
                category: "CUSTOMER_AR",
                message: format!(
                    "txn #{}, leg {} references customer #{id} which does not exist",
                    l.txn_id, l.leg_seq
                ),
            });
        }
    }
}

fn check_tenancy_refs(
    tenancies: &[TenancyRow],
    customers: &[CustomerRow],
    connections: &[ConnectionRow],
    v: &mut Vec<Violation>,
) {
    let cust_ids: HashSet<i32> = customers.iter().map(|c| c.customer_id).collect();
    let conn_ids: HashSet<i32> = connections.iter().map(|c| c.connection_id).collect();
    for t in tenancies {
        if !cust_ids.contains(&t.customer_id) {
            v.push(Violation {
                category: "TENANCY_REFS",
                message: format!(
                    "tenancy #{} references customer #{} which does not exist",
                    t.tenancy_id, t.customer_id
                ),
            });
        }
        if !conn_ids.contains(&t.connection_id) {
            v.push(Violation {
                category: "TENANCY_REFS",
                message: format!(
                    "tenancy #{} references connection #{} which does not exist",
                    t.tenancy_id, t.connection_id
                ),
            });
        }
    }
}

fn check_tenancy_no_overlap(tenancies: &[TenancyRow], v: &mut Vec<Violation>) {
    // Group by connection_id, then check pairwise overlap within each group.
    let mut by_conn: HashMap<i32, Vec<&TenancyRow>> = HashMap::new();
    for t in tenancies {
        by_conn.entry(t.connection_id).or_default().push(t);
    }
    for (conn_id, mut ts) in by_conn {
        ts.sort_by_key(|t| t.start_date);
        for i in 0..ts.len() {
            for j in (i + 1)..ts.len() {
                let a = ts[i];
                let b = ts[j];
                let a_end = a.end_date.unwrap_or(NaiveDate::MAX);
                let b_end = b.end_date.unwrap_or(NaiveDate::MAX);
                if a.start_date.max(b.start_date) <= a_end.min(b_end) {
                    v.push(Violation {
                        category: "TENANCY_NO_OVERLAP",
                        message: format!(
                            "connection #{conn_id}: tenancy #{} ({} - {}) overlaps tenancy #{} ({} - {})",
                            a.tenancy_id,
                            a.start_date,
                            a.end_date
                                .map(|d| d.to_string())
                                .unwrap_or_else(|| "open".into()),
                            b.tenancy_id,
                            b.start_date,
                            b.end_date
                                .map(|d| d.to_string())
                                .unwrap_or_else(|| "open".into()),
                        ),
                    });
                }
            }
        }
    }
}

fn check_cycle_policy(cycles: &[CycleRow], policies: &[BillingPolicyRow], v: &mut Vec<Violation>) {
    let by_id: HashMap<i32, &BillingPolicyRow> =
        policies.iter().map(|p| (p.policy_id, p)).collect();
    for c in cycles {
        let p = match by_id.get(&c.policy_id) {
            Some(p) => *p,
            None => {
                v.push(Violation {
                    category: "CYCLE_POLICY",
                    message: format!(
                        "cycle #{} `{}` references policy #{} which does not exist",
                        c.cycle_id, c.name, c.policy_id
                    ),
                });
                continue;
            }
        };
        if c.issued {
            if c.bill_date < p.effective_from {
                v.push(Violation {
                    category: "CYCLE_POLICY",
                    message: format!(
                        "cycle `{}` issued on {} but policy `{}` not effective until {}",
                        c.name, c.bill_date, p.name, p.effective_from
                    ),
                });
            }
            if let Some(to) = p.effective_to
                && c.bill_date > to
            {
                v.push(Violation {
                    category: "CYCLE_POLICY",
                    message: format!(
                        "cycle `{}` issued on {} but policy `{}` expired on {}",
                        c.name, c.bill_date, p.name, to
                    ),
                });
            }
        }
    }
}

fn check_cycle_issued_txn(cycles: &[CycleRow], journal: &[JournalLegRow], v: &mut Vec<Violation>) {
    let txns: HashSet<i32> = journal.iter().map(|l| l.txn_id).collect();
    for c in cycles {
        match (c.issued, c.bill_txn_id) {
            (true, None) => v.push(Violation {
                category: "CYCLE_ISSUED_TXN",
                message: format!("cycle `{}` issued=true but bill_txn_id is null", c.name),
            }),
            (false, Some(t)) => v.push(Violation {
                category: "CYCLE_ISSUED_TXN",
                message: format!("cycle `{}` issued=false but bill_txn_id={t}", c.name),
            }),
            (true, Some(t)) => {
                if !txns.contains(&t) {
                    v.push(Violation {
                        category: "CYCLE_ISSUED_TXN",
                        message: format!(
                            "cycle `{}` references bill_txn_id={t} but no journal legs match",
                            c.name
                        ),
                    });
                }
            }
            (false, None) => {} // OK
        }
    }
}

async fn check_cycle_tenancy_coverage(
    cycles: &[CycleRow],
    connections: &[ConnectionRow],
    wd: &WD,
    v: &mut Vec<Violation>,
) -> Result<()> {
    for c in cycles.iter().filter(|c| c.issued) {
        let active = active_connections::active_for_cycle(connections, c);
        let resp = tenancy_resolver::responsible_customer_map(wd, c.bill_date).await?;
        for conn in active {
            if !resp.contains_key(&conn.connection_id) {
                v.push(Violation {
                    category: "CYCLE_TENANCY_COVERAGE",
                    message: format!(
                        "issued cycle `{}` (bill_date={}): connection #{} `{}` is active but \
                         has no covering tenancy",
                        c.name, c.bill_date, conn.connection_id, conn.name
                    ),
                });
            }
        }
    }
    Ok(())
}

fn check_bill_breakdown_journal(
    breakdowns: &[BillBreakdownRow],
    journal: &[JournalLegRow],
    cycles: &[CycleRow],
    v: &mut Vec<Violation>,
) {
    // For each breakdown row, find the matching DR 1100 leg by
    // (txn_id from cycle.bill_txn_id, connection_id). Verify
    // debit_cents == base_cents + margin_cents.
    let cycle_txn: HashMap<i32, Option<i32>> =
        cycles.iter().map(|c| (c.cycle_id, c.bill_txn_id)).collect();
    for b in breakdowns {
        let txn_id = match cycle_txn.get(&b.cycle_id).copied().flatten() {
            Some(t) => t,
            None => {
                v.push(Violation {
                    category: "BILL_BREAKDOWN_JOURNAL",
                    message: format!(
                        "breakdown for cycle #{}, connection #{}: cycle has no bill_txn_id",
                        b.cycle_id, b.connection_id
                    ),
                });
                continue;
            }
        };
        let leg = journal.iter().find(|l| {
            l.txn_id == txn_id
                && l.connection_id == Some(b.connection_id)
                && l.account_code == 1100
                && l.debit_cents > 0
        });
        match leg {
            Some(l) => {
                let expected = b.base_cents + b.margin_cents;
                if l.debit_cents != expected {
                    v.push(Violation {
                        category: "BILL_BREAKDOWN_JOURNAL",
                        message: format!(
                            "cycle #{}, connection #{}: journal DR 1100 = {} but \
                             breakdown.base_cents + breakdown.margin_cents = {}",
                            b.cycle_id,
                            b.connection_id,
                            Cents::from_units(l.debit_cents).display(),
                            Cents::from_units(expected).display()
                        ),
                    });
                }
            }
            None => v.push(Violation {
                category: "BILL_BREAKDOWN_JOURNAL",
                message: format!(
                    "cycle #{}, connection #{}: no DR 1100 leg in journal txn #{txn_id}",
                    b.cycle_id, b.connection_id
                ),
            }),
        }
    }
}

async fn check_bill_drift(
    breakdowns: &[BillBreakdownRow],
    cycles: &[CycleRow],
    wd: &WD,
    v: &mut Vec<Violation>,
) -> Result<()> {
    // For each issued cycle, re-run the strategy and compare to the
    // recorded breakdown rows.
    for c in cycles.iter().filter(|c| c.issued) {
        let computed = match crate::cli::bills::compute(wd, c).await {
            Ok(r) => r,
            Err(e) => {
                v.push(Violation {
                    category: "BILL_DRIFT",
                    message: format!("cycle `{}`: failed to recompute strategy: {e}", c.name),
                });
                continue;
            }
        };
        let recorded: Vec<&BillBreakdownRow> = breakdowns
            .iter()
            .filter(|b| b.cycle_id == c.cycle_id)
            .collect();
        // Match by connection_id.
        for cr in &computed {
            let rec = recorded
                .iter()
                .find(|b| b.connection_id == cr.connection_id);
            match rec {
                Some(r) => {
                    let drift_total = cr.total_cents != (r.base_cents + r.margin_cents);
                    let drift_base = cr.base_cents != r.base_cents;
                    let drift_margin = cr.margin_cents != r.margin_cents;
                    if drift_total || drift_base || drift_margin {
                        v.push(Violation {
                            category: "BILL_DRIFT",
                            message: format!(
                                "cycle `{}`, connection #{}: strategy now produces \
                                 base={}, margin={} (sum {}) but breakdown stored \
                                 base={}, margin={} (sum {})",
                                c.name,
                                cr.connection_id,
                                Cents::from_units(cr.base_cents).display(),
                                Cents::from_units(cr.margin_cents).display(),
                                Cents::from_units(cr.base_cents + cr.margin_cents).display(),
                                Cents::from_units(r.base_cents).display(),
                                Cents::from_units(r.margin_cents).display(),
                                Cents::from_units(r.base_cents + r.margin_cents).display()
                            ),
                        });
                    }
                }
                None => v.push(Violation {
                    category: "BILL_DRIFT",
                    message: format!(
                        "cycle `{}`, connection #{}: strategy produces a row for this \
                         connection but bill_breakdowns has none",
                        c.name, cr.connection_id
                    ),
                }),
            }
        }
        // And the reverse: any breakdown row with no matching computed row.
        for r in &recorded {
            if !computed
                .iter()
                .any(|cr| cr.connection_id == r.connection_id)
            {
                v.push(Violation {
                    category: "BILL_DRIFT",
                    message: format!(
                        "cycle `{}`, connection #{}: bill_breakdowns has a row but \
                         strategy now produces no row for it",
                        c.name, r.connection_id
                    ),
                });
            }
        }
    }
    Ok(())
}

fn check_customer_merge(customers: &[CustomerRow], v: &mut Vec<Violation>) {
    let by_id: HashMap<i32, &CustomerRow> = customers.iter().map(|c| (c.customer_id, c)).collect();
    for c in customers {
        if let Some(into) = c.merged_into_customer_id {
            match by_id.get(&into) {
                None => v.push(Violation {
                    category: "CUSTOMER_MERGE",
                    message: format!(
                        "customer #{} merged_into=#{into} which does not exist",
                        c.customer_id
                    ),
                }),
                Some(target) => {
                    if target.merged_into_customer_id.is_some() {
                        v.push(Violation {
                            category: "CUSTOMER_MERGE",
                            message: format!(
                                "customer #{} merged_into=#{into} which is itself merged \
                                 (chain merging not supported)",
                                c.customer_id
                            ),
                        });
                    }
                    if !target.active {
                        v.push(Violation {
                            category: "CUSTOMER_MERGE",
                            message: format!(
                                "customer #{} merged_into=#{into} which is inactive",
                                c.customer_id
                            ),
                        });
                    }
                }
            }
        }
    }
}

/// Returns `true` if the materialized totals are stale relative to a
/// fresh recompute. Pushes a `EXPENSE_CYCLE_TOTALS` violation per
/// mismatching row.
fn check_cycle_totals_fresh(
    expenses: &[ExpenseRow],
    cycles: &[CycleRow],
    rules: &[AmortizationRuleRow],
    materialized: &[CycleTotalRow],
    v: &mut Vec<Violation>,
) -> bool {
    let (computed, _unassigned) = amortization::compute_totals(expenses, cycles, rules);
    let mat_sorted: Vec<&CycleTotalRow> = {
        let mut x: Vec<&CycleTotalRow> = materialized.iter().collect();
        x.sort_by_key(|r| (r.cycle_id, r.account_code));
        x
    };
    if computed.len() != mat_sorted.len() {
        v.push(Violation {
            category: "EXPENSE_CYCLE_TOTALS",
            message: format!(
                "cycle_totals_materialized has {} row(s) but recomputation produces {} row(s); \
                 run `cycle totals-refresh`",
                mat_sorted.len(),
                computed.len()
            ),
        });
        return true;
    }
    let mut stale = false;
    for (c, m) in computed.iter().zip(mat_sorted.iter()) {
        if c.cycle_id != m.cycle_id
            || c.account_code != m.account_code
            || c.amount_cents != m.amount_cents
        {
            v.push(Violation {
                category: "EXPENSE_CYCLE_TOTALS",
                message: format!(
                    "cycle_totals stale: row (cycle={}, account={}) computed={}, stored={}; \
                     run `cycle totals-refresh`",
                    c.cycle_id,
                    c.account_code,
                    Cents::from_units(c.amount_cents).display(),
                    Cents::from_units(m.amount_cents).display()
                ),
            });
            stale = true;
        }
    }
    stale
}

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------

fn print_summary(violations: &[Violation]) {
    if violations.is_empty() {
        log::info!("[OK] verify: all invariants hold");
        return;
    }
    log::info!("[ERR] verify: {} violation(s) found", violations.len());
    let mut by_cat: HashMap<&'static str, Vec<&Violation>> = HashMap::new();
    for v in violations {
        by_cat.entry(v.category).or_default().push(v);
    }
    let mut cats: Vec<&&'static str> = by_cat.keys().collect();
    cats.sort();
    for cat in cats {
        log::info!("  {}:", cat);
        for v in &by_cat[*cat] {
            log::info!("    - {}", v.message);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chart::seed as chart_seed;
    use crate::cli::{
        bills, connection as conn_cli, customer as cust_cli, cycle as cycle_cli, expense, payment,
        policy as policy_cli, tenancy,
    };

    /// Same fixture as bills::tests::fixture but exposes more state for
    /// verify scenarios.
    async fn fixture() -> (WD, /* cycle id */ i32) {
        let fs = tinyfs::memory::new_fs().await;
        let wd = fs.root().await.unwrap();
        let _ = wd.create_dir_all("/data").await.unwrap();
        chart_seed(&wd).await.unwrap();
        policy_cli::seed_default_amortization_rules(&wd)
            .await
            .unwrap();

        let alice = cust_cli::add(&wd, "Alice".into(), "addr1".into(), None, None)
            .await
            .unwrap();
        let bob = cust_cli::add(&wd, "Bob".into(), "addr2".into(), None, None)
            .await
            .unwrap();
        let c1 = conn_cli::add(
            &wd,
            "CommCtr".into(),
            "100 Main".into(),
            "2010-01-01",
            true,
            None,
            None,
        )
        .await
        .unwrap();
        let c2 = conn_cli::add(
            &wd,
            "200 Main".into(),
            "200 Main".into(),
            "2010-01-01",
            false,
            None,
            None,
        )
        .await
        .unwrap();

        let _ = tenancy::start(&wd, &c1.to_string(), &alice.to_string(), "2020-01-01", None)
            .await
            .unwrap();
        let _ = tenancy::start(&wd, &c2.to_string(), &bob.to_string(), "2020-01-01", None)
            .await
            .unwrap();

        let yaml = "margin: 0.2\nweights:\n  commercial: 2.0\n  residential: 1.0\n";
        let _ = policy_cli::add(
            &wd,
            "p1".into(),
            "share-by-weight".into(),
            yaml.into(),
            None,
            None,
        )
        .await
        .unwrap();
        let cycle_id = cycle_cli::add(
            &wd,
            "2024H1".into(),
            "2024-04-01",
            "2024-10-01",
            "p1",
            None,
            None,
            false,
        )
        .await
        .unwrap();
        let _ = expense::add(
            &wd,
            "2024-05-01",
            5100,
            "$200.00",
            None,
            None,
            Some(&cycle_id.to_string()),
        )
        .await
        .unwrap();
        let _ = expense::add(
            &wd,
            "2024-06-01",
            5200,
            "$100.00",
            None,
            None,
            Some(&cycle_id.to_string()),
        )
        .await
        .unwrap();
        cycle_cli::refresh(&wd).await.unwrap();
        (wd, cycle_id)
    }

    #[tokio::test]
    async fn ok_on_clean_pond() {
        let (wd, cycle_id) = fixture().await;
        let _ = bills::issue(&wd, &cycle_id.to_string()).await.unwrap();
        run(&wd).await.expect("verify clean");
    }

    #[tokio::test]
    async fn ok_after_full_lifecycle() {
        let (wd, cycle_id) = fixture().await;
        let _ = bills::issue(&wd, &cycle_id.to_string()).await.unwrap();
        let _ = payment::add(&wd, "2024-10-15", "Alice", "$100.00", "check".into(), None)
            .await
            .unwrap();
        let _ = bills::reverse(&wd, &cycle_id.to_string(), None)
            .await
            .unwrap();
        let _ = bills::issue(&wd, &cycle_id.to_string()).await.unwrap();
        run(&wd).await.expect("verify ok after reverse + reissue");
    }

    /// Detection: corrupt the journal so that one txn's debits don't sum
    /// to its credits.
    #[tokio::test]
    async fn detects_unbalanced_txn() {
        let (wd, cycle_id) = fixture().await;
        let _ = bills::issue(&wd, &cycle_id.to_string()).await.unwrap();
        // Corrupt: change one leg's debit_cents directly in the series.
        let mut legs: Vec<JournalLegRow> = store::read_series(&wd, JOURNAL_PATH).await.unwrap();
        legs[0].debit_cents += 1;
        store::write_series(&wd, JOURNAL_PATH, &legs, JournalLegRow::TIMESTAMP_COLUMN)
            .await
            .unwrap();
        let err = run(&wd).await.unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
        let msg = format!("{err}");
        assert!(msg.contains("violation"), "got: {msg}");
    }

    #[tokio::test]
    async fn detects_cycle_issued_without_txn_id() {
        let (wd, cycle_id) = fixture().await;
        let _ = bills::issue(&wd, &cycle_id.to_string()).await.unwrap();
        // Corrupt: clear bill_txn_id but leave issued=true.
        let mut cycles: Vec<CycleRow> = store::read_table(&wd, CYCLES_PATH).await.unwrap();
        cycles[0].bill_txn_id = None;
        store::write_table(&wd, CYCLES_PATH, &cycles).await.unwrap();
        let err = run(&wd).await.unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn detects_orphan_customer_in_journal() {
        let (wd, _cycle_id) = fixture().await;
        // Post a leg to the journal referring to customer #999 (doesn't exist).
        let _ = crate::cli::journal::write_transaction(
            &wd,
            NaiveDate::from_ymd_opt(2024, 10, 1).unwrap(),
            vec![
                crate::cli::journal::LegSpec::debit(1100, Cents::from_units(100), "adjustment")
                    .with_customer(999),
                crate::cli::journal::LegSpec::credit(4000, Cents::from_units(100), "adjustment"),
            ],
        )
        .await
        .unwrap();
        let err = run(&wd).await.unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn detects_overlapping_tenancies() {
        let (wd, _cycle_id) = fixture().await;
        let mut tenancies: Vec<TenancyRow> = store::read_table(&wd, TENANCIES_PATH).await.unwrap();
        // Add a second open tenancy on connection 1 starting same day.
        let id = store::next_id(&tenancies, |t| t.tenancy_id);
        tenancies.push(TenancyRow {
            tenancy_id: id,
            connection_id: 1,
            customer_id: 2,
            start_date: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
            end_date: None,
            notes: None,
        });
        store::write_table(&wd, TENANCIES_PATH, &tenancies)
            .await
            .unwrap();
        let err = run(&wd).await.unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn detects_breakdown_journal_mismatch() {
        let (wd, cycle_id) = fixture().await;
        let _ = bills::issue(&wd, &cycle_id.to_string()).await.unwrap();
        // Corrupt one breakdown row's base_cents.
        let mut breakdowns: Vec<BillBreakdownRow> =
            store::read_table(&wd, BILL_BREAKDOWNS_PATH).await.unwrap();
        breakdowns[0].base_cents += 1;
        store::write_table(&wd, BILL_BREAKDOWNS_PATH, &breakdowns)
            .await
            .unwrap();
        let err = run(&wd).await.unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn detects_stale_cycle_totals() {
        let (wd, _cycle_id) = fixture().await;
        // Add a new expense AFTER cycle totals were materialized.
        let _ = expense::add(&wd, "2024-07-01", 5100, "$50.00", None, None, Some("1"))
            .await
            .unwrap();
        let err = run(&wd).await.unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("violation"), "got: {msg}");
    }

    /// Sanity: after corrupting AND fixing, verify is clean again.
    #[tokio::test]
    async fn corruption_then_repair_returns_to_ok() {
        let (wd, _cycle_id) = fixture().await;
        // Add expense then fail verify (stale totals).
        let _ = expense::add(&wd, "2024-07-01", 5100, "$50.00", None, None, Some("1"))
            .await
            .unwrap();
        assert!(run(&wd).await.is_err());
        // Refresh totals -> verify clean.
        cycle_cli::refresh(&wd).await.unwrap();
        run(&wd).await.expect("clean after refresh");
    }

    /// "Property test": run a representative sequence of valid commands
    /// and verify after EACH step. Smaller than full proptest harness but
    /// exercises the lifecycle.
    #[tokio::test]
    async fn lifecycle_stress() {
        let (wd, cycle_id) = fixture().await;
        run(&wd).await.expect("clean before issue");
        let _ = bills::issue(&wd, &cycle_id.to_string()).await.unwrap();
        run(&wd).await.expect("clean after issue");
        let _ = payment::add(&wd, "2024-10-15", "Alice", "$50.00", "check".into(), None)
            .await
            .unwrap();
        run(&wd).await.expect("clean after partial payment");
        let _ = payment::add(&wd, "2024-10-20", "Bob", "$120.00", "ach".into(), None)
            .await
            .unwrap();
        run(&wd).await.expect("clean after full payment");
        // payment void
        let _ = payment::void(&wd, 1, None).await.unwrap();
        run(&wd).await.expect("clean after void");
        // bills reverse
        let _ = bills::reverse(&wd, &cycle_id.to_string(), None)
            .await
            .unwrap();
        run(&wd).await.expect("clean after reverse");
        // re-issue
        let _ = bills::issue(&wd, &cycle_id.to_string()).await.unwrap();
        run(&wd).await.expect("clean after re-issue");
    }
}
