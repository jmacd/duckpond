// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `accounts bills preview/issue/reverse`.
//!
//! ## `bills preview --cycle=...`
//!
//! Computes the AllocateContext for the cycle, dispatches to the policy
//! strategy, prints the resulting BillRow set. No writes. Warns on rows
//! with `customer_id = None`.
//!
//! ## `bills issue --cycle=...`
//!
//! Refuses if:
//! - `cycle.issued = true` (operator must `bills reverse` first)
//! - any active connection has no covering tenancy on `bill_date`
//! - the policy strategy errors
//!
//! Then writes (in a single `pond run` transaction):
//! - one journal transaction with 2N legs (DR 1100/customer + CR 4000 per
//!   row; all sharing one txn_id)
//! - N rows in `bill_breakdowns.parquet` (the strategy's per-row reasoning)
//! - cycle update: `issued = true`, `bill_txn_id = <new txn>`
//!
//! ## `bills reverse --cycle=...`
//!
//! Refuses if `cycle.issued = false` or `bill_txn_id is None`. Then:
//! - writes a journal reversal transaction (2N swapped legs sharing one
//!   txn_id; `source = 'reversal'`, `source_ref = 'reversal-of:txn=N'`)
//! - removes the cycle's rows from `bill_breakdowns.parquet`
//! - clears `cycle.bill_txn_id` and sets `issued = false`

use crate::cli::journal::{LegSpec, write_transaction};
use crate::cli::{CliError, Result};
use crate::domain::{active_connections, cycle_totals, tenancy_resolver};
use crate::lookup::find_one;
use crate::money::Cents;
use crate::policy::{AllocateContext, BillRow};
use crate::schema::{
    BILL_BREAKDOWNS_PATH, BILLING_POLICIES_PATH, BillBreakdownRow, BillingPolicyRow,
    CONNECTIONS_PATH, CYCLES_PATH, ConnectionRow, CycleRow, JOURNAL_PATH, JournalLegRow,
};
use crate::store;
use tinyfs::WD;

const AR_ACCOUNT: i32 = 1100;
const REVENUE_ACCOUNT: i32 = 4000;

const KIND: &str = "cycle";

// ---------------------------------------------------------------------------
// preview
// ---------------------------------------------------------------------------

pub async fn preview(wd: &WD, cycle_query: &str) -> Result<()> {
    let cycles: Vec<CycleRow> = store::read_table(wd, CYCLES_PATH).await?;
    let (_, cycle) = find_one(KIND, cycle_query, &cycles, |c| c.cycle_id, |c| &c.name)?;
    if cycle.issued {
        return Err(CliError::Invalid(format!(
            "cycle `{}` is already issued (bill_txn_id={:?}); use `bills reverse` first",
            cycle.name, cycle.bill_txn_id
        )));
    }
    let rows = compute(wd, cycle).await?;
    print_rows(&rows);
    let no_customer: Vec<_> = rows.iter().filter(|r| r.customer_id.is_none()).collect();
    if !no_customer.is_empty() {
        log::warn!(
            "{} active connection(s) have NO covering tenancy on bill_date {}; \
             `bills issue` will refuse:",
            no_customer.len(),
            cycle.bill_date
        );
        for r in no_customer {
            log::warn!("  connection #{}", r.connection_id);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// issue
// ---------------------------------------------------------------------------

pub async fn issue(wd: &WD, cycle_query: &str) -> Result<i32> {
    let cycles: Vec<CycleRow> = store::read_table(wd, CYCLES_PATH).await?;
    let (cycle_idx, cycle) = find_one(KIND, cycle_query, &cycles, |c| c.cycle_id, |c| &c.name)?;
    if cycle.issued {
        return Err(CliError::Invalid(format!(
            "cycle `{}` is already issued (bill_txn_id={:?}); use `bills reverse` first",
            cycle.name, cycle.bill_txn_id
        )));
    }
    let cycle_id = cycle.cycle_id;
    let cycle_name = cycle.name.clone();
    let bill_date = cycle.bill_date;

    let rows = compute(wd, cycle).await?;
    if rows.is_empty() {
        return Err(CliError::Invalid(format!(
            "cycle `{cycle_name}` has no active connections; nothing to issue"
        )));
    }
    let no_customer: Vec<_> = rows
        .iter()
        .filter(|r| r.customer_id.is_none())
        .map(|r| r.connection_id)
        .collect();
    if !no_customer.is_empty() {
        return Err(CliError::Invalid(format!(
            "cycle `{cycle_name}` has {} active connection(s) without a covering tenancy on \
             {bill_date}: {:?}. Run `tenancy start` for each before issuing.",
            no_customer.len(),
            no_customer
        )));
    }

    let policy_kind = lookup_policy_kind(wd, cycle.policy_id).await?;

    // 1. Build N pairs of journal legs (one txn).
    let mut legs: Vec<LegSpec> = Vec::with_capacity(rows.len() * 2);
    for r in &rows {
        let amount = Cents::from_units(r.total_cents);
        let cust = r
            .customer_id
            .expect("checked: no rows with None customer_id");
        legs.push(
            LegSpec::debit(AR_ACCOUNT, amount, "bill")
                .with_customer(cust)
                .with_connection(r.connection_id)
                .with_cycle(cycle_id)
                .with_source_ref(format!(
                    "bill:cycle={cycle_id},connection={}",
                    r.connection_id
                )),
        );
        legs.push(
            LegSpec::credit(REVENUE_ACCOUNT, amount, "bill")
                .with_connection(r.connection_id)
                .with_cycle(cycle_id)
                .with_source_ref(format!(
                    "bill:cycle={cycle_id},connection={}",
                    r.connection_id
                )),
        );
    }
    let txn_id = write_transaction(wd, bill_date, legs).await?;

    // 2. Write bill_breakdowns rows for this cycle (atomic with cycle update).
    let mut all_breakdowns: Vec<BillBreakdownRow> =
        store::read_table(wd, BILL_BREAKDOWNS_PATH).await?;
    // Defensive: remove any pre-existing rows for this cycle (should not
    // happen since cycle.issued was false, but keep the file consistent).
    all_breakdowns.retain(|b| b.cycle_id != cycle_id);
    for r in &rows {
        all_breakdowns.push(BillBreakdownRow {
            cycle_id,
            connection_id: r.connection_id,
            weight: r.weight,
            share_fraction: r.share_fraction,
            base_cents: r.base_cents,
            margin_cents: r.margin_cents,
            policy_kind: policy_kind.clone(),
        });
    }
    store::write_table(wd, BILL_BREAKDOWNS_PATH, &all_breakdowns).await?;

    // 3. Update the cycle row.
    let mut updated = cycles;
    updated[cycle_idx].issued = true;
    updated[cycle_idx].bill_txn_id = Some(txn_id);
    store::write_table(wd, CYCLES_PATH, &updated).await?;

    let total_revenue: i64 = rows.iter().map(|r| r.total_cents).sum();
    log::info!(
        "[OK] bills issue cycle `{cycle_name}` -> txn #{txn_id}: {} bills totaling {}",
        rows.len(),
        Cents::from_units(total_revenue).display()
    );
    Ok(txn_id)
}

// ---------------------------------------------------------------------------
// reverse
// ---------------------------------------------------------------------------

pub async fn reverse(wd: &WD, cycle_query: &str, reason: Option<String>) -> Result<i32> {
    let cycles: Vec<CycleRow> = store::read_table(wd, CYCLES_PATH).await?;
    let (cycle_idx, cycle) = find_one(KIND, cycle_query, &cycles, |c| c.cycle_id, |c| &c.name)?;
    if !cycle.issued {
        return Err(CliError::Invalid(format!(
            "cycle `{}` is not issued; nothing to reverse",
            cycle.name
        )));
    }
    let original_txn = cycle.bill_txn_id.ok_or_else(|| {
        CliError::Invalid(format!(
            "cycle `{}` has issued=true but bill_txn_id is null (data corruption); fix manually",
            cycle.name
        ))
    })?;
    let cycle_id = cycle.cycle_id;
    let cycle_name = cycle.name.clone();

    // 1. Find the original bill legs by txn_id and post a reversal txn.
    let journal: Vec<JournalLegRow> = store::read_series(wd, JOURNAL_PATH).await?;
    let original_legs: Vec<&JournalLegRow> = journal
        .iter()
        .filter(|l| l.txn_id == original_txn)
        .collect();
    if original_legs.is_empty() {
        return Err(CliError::Invalid(format!(
            "cycle `{cycle_name}` references bill txn #{original_txn} but no journal legs match; data corruption"
        )));
    }
    let memo = reason.unwrap_or_else(|| format!("reverse cycle {cycle_name}"));
    let mut reversal_legs: Vec<LegSpec> = Vec::with_capacity(original_legs.len());
    for orig in &original_legs {
        let mut leg = LegSpec {
            account_code: orig.account_code,
            customer_id: orig.customer_id,
            connection_id: orig.connection_id,
            cycle_id: orig.cycle_id,
            // Swap debit and credit.
            debit: Cents::from_units(orig.credit_cents),
            credit: Cents::from_units(orig.debit_cents),
            source: "reversal",
            source_ref: Some(format!("reversal-of:txn={original_txn}")),
            memo: Some(memo.clone()),
        };
        // Don't transcribe the original memo across.
        let _ = leg.memo.as_mut();
        reversal_legs.push(leg);
    }
    let reversal_txn = write_transaction(wd, cycle.bill_date, reversal_legs).await?;

    // 2. Remove this cycle's rows from bill_breakdowns.
    let mut all_breakdowns: Vec<BillBreakdownRow> =
        store::read_table(wd, BILL_BREAKDOWNS_PATH).await?;
    all_breakdowns.retain(|b| b.cycle_id != cycle_id);
    store::write_table(wd, BILL_BREAKDOWNS_PATH, &all_breakdowns).await?;

    // 3. Clear cycle.issued + bill_txn_id.
    let mut updated = cycles;
    updated[cycle_idx].issued = false;
    updated[cycle_idx].bill_txn_id = None;
    store::write_table(wd, CYCLES_PATH, &updated).await?;

    log::info!(
        "[OK] bills reverse cycle `{cycle_name}` -> reversal txn #{reversal_txn} \
         (reverses original txn #{original_txn})"
    );
    Ok(reversal_txn)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Build the AllocateContext for a cycle and run the policy strategy.
/// `pub(crate)` so that `verify` can re-run the strategy for issued
/// cycles to detect drift between current strategy output and the
/// recorded `bill_breakdowns`.
///
/// **Stale-totals guard**: the strategy reads
/// `cycle_totals_materialized.parquet` for the cycle cost. If that file
/// disagrees with a fresh recompute from `expenses` + `amortization_rules`,
/// every bill produced will be wrong. We refuse to proceed with a clear
/// error pointing at `cycle totals-refresh`.
pub(crate) async fn compute(wd: &WD, cycle: &CycleRow) -> Result<Vec<BillRow>> {
    // Detect stale materialization BEFORE running the strategy. Re-uses
    // the same algorithm `cycle totals-refresh` would write.
    let expenses: Vec<crate::schema::ExpenseRow> =
        store::read_series(wd, crate::schema::EXPENSES_PATH).await?;
    let cycles_all: Vec<CycleRow> = store::read_table(wd, CYCLES_PATH).await?;
    let rules: Vec<crate::schema::AmortizationRuleRow> =
        store::read_table(wd, crate::schema::AMORTIZATION_RULES_PATH).await?;
    let (fresh, _unassigned) =
        crate::domain::amortization::compute_totals(&expenses, &cycles_all, &rules);
    let stored: Vec<crate::schema::CycleTotalRow> = cycle_totals::read(wd).await?;
    if !same_cycle_totals(&fresh, &stored) {
        return Err(CliError::Invalid(
            "cycle_totals_materialized.parquet is stale relative to expenses + \
             amortization_rules; run `cycle totals-refresh` first"
                .into(),
        ));
    }

    let connections: Vec<ConnectionRow> = store::read_table(wd, CONNECTIONS_PATH).await?;
    let active = active_connections::active_for_cycle(&connections, cycle);
    let active_owned: Vec<ConnectionRow> = active.into_iter().cloned().collect();
    let resp = tenancy_resolver::responsible_customer_map(wd, cycle.bill_date).await?;
    let cycle_cost_units: i64 = stored
        .iter()
        .filter(|r| r.cycle_id == cycle.cycle_id)
        .map(|r| r.amount_cents)
        .sum();
    let cycle_cost = Cents::from_units(cycle_cost_units);

    let policy_yaml = lookup_policy_yaml(wd, cycle.policy_id).await?;
    let kind = lookup_policy_kind(wd, cycle.policy_id).await?;
    let strategy = crate::policy::require(&kind)?;

    let ctx = AllocateContext {
        cycle,
        active_connections: &active_owned,
        responsible_customer: &resp,
        cycle_cost,
    };
    strategy.allocate(&policy_yaml, &ctx)
}

/// Equality check on (cycle_id, account_code, amount_cents) across two
/// cycle_totals row sets, order-independent.
fn same_cycle_totals(
    fresh: &[crate::schema::CycleTotalRow],
    stored: &[crate::schema::CycleTotalRow],
) -> bool {
    if fresh.len() != stored.len() {
        return false;
    }
    let mut a: Vec<&crate::schema::CycleTotalRow> = fresh.iter().collect();
    let mut b: Vec<&crate::schema::CycleTotalRow> = stored.iter().collect();
    a.sort_by_key(|r| (r.cycle_id, r.account_code));
    b.sort_by_key(|r| (r.cycle_id, r.account_code));
    a.iter().zip(b.iter()).all(|(x, y)| {
        x.cycle_id == y.cycle_id
            && x.account_code == y.account_code
            && x.amount_cents == y.amount_cents
    })
}

pub(crate) async fn lookup_policy_yaml(wd: &WD, policy_id: i32) -> Result<String> {
    let policies: Vec<BillingPolicyRow> = store::read_table(wd, BILLING_POLICIES_PATH).await?;
    let p = policies
        .iter()
        .find(|p| p.policy_id == policy_id)
        .ok_or_else(|| CliError::Invalid(format!("policy #{policy_id} not found")))?;
    Ok(p.config_yaml.clone())
}

async fn lookup_policy_kind(wd: &WD, policy_id: i32) -> Result<String> {
    let policies: Vec<BillingPolicyRow> = store::read_table(wd, BILLING_POLICIES_PATH).await?;
    let p = policies
        .iter()
        .find(|p| p.policy_id == policy_id)
        .ok_or_else(|| CliError::Invalid(format!("policy #{policy_id} not found")))?;
    Ok(p.kind.clone())
}

fn print_rows(rows: &[BillRow]) {
    log::info!(
        "{:>4}  {:<8}  {:>4}  {:>10}  {:>12}  {:>12}  {:>12}  share",
        "conn",
        "cust",
        "wgt",
        "share_frac",
        "base",
        "margin",
        "total"
    );
    let mut total_revenue: i64 = 0;
    for r in rows {
        log::info!(
            "{:>4}  {:<8}  {:>4.1}  {:>10.4}  {:>12}  {:>12}  {:>12}",
            r.connection_id,
            r.customer_id
                .map(|i| format!("#{i}"))
                .unwrap_or_else(|| "(none)".into()),
            r.weight,
            r.share_fraction,
            Cents::from_units(r.base_cents).display(),
            Cents::from_units(r.margin_cents).display(),
            Cents::from_units(r.total_cents).display(),
        );
        total_revenue += r.total_cents;
    }
    log::info!(
        "{:>4}  {:<8}  {:>4}  {:>10}  {:>12}  {:>12}  {:>12}",
        "",
        "TOTAL",
        "",
        "",
        "",
        "",
        Cents::from_units(total_revenue).display()
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chart::seed as chart_seed;
    use crate::cli::cycle as cycle_cli;
    use crate::cli::policy as policy_cli;
    use crate::cli::{connection as conn_cli, customer as cust_cli, expense, tenancy};

    /// End-to-end fixture: a 2-cycle pond with 3 connections (1 commercial),
    /// 2 customers, 1 policy (share-by-weight, margin 0.2), tenancies,
    /// and a $200 + $100 expense pair making cycle_cost = $300 in cycle 1.
    async fn fixture() -> (WD, /* cycle id */ i32) {
        let fs = tinyfs::memory::new_fs().await;
        let wd = fs.root().await.unwrap();
        let _ = wd.create_dir_all("/data").await.unwrap();
        chart_seed(&wd).await.unwrap();
        policy_cli::seed_default_amortization_rules(&wd)
            .await
            .unwrap();

        // Customers
        let alice = cust_cli::add(&wd, "Alice".into(), "addr1".into(), None, None)
            .await
            .unwrap();
        let bob = cust_cli::add(&wd, "Bob".into(), "addr2".into(), None, None)
            .await
            .unwrap();

        // Connections (id 1 commercial, ids 2,3 residential)
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
        let _c3 = conn_cli::add(
            &wd,
            "300 Main".into(),
            "300 Main".into(),
            "2010-01-01",
            false,
            None,
            None,
        )
        .await
        .unwrap();

        // Tenancies (start before any cycle bill_date)
        let _ = tenancy::start(&wd, &c1.to_string(), &alice.to_string(), "2020-01-01", None)
            .await
            .unwrap();
        let _ = tenancy::start(&wd, &c2.to_string(), &bob.to_string(), "2020-01-01", None)
            .await
            .unwrap();
        // Connection 3 deliberately has NO tenancy -> bills issue should refuse.

        // Policy
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

        // Cycle 1 (Apr-Sep 2024)
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
        // Expenses: $200 operations + $100 utilities in this cycle.
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
    async fn issue_refused_when_active_connection_lacks_tenancy() {
        let (wd, cycle_id) = fixture().await;
        // Connection 3 has no tenancy -> issue refuses.
        let err = issue(&wd, &cycle_id.to_string()).await.unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)), "got {err:?}");
        let msg = format!("{err}");
        assert!(msg.contains("without a covering tenancy"), "got: {msg}");
    }

    #[tokio::test]
    async fn issue_after_excluding_connection3_inactive() {
        let (wd, cycle_id) = fixture().await;
        // Mark connection 3 inactive for this cycle and re-try.
        cycle_cli::edit(&wd, &cycle_id.to_string(), None, Some("3".into()), None)
            .await
            .unwrap();

        let txn = issue(&wd, &cycle_id.to_string()).await.unwrap();

        // Verify journal: 2 active connections * 2 legs = 4 legs in one txn.
        let legs: Vec<JournalLegRow> = store::read_series(&wd, JOURNAL_PATH).await.unwrap();
        let bill_legs: Vec<&JournalLegRow> = legs.iter().filter(|l| l.txn_id == txn).collect();
        assert_eq!(bill_legs.len(), 4, "2 connections * 2 legs");
        assert!(bill_legs.iter().all(|l| l.source == "bill"));
        let total_dr_1100: i64 = bill_legs
            .iter()
            .filter(|l| l.account_code == AR_ACCOUNT)
            .map(|l| l.debit_cents)
            .sum();
        let total_cr_4000: i64 = bill_legs
            .iter()
            .filter(|l| l.account_code == REVENUE_ACCOUNT)
            .map(|l| l.credit_cents)
            .sum();
        assert_eq!(total_dr_1100, total_cr_4000);
        // Cycle cost was $300; margin 0.2 -> revenue $360 = 36000c.
        assert_eq!(total_dr_1100, 36000);

        // Verify bill_breakdowns
        let breakdowns: Vec<BillBreakdownRow> =
            store::read_table(&wd, BILL_BREAKDOWNS_PATH).await.unwrap();
        assert_eq!(breakdowns.len(), 2);
        assert!(breakdowns.iter().all(|b| b.cycle_id == cycle_id));

        // Verify cycle row
        let cycles: Vec<CycleRow> = store::read_table(&wd, CYCLES_PATH).await.unwrap();
        let c = cycles.iter().find(|c| c.cycle_id == cycle_id).unwrap();
        assert!(c.issued);
        assert_eq!(c.bill_txn_id, Some(txn));
    }

    #[tokio::test]
    async fn double_issue_rejected() {
        let (wd, cycle_id) = fixture().await;
        cycle_cli::edit(&wd, &cycle_id.to_string(), None, Some("3".into()), None)
            .await
            .unwrap();
        let _ = issue(&wd, &cycle_id.to_string()).await.unwrap();
        let err = issue(&wd, &cycle_id.to_string()).await.unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn reverse_clears_state_and_keeps_audit_trail() {
        let (wd, cycle_id) = fixture().await;
        cycle_cli::edit(&wd, &cycle_id.to_string(), None, Some("3".into()), None)
            .await
            .unwrap();
        let issue_txn = issue(&wd, &cycle_id.to_string()).await.unwrap();
        let reversal_txn = reverse(&wd, &cycle_id.to_string(), Some("test".into()))
            .await
            .unwrap();
        assert_ne!(issue_txn, reversal_txn);

        // Cycle is no longer issued.
        let cycles: Vec<CycleRow> = store::read_table(&wd, CYCLES_PATH).await.unwrap();
        let c = cycles.iter().find(|c| c.cycle_id == cycle_id).unwrap();
        assert!(!c.issued);
        assert_eq!(c.bill_txn_id, None);

        // bill_breakdowns rows for this cycle removed.
        let breakdowns: Vec<BillBreakdownRow> =
            store::read_table(&wd, BILL_BREAKDOWNS_PATH).await.unwrap();
        assert!(breakdowns.iter().all(|b| b.cycle_id != cycle_id));

        // Both the original and reversal txns are still in the journal.
        let legs: Vec<JournalLegRow> = store::read_series(&wd, JOURNAL_PATH).await.unwrap();
        assert!(legs.iter().any(|l| l.txn_id == issue_txn));
        assert!(legs.iter().any(|l| l.txn_id == reversal_txn));

        // Per-customer AR balance returns to zero (DR + CR cancel).
        let alice_legs: Vec<&JournalLegRow> = legs
            .iter()
            .filter(|l| l.customer_id == Some(1) && l.account_code == AR_ACCOUNT)
            .collect();
        let net: i64 = alice_legs
            .iter()
            .map(|l| l.debit_cents - l.credit_cents)
            .sum();
        assert_eq!(net, 0, "Alice's AR returns to zero after issue+reverse");
    }

    #[tokio::test]
    async fn reverse_then_reissue_with_corrected_inactive() {
        let (wd, cycle_id) = fixture().await;
        cycle_cli::edit(&wd, &cycle_id.to_string(), None, Some("3".into()), None)
            .await
            .unwrap();
        let _ = issue(&wd, &cycle_id.to_string()).await.unwrap();
        let _ = reverse(&wd, &cycle_id.to_string(), None).await.unwrap();

        // Re-issue (cycle is again unissued).
        let new_txn = issue(&wd, &cycle_id.to_string()).await.unwrap();
        let cycles: Vec<CycleRow> = store::read_table(&wd, CYCLES_PATH).await.unwrap();
        let c = cycles.iter().find(|c| c.cycle_id == cycle_id).unwrap();
        assert!(c.issued);
        assert_eq!(c.bill_txn_id, Some(new_txn));
    }

    #[tokio::test]
    async fn reverse_unissued_rejected() {
        let (wd, cycle_id) = fixture().await;
        let err = reverse(&wd, &cycle_id.to_string(), None).await.unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn preview_warns_about_missing_tenancies() {
        // Just verify it doesn't error on the missing-tenancy case (it's
        // a preview, not an issue).
        let (wd, cycle_id) = fixture().await;
        preview(&wd, &cycle_id.to_string())
            .await
            .expect("preview ok");
    }

    /// Per final rubber-duck (#1): bills issue/preview must refuse if
    /// cycle_totals_materialized is stale.
    #[tokio::test]
    async fn issue_refuses_stale_totals() {
        let (wd, cycle_id) = fixture().await;
        cycle_cli::edit(&wd, &cycle_id.to_string(), None, Some("3".into()), None)
            .await
            .unwrap();
        // Add an expense WITHOUT calling totals-refresh -> stale.
        let _ = expense::add(&wd, "2024-07-01", 5100, "$25.00", None, None, Some("1"))
            .await
            .unwrap();
        let err = issue(&wd, &cycle_id.to_string()).await.unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)), "got {err:?}");
        let msg = format!("{err}");
        assert!(
            msg.contains("cycle_totals_materialized.parquet is stale"),
            "got: {msg}"
        );
        // After refresh, issue succeeds.
        cycle_cli::refresh(&wd).await.unwrap();
        let _ = issue(&wd, &cycle_id.to_string())
            .await
            .expect("issue after refresh");
    }
}
