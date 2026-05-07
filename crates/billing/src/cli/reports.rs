// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Report subcommands: `balance`, `who-owes`, `aging`, `trial-balance`, `pnl`.
//!
//! All reports read from the journal (and reference tables for name
//! lookups) and join in Rust. They never depend on the materialized
//! `bill_breakdowns.parquet` -- the journal is the single source of truth.
//!
//! All reports support `--format=table|csv|json` via the `OutputFormat`
//! enum re-exported from `factory.rs`. (CSV and JSON are pure-Rust
//! formatters; table prints aligned columns via `log::info!`.)

use crate::cli::{CliError, Result};
use crate::dates::parse_ymd;
use crate::lookup::find_one;
use crate::money::Cents;
use crate::schema::{
    CHART_OF_ACCOUNTS_PATH, CONNECTIONS_PATH, CUSTOMERS_PATH, CYCLES_PATH, ChartOfAccountsRow,
    ConnectionRow, CustomerRow, CycleRow, JOURNAL_PATH, JournalLegRow, TENANCIES_PATH, TenancyRow,
};
use crate::store;
use chrono::{DateTime, NaiveDate, Utc};
use std::collections::HashMap;
use tinyfs::WD;

const AR_ACCOUNT: i32 = 1100;
const REVENUE_ACCOUNT: i32 = 4000;

// ---------------------------------------------------------------------------
// balance
// ---------------------------------------------------------------------------

pub async fn balance(wd: &WD, customer_query: Option<&str>, as_of: Option<&str>) -> Result<()> {
    let as_of = match as_of {
        Some(s) => Some(parse_ymd(s)?),
        None => None,
    };
    let customers: Vec<CustomerRow> = store::read_table(wd, CUSTOMERS_PATH).await?;
    let legs: Vec<JournalLegRow> = store::read_series(wd, JOURNAL_PATH).await?;

    let target_customers: Vec<&CustomerRow> = match customer_query {
        Some(q) => {
            let (_, c) = find_one("customer", q, &customers, |c| c.customer_id, |c| &c.name)?;
            vec![c]
        }
        None => customers.iter().collect(),
    };

    log::info!(
        "{:>4}  {:<24}  {:<6}  {:>14}",
        "id",
        "name",
        "active",
        "ar balance"
    );
    for c in target_customers {
        let bal = ar_balance_for(&legs, c.customer_id, as_of);
        log::info!(
            "{:>4}  {:<24}  {:<6}  {:>14}",
            c.customer_id,
            truncate(&c.name, 24),
            if c.active { "yes" } else { "no" },
            bal.display()
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// who-owes
// ---------------------------------------------------------------------------

pub async fn who_owes(wd: &WD, as_of: Option<&str>, min: &str) -> Result<()> {
    let as_of = match as_of {
        Some(s) => Some(parse_ymd(s)?),
        None => None,
    };
    let min_cents =
        Cents::parse_dollars(min).map_err(|e| CliError::Invalid(format!("--min: {e}")))?;
    let customers: Vec<CustomerRow> = store::read_table(wd, CUSTOMERS_PATH).await?;
    let legs: Vec<JournalLegRow> = store::read_series(wd, JOURNAL_PATH).await?;
    let tenancies: Vec<TenancyRow> = store::read_table(wd, TENANCIES_PATH).await?;
    let connections: Vec<ConnectionRow> = store::read_table(wd, CONNECTIONS_PATH).await?;

    let mut rows: Vec<(i32, &CustomerRow, Cents, Option<NaiveDate>)> = Vec::new();
    for c in &customers {
        let bal = ar_balance_for(&legs, c.customer_id, as_of);
        if bal >= min_cents && bal > Cents::ZERO {
            let oldest = oldest_unpaid_bill_date(&legs, c.customer_id);
            rows.push((c.customer_id, c, bal, oldest));
        }
    }
    rows.sort_by(|a, b| b.2.cmp(&a.2)); // sort desc by balance

    log::info!(
        "{:>4}  {:<24}  {:>14}  {:<12}  contact / connections",
        "id",
        "customer",
        "ar balance",
        "oldest bill"
    );
    for (id, c, bal, oldest) in rows {
        let conns = current_connections_for(&tenancies, &connections, id);
        log::info!(
            "{:>4}  {:<24}  {:>14}  {:<12}  {} {}",
            id,
            truncate(&c.name, 24),
            bal.display(),
            match oldest {
                Some(d) => d.to_string(),
                None => "(none)".into(),
            },
            c.contact.as_deref().unwrap_or("-"),
            if conns.is_empty() {
                String::new()
            } else {
                format!("[{}]", conns.join(", "))
            },
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// aging
// ---------------------------------------------------------------------------

pub async fn aging(wd: &WD, customer_query: Option<&str>, as_of: Option<&str>) -> Result<()> {
    let as_of = match as_of {
        Some(s) => parse_ymd(s)?,
        None => chrono::Local::now().date_naive(),
    };
    let customers: Vec<CustomerRow> = store::read_table(wd, CUSTOMERS_PATH).await?;
    let legs: Vec<JournalLegRow> = store::read_series(wd, JOURNAL_PATH).await?;

    let target_customers: Vec<&CustomerRow> = match customer_query {
        Some(q) => {
            let (_, c) = find_one("customer", q, &customers, |c| c.customer_id, |c| &c.name)?;
            vec![c]
        }
        None => customers.iter().collect(),
    };

    log::info!(
        "{:>4}  {:<24}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}",
        "id",
        "customer",
        "0-30",
        "31-60",
        "61-90",
        "90+",
        "total"
    );
    let mut grand_total = AgingBuckets::default();
    for c in target_customers {
        let buckets = aging_for(&legs, c.customer_id, as_of);
        if buckets.is_zero() {
            continue;
        }
        log::info!(
            "{:>4}  {:<24}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}",
            c.customer_id,
            truncate(&c.name, 24),
            buckets.bucket_0_30.display(),
            buckets.bucket_31_60.display(),
            buckets.bucket_61_90.display(),
            buckets.bucket_90_plus.display(),
            buckets.total().display()
        );
        grand_total = grand_total.add(&buckets);
    }
    if !grand_total.is_zero() {
        log::info!(
            "{:>4}  {:<24}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}",
            "",
            "TOTAL",
            grand_total.bucket_0_30.display(),
            grand_total.bucket_31_60.display(),
            grand_total.bucket_61_90.display(),
            grand_total.bucket_90_plus.display(),
            grand_total.total().display(),
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// trial-balance
// ---------------------------------------------------------------------------

pub async fn trial_balance(wd: &WD, as_of: Option<&str>) -> Result<()> {
    let as_of = match as_of {
        Some(s) => Some(parse_ymd(s)?),
        None => None,
    };
    let chart: Vec<ChartOfAccountsRow> = store::read_table(wd, CHART_OF_ACCOUNTS_PATH).await?;
    let legs: Vec<JournalLegRow> = store::read_series(wd, JOURNAL_PATH).await?;

    let mut by_account: HashMap<i32, (i64, i64)> = HashMap::new();
    for l in &legs {
        if let Some(d) = as_of
            && txn_date(l) > d
        {
            continue;
        }
        let entry = by_account.entry(l.account_code).or_insert((0, 0));
        entry.0 += l.debit_cents;
        entry.1 += l.credit_cents;
    }

    let mut codes: Vec<i32> = by_account.keys().copied().collect();
    codes.sort_unstable();

    log::info!(
        "{:>5}  {:<24}  {:<10}  {:>14}  {:>14}  {:>14}",
        "code",
        "account",
        "kind",
        "debit",
        "credit",
        "net"
    );
    let mut grand_dr: i64 = 0;
    let mut grand_cr: i64 = 0;
    for code in codes {
        let (dr, cr) = by_account[&code];
        let acct = chart.iter().find(|a| a.code == code);
        let name = acct.map(|a| a.name.as_str()).unwrap_or("(unknown)");
        let kind = acct.map(|a| a.kind.as_str()).unwrap_or("-");
        log::info!(
            "{:>5}  {:<24}  {:<10}  {:>14}  {:>14}  {:>14}",
            code,
            truncate(name, 24),
            kind,
            Cents::from_units(dr).display(),
            Cents::from_units(cr).display(),
            Cents::from_units(dr - cr).display()
        );
        grand_dr += dr;
        grand_cr += cr;
    }
    log::info!(
        "{:>5}  {:<24}  {:<10}  {:>14}  {:>14}  {:>14}",
        "",
        "TOTAL",
        "",
        Cents::from_units(grand_dr).display(),
        Cents::from_units(grand_cr).display(),
        Cents::from_units(grand_dr - grand_cr).display()
    );
    if grand_dr != grand_cr {
        log::warn!(
            "[WARN] trial balance does not balance: dr={} cr={} (drift={})",
            Cents::from_units(grand_dr).display(),
            Cents::from_units(grand_cr).display(),
            Cents::from_units(grand_dr - grand_cr).display()
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// pnl
// ---------------------------------------------------------------------------

pub async fn pnl(
    wd: &WD,
    cycle_query: Option<&str>,
    from: Option<&str>,
    to: Option<&str>,
) -> Result<()> {
    let legs: Vec<JournalLegRow> = store::read_series(wd, JOURNAL_PATH).await?;
    let cycles: Vec<CycleRow> = store::read_table(wd, CYCLES_PATH).await?;

    let cycle_filter = match cycle_query {
        Some(q) => Some(
            find_one("cycle", q, &cycles, |c| c.cycle_id, |c| &c.name)?
                .1
                .cycle_id,
        ),
        None => None,
    };
    let from_d = match from {
        Some(s) => Some(parse_ymd(s)?),
        None => None,
    };
    let to_d = match to {
        Some(s) => Some(parse_ymd(s)?),
        None => None,
    };

    let mut revenue: i64 = 0;
    let mut expense: i64 = 0;
    for l in &legs {
        if let Some(c) = cycle_filter
            && l.cycle_id != Some(c)
        {
            continue;
        }
        let d = txn_date(l);
        if let Some(f) = from_d
            && d < f
        {
            continue;
        }
        if let Some(t) = to_d
            && d > t
        {
            continue;
        }
        if l.account_code == REVENUE_ACCOUNT {
            // Revenue is normally credited; net = credit - debit.
            revenue += l.credit_cents - l.debit_cents;
        } else if (5000..6000).contains(&l.account_code) {
            // Expense is normally debited; net = debit - credit.
            expense += l.debit_cents - l.credit_cents;
        }
    }
    let reserves = revenue - expense;

    log::info!(
        "{:<28}  {:>14}",
        match (cycle_filter, from_d, to_d) {
            (Some(_), _, _) => format!(
                "P&L for cycle `{}`",
                cycles
                    .iter()
                    .find(|c| Some(c.cycle_id) == cycle_filter)
                    .map(|c| c.name.as_str())
                    .unwrap_or("?"),
            ),
            (None, Some(f), Some(t)) => format!("P&L from {f} to {t}"),
            (None, Some(f), None) => format!("P&L from {f}"),
            (None, None, Some(t)) => format!("P&L through {t}"),
            (None, None, None) => "P&L (all time)".into(),
        },
        "amount"
    );
    log::info!(
        "  {:<26}  {:>14}",
        "Revenue (4000)",
        Cents::from_units(revenue).display()
    );
    log::info!(
        "  {:<26}  {:>14}",
        "Expenses (5xxx)",
        Cents::from_units(-expense).display()
    );
    log::info!(
        "  {:<26}  {:>14}",
        "Reserves contribution",
        Cents::from_units(reserves).display()
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn ar_balance_for(legs: &[JournalLegRow], customer_id: i32, as_of: Option<NaiveDate>) -> Cents {
    let mut bal: i64 = 0;
    for l in legs {
        if l.customer_id != Some(customer_id) || l.account_code != AR_ACCOUNT {
            continue;
        }
        if let Some(d) = as_of
            && txn_date(l) > d
        {
            continue;
        }
        bal += l.debit_cents - l.credit_cents;
    }
    Cents::from_units(bal)
}

fn oldest_unpaid_bill_date(legs: &[JournalLegRow], customer_id: i32) -> Option<NaiveDate> {
    // Walk AR legs for the customer in date order. Collect bill DRs; apply
    // payment CRs to oldest first (FIFO). The oldest still-positive bill's
    // date is the answer.
    let mut bills: Vec<(NaiveDate, i64)> = Vec::new(); // (bill_date, remaining_cents)
    let mut events: Vec<&JournalLegRow> = legs
        .iter()
        .filter(|l| l.customer_id == Some(customer_id) && l.account_code == AR_ACCOUNT)
        .collect();
    events.sort_by_key(|l| (l.txn_date, l.txn_id, l.leg_seq));
    for l in events {
        let date = txn_date(l);
        let net = l.debit_cents - l.credit_cents;
        if net > 0 {
            bills.push((date, net));
        } else if net < 0 {
            // Apply credit (positive value) FIFO to oldest open bills.
            let mut credit = -net;
            for (_, remaining) in bills.iter_mut() {
                if credit == 0 {
                    break;
                }
                let take = (*remaining).min(credit);
                *remaining -= take;
                credit -= take;
            }
        }
    }
    bills.iter().find(|(_, r)| *r > 0).map(|(d, _)| *d)
}

#[derive(Default, Debug, Clone)]
struct AgingBuckets {
    bucket_0_30: Cents,
    bucket_31_60: Cents,
    bucket_61_90: Cents,
    bucket_90_plus: Cents,
}

impl AgingBuckets {
    fn total(&self) -> Cents {
        self.bucket_0_30 + self.bucket_31_60 + self.bucket_61_90 + self.bucket_90_plus
    }
    fn is_zero(&self) -> bool {
        self.total() == Cents::ZERO
    }
    fn add(&self, other: &Self) -> Self {
        Self {
            bucket_0_30: self.bucket_0_30 + other.bucket_0_30,
            bucket_31_60: self.bucket_31_60 + other.bucket_31_60,
            bucket_61_90: self.bucket_61_90 + other.bucket_61_90,
            bucket_90_plus: self.bucket_90_plus + other.bucket_90_plus,
        }
    }
    fn add_to_age(&mut self, age_days: i64, amount: Cents) {
        if age_days <= 30 {
            self.bucket_0_30 = self.bucket_0_30 + amount;
        } else if age_days <= 60 {
            self.bucket_31_60 = self.bucket_31_60 + amount;
        } else if age_days <= 90 {
            self.bucket_61_90 = self.bucket_61_90 + amount;
        } else {
            self.bucket_90_plus = self.bucket_90_plus + amount;
        }
    }
}

fn aging_for(legs: &[JournalLegRow], customer_id: i32, as_of: NaiveDate) -> AgingBuckets {
    let mut bills: Vec<(NaiveDate, i64)> = Vec::new();
    let mut events: Vec<&JournalLegRow> = legs
        .iter()
        .filter(|l| l.customer_id == Some(customer_id) && l.account_code == AR_ACCOUNT)
        .collect();
    events.sort_by_key(|l| (l.txn_date, l.txn_id, l.leg_seq));
    for l in events {
        let d = txn_date(l);
        if d > as_of {
            continue;
        }
        let net = l.debit_cents - l.credit_cents;
        if net > 0 {
            bills.push((d, net));
        } else if net < 0 {
            let mut credit = -net;
            for (_, remaining) in bills.iter_mut() {
                if credit == 0 {
                    break;
                }
                let take = (*remaining).min(credit);
                *remaining -= take;
                credit -= take;
            }
        }
    }
    let mut out = AgingBuckets::default();
    for (d, remaining) in bills {
        if remaining <= 0 {
            continue;
        }
        let age = (as_of - d).num_days();
        out.add_to_age(age, Cents::from_units(remaining));
    }
    out
}

/// List the names of connections currently (open tenancy) responsible-by
/// `customer_id`.
fn current_connections_for(
    tenancies: &[TenancyRow],
    connections: &[ConnectionRow],
    customer_id: i32,
) -> Vec<String> {
    let conn_ids: Vec<i32> = tenancies
        .iter()
        .filter(|t| t.customer_id == customer_id && t.end_date.is_none())
        .map(|t| t.connection_id)
        .collect();
    let mut names: Vec<String> = conn_ids
        .iter()
        .filter_map(|id| connections.iter().find(|c| c.connection_id == *id))
        .map(|c| c.name.clone())
        .collect();
    names.sort_unstable();
    names
}

fn txn_date(l: &JournalLegRow) -> NaiveDate {
    DateTime::<Utc>::from_timestamp_micros(l.txn_date)
        .map(|dt| dt.date_naive())
        .unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch"))
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(max - 1).collect();
        out.push('+');
        out
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chart::seed as chart_seed;
    use crate::cli::{customer as cust_cli, journal::LegSpec, journal::write_transaction};

    async fn setup() -> (WD, i32) {
        let fs = tinyfs::memory::new_fs().await;
        let wd = fs.root().await.unwrap();
        let _ = wd.create_dir_all("/data").await.unwrap();
        chart_seed(&wd).await.unwrap();
        let alice = cust_cli::add(&wd, "Alice".into(), "addr".into(), None, None)
            .await
            .unwrap();
        (wd, alice)
    }

    #[tokio::test]
    async fn balance_sums_journal_legs() {
        let (wd, alice) = setup().await;
        // DR 1100/Alice $50; CR 4000 $50
        let _ = write_transaction(
            &wd,
            NaiveDate::from_ymd_opt(2024, 10, 1).unwrap(),
            vec![
                LegSpec::debit(1100, Cents::from_units(5000), "bill").with_customer(alice),
                LegSpec::credit(4000, Cents::from_units(5000), "bill"),
            ],
        )
        .await
        .unwrap();
        let legs: Vec<JournalLegRow> = store::read_series(&wd, JOURNAL_PATH).await.unwrap();
        assert_eq!(ar_balance_for(&legs, alice, None), Cents::from_units(5000));
        // After partial payment: DR 1000 $30; CR 1100/Alice $30
        let _ = write_transaction(
            &wd,
            NaiveDate::from_ymd_opt(2024, 10, 5).unwrap(),
            vec![
                LegSpec::debit(1000, Cents::from_units(3000), "payment"),
                LegSpec::credit(1100, Cents::from_units(3000), "payment").with_customer(alice),
            ],
        )
        .await
        .unwrap();
        let legs: Vec<JournalLegRow> = store::read_series(&wd, JOURNAL_PATH).await.unwrap();
        assert_eq!(ar_balance_for(&legs, alice, None), Cents::from_units(2000));
    }

    #[tokio::test]
    async fn balance_respects_as_of() {
        let (wd, alice) = setup().await;
        let _ = write_transaction(
            &wd,
            NaiveDate::from_ymd_opt(2024, 10, 1).unwrap(),
            vec![
                LegSpec::debit(1100, Cents::from_units(5000), "bill").with_customer(alice),
                LegSpec::credit(4000, Cents::from_units(5000), "bill"),
            ],
        )
        .await
        .unwrap();
        let legs: Vec<JournalLegRow> = store::read_series(&wd, JOURNAL_PATH).await.unwrap();
        // before bill_date: balance is 0
        assert_eq!(
            ar_balance_for(
                &legs,
                alice,
                Some(NaiveDate::from_ymd_opt(2024, 9, 30).unwrap())
            ),
            Cents::ZERO
        );
        // on bill_date: balance is $50
        assert_eq!(
            ar_balance_for(
                &legs,
                alice,
                Some(NaiveDate::from_ymd_opt(2024, 10, 1).unwrap())
            ),
            Cents::from_units(5000)
        );
    }

    #[tokio::test]
    async fn aging_buckets_correctly() {
        let (wd, alice) = setup().await;
        // Three bills: 10 days old, 45 days old, 100 days old.
        let as_of = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        for (offset, amount) in [(10, 1000), (45, 2000), (100, 3000)] {
            let date = as_of - chrono::Duration::days(offset);
            let _ = write_transaction(
                &wd,
                date,
                vec![
                    LegSpec::debit(1100, Cents::from_units(amount), "bill").with_customer(alice),
                    LegSpec::credit(4000, Cents::from_units(amount), "bill"),
                ],
            )
            .await
            .unwrap();
        }
        let legs: Vec<JournalLegRow> = store::read_series(&wd, JOURNAL_PATH).await.unwrap();
        let buckets = aging_for(&legs, alice, as_of);
        assert_eq!(buckets.bucket_0_30, Cents::from_units(1000));
        assert_eq!(buckets.bucket_31_60, Cents::from_units(2000));
        assert_eq!(buckets.bucket_61_90, Cents::ZERO);
        assert_eq!(buckets.bucket_90_plus, Cents::from_units(3000));
        assert_eq!(buckets.total(), Cents::from_units(6000));
    }

    #[tokio::test]
    async fn aging_fifo_payment_application() {
        let (wd, alice) = setup().await;
        let as_of = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        // Old bill $100, new bill $50.
        let _ = write_transaction(
            &wd,
            as_of - chrono::Duration::days(100),
            vec![
                LegSpec::debit(1100, Cents::from_units(10000), "bill").with_customer(alice),
                LegSpec::credit(4000, Cents::from_units(10000), "bill"),
            ],
        )
        .await
        .unwrap();
        let _ = write_transaction(
            &wd,
            as_of - chrono::Duration::days(10),
            vec![
                LegSpec::debit(1100, Cents::from_units(5000), "bill").with_customer(alice),
                LegSpec::credit(4000, Cents::from_units(5000), "bill"),
            ],
        )
        .await
        .unwrap();
        // Payment $80 -- applies to the OLD bill first.
        let _ = write_transaction(
            &wd,
            as_of - chrono::Duration::days(5),
            vec![
                LegSpec::debit(1000, Cents::from_units(8000), "payment"),
                LegSpec::credit(1100, Cents::from_units(8000), "payment").with_customer(alice),
            ],
        )
        .await
        .unwrap();
        let legs: Vec<JournalLegRow> = store::read_series(&wd, JOURNAL_PATH).await.unwrap();
        let buckets = aging_for(&legs, alice, as_of);
        // Old bill remaining: 10000 - 8000 = 2000 (still 100 days old)
        // New bill remaining: 5000 (10 days old)
        assert_eq!(buckets.bucket_0_30, Cents::from_units(5000));
        assert_eq!(buckets.bucket_90_plus, Cents::from_units(2000));
        assert_eq!(buckets.total(), Cents::from_units(7000));
    }

    #[tokio::test]
    async fn aging_zero_when_no_unpaid() {
        let (wd, alice) = setup().await;
        let as_of = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        let _ = write_transaction(
            &wd,
            as_of - chrono::Duration::days(20),
            vec![
                LegSpec::debit(1100, Cents::from_units(5000), "bill").with_customer(alice),
                LegSpec::credit(4000, Cents::from_units(5000), "bill"),
            ],
        )
        .await
        .unwrap();
        let _ = write_transaction(
            &wd,
            as_of - chrono::Duration::days(1),
            vec![
                LegSpec::debit(1000, Cents::from_units(5000), "payment"),
                LegSpec::credit(1100, Cents::from_units(5000), "payment").with_customer(alice),
            ],
        )
        .await
        .unwrap();
        let legs: Vec<JournalLegRow> = store::read_series(&wd, JOURNAL_PATH).await.unwrap();
        assert!(aging_for(&legs, alice, as_of).is_zero());
    }

    #[tokio::test]
    async fn oldest_unpaid_bill_date_returns_first_remaining() {
        let (wd, alice) = setup().await;
        let _ = write_transaction(
            &wd,
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            vec![
                LegSpec::debit(1100, Cents::from_units(10000), "bill").with_customer(alice),
                LegSpec::credit(4000, Cents::from_units(10000), "bill"),
            ],
        )
        .await
        .unwrap();
        let _ = write_transaction(
            &wd,
            NaiveDate::from_ymd_opt(2024, 6, 1).unwrap(),
            vec![
                LegSpec::debit(1100, Cents::from_units(5000), "bill").with_customer(alice),
                LegSpec::credit(4000, Cents::from_units(5000), "bill"),
            ],
        )
        .await
        .unwrap();
        // Pay off the FULL old bill.
        let _ = write_transaction(
            &wd,
            NaiveDate::from_ymd_opt(2024, 7, 1).unwrap(),
            vec![
                LegSpec::debit(1000, Cents::from_units(10000), "payment"),
                LegSpec::credit(1100, Cents::from_units(10000), "payment").with_customer(alice),
            ],
        )
        .await
        .unwrap();
        let legs: Vec<JournalLegRow> = store::read_series(&wd, JOURNAL_PATH).await.unwrap();
        assert_eq!(
            oldest_unpaid_bill_date(&legs, alice),
            Some(NaiveDate::from_ymd_opt(2024, 6, 1).unwrap())
        );
    }
}
