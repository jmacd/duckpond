// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Journal write helper + `journal show` subcommand.
//!
//! ## `JournalWriter::write_transaction`
//!
//! Every domain command (expense, payment, opening-balance, adjust, bills
//! issue, bills reverse, customer merge) routes through `write_transaction`.
//! The helper:
//!
//! 1. Reads the existing journal series, computes `txn_id = max + 1`.
//! 2. Validates `Σ debit_cents == Σ credit_cents` AND >= 2 legs.
//! 3. Stamps `recorded_at = now()`, assigns `leg_seq = 0..N-1`, and writes
//!    one new version of `journal.series` containing all N legs.
//!
//! Returns the assigned `txn_id` so the caller can print it for the
//! operator's audit trail.

use crate::cli::{CliError, Result};
use crate::dates::parse_ymd;
use crate::money::Cents;
use crate::schema::{JOURNAL_PATH, JournalLegRow};
use crate::store;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Utc};
use tinyfs::WD;

/// One leg of a transaction passed in by a caller. `txn_id`, `recorded_at`,
/// `leg_seq` are filled in by the writer.
#[derive(Debug, Clone)]
pub struct LegSpec {
    pub account_code: i32,
    pub customer_id: Option<i32>,
    pub connection_id: Option<i32>,
    pub cycle_id: Option<i32>,
    pub debit: Cents,
    pub credit: Cents,
    pub source: &'static str, // "bill" / "payment" / "expense" / "adjustment" / "opening" / "reversal" / "merge"
    pub source_ref: Option<String>,
    pub memo: Option<String>,
}

impl LegSpec {
    /// Convenience: a leg that posts a debit only.
    #[must_use]
    pub fn debit(account_code: i32, amount: Cents, source: &'static str) -> Self {
        LegSpec {
            account_code,
            customer_id: None,
            connection_id: None,
            cycle_id: None,
            debit: amount,
            credit: Cents::ZERO,
            source,
            source_ref: None,
            memo: None,
        }
    }
    /// Convenience: a leg that posts a credit only.
    #[must_use]
    pub fn credit(account_code: i32, amount: Cents, source: &'static str) -> Self {
        LegSpec {
            account_code,
            customer_id: None,
            connection_id: None,
            cycle_id: None,
            debit: Cents::ZERO,
            credit: amount,
            source,
            source_ref: None,
            memo: None,
        }
    }
    #[must_use]
    pub fn with_customer(mut self, id: i32) -> Self {
        self.customer_id = Some(id);
        self
    }
    #[must_use]
    pub fn with_connection(mut self, id: i32) -> Self {
        self.connection_id = Some(id);
        self
    }
    #[must_use]
    pub fn with_cycle(mut self, id: i32) -> Self {
        self.cycle_id = Some(id);
        self
    }
    #[must_use]
    pub fn with_source_ref(mut self, r: impl Into<String>) -> Self {
        self.source_ref = Some(r.into());
        self
    }
    #[must_use]
    pub fn with_memo(mut self, m: impl Into<String>) -> Self {
        self.memo = Some(m.into());
        self
    }
}

/// Write one balanced transaction. Returns the assigned `txn_id`.
pub async fn write_transaction(wd: &WD, txn_date: NaiveDate, legs: Vec<LegSpec>) -> Result<i32> {
    if legs.len() < 2 {
        return Err(CliError::Invalid(format!(
            "transaction must have >= 2 legs (got {})",
            legs.len()
        )));
    }
    let total_debit: i64 = legs.iter().map(|l| l.debit.units()).sum();
    let total_credit: i64 = legs.iter().map(|l| l.credit.units()).sum();
    if total_debit != total_credit {
        return Err(CliError::Invalid(format!(
            "transaction is unbalanced: sum(debit)={} sum(credit)={}",
            total_debit, total_credit
        )));
    }
    for (i, l) in legs.iter().enumerate() {
        if l.debit.units() != 0 && l.credit.units() != 0 {
            return Err(CliError::Invalid(format!(
                "leg {i} has both debit and credit non-zero (acct {}, debit {}, credit {})",
                l.account_code,
                l.debit.display(),
                l.credit.display()
            )));
        }
        if l.debit.units() < 0 || l.credit.units() < 0 {
            return Err(CliError::Invalid(format!(
                "leg {i} has negative amount (use the opposite side instead)"
            )));
        }
    }

    let existing: Vec<JournalLegRow> = store::read_series(wd, JOURNAL_PATH).await?;
    let txn_id = existing.iter().map(|r| r.txn_id).max().unwrap_or(0) + 1;

    let txn_date_us = ymd_midnight_us(txn_date);
    let recorded_at_us = Utc::now().timestamp_micros();

    let rows: Vec<JournalLegRow> = legs
        .into_iter()
        .enumerate()
        .map(|(i, l)| JournalLegRow {
            txn_id,
            txn_date: txn_date_us,
            recorded_at: recorded_at_us,
            leg_seq: i as i32,
            account_code: l.account_code,
            customer_id: l.customer_id,
            connection_id: l.connection_id,
            cycle_id: l.cycle_id,
            debit_cents: l.debit.units(),
            credit_cents: l.credit.units(),
            source: l.source.to_string(),
            source_ref: l.source_ref,
            memo: l.memo,
        })
        .collect();

    store::write_series(wd, JOURNAL_PATH, &rows, JournalLegRow::TIMESTAMP_COLUMN).await?;
    Ok(txn_id)
}

/// Sub-ledger query: customer's current AR balance from journal account
/// 1100. Positive = customer owes us. Negative = we owe customer
/// (overpayment / credit).
///
/// Walks merges: if a customer was merged into another, returns their
/// pre-merge legs only (post-merge balance lives on the surviving
/// customer).
pub async fn customer_ar_balance(wd: &WD, customer_id: i32) -> Result<Cents> {
    let legs: Vec<JournalLegRow> = store::read_series(wd, JOURNAL_PATH).await?;
    let mut bal: i64 = 0;
    for l in legs.iter() {
        if l.customer_id == Some(customer_id) && l.account_code == 1100 {
            bal += l.debit_cents - l.credit_cents;
        }
    }
    Ok(Cents::from_units(bal))
}

/// `accounts journal-show ...` -- read+filter.
#[allow(clippy::too_many_arguments)]
pub async fn show(
    wd: &WD,
    cycle: Option<i32>,
    customer: Option<i32>,
    connection: Option<i32>,
    account: Option<i32>,
    from: Option<NaiveDate>,
    to: Option<NaiveDate>,
    limit: usize,
) -> Result<()> {
    let legs: Vec<JournalLegRow> = store::read_series(wd, JOURNAL_PATH).await?;
    let mut shown = 0;
    log::info!(
        "{:>5}  {:>3}  {:<19}  {:>4}  {:<10}  {:>5}  {:>5}  {:>5}  {:>12}  {:>12}  source",
        "txn",
        "leg",
        "txn_date",
        "acct",
        "src",
        "cust",
        "conn",
        "cycle",
        "debit",
        "credit"
    );
    for l in legs.iter() {
        if let Some(c) = cycle
            && l.cycle_id != Some(c)
        {
            continue;
        }
        if let Some(c) = customer
            && l.customer_id != Some(c)
        {
            continue;
        }
        if let Some(c) = connection
            && l.connection_id != Some(c)
        {
            continue;
        }
        if let Some(a) = account
            && l.account_code != a
        {
            continue;
        }
        let leg_date = txn_date_to_naive(l.txn_date);
        if let Some(d) = from
            && leg_date < d
        {
            continue;
        }
        if let Some(d) = to
            && leg_date > d
        {
            continue;
        }
        log::info!(
            "{:>5}  {:>3}  {:<19}  {:>4}  {:<10}  {:>5}  {:>5}  {:>5}  {:>12}  {:>12}  {}",
            l.txn_id,
            l.leg_seq,
            leg_date,
            l.account_code,
            truncate(&l.source, 10),
            l.customer_id
                .map(|i| i.to_string())
                .unwrap_or_else(|| "-".into()),
            l.connection_id
                .map(|i| i.to_string())
                .unwrap_or_else(|| "-".into()),
            l.cycle_id
                .map(|i| i.to_string())
                .unwrap_or_else(|| "-".into()),
            Cents::from_units(l.debit_cents).display(),
            Cents::from_units(l.credit_cents).display(),
            l.source_ref.as_deref().unwrap_or("-"),
        );
        shown += 1;
        if shown >= limit {
            log::info!("(limit {limit} reached; pass --limit to see more)");
            break;
        }
    }
    Ok(())
}

/// `--from=YYYY-MM-DD` / `--to=YYYY-MM-DD` parsing helper used by handler
/// dispatch.
pub fn parse_date_filter(s: Option<&str>) -> Result<Option<NaiveDate>> {
    match s {
        Some(s) => Ok(Some(parse_ymd(s)?)),
        None => Ok(None),
    }
}

fn ymd_midnight_us(d: NaiveDate) -> i64 {
    let dt = NaiveDateTime::new(d, NaiveTime::from_hms_opt(0, 0, 0).expect("midnight"));
    dt.and_utc().timestamp_micros()
}

fn txn_date_to_naive(us: i64) -> NaiveDate {
    chrono::DateTime::<Utc>::from_timestamp_micros(us)
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

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup() -> WD {
        let fs = tinyfs::memory::new_fs().await;
        let wd = fs.root().await.expect("root");
        let _ = wd.create_dir_all("/data").await.expect("mkdir");
        wd
    }

    #[tokio::test]
    async fn first_txn_assigned_id_one() {
        let wd = setup().await;
        let id = write_transaction(
            &wd,
            NaiveDate::from_ymd_opt(2024, 10, 1).unwrap(),
            vec![
                LegSpec::debit(1000, Cents(100), "expense"),
                LegSpec::credit(5100, Cents(100), "expense"),
            ],
        )
        .await
        .expect("write");
        assert_eq!(id, 1);
    }

    #[tokio::test]
    async fn second_txn_assigned_id_two() {
        let wd = setup().await;
        let _ = write_transaction(
            &wd,
            NaiveDate::from_ymd_opt(2024, 10, 1).unwrap(),
            vec![
                LegSpec::debit(5100, Cents(100), "expense"),
                LegSpec::credit(1000, Cents(100), "expense"),
            ],
        )
        .await
        .expect("first");
        let id = write_transaction(
            &wd,
            NaiveDate::from_ymd_opt(2024, 10, 2).unwrap(),
            vec![
                LegSpec::debit(5200, Cents(50), "expense"),
                LegSpec::credit(1000, Cents(50), "expense"),
            ],
        )
        .await
        .expect("second");
        assert_eq!(id, 2);
    }

    #[tokio::test]
    async fn unbalanced_rejected() {
        let wd = setup().await;
        let err = write_transaction(
            &wd,
            NaiveDate::from_ymd_opt(2024, 10, 1).unwrap(),
            vec![
                LegSpec::debit(5100, Cents(100), "expense"),
                LegSpec::credit(1000, Cents(99), "expense"),
            ],
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn single_leg_rejected() {
        let wd = setup().await;
        let err = write_transaction(
            &wd,
            NaiveDate::from_ymd_opt(2024, 10, 1).unwrap(),
            vec![LegSpec::debit(5100, Cents(100), "expense")],
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn negative_amount_rejected() {
        let wd = setup().await;
        let err = write_transaction(
            &wd,
            NaiveDate::from_ymd_opt(2024, 10, 1).unwrap(),
            vec![
                LegSpec::debit(5100, Cents(-100), "expense"),
                LegSpec::credit(1000, Cents(-100), "expense"),
            ],
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn customer_balance_after_bill_and_payment() {
        let wd = setup().await;
        // Bill: DR 1100/Alice 5000; CR 4000 5000
        let _ = write_transaction(
            &wd,
            NaiveDate::from_ymd_opt(2024, 10, 1).unwrap(),
            vec![
                LegSpec::debit(1100, Cents(5000), "bill").with_customer(12),
                LegSpec::credit(4000, Cents(5000), "bill"),
            ],
        )
        .await
        .expect("bill");
        // Partial payment: DR 1000 3000; CR 1100/Alice 3000
        let _ = write_transaction(
            &wd,
            NaiveDate::from_ymd_opt(2024, 10, 5).unwrap(),
            vec![
                LegSpec::debit(1000, Cents(3000), "payment"),
                LegSpec::credit(1100, Cents(3000), "payment").with_customer(12),
            ],
        )
        .await
        .expect("pay");

        let bal = customer_ar_balance(&wd, 12).await.expect("balance");
        assert_eq!(bal, Cents(2000), "Alice owes $20.00 after partial payment");
    }
}
