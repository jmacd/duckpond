// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `accounts expense ...` -- add/void/import/list.
//!
//! `expense add` writes one row to `expenses.series` AND posts a balanced
//! 2-leg journal transaction:
//!   DR <account>  amount   (source=expense)
//!   CR 1000 (Cash) amount   (source=expense)
//!
//! `expense void` writes a reversing expense row (with `void_of=N`) AND
//! posts a reversing 2-leg journal transaction.

use crate::cli::journal::{LegSpec, write_transaction};
use crate::cli::{CliError, Result};
use crate::dates::parse_ymd;
use crate::money::Cents;
use crate::schema::{CHART_OF_ACCOUNTS_PATH, ChartOfAccountsRow, EXPENSES_PATH, ExpenseRow};
#[cfg(test)]
use crate::schema::{JOURNAL_PATH, JournalLegRow};
use crate::store;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Utc};
use tinyfs::WD;

const CASH_ACCOUNT: i32 = 1000;

pub async fn add(
    wd: &WD,
    date: &str,
    account: i32,
    amount: &str,
    vendor: Option<String>,
    memo: Option<String>,
    cycle_query: Option<&str>,
) -> Result<i32> {
    let paid_date = parse_ymd(date)?;
    let amount =
        Cents::parse_dollars(amount).map_err(|e| CliError::Invalid(format!("--amount: {e}")))?;
    if amount.units() <= 0 {
        return Err(CliError::Invalid(
            "expense amount must be positive (use `expense void` to reverse)".into(),
        ));
    }
    require_account_kind(wd, account, "expense").await?;

    let cycle_id = match cycle_query {
        Some(_q) => {
            // `--cycle` accepts an integer cycle_id only for now. Looking up
            // by name would require either propagating the resolver here or
            // exposing it in cli/cycle.rs as a public helper.
            cycle_query
                .and_then(|s| s.parse::<i32>().ok())
                .map(Some)
                .ok_or_else(|| {
                    CliError::Invalid("--cycle currently accepts an integer cycle_id only".into())
                })?
        }
        None => None,
    };

    let existing: Vec<ExpenseRow> = store::read_series(wd, EXPENSES_PATH).await?;
    let expense_id = existing.iter().map(|r| r.expense_id).max().unwrap_or(0) + 1;
    let row = ExpenseRow {
        expense_id,
        paid_date: ymd_midnight_us(paid_date),
        account_code: account,
        vendor: vendor.clone(),
        amount_cents: amount.units(),
        memo: memo.clone(),
        cycle_id,
        void_of: None,
    };

    // Append-only series semantics: each operator action shows up as a
    // distinct version in the audit log.
    store::write_series(wd, EXPENSES_PATH, &[row], ExpenseRow::TIMESTAMP_COLUMN).await?;

    // Post the journal transaction.
    let txn = write_transaction(
        wd,
        paid_date,
        vec![
            LegSpec::debit(account, amount, "expense")
                .with_source_ref(format!("expense:{expense_id}"))
                .with_memo(format_expense_memo(vendor.as_deref(), memo.as_deref())),
            LegSpec::credit(CASH_ACCOUNT, amount, "expense")
                .with_source_ref(format!("expense:{expense_id}")),
        ],
    )
    .await?;

    log::info!(
        "[OK] expense add #{expense_id} {} acct={account} {} -> txn #{txn}",
        amount.display(),
        vendor.as_deref().unwrap_or("")
    );
    Ok(expense_id)
}

pub async fn void(wd: &WD, id: i32, memo: Option<String>) -> Result<i32> {
    let existing: Vec<ExpenseRow> = store::read_series(wd, EXPENSES_PATH).await?;
    let target = existing
        .iter()
        .find(|e| e.expense_id == id)
        .ok_or_else(|| CliError::Invalid(format!("expense #{id} not found")))?;
    if target.void_of.is_some() {
        return Err(CliError::Invalid(format!(
            "expense #{id} is itself a void; nothing to void"
        )));
    }
    if existing.iter().any(|e| e.void_of == Some(id)) {
        return Err(CliError::Invalid(format!(
            "expense #{id} is already voided"
        )));
    }

    let void_id = existing.iter().map(|e| e.expense_id).max().unwrap_or(0) + 1;
    let memo_text = memo
        .clone()
        .unwrap_or_else(|| format!("void of expense #{id}"));
    let row = ExpenseRow {
        expense_id: void_id,
        paid_date: target.paid_date,
        account_code: target.account_code,
        vendor: target.vendor.clone(),
        amount_cents: -target.amount_cents,
        memo: Some(memo_text.clone()),
        cycle_id: target.cycle_id,
        void_of: Some(id),
    };
    store::write_series(wd, EXPENSES_PATH, &[row], ExpenseRow::TIMESTAMP_COLUMN).await?;

    // Reversing journal transaction: original was
    //   DR <account>  amount; CR 1000 amount
    // Reversal swaps:
    //   DR 1000 amount; CR <account> amount
    let amount = Cents::from_units(target.amount_cents);
    let txn_date = target_naive_date(target);
    let txn = write_transaction(
        wd,
        txn_date,
        vec![
            LegSpec::debit(CASH_ACCOUNT, amount, "reversal")
                .with_source_ref(format!("reversal-of:expense={id}"))
                .with_memo(memo_text.clone()),
            LegSpec::credit(target.account_code, amount, "reversal")
                .with_source_ref(format!("reversal-of:expense={id}")),
        ],
    )
    .await?;

    log::info!(
        "[OK] expense void #{void_id} reverses #{id} {} -> txn #{txn}",
        amount.display()
    );
    Ok(void_id)
}

pub async fn list(
    wd: &WD,
    cycle: Option<i32>,
    account: Option<i32>,
    from: Option<NaiveDate>,
    to: Option<NaiveDate>,
) -> Result<()> {
    let existing: Vec<ExpenseRow> = store::read_series(wd, EXPENSES_PATH).await?;
    log::info!(
        "{:>4}  {:<12}  {:>4}  {:<20}  {:>12}  {:>5}  {:>5}  memo",
        "id",
        "paid",
        "acct",
        "vendor",
        "amount",
        "cycle",
        "void"
    );
    for e in existing.iter() {
        if let Some(c) = cycle
            && e.cycle_id != Some(c)
        {
            continue;
        }
        if let Some(a) = account
            && e.account_code != a
        {
            continue;
        }
        let date = us_to_naive_date(e.paid_date);
        if let Some(d) = from
            && date < d
        {
            continue;
        }
        if let Some(d) = to
            && date > d
        {
            continue;
        }
        log::info!(
            "{:>4}  {:<12}  {:>4}  {:<20}  {:>12}  {:>5}  {:>5}  {}",
            e.expense_id,
            date,
            e.account_code,
            truncate(e.vendor.as_deref().unwrap_or("-"), 20),
            Cents::from_units(e.amount_cents).display(),
            e.cycle_id
                .map(|i| i.to_string())
                .unwrap_or_else(|| "-".into()),
            e.void_of
                .map(|i| format!("v#{i}"))
                .unwrap_or_else(|| "-".into()),
            e.memo.as_deref().unwrap_or(""),
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

async fn require_account_kind(wd: &WD, account_code: i32, expected: &str) -> Result<()> {
    let chart: Vec<ChartOfAccountsRow> = store::read_table(wd, CHART_OF_ACCOUNTS_PATH).await?;
    let row = chart
        .iter()
        .find(|c| c.code == account_code)
        .ok_or_else(|| CliError::Invalid(format!("account code {account_code} not in chart")))?;
    if row.kind != expected {
        return Err(CliError::Invalid(format!(
            "account {account_code} {} is kind={}; expected kind={expected}",
            row.name, row.kind
        )));
    }
    Ok(())
}

fn ymd_midnight_us(d: NaiveDate) -> i64 {
    let dt = NaiveDateTime::new(d, NaiveTime::from_hms_opt(0, 0, 0).expect("midnight"));
    dt.and_utc().timestamp_micros()
}

fn us_to_naive_date(us: i64) -> NaiveDate {
    chrono::DateTime::<Utc>::from_timestamp_micros(us)
        .map(|dt| dt.date_naive())
        .unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch"))
}

fn target_naive_date(e: &ExpenseRow) -> NaiveDate {
    us_to_naive_date(e.paid_date)
}

fn format_expense_memo(vendor: Option<&str>, memo: Option<&str>) -> String {
    match (vendor, memo) {
        (Some(v), Some(m)) => format!("{v}: {m}"),
        (Some(v), None) => v.to_string(),
        (None, Some(m)) => m.to_string(),
        (None, None) => String::new(),
    }
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
    use crate::chart::seed;

    async fn setup() -> WD {
        let fs = tinyfs::memory::new_fs().await;
        let wd = fs.root().await.expect("root");
        let _ = wd.create_dir_all("/data").await.expect("mkdir");
        seed(&wd).await.expect("seed chart");
        wd
    }

    #[tokio::test]
    async fn add_writes_expense_and_journal() {
        let wd = setup().await;
        let id = add(
            &wd,
            "2024-10-01",
            5100,
            "$123.45",
            Some("ChemCo".into()),
            None,
            None,
        )
        .await
        .expect("add");
        assert_eq!(id, 1);

        let exps: Vec<ExpenseRow> = store::read_series(&wd, EXPENSES_PATH).await.unwrap();
        assert_eq!(exps.len(), 1);
        assert_eq!(exps[0].amount_cents, 12345);

        let legs: Vec<JournalLegRow> = store::read_series(&wd, JOURNAL_PATH).await.unwrap();
        assert_eq!(legs.len(), 2);
        let dr = legs.iter().find(|l| l.account_code == 5100).unwrap();
        let cr = legs
            .iter()
            .find(|l| l.account_code == CASH_ACCOUNT)
            .unwrap();
        assert_eq!(dr.debit_cents, 12345);
        assert_eq!(dr.credit_cents, 0);
        assert_eq!(cr.debit_cents, 0);
        assert_eq!(cr.credit_cents, 12345);
        assert_eq!(dr.txn_id, cr.txn_id, "legs share one txn_id");
        assert_eq!(dr.source, "expense");
    }

    #[tokio::test]
    async fn add_rejects_negative_amount() {
        let wd = setup().await;
        let err = add(&wd, "2024-10-01", 5100, "-$1.00", None, None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn add_rejects_non_expense_account() {
        let wd = setup().await;
        // 1000 is asset (Cash), not expense.
        let err = add(&wd, "2024-10-01", 1000, "$1.00", None, None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn void_creates_reversing_expense_and_journal() {
        let wd = setup().await;
        let id = add(&wd, "2024-10-01", 5100, "$50.00", None, None, None)
            .await
            .expect("add");
        let void_id = void(&wd, id, None).await.expect("void");
        assert_eq!(void_id, 2);

        let exps: Vec<ExpenseRow> = store::read_series(&wd, EXPENSES_PATH).await.unwrap();
        let void_row = exps.iter().find(|e| e.expense_id == void_id).unwrap();
        assert_eq!(void_row.amount_cents, -5000);
        assert_eq!(void_row.void_of, Some(id));

        let legs: Vec<JournalLegRow> = store::read_series(&wd, JOURNAL_PATH).await.unwrap();
        // 4 legs total: 2 from add, 2 from void.
        assert_eq!(legs.len(), 4);
        let void_legs: Vec<&JournalLegRow> =
            legs.iter().filter(|l| l.source == "reversal").collect();
        assert_eq!(void_legs.len(), 2);
        // Cash side now DEBITED back (reversal of original credit).
        assert!(
            void_legs
                .iter()
                .any(|l| l.account_code == CASH_ACCOUNT && l.debit_cents == 5000)
        );
        assert!(
            void_legs
                .iter()
                .any(|l| l.account_code == 5100 && l.credit_cents == 5000)
        );
    }

    #[tokio::test]
    async fn double_void_rejected() {
        let wd = setup().await;
        let id = add(&wd, "2024-10-01", 5100, "$50.00", None, None, None)
            .await
            .unwrap();
        let _ = void(&wd, id, None).await.unwrap();
        let err = void(&wd, id, None).await.unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn voiding_a_void_rejected() {
        let wd = setup().await;
        let id = add(&wd, "2024-10-01", 5100, "$50.00", None, None, None)
            .await
            .unwrap();
        let void_id = void(&wd, id, None).await.unwrap();
        let err = void(&wd, void_id, None).await.unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }
}
