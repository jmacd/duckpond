// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `accounts payment ...` -- add/void/list.
//!
//! `payment add` writes one row to `payments.series` AND posts:
//!   DR 1000 (Cash) amount
//!   CR 1100 (AR) amount [tagged with customer_id]
//!
//! `payment void` writes a reversing payment + reversing journal txn.

use crate::cli::journal::{LegSpec, write_transaction};
use crate::cli::{CliError, Result};
use crate::dates::parse_ymd;
use crate::lookup::find_one;
use crate::money::Cents;
use crate::schema::{CUSTOMERS_PATH, CustomerRow, PAYMENTS_PATH, PaymentRow};
#[cfg(test)]
use crate::schema::{JOURNAL_PATH, JournalLegRow};
use crate::store;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Utc};
use tinyfs::WD;

const CASH_ACCOUNT: i32 = 1000;
const AR_ACCOUNT: i32 = 1100;

pub async fn add(
    wd: &WD,
    date: &str,
    customer_query: &str,
    amount: &str,
    method: String,
    memo: Option<String>,
) -> Result<i32> {
    let received_date = parse_ymd(date)?;
    let amount =
        Cents::parse_dollars(amount).map_err(|e| CliError::Invalid(format!("--amount: {e}")))?;
    if amount.units() <= 0 {
        return Err(CliError::Invalid(
            "payment amount must be positive (use `payment void` to reverse)".into(),
        ));
    }
    let customers: Vec<CustomerRow> = store::read_table(wd, CUSTOMERS_PATH).await?;
    let (_, cust) = find_one(
        "customer",
        customer_query,
        &customers,
        |c| c.customer_id,
        |c| &c.name,
    )?;
    if let Some(into) = cust.merged_into_customer_id {
        return Err(CliError::Invalid(format!(
            "customer #{} {} is merged into #{into}; record the payment against the surviving customer",
            cust.customer_id, cust.name
        )));
    }
    let customer_id = cust.customer_id;
    let customer_name = cust.name.clone();

    let existing: Vec<PaymentRow> = store::read_series(wd, PAYMENTS_PATH).await?;
    let payment_id = existing.iter().map(|p| p.payment_id).max().unwrap_or(0) + 1;
    let row = PaymentRow {
        payment_id,
        received_date: ymd_midnight_us(received_date),
        customer_id,
        amount_cents: amount.units(),
        method: Some(method.clone()),
        memo: memo.clone(),
        void_of: None,
    };
    store::write_series(wd, PAYMENTS_PATH, &[row], PaymentRow::TIMESTAMP_COLUMN).await?;

    let txn = write_transaction(
        wd,
        received_date,
        vec![
            LegSpec::debit(CASH_ACCOUNT, amount, "payment")
                .with_source_ref(format!("payment:{payment_id}"))
                .with_memo(format!("{customer_name} ({method})")),
            LegSpec::credit(AR_ACCOUNT, amount, "payment")
                .with_customer(customer_id)
                .with_source_ref(format!("payment:{payment_id}")),
        ],
    )
    .await?;

    log::info!(
        "[OK] payment add #{payment_id} {} from #{customer_id} {customer_name} ({method}) -> txn #{txn}",
        amount.display()
    );
    Ok(payment_id)
}

pub async fn void(wd: &WD, id: i32, memo: Option<String>) -> Result<i32> {
    let existing: Vec<PaymentRow> = store::read_series(wd, PAYMENTS_PATH).await?;
    let target = existing
        .iter()
        .find(|p| p.payment_id == id)
        .ok_or_else(|| CliError::Invalid(format!("payment #{id} not found")))?;
    if target.void_of.is_some() {
        return Err(CliError::Invalid(format!(
            "payment #{id} is itself a void; nothing to void"
        )));
    }
    if existing.iter().any(|p| p.void_of == Some(id)) {
        return Err(CliError::Invalid(format!(
            "payment #{id} is already voided"
        )));
    }
    // Refuse if the original payment's customer has been merged. The
    // reversal would post a CR back to the merged-from id, reopening AR
    // there. Operator must `customer merge` direction is one-way; voiding
    // historical payments after merge requires a manual `adjust`.
    let customers: Vec<CustomerRow> = store::read_table(wd, CUSTOMERS_PATH).await?;
    if let Some(c) = customers
        .iter()
        .find(|c| c.customer_id == target.customer_id)
        && let Some(into) = c.merged_into_customer_id
    {
        return Err(CliError::Invalid(format!(
            "payment #{id} belongs to customer #{} {} which is merged into #{into}; \
             use `adjust` to handle this case manually",
            target.customer_id, c.name
        )));
    }

    let void_id = existing.iter().map(|p| p.payment_id).max().unwrap_or(0) + 1;
    let memo_text = memo
        .clone()
        .unwrap_or_else(|| format!("void of payment #{id}"));
    let row = PaymentRow {
        payment_id: void_id,
        received_date: target.received_date,
        customer_id: target.customer_id,
        amount_cents: -target.amount_cents,
        method: target.method.clone(),
        memo: Some(memo_text.clone()),
        void_of: Some(id),
    };
    store::write_series(wd, PAYMENTS_PATH, &[row], PaymentRow::TIMESTAMP_COLUMN).await?;

    // Reverse: original = DR 1000; CR 1100/customer. Reversal swaps to
    //   DR 1100/customer; CR 1000  (reinstates the receivable, removes
    //   cash on hand).
    let amount = Cents::from_units(target.amount_cents);
    let txn = write_transaction(
        wd,
        us_to_naive_date(target.received_date),
        vec![
            LegSpec::debit(AR_ACCOUNT, amount, "reversal")
                .with_customer(target.customer_id)
                .with_source_ref(format!("reversal-of:payment={id}"))
                .with_memo(memo_text),
            LegSpec::credit(CASH_ACCOUNT, amount, "reversal")
                .with_source_ref(format!("reversal-of:payment={id}")),
        ],
    )
    .await?;

    log::info!(
        "[OK] payment void #{void_id} reverses #{id} {} -> txn #{txn}",
        amount.display()
    );
    Ok(void_id)
}

pub async fn list(
    wd: &WD,
    customer_query: Option<&str>,
    _cycle: Option<&str>,
    from: Option<NaiveDate>,
    to: Option<NaiveDate>,
) -> Result<()> {
    let payments: Vec<PaymentRow> = store::read_series(wd, PAYMENTS_PATH).await?;
    let customers: Vec<CustomerRow> = store::read_table(wd, CUSTOMERS_PATH).await?;

    let customer_filter: Option<i32> = match customer_query {
        Some(q) => Some(
            find_one("customer", q, &customers, |c| c.customer_id, |c| &c.name)?
                .1
                .customer_id,
        ),
        None => None,
    };

    log::info!(
        "{:>4}  {:<12}  {:>4}  {:<20}  {:>12}  {:<10}  {:>5}  memo",
        "id",
        "received",
        "cust",
        "name",
        "amount",
        "method",
        "void"
    );
    for p in payments.iter() {
        if let Some(c) = customer_filter
            && p.customer_id != c
        {
            continue;
        }
        let date = us_to_naive_date(p.received_date);
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
        let cust_name = customers
            .iter()
            .find(|c| c.customer_id == p.customer_id)
            .map(|c| c.name.as_str())
            .unwrap_or("(orphan)");
        log::info!(
            "{:>4}  {:<12}  {:>4}  {:<20}  {:>12}  {:<10}  {:>5}  {}",
            p.payment_id,
            date,
            p.customer_id,
            truncate(cust_name, 20),
            Cents::from_units(p.amount_cents).display(),
            p.method.as_deref().unwrap_or("-"),
            p.void_of
                .map(|i| format!("v#{i}"))
                .unwrap_or_else(|| "-".into()),
            p.memo.as_deref().unwrap_or(""),
        );
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
    use crate::cli::customer as cust_cli;

    async fn setup() -> (WD, i32) {
        let fs = tinyfs::memory::new_fs().await;
        let wd = fs.root().await.expect("root");
        let _ = wd.create_dir_all("/data").await.expect("mkdir");
        seed(&wd).await.expect("seed chart");
        let alice = cust_cli::add(&wd, "Alice".into(), "addr".into(), None, None)
            .await
            .expect("alice");
        (wd, alice)
    }

    #[tokio::test]
    async fn add_writes_payment_and_journal() {
        let (wd, alice) = setup().await;
        let id = add(
            &wd,
            "2024-10-15",
            &alice.to_string(),
            "$50.00",
            "check".into(),
            None,
        )
        .await
        .expect("add");
        assert_eq!(id, 1);

        let pays: Vec<PaymentRow> = store::read_series(&wd, PAYMENTS_PATH).await.unwrap();
        assert_eq!(pays.len(), 1);
        assert_eq!(pays[0].amount_cents, 5000);

        let legs: Vec<JournalLegRow> = store::read_series(&wd, JOURNAL_PATH).await.unwrap();
        assert_eq!(legs.len(), 2);
        let dr = legs
            .iter()
            .find(|l| l.account_code == CASH_ACCOUNT)
            .unwrap();
        let cr = legs.iter().find(|l| l.account_code == AR_ACCOUNT).unwrap();
        assert_eq!(dr.debit_cents, 5000);
        assert_eq!(cr.credit_cents, 5000);
        assert_eq!(cr.customer_id, Some(alice));
        assert_eq!(dr.source, "payment");
    }

    #[tokio::test]
    async fn add_into_merged_customer_rejected() {
        let (wd, alice) = setup().await;
        let bob = cust_cli::add(&wd, "Bob".into(), "addr".into(), None, None)
            .await
            .expect("bob");
        cust_cli::merge(&wd, &alice.to_string(), &bob.to_string())
            .await
            .expect("merge");
        let err = add(
            &wd,
            "2024-10-15",
            &alice.to_string(),
            "$1.00",
            "check".into(),
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn void_reverses_payment_and_journal() {
        let (wd, alice) = setup().await;
        let id = add(
            &wd,
            "2024-10-15",
            &alice.to_string(),
            "$50.00",
            "check".into(),
            None,
        )
        .await
        .unwrap();
        let void_id = void(&wd, id, None).await.expect("void");
        assert_eq!(void_id, 2);

        let pays: Vec<PaymentRow> = store::read_series(&wd, PAYMENTS_PATH).await.unwrap();
        let void_row = pays.iter().find(|p| p.payment_id == void_id).unwrap();
        assert_eq!(void_row.amount_cents, -5000);
        assert_eq!(void_row.void_of, Some(id));

        let legs: Vec<JournalLegRow> = store::read_series(&wd, JOURNAL_PATH).await.unwrap();
        let void_legs: Vec<&JournalLegRow> =
            legs.iter().filter(|l| l.source == "reversal").collect();
        assert_eq!(void_legs.len(), 2);
        // The reversal puts AR back UP for Alice.
        let ar_dr = void_legs
            .iter()
            .find(|l| l.account_code == AR_ACCOUNT)
            .unwrap();
        assert_eq!(ar_dr.debit_cents, 5000);
        assert_eq!(ar_dr.customer_id, Some(alice));
    }

    /// Per final rubber-duck (#2): voiding a payment whose customer was
    /// later merged would re-open AR on the merged-from id. Refuse.
    #[tokio::test]
    async fn void_refused_when_customer_merged() {
        let (wd, alice) = setup().await;
        let bob = cust_cli::add(&wd, "Bob".into(), "addr".into(), None, None)
            .await
            .unwrap();
        // Alice pays $50 BEFORE the merge.
        let pid = add(
            &wd,
            "2024-09-01",
            &alice.to_string(),
            "$50.00",
            "check".into(),
            None,
        )
        .await
        .unwrap();
        // Now merge Alice into Bob.
        cust_cli::merge(&wd, &alice.to_string(), &bob.to_string())
            .await
            .unwrap();
        // Voiding Alice's pre-merge payment is now refused.
        let err = void(&wd, pid, None).await.unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)), "got {err:?}");
        let msg = format!("{err}");
        assert!(msg.contains("merged into"), "got: {msg}");
    }
}
