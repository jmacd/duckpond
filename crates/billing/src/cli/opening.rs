// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `accounts opening-balance` -- one-time AR roll-forward for a customer.
//!
//! Posts:
//!   DR 1100 (AR) [customer_id] amount
//!   CR 3000 (Owner's Equity)   amount
//! (with both sides flipped if `amount` is negative -- a credit balance).
//!
//! Refused if the customer already has any non-`opening` activity in the
//! journal (any leg whose `source != 'opening'`). Operators can run this
//! exactly once per customer when they migrate from the legacy CSV books.

use crate::cli::journal::{LegSpec, write_transaction};
use crate::cli::{CliError, Result};
use crate::dates::parse_ymd;
use crate::lookup::find_one;
use crate::money::Cents;
use crate::schema::{CUSTOMERS_PATH, CustomerRow, JOURNAL_PATH, JournalLegRow};
use crate::store;
use chrono::Local;
use tinyfs::WD;

const AR_ACCOUNT: i32 = 1100;
const EQUITY_ACCOUNT: i32 = 3000;

pub async fn run(
    wd: &WD,
    customer_query: &str,
    amount: &str,
    date: Option<&str>,
    memo: Option<String>,
) -> Result<i32> {
    let amount =
        Cents::parse_dollars(amount).map_err(|e| CliError::Invalid(format!("--amount: {e}")))?;
    if amount.units() == 0 {
        return Err(CliError::Invalid(
            "opening-balance amount cannot be zero".into(),
        ));
    }
    let date = match date {
        Some(s) => parse_ymd(s)?,
        None => Local::now().date_naive(),
    };

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
            "customer #{} is merged into #{into}; opening-balance the surviving customer instead",
            cust.customer_id
        )));
    }
    let customer_id = cust.customer_id;
    let customer_name = cust.name.clone();

    // Refuse if any non-opening activity already exists for this customer.
    let legs: Vec<JournalLegRow> = store::read_series(wd, JOURNAL_PATH).await?;
    if let Some(other) = legs
        .iter()
        .find(|l| l.customer_id == Some(customer_id) && l.source != "opening")
    {
        return Err(CliError::Invalid(format!(
            "customer #{customer_id} {customer_name} already has {} activity (txn #{}); \
             opening-balance is for one-time roll-forward only",
            other.source, other.txn_id
        )));
    }
    if legs
        .iter()
        .any(|l| l.customer_id == Some(customer_id) && l.source == "opening")
    {
        return Err(CliError::Invalid(format!(
            "customer #{customer_id} {customer_name} already has an opening-balance posted"
        )));
    }

    // Positive amount = customer owes us: DR 1100; CR 3000.
    // Negative amount = we owe customer (credit on AR): DR 3000; CR 1100.
    let positive = amount.units() > 0;
    let mag = Cents::from_units(amount.units().unsigned_abs() as i64);
    let memo_text = memo
        .clone()
        .unwrap_or_else(|| "opening balance".to_string());
    let legs_to_post = if positive {
        vec![
            LegSpec::debit(AR_ACCOUNT, mag, "opening")
                .with_customer(customer_id)
                .with_source_ref(format!("opening:customer={customer_id}"))
                .with_memo(memo_text.clone()),
            LegSpec::credit(EQUITY_ACCOUNT, mag, "opening")
                .with_source_ref(format!("opening:customer={customer_id}")),
        ]
    } else {
        vec![
            LegSpec::debit(EQUITY_ACCOUNT, mag, "opening")
                .with_source_ref(format!("opening:customer={customer_id}"))
                .with_memo(memo_text.clone()),
            LegSpec::credit(AR_ACCOUNT, mag, "opening")
                .with_customer(customer_id)
                .with_source_ref(format!("opening:customer={customer_id}")),
        ]
    };
    let txn = write_transaction(wd, date, legs_to_post).await?;
    log::info!(
        "[OK] opening-balance #{customer_id} {customer_name} {} -> txn #{txn}",
        amount.display()
    );
    Ok(txn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chart::seed;
    use crate::cli::customer as cust_cli;
    use crate::cli::payment as pay_cli;

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
    async fn positive_amount_posts_dr_ar_cr_equity() {
        let (wd, alice) = setup().await;
        let _ = run(&wd, &alice.to_string(), "$100.00", Some("2024-01-01"), None)
            .await
            .expect("opening");

        let legs: Vec<JournalLegRow> = store::read_series(&wd, JOURNAL_PATH).await.unwrap();
        assert_eq!(legs.len(), 2);
        let dr = legs.iter().find(|l| l.account_code == AR_ACCOUNT).unwrap();
        let cr = legs
            .iter()
            .find(|l| l.account_code == EQUITY_ACCOUNT)
            .unwrap();
        assert_eq!(dr.debit_cents, 10000);
        assert_eq!(dr.customer_id, Some(alice));
        assert_eq!(cr.credit_cents, 10000);
        assert_eq!(dr.source, "opening");
    }

    #[tokio::test]
    async fn negative_amount_posts_dr_equity_cr_ar() {
        let (wd, alice) = setup().await;
        let _ = run(&wd, &alice.to_string(), "-$25.00", Some("2024-01-01"), None)
            .await
            .expect("credit-balance opening");

        let legs: Vec<JournalLegRow> = store::read_series(&wd, JOURNAL_PATH).await.unwrap();
        let dr = legs.iter().find(|l| l.debit_cents > 0).unwrap();
        let cr = legs.iter().find(|l| l.credit_cents > 0).unwrap();
        assert_eq!(dr.account_code, EQUITY_ACCOUNT);
        assert_eq!(cr.account_code, AR_ACCOUNT);
        assert_eq!(cr.customer_id, Some(alice));
    }

    #[tokio::test]
    async fn refused_if_payment_already_recorded() {
        let (wd, alice) = setup().await;
        let _ = pay_cli::add(
            &wd,
            "2024-01-15",
            &alice.to_string(),
            "$10.00",
            "check".into(),
            None,
        )
        .await
        .expect("payment");
        let err = run(&wd, &alice.to_string(), "$100.00", Some("2024-01-01"), None)
            .await
            .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn refused_if_already_opening() {
        let (wd, alice) = setup().await;
        let _ = run(&wd, &alice.to_string(), "$100.00", Some("2024-01-01"), None)
            .await
            .expect("first");
        let err = run(&wd, &alice.to_string(), "$50.00", Some("2024-01-02"), None)
            .await
            .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn zero_amount_rejected() {
        let (wd, alice) = setup().await;
        let err = run(&wd, &alice.to_string(), "$0.00", None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }
}
