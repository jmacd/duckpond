// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `accounts adjust` -- hand-written balanced transaction.
//!
//! Each `--leg=ACCT[:CUSTOMER_ID]:DEBIT_CENTS:CREDIT_CENTS` parses to one
//! `LegSpec`. The journal writer enforces `Σ debit = Σ credit` and >= 2
//! legs.

use crate::cli::journal::{LegSpec, write_transaction};
use crate::cli::{CliError, Result};
use crate::dates::parse_ymd;
use crate::money::Cents;
use crate::schema::{CHART_OF_ACCOUNTS_PATH, ChartOfAccountsRow};
use crate::store;
use tinyfs::WD;

pub async fn run(wd: &WD, date: &str, memo: String, leg_strs: Vec<String>) -> Result<i32> {
    let date = parse_ymd(date)?;
    if leg_strs.is_empty() {
        return Err(CliError::Invalid(
            "adjust: at least one --leg required".into(),
        ));
    }
    let chart: Vec<ChartOfAccountsRow> = store::read_table(wd, CHART_OF_ACCOUNTS_PATH).await?;

    let mut legs = Vec::with_capacity(leg_strs.len());
    for (i, s) in leg_strs.iter().enumerate() {
        let leg = parse_leg(s, &chart)
            .map_err(|e| CliError::Invalid(format!("--leg #{} `{s}`: {e}", i + 1)))?
            .with_memo(memo.clone());
        legs.push(leg);
    }

    let txn = write_transaction(wd, date, legs).await?;
    log::info!("[OK] adjust posted txn #{txn}: {memo}");
    Ok(txn)
}

/// Parse `ACCT[:CUSTOMER_ID]:DEBIT_CENTS:CREDIT_CENTS`.
///
/// Forms accepted:
/// - `1100:12:5000:0`     -- AR debit on customer 12
/// - `4000::0:5000`        -- revenue credit, no customer (empty cust slot)
/// - `5100:0:5000` (3 fields) -- legacy short form; account, debit, credit; no customer
fn parse_leg(s: &str, chart: &[ChartOfAccountsRow]) -> std::result::Result<LegSpec, String> {
    let parts: Vec<&str> = s.split(':').collect();
    let (account_str, customer_str, debit_str, credit_str) = match parts.len() {
        4 => (parts[0], parts[1], parts[2], parts[3]),
        3 => (parts[0], "", parts[1], parts[2]),
        n => {
            return Err(format!("expected 3 or 4 colon-separated fields, got {n}"));
        }
    };
    let account_code: i32 = account_str
        .parse()
        .map_err(|_| format!("account `{account_str}` is not an integer"))?;
    if !chart.iter().any(|c| c.code == account_code) {
        return Err(format!("account {account_code} not in chart of accounts"));
    }
    let customer_id: Option<i32> = if customer_str.is_empty() {
        None
    } else {
        Some(
            customer_str
                .parse()
                .map_err(|_| format!("customer `{customer_str}` is not an integer"))?,
        )
    };
    let debit: i64 = debit_str
        .parse()
        .map_err(|_| format!("debit `{debit_str}` is not an integer"))?;
    let credit: i64 = credit_str
        .parse()
        .map_err(|_| format!("credit `{credit_str}` is not an integer"))?;
    if debit < 0 || credit < 0 {
        return Err("negative cents not allowed; flip sides instead".into());
    }
    if debit > 0 && credit > 0 {
        return Err("a leg cannot have both debit and credit non-zero".into());
    }
    if debit == 0 && credit == 0 {
        return Err("a leg cannot be zero".into());
    }
    let mut leg = LegSpec {
        account_code,
        customer_id,
        connection_id: None,
        cycle_id: None,
        debit: Cents::from_units(debit),
        credit: Cents::from_units(credit),
        source: "adjustment",
        source_ref: None,
        memo: None,
    };
    leg.source_ref = Some(format!("adjust:acct={account_code}"));
    Ok(leg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chart::seed;
    use crate::schema::{JOURNAL_PATH, JournalLegRow};

    async fn setup() -> WD {
        let fs = tinyfs::memory::new_fs().await;
        let wd = fs.root().await.expect("root");
        let _ = wd.create_dir_all("/data").await.expect("mkdir");
        seed(&wd).await.expect("seed");
        wd
    }

    #[tokio::test]
    async fn balanced_adjust_posts_txn() {
        let wd = setup().await;
        let txn = run(
            &wd,
            "2024-10-01",
            "manual fix".into(),
            vec!["1100:12:5000:0".into(), "4000::0:5000".into()],
        )
        .await
        .expect("adjust");
        assert_eq!(txn, 1);
        let legs: Vec<JournalLegRow> = store::read_series(&wd, JOURNAL_PATH).await.unwrap();
        assert_eq!(legs.len(), 2);
        assert!(legs.iter().all(|l| l.source == "adjustment"));
        let ar = legs.iter().find(|l| l.account_code == 1100).unwrap();
        assert_eq!(ar.customer_id, Some(12));
        assert_eq!(ar.debit_cents, 5000);
    }

    #[tokio::test]
    async fn unbalanced_rejected() {
        let wd = setup().await;
        let err = run(
            &wd,
            "2024-10-01",
            "off".into(),
            vec!["1100::5000:0".into(), "4000::0:4999".into()],
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn unknown_account_rejected() {
        let wd = setup().await;
        let err = run(
            &wd,
            "2024-10-01",
            "x".into(),
            vec!["9999:0:1000:0".into(), "4000::0:1000".into()],
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn three_field_short_form_works() {
        let wd = setup().await;
        let _ = run(
            &wd,
            "2024-10-01",
            "x".into(),
            vec!["5100:1000:0".into(), "1000:0:1000".into()],
        )
        .await
        .expect("short form");
    }

    #[test]
    fn parse_leg_variants() {
        let chart = vec![
            ChartOfAccountsRow {
                code: 1100,
                name: "AR".into(),
                kind: "asset".into(),
                normal_side: "debit".into(),
                description: None,
            },
            ChartOfAccountsRow {
                code: 4000,
                name: "Rev".into(),
                kind: "income".into(),
                normal_side: "credit".into(),
                description: None,
            },
        ];
        // 4-field with customer
        let l = parse_leg("1100:12:5000:0", &chart).unwrap();
        assert_eq!(l.account_code, 1100);
        assert_eq!(l.customer_id, Some(12));
        assert_eq!(l.debit, Cents(5000));
        assert_eq!(l.credit, Cents(0));
        // 4-field no customer
        let l = parse_leg("4000::0:5000", &chart).unwrap();
        assert_eq!(l.customer_id, None);
        assert_eq!(l.credit, Cents(5000));
        // 3-field short form
        let l = parse_leg("4000:0:5000", &chart).unwrap();
        assert_eq!(l.customer_id, None);
        assert_eq!(l.credit, Cents(5000));
        // both sides positive
        assert!(parse_leg("1100::100:100", &chart).is_err());
        // both zero
        assert!(parse_leg("1100::0:0", &chart).is_err());
        // unknown account
        assert!(parse_leg("9999::100:0", &chart).is_err());
    }
}
