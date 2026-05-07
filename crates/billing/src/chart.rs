// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Default chart of accounts seed.

use crate::schema::{CHART_OF_ACCOUNTS_PATH, ChartOfAccountsRow};
use crate::store;
use tinyfs::Result;
use tinyfs::WD;

/// The default chart, exactly per design doc section 4.
#[must_use]
pub fn default() -> Vec<ChartOfAccountsRow> {
    vec![
        coa(1000, "Cash", "asset", "debit", None),
        coa(1100, "Accounts Receivable", "asset", "debit", None),
        coa(
            2000,
            "Customer Deposits",
            "liability",
            "credit",
            Some("future use, e.g. overpayments"),
        ),
        coa(3000, "Owner's Equity", "equity", "credit", None),
        coa(3500, "Reserves (Margin)", "equity", "credit", None),
        coa(4000, "Service Revenue", "income", "credit", None),
        coa(5100, "Operations", "expense", "debit", None),
        coa(5200, "Utilities", "expense", "debit", None),
        coa(5300, "Insurance", "expense", "debit", None),
        coa(5400, "Taxes", "expense", "debit", None),
        coa(5900, "Other Expenses", "expense", "debit", None),
        coa(
            9000,
            "Suspense",
            "asset",
            "debit",
            Some("parking lot for bad data"),
        ),
    ]
}

/// Seed the chart-of-accounts file if it does not yet exist. Idempotent.
pub async fn seed(wd: &WD) -> Result<()> {
    let existing: Vec<ChartOfAccountsRow> = store::read_table(wd, CHART_OF_ACCOUNTS_PATH).await?;
    if !existing.is_empty() {
        return Ok(());
    }
    store::write_table(wd, CHART_OF_ACCOUNTS_PATH, &default()).await
}

fn coa(
    code: i32,
    name: &str,
    kind: &str,
    normal_side: &str,
    desc: Option<&str>,
) -> ChartOfAccountsRow {
    ChartOfAccountsRow {
        code,
        name: name.into(),
        kind: kind.into(),
        normal_side: normal_side.into(),
        description: desc.map(str::to_string),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_has_required_accounts() {
        let chart = default();
        for code in [1000, 1100, 3000, 4000, 5100, 5200, 5300, 5400] {
            assert!(
                chart.iter().any(|c| c.code == code),
                "missing required account {code}"
            );
        }
    }

    #[test]
    fn normal_side_matches_kind() {
        for c in default() {
            let expected = match c.kind.as_str() {
                "asset" | "expense" => "debit",
                "liability" | "equity" | "income" => "credit",
                _ => panic!("unknown kind {}", c.kind),
            };
            assert_eq!(c.normal_side, expected, "{} has wrong normal_side", c.code);
        }
    }

    #[tokio::test]
    async fn seed_is_idempotent() {
        let fs = tinyfs::memory::new_fs().await;
        let wd = fs.root().await.unwrap();
        let _ = wd.create_dir_all("/data").await.unwrap();
        seed(&wd).await.expect("seed1");
        seed(&wd).await.expect("seed2");
        let chart: Vec<ChartOfAccountsRow> = store::read_table(&wd, CHART_OF_ACCOUNTS_PATH)
            .await
            .unwrap();
        assert_eq!(chart.len(), default().len());
    }
}
