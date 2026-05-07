// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Tenancy resolver: who is responsible for which connection on a date.

use crate::cli::Result;
use crate::schema::{TENANCIES_PATH, TenancyRow};
use crate::store;
use chrono::NaiveDate;
use std::collections::HashMap;
use tinyfs::WD;

/// Build a `connection_id -> customer_id` map for connections that have a
/// covering tenancy on `on_date`. Connections with no covering tenancy
/// are absent from the map.
///
/// A tenancy "covers" `on_date` when
/// `start_date <= on_date < (end_date OR +infinity)`.
///
/// **Invariant** (asserted): at most one tenancy covers a given connection
/// on a given date. Phase-2 invariants on the tenancy CLI guarantee this
/// for normal operator workflows; if the file is corrupt the function
/// returns an error naming the offending connection.
pub async fn responsible_customer_map(wd: &WD, on_date: NaiveDate) -> Result<HashMap<i32, i32>> {
    let tenancies: Vec<TenancyRow> = store::read_table(wd, TENANCIES_PATH).await?;
    let mut out = HashMap::new();
    for t in &tenancies {
        if covers(t, on_date)
            && let Some(prior) = out.insert(t.connection_id, t.customer_id)
        {
            return Err(crate::cli::CliError::Invalid(format!(
                "tenancy invariant violated: connection #{} has two covering \
                 tenancies on {} (customer #{} and customer #{})",
                t.connection_id, on_date, prior, t.customer_id
            )));
        }
    }
    Ok(out)
}

fn covers(t: &TenancyRow, d: NaiveDate) -> bool {
    if d < t.start_date {
        return false;
    }
    match t.end_date {
        Some(end) => d <= end,
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(
        id: i32,
        conn: i32,
        cust: i32,
        start: (i32, u32, u32),
        end: Option<(i32, u32, u32)>,
    ) -> TenancyRow {
        TenancyRow {
            tenancy_id: id,
            connection_id: conn,
            customer_id: cust,
            start_date: NaiveDate::from_ymd_opt(start.0, start.1, start.2).unwrap(),
            end_date: end.map(|(y, m, d)| NaiveDate::from_ymd_opt(y, m, d).unwrap()),
            notes: None,
        }
    }

    async fn setup(rows: Vec<TenancyRow>) -> WD {
        let fs = tinyfs::memory::new_fs().await;
        let wd = fs.root().await.unwrap();
        let _ = wd.create_dir_all("/data").await.unwrap();
        store::write_table(&wd, TENANCIES_PATH, &rows)
            .await
            .unwrap();
        wd
    }

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    #[tokio::test]
    async fn open_tenancy_covers_indefinitely() {
        let wd = setup(vec![t(1, 5, 12, (2021, 10, 1), None)]).await;
        let m = responsible_customer_map(&wd, d(2030, 1, 1)).await.unwrap();
        assert_eq!(m.get(&5), Some(&12));
    }

    #[tokio::test]
    async fn closed_tenancy_covers_inclusive_of_end() {
        let wd = setup(vec![t(1, 5, 12, (2021, 10, 1), Some((2024, 6, 30)))]).await;
        let m = responsible_customer_map(&wd, d(2024, 6, 30)).await.unwrap();
        assert_eq!(m.get(&5), Some(&12));
        let m2 = responsible_customer_map(&wd, d(2024, 7, 1)).await.unwrap();
        assert!(!m2.contains_key(&5));
    }

    #[tokio::test]
    async fn date_before_start_excludes() {
        let wd = setup(vec![t(1, 5, 12, (2021, 10, 1), None)]).await;
        let m = responsible_customer_map(&wd, d(2020, 1, 1)).await.unwrap();
        assert!(m.is_empty());
    }

    #[tokio::test]
    async fn two_consecutive_tenancies_pick_correct_one() {
        let wd = setup(vec![
            t(1, 5, 12, (2021, 1, 1), Some((2024, 12, 31))),
            t(2, 5, 99, (2025, 1, 1), None),
        ])
        .await;
        let m_old = responsible_customer_map(&wd, d(2024, 6, 1)).await.unwrap();
        assert_eq!(m_old.get(&5), Some(&12));
        let m_new = responsible_customer_map(&wd, d(2025, 6, 1)).await.unwrap();
        assert_eq!(m_new.get(&5), Some(&99));
    }

    #[tokio::test]
    async fn overlapping_tenancies_error() {
        let wd = setup(vec![
            t(1, 5, 12, (2021, 1, 1), Some((2025, 1, 1))),
            t(2, 5, 99, (2024, 1, 1), Some((2025, 6, 1))),
        ])
        .await;
        let err = responsible_customer_map(&wd, d(2024, 6, 1))
            .await
            .unwrap_err();
        assert!(matches!(err, crate::cli::CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn empty_tenancies_yields_empty_map() {
        let wd = setup(vec![]).await;
        let m = responsible_customer_map(&wd, d(2024, 1, 1)).await.unwrap();
        assert!(m.is_empty());
    }
}
