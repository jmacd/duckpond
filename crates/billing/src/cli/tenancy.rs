// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `accounts tenancy ...` -- start/end/edit/list.
//!
//! Invariants enforced here:
//! - At most ONE open tenancy per connection at any time. `start` for a new
//!   tenancy auto-closes the prior open tenancy on that connection at
//!   `start_date - 1 day` (so they meet end-to-end, no gap, no overlap).
//! - No two tenancies for the same connection have overlapping date ranges
//!   ([start_date, end_date)).
//! - Both customer and connection must exist and be referenceable.
//! - `tenancy edit` is refused if the existing or proposed range covers
//!   the bill_date of any issued cycle (operator must `bills reverse`
//!   first).

use crate::cli::{CliError, Result};
use crate::dates::{fmt_ymd_opt, parse_ymd};
use crate::lookup::find_one;
use crate::schema::{
    CONNECTIONS_PATH, CUSTOMERS_PATH, CYCLES_PATH, ConnectionRow, CustomerRow, CycleRow,
    TENANCIES_PATH, TenancyRow,
};
use crate::store;
use chrono::{Duration, NaiveDate};
use tinyfs::WD;

pub async fn start(
    wd: &WD,
    connection_q: &str,
    customer_q: &str,
    start_str: &str,
    notes: Option<String>,
) -> Result<i32> {
    let start_date = parse_ymd(start_str)?;
    let connections: Vec<ConnectionRow> = store::read_table(wd, CONNECTIONS_PATH).await?;
    let customers: Vec<CustomerRow> = store::read_table(wd, CUSTOMERS_PATH).await?;
    let (_, conn) = find_one(
        "connection",
        connection_q,
        &connections,
        |c| c.connection_id,
        |c| &c.name,
    )?;
    let (_, cust) = find_one(
        "customer",
        customer_q,
        &customers,
        |c| c.customer_id,
        |c| &c.name,
    )?;
    if !cust.active {
        return Err(CliError::Invalid(format!(
            "customer #{} {} is inactive; reactivate or merge target instead",
            cust.customer_id, cust.name
        )));
    }
    if cust.merged_into_customer_id.is_some() {
        return Err(CliError::Invalid(format!(
            "customer #{} {} is merged; use the surviving customer",
            cust.customer_id, cust.name
        )));
    }
    if start_date < conn.first_active {
        return Err(CliError::Invalid(format!(
            "tenancy start {start_date} precedes connection #{} first_active {}",
            conn.connection_id, conn.first_active
        )));
    }
    if let Some(retired) = conn.last_active
        && start_date >= retired
    {
        return Err(CliError::Invalid(format!(
            "connection #{} retired on {retired}; cannot start tenancy on or after",
            conn.connection_id
        )));
    }

    let connection_id = conn.connection_id;
    let customer_id = cust.customer_id;

    let mut tenancies: Vec<TenancyRow> = store::read_table(wd, TENANCIES_PATH).await?;

    // Auto-close prior open tenancy on the same connection at start_date - 1 day.
    let prev_close_date = start_date
        .checked_sub_signed(Duration::days(1))
        .ok_or_else(|| CliError::Invalid("start date underflows on auto-close".into()))?;
    let mut auto_closed: Option<i32> = None;
    for t in tenancies.iter_mut() {
        if t.connection_id == connection_id && t.end_date.is_none() {
            if t.start_date > prev_close_date {
                return Err(CliError::Invalid(format!(
                    "open tenancy #{} starts {} > new start {}; refusing to auto-close into the past",
                    t.tenancy_id, t.start_date, start_date
                )));
            }
            t.end_date = Some(prev_close_date);
            auto_closed = Some(t.tenancy_id);
        }
    }

    // Reject any overlap with closed tenancies on the same connection.
    for t in tenancies
        .iter()
        .filter(|t| t.connection_id == connection_id && Some(t.tenancy_id) != auto_closed)
    {
        let t_end = t.end_date.unwrap_or(NaiveDate::MAX);
        // open interval is [start_date, +inf); existing is [t.start_date, t_end].
        // overlap iff start_date <= t_end && (open intervals -- already auto-closed
        // the only-possible open one above, so all remaining are closed).
        if start_date <= t_end && t.start_date <= NaiveDate::MAX {
            return Err(CliError::Invalid(format!(
                "tenancy start {start_date} overlaps existing tenancy #{} ({} - {})",
                t.tenancy_id,
                t.start_date,
                fmt_ymd_opt(t.end_date)
            )));
        }
    }

    let tenancy_id = store::next_id(&tenancies, |t| t.tenancy_id);
    let row = TenancyRow {
        tenancy_id,
        connection_id,
        customer_id,
        start_date,
        end_date: None,
        notes,
    };
    log::info!(
        "[OK] tenancy start #{} connection #{} customer #{} start={}{}",
        row.tenancy_id,
        row.connection_id,
        row.customer_id,
        row.start_date,
        match auto_closed {
            Some(prior) => format!(" (auto-closed prior tenancy #{prior} on {prev_close_date})"),
            None => String::new(),
        }
    );
    tenancies.push(row);
    store::write_table(wd, TENANCIES_PATH, &tenancies).await?;
    Ok(tenancy_id)
}

pub async fn end(
    wd: &WD,
    connection_q: Option<&str>,
    tenancy_id: Option<i32>,
    end_str: &str,
    notes: Option<String>,
) -> Result<()> {
    let end_date = parse_ymd(end_str)?;
    let mut tenancies: Vec<TenancyRow> = store::read_table(wd, TENANCIES_PATH).await?;

    let idx = match (tenancy_id, connection_q) {
        (Some(id), _) => tenancies
            .iter()
            .position(|t| t.tenancy_id == id)
            .ok_or_else(|| CliError::Invalid(format!("tenancy #{id} not found")))?,
        (None, Some(q)) => {
            let connections: Vec<ConnectionRow> = store::read_table(wd, CONNECTIONS_PATH).await?;
            let (_, conn) = find_one(
                "connection",
                q,
                &connections,
                |c| c.connection_id,
                |c| &c.name,
            )?;
            tenancies
                .iter()
                .position(|t| t.connection_id == conn.connection_id && t.end_date.is_none())
                .ok_or_else(|| {
                    CliError::Invalid(format!(
                        "no open tenancy on connection #{}",
                        conn.connection_id
                    ))
                })?
        }
        (None, None) => {
            return Err(CliError::Invalid(
                "tenancy end: pass --tenancy=<id> or --connection=<id|name>".into(),
            ));
        }
    };

    if let Some(prev) = tenancies[idx].end_date {
        return Err(CliError::Invalid(format!(
            "tenancy #{} already ended on {prev}",
            tenancies[idx].tenancy_id
        )));
    }
    if end_date < tenancies[idx].start_date {
        return Err(CliError::Invalid(format!(
            "end {end_date} < start {}",
            tenancies[idx].start_date
        )));
    }
    tenancies[idx].end_date = Some(end_date);
    if let Some(n) = notes {
        tenancies[idx].notes = Some(n);
    }
    log::info!(
        "[OK] tenancy end #{} on {end_date}",
        tenancies[idx].tenancy_id
    );
    store::write_table(wd, TENANCIES_PATH, &tenancies).await?;
    Ok(())
}

pub async fn edit(
    wd: &WD,
    tenancy_id: i32,
    start: Option<&str>,
    end: Option<&str>,
    customer_q: Option<&str>,
    notes: Option<String>,
) -> Result<()> {
    let mut tenancies: Vec<TenancyRow> = store::read_table(wd, TENANCIES_PATH).await?;
    let idx = tenancies
        .iter()
        .position(|t| t.tenancy_id == tenancy_id)
        .ok_or_else(|| CliError::Invalid(format!("tenancy #{tenancy_id} not found")))?;

    // Refuse the edit if this tenancy covers the bill_date of any issued
    // cycle. The operator must `bills reverse` first, edit, then re-issue.
    // We check using BOTH the existing AND the proposed date range, so a
    // shrinking edit that no longer covers an issued cycle is also caught
    // (because it changes the historical fact that was billed).
    let cycles: Vec<CycleRow> = store::read_table(wd, CYCLES_PATH).await?;
    let existing = &tenancies[idx];
    if let Some(bad) = cycles
        .iter()
        .find(|c| c.issued && c.policy_id != 0 && tenancy_covers(existing, c.bill_date))
    {
        return Err(CliError::Invalid(format!(
            "tenancy #{tenancy_id} covers bill_date {} of issued cycle `{}` (txn #{:?}); \
             use `bills reverse` first",
            bad.bill_date, bad.name, bad.bill_txn_id
        )));
    }

    let mut new_row = tenancies[idx].clone();
    if let Some(s) = start {
        new_row.start_date = parse_ymd(s)?;
    }
    if let Some(e) = end {
        // Empty string is the operator's signal to clear end_date (re-open).
        if e.is_empty() || e == "-" {
            new_row.end_date = None;
        } else {
            new_row.end_date = Some(parse_ymd(e)?);
        }
    }
    if let Some(q) = customer_q {
        let customers: Vec<CustomerRow> = store::read_table(wd, CUSTOMERS_PATH).await?;
        let (_, cust) = find_one("customer", q, &customers, |c| c.customer_id, |c| &c.name)?;
        new_row.customer_id = cust.customer_id;
    }
    if let Some(n) = notes {
        new_row.notes = Some(n);
    }

    if let Some(end_date) = new_row.end_date
        && end_date < new_row.start_date
    {
        return Err(CliError::Invalid(format!(
            "edited end {end_date} < start {}",
            new_row.start_date
        )));
    }

    // Recheck no-overlap invariant against all OTHER tenancies on the same
    // connection.
    let conn_id = new_row.connection_id;
    let new_end = new_row.end_date.unwrap_or(NaiveDate::MAX);
    for (i, t) in tenancies.iter().enumerate() {
        if i == idx || t.connection_id != conn_id {
            continue;
        }
        let t_end = t.end_date.unwrap_or(NaiveDate::MAX);
        // Overlap iff intervals intersect:
        //   max(start) <= min(end)
        if new_row.start_date.max(t.start_date) <= new_end.min(t_end) {
            return Err(CliError::Invalid(format!(
                "edit would overlap tenancy #{} ({} - {})",
                t.tenancy_id,
                t.start_date,
                fmt_ymd_opt(t.end_date)
            )));
        }
    }

    // Also refuse if the EDITED tenancy would no longer cover an issued
    // cycle's bill_date that previously fell inside it. (Already caught
    // above; here for clarity if the existing didn't cover but the new
    // would -- belt and braces.)
    if let Some(bad) = cycles
        .iter()
        .find(|c| c.issued && c.policy_id != 0 && tenancy_covers(&new_row, c.bill_date))
    {
        return Err(CliError::Invalid(format!(
            "edit would put tenancy #{tenancy_id} over bill_date {} of issued cycle `{}`; \
             use `bills reverse` first",
            bad.bill_date, bad.name
        )));
    }

    tenancies[idx] = new_row;
    log::info!("[OK] tenancy edit #{tenancy_id}");
    store::write_table(wd, TENANCIES_PATH, &tenancies).await?;
    Ok(())
}

fn tenancy_covers(t: &TenancyRow, d: NaiveDate) -> bool {
    if d < t.start_date {
        return false;
    }
    match t.end_date {
        Some(end) => d <= end,
        None => true,
    }
}

pub async fn list(
    wd: &WD,
    connection_q: Option<&str>,
    customer_q: Option<&str>,
    as_of: Option<&str>,
) -> Result<()> {
    let tenancies: Vec<TenancyRow> = store::read_table(wd, TENANCIES_PATH).await?;
    let connections: Vec<ConnectionRow> = store::read_table(wd, CONNECTIONS_PATH).await?;
    let customers: Vec<CustomerRow> = store::read_table(wd, CUSTOMERS_PATH).await?;

    let connection_filter: Option<i32> = match connection_q {
        Some(q) => Some(
            find_one(
                "connection",
                q,
                &connections,
                |c| c.connection_id,
                |c| &c.name,
            )?
            .1
            .connection_id,
        ),
        None => None,
    };
    let customer_filter: Option<i32> = match customer_q {
        Some(q) => Some(
            find_one("customer", q, &customers, |c| c.customer_id, |c| &c.name)?
                .1
                .customer_id,
        ),
        None => None,
    };
    let date_filter: Option<NaiveDate> = match as_of {
        Some(s) => Some(parse_ymd(s)?),
        None => None,
    };

    log::info!(
        "{:>4}  connection                customer                  {:<12}  end",
        "id",
        "start"
    );
    for t in tenancies.iter() {
        if let Some(cid) = connection_filter
            && t.connection_id != cid
        {
            continue;
        }
        if let Some(cid) = customer_filter
            && t.customer_id != cid
        {
            continue;
        }
        if let Some(d) = date_filter {
            let end_eff = t.end_date.unwrap_or(NaiveDate::MAX);
            if !(t.start_date <= d && d <= end_eff) {
                continue;
            }
        }
        let conn_name = connections
            .iter()
            .find(|c| c.connection_id == t.connection_id)
            .map(|c| c.name.as_str())
            .unwrap_or("(orphan)");
        let cust_name = customers
            .iter()
            .find(|c| c.customer_id == t.customer_id)
            .map(|c| c.name.as_str())
            .unwrap_or("(orphan)");
        log::info!(
            "{:>4}  #{:<3} {:<22}  #{:<3} {:<22}  {:<12}  {}",
            t.tenancy_id,
            t.connection_id,
            truncate(conn_name, 22),
            t.customer_id,
            truncate(cust_name, 22),
            t.start_date,
            fmt_ymd_opt(t.end_date),
        );
    }
    Ok(())
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
    use crate::cli::{connection as conn_cli, customer as cust_cli};

    async fn setup_with_one_connection_and_two_customers() -> (WD, i32, i32, i32) {
        let fs = tinyfs::memory::new_fs().await;
        let wd = fs.root().await.expect("root");
        let _ = wd.create_dir_all("/data").await.expect("mkdir");

        let conn = conn_cli::add(
            &wd,
            "CommCtr".into(),
            "100 Example St".into(),
            "2000-01-01",
            true,
            None,
            None,
        )
        .await
        .expect("conn");
        let alice = cust_cli::add(&wd, "Alice".into(), "addr".into(), None, None)
            .await
            .expect("alice");
        let bob = cust_cli::add(&wd, "Bob".into(), "addr".into(), None, None)
            .await
            .expect("bob");
        (wd, conn, alice, bob)
    }

    #[tokio::test]
    async fn start_creates_first_tenancy() {
        let (wd, conn, alice, _) = setup_with_one_connection_and_two_customers().await;
        let id = start(
            &wd,
            &conn.to_string(),
            &alice.to_string(),
            "2021-10-01",
            None,
        )
        .await
        .expect("start");
        assert_eq!(id, 1);
        let rows: Vec<TenancyRow> = store::read_table(&wd, TENANCIES_PATH).await.expect("read");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].connection_id, conn);
        assert_eq!(rows[0].customer_id, alice);
        assert!(rows[0].end_date.is_none());
    }

    #[tokio::test]
    async fn second_start_auto_closes_prior() {
        let (wd, conn, alice, bob) = setup_with_one_connection_and_two_customers().await;
        let _alice_t = start(
            &wd,
            &conn.to_string(),
            &alice.to_string(),
            "2021-10-01",
            None,
        )
        .await
        .expect("alice tenancy");
        let _bob_t = start(&wd, &conn.to_string(), &bob.to_string(), "2026-07-01", None)
            .await
            .expect("bob tenancy");

        let rows: Vec<TenancyRow> = store::read_table(&wd, TENANCIES_PATH).await.expect("read");
        assert_eq!(rows.len(), 2);
        // Alice's row should now be closed at 2026-06-30.
        let alice_row = rows.iter().find(|t| t.customer_id == alice).unwrap();
        assert_eq!(
            alice_row.end_date,
            Some(NaiveDate::from_ymd_opt(2026, 6, 30).unwrap())
        );
        // Bob's row is open.
        let bob_row = rows.iter().find(|t| t.customer_id == bob).unwrap();
        assert!(bob_row.end_date.is_none());
        assert_eq!(
            bob_row.start_date,
            NaiveDate::from_ymd_opt(2026, 7, 1).unwrap()
        );
    }

    #[tokio::test]
    async fn start_before_first_active_rejected() {
        let (wd, conn, alice, _) = setup_with_one_connection_and_two_customers().await;
        let err = start(
            &wd,
            &conn.to_string(),
            &alice.to_string(),
            "1999-01-01",
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn start_after_retire_rejected() {
        let (wd, conn, alice, _) = setup_with_one_connection_and_two_customers().await;
        conn_cli::retire(&wd, &conn.to_string(), "2024-01-01")
            .await
            .expect("retire");
        let err = start(
            &wd,
            &conn.to_string(),
            &alice.to_string(),
            "2024-06-01",
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn start_into_inactive_customer_rejected() {
        let (wd, conn, alice, _) = setup_with_one_connection_and_two_customers().await;
        cust_cli::edit(&wd, &alice.to_string(), None, None, None, Some(false))
            .await
            .expect("deactivate");
        let err = start(
            &wd,
            &conn.to_string(),
            &alice.to_string(),
            "2021-10-01",
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn end_explicit_works() {
        let (wd, conn, alice, _) = setup_with_one_connection_and_two_customers().await;
        let tid = start(
            &wd,
            &conn.to_string(),
            &alice.to_string(),
            "2021-10-01",
            None,
        )
        .await
        .expect("start");
        end(&wd, None, Some(tid), "2024-12-31", None)
            .await
            .expect("end");
        let rows: Vec<TenancyRow> = store::read_table(&wd, TENANCIES_PATH).await.expect("read");
        assert_eq!(
            rows[0].end_date,
            Some(NaiveDate::from_ymd_opt(2024, 12, 31).unwrap())
        );
    }

    #[tokio::test]
    async fn end_by_connection_finds_open() {
        let (wd, conn, alice, _) = setup_with_one_connection_and_two_customers().await;
        let _ = start(
            &wd,
            &conn.to_string(),
            &alice.to_string(),
            "2021-10-01",
            None,
        )
        .await
        .unwrap();
        end(&wd, Some(&conn.to_string()), None, "2024-12-31", None)
            .await
            .expect("end");
    }

    #[tokio::test]
    async fn end_with_no_open_rejected() {
        let (wd, conn, _, _) = setup_with_one_connection_and_two_customers().await;
        let err = end(&wd, Some(&conn.to_string()), None, "2024-12-31", None)
            .await
            .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn end_before_start_rejected() {
        let (wd, conn, alice, _) = setup_with_one_connection_and_two_customers().await;
        let tid = start(
            &wd,
            &conn.to_string(),
            &alice.to_string(),
            "2021-10-01",
            None,
        )
        .await
        .unwrap();
        let err = end(&wd, None, Some(tid), "2020-01-01", None)
            .await
            .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn edit_overlap_rejected() {
        let (wd, conn, alice, bob) = setup_with_one_connection_and_two_customers().await;
        // Two consecutive tenancies, no overlap.
        let alice_t = start(
            &wd,
            &conn.to_string(),
            &alice.to_string(),
            "2021-01-01",
            None,
        )
        .await
        .unwrap();
        end(&wd, None, Some(alice_t), "2023-12-31", None)
            .await
            .unwrap();
        let bob_t = start(&wd, &conn.to_string(), &bob.to_string(), "2024-01-01", None)
            .await
            .unwrap();

        // Try to extend Alice's end_date into Bob's range -- should fail.
        let err = edit(&wd, alice_t, None, Some("2024-06-01"), None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)), "got {err:?}");
        // Bob's tenancy untouched.
        let rows: Vec<TenancyRow> = store::read_table(&wd, TENANCIES_PATH).await.unwrap();
        let bob_row = rows.iter().find(|t| t.tenancy_id == bob_t).unwrap();
        assert!(bob_row.end_date.is_none());
    }
}
