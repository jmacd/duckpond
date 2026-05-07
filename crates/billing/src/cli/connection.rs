// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `accounts connection ...` -- add/edit/retire/list/show.
//!
//! Per the architectural decision in plan.md section 2: `connection edit`
//! only allows `--name` and `--notes`. Billing-affecting fields
//! (`service_address`, `commercial`, `weight_class`, `first_active`) are
//! write-once. To fix a mistake, retire and add a new connection with a
//! new id.

use crate::cli::{CliError, Result};
use crate::dates::{fmt_ymd_opt, parse_ymd};
use crate::lookup::find_one;
use crate::schema::{CONNECTIONS_PATH, ConnectionRow, TENANCIES_PATH, TenancyRow};
use crate::store;
use tinyfs::WD;

const KIND: &str = "connection";

#[allow(clippy::too_many_arguments)]
pub async fn add(
    wd: &WD,
    name: String,
    service: String,
    first_active: &str,
    commercial: bool,
    weight_class: Option<String>,
    notes: Option<String>,
) -> Result<i32> {
    let first_active = parse_ymd(first_active)?;
    let mut rows: Vec<ConnectionRow> = store::read_table(wd, CONNECTIONS_PATH).await?;
    if rows.iter().any(|c| c.name == name) {
        return Err(CliError::Invalid(format!(
            "connection name `{name}` already exists; names are unique"
        )));
    }
    let connection_id = store::next_id(&rows, |c| c.connection_id);
    let row = ConnectionRow {
        connection_id,
        name,
        service_address: service,
        first_active,
        last_active: None,
        commercial,
        weight_class,
        notes,
    };
    log::info!(
        "[OK] connection add #{} {} (commercial={})",
        row.connection_id,
        row.name,
        row.commercial
    );
    rows.push(row);
    store::write_table(wd, CONNECTIONS_PATH, &rows).await?;
    Ok(connection_id)
}

pub async fn edit(
    wd: &WD,
    target: &str,
    name: Option<String>,
    notes: Option<String>,
) -> Result<()> {
    let mut rows: Vec<ConnectionRow> = store::read_table(wd, CONNECTIONS_PATH).await?;
    let (idx, _) = find_one(KIND, target, &rows, |c| c.connection_id, |c| &c.name)?;
    if let Some(new_name) = name.as_deref()
        && rows
            .iter()
            .enumerate()
            .any(|(i, c)| i != idx && c.name == new_name)
    {
        return Err(CliError::Invalid(format!(
            "connection name `{new_name}` already used"
        )));
    }
    let row = &mut rows[idx];
    if let Some(n) = name {
        row.name = n;
    }
    if let Some(n) = notes {
        row.notes = Some(n);
    }
    log::info!("[OK] connection edit #{} {}", row.connection_id, row.name);
    store::write_table(wd, CONNECTIONS_PATH, &rows).await?;
    Ok(())
}

pub async fn retire(wd: &WD, target: &str, as_of: &str) -> Result<()> {
    let as_of = parse_ymd(as_of)?;
    let mut rows: Vec<ConnectionRow> = store::read_table(wd, CONNECTIONS_PATH).await?;
    let (idx, _) = find_one(KIND, target, &rows, |c| c.connection_id, |c| &c.name)?;
    let connection_id = rows[idx].connection_id;

    if let Some(prev) = rows[idx].last_active {
        return Err(CliError::Invalid(format!(
            "connection #{connection_id} already retired on {prev}"
        )));
    }

    // Refuse if any open tenancy exists for this connection.
    let tenancies: Vec<TenancyRow> = store::read_table(wd, TENANCIES_PATH).await?;
    if let Some(open) = tenancies
        .iter()
        .find(|t| t.connection_id == connection_id && t.end_date.is_none())
    {
        return Err(CliError::Invalid(format!(
            "connection #{connection_id} has open tenancy #{} (customer #{}); end the tenancy first",
            open.tenancy_id, open.customer_id
        )));
    }

    rows[idx].last_active = Some(as_of);
    log::info!(
        "[OK] connection retire #{} {} as_of={}",
        rows[idx].connection_id,
        rows[idx].name,
        as_of
    );
    store::write_table(wd, CONNECTIONS_PATH, &rows).await?;
    Ok(())
}

pub async fn list(wd: &WD, only_active: bool, only_commercial: bool) -> Result<()> {
    let rows: Vec<ConnectionRow> = store::read_table(wd, CONNECTIONS_PATH).await?;
    log::info!(
        "{:>4}  {:<24}  {:<10}  {:<12}  {:<12}  service",
        "id",
        "name",
        "commercial",
        "first",
        "last"
    );
    for c in rows.iter() {
        if only_active && c.last_active.is_some() {
            continue;
        }
        if only_commercial && !c.commercial {
            continue;
        }
        log::info!(
            "{:>4}  {:<24}  {:<10}  {:<12}  {:<12}  {}",
            c.connection_id,
            truncate(&c.name, 24),
            if c.commercial { "yes" } else { "no" },
            c.first_active,
            fmt_ymd_opt(c.last_active),
            c.service_address,
        );
    }
    Ok(())
}

pub async fn show(wd: &WD, target: &str) -> Result<()> {
    let rows: Vec<ConnectionRow> = store::read_table(wd, CONNECTIONS_PATH).await?;
    let (_, c) = find_one(KIND, target, &rows, |c| c.connection_id, |c| &c.name)?;
    log::info!("connection #{} {}", c.connection_id, c.name);
    log::info!("  service address  : {}", c.service_address);
    log::info!("  commercial       : {}", c.commercial);
    log::info!(
        "  weight class     : {}",
        c.weight_class.as_deref().unwrap_or("-")
    );
    log::info!("  first active     : {}", c.first_active);
    log::info!("  last active      : {}", fmt_ymd_opt(c.last_active));
    log::info!("  notes            : {}", c.notes.as_deref().unwrap_or("-"));

    let tenancies: Vec<TenancyRow> = store::read_table(wd, TENANCIES_PATH).await?;
    let mine: Vec<&TenancyRow> = tenancies
        .iter()
        .filter(|t| t.connection_id == c.connection_id)
        .collect();
    if mine.is_empty() {
        log::info!("  tenancies        : (none)");
    } else {
        log::info!("  tenancies        :");
        let customers: Vec<crate::schema::CustomerRow> =
            store::read_table(wd, crate::schema::CUSTOMERS_PATH).await?;
        for t in &mine {
            let cust_name = customers
                .iter()
                .find(|c| c.customer_id == t.customer_id)
                .map(|c| c.name.as_str())
                .unwrap_or("(orphan)");
            log::info!(
                "    #{} customer #{} {}  {} - {}",
                t.tenancy_id,
                t.customer_id,
                cust_name,
                t.start_date,
                fmt_ymd_opt(t.end_date)
            );
        }
        // Phase-7 enrichment: current responsible customer.
        let today = chrono::Local::now().date_naive();
        let current = mine
            .iter()
            .find(|t| t.start_date <= today && t.end_date.is_none_or(|e| today <= e));
        match current {
            Some(t) => {
                let cust_name = customers
                    .iter()
                    .find(|c| c.customer_id == t.customer_id)
                    .map(|c| c.name.as_str())
                    .unwrap_or("(orphan)");
                log::info!(
                    "  current customer : #{} {} (since {})",
                    t.customer_id,
                    cust_name,
                    t.start_date
                );
            }
            None => log::info!("  current customer : (none)"),
        }
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
    use chrono::NaiveDate;

    async fn setup() -> WD {
        let fs = tinyfs::memory::new_fs().await;
        let wd = fs.root().await.expect("root");
        let _ = wd.create_dir_all("/data").await.expect("mkdir");
        wd
    }

    #[tokio::test]
    async fn add_assigns_sequential_ids() {
        let wd = setup().await;
        let id1 = add(
            &wd,
            "CommCtr".into(),
            "100 Example St".into(),
            "2010-01-01",
            true,
            None,
            None,
        )
        .await
        .expect("add1");
        let id2 = add(
            &wd,
            "Other".into(),
            "200 Example St".into(),
            "2012-06-01",
            false,
            None,
            None,
        )
        .await
        .expect("add2");
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
    }

    #[tokio::test]
    async fn add_rejects_invalid_date() {
        let wd = setup().await;
        let err = add(
            &wd,
            "CommCtr".into(),
            "x".into(),
            "not-a-date",
            true,
            None,
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CliError::Date(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn edit_changes_only_name_and_notes() {
        let wd = setup().await;
        let id = add(
            &wd,
            "CommCtr".into(),
            "100 Example St".into(),
            "2010-01-01",
            true,
            None,
            None,
        )
        .await
        .expect("add");
        edit(
            &wd,
            &id.to_string(),
            Some("Community Center".into()),
            Some("renamed".into()),
        )
        .await
        .expect("edit");
        let rows: Vec<ConnectionRow> = store::read_table(&wd, CONNECTIONS_PATH)
            .await
            .expect("read");
        assert_eq!(rows[0].name, "Community Center");
        assert_eq!(rows[0].notes.as_deref(), Some("renamed"));
        // service_address and commercial are unchanged.
        assert_eq!(rows[0].service_address, "100 Example St");
        assert!(rows[0].commercial);
    }

    #[tokio::test]
    async fn retire_refuses_with_open_tenancy() {
        let wd = setup().await;
        let conn = add(
            &wd,
            "CommCtr".into(),
            "x".into(),
            "2010-01-01",
            true,
            None,
            None,
        )
        .await
        .expect("add");

        store::write_table(
            &wd,
            TENANCIES_PATH,
            &[TenancyRow {
                tenancy_id: 1,
                connection_id: conn,
                customer_id: 99,
                start_date: NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
                end_date: None,
                notes: None,
            }],
        )
        .await
        .expect("seed tenancy");

        let err = retire(&wd, &conn.to_string(), "2024-12-31")
            .await
            .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn retire_succeeds_with_no_open_tenancy() {
        let wd = setup().await;
        let conn = add(
            &wd,
            "CommCtr".into(),
            "x".into(),
            "2010-01-01",
            true,
            None,
            None,
        )
        .await
        .expect("add");

        retire(&wd, &conn.to_string(), "2024-12-31")
            .await
            .expect("retire");

        let rows: Vec<ConnectionRow> = store::read_table(&wd, CONNECTIONS_PATH)
            .await
            .expect("read");
        assert_eq!(
            rows[0].last_active,
            Some(NaiveDate::from_ymd_opt(2024, 12, 31).unwrap())
        );
    }

    #[tokio::test]
    async fn retire_idempotent_call_rejected() {
        let wd = setup().await;
        let conn = add(
            &wd,
            "CommCtr".into(),
            "x".into(),
            "2010-01-01",
            true,
            None,
            None,
        )
        .await
        .expect("add");
        retire(&wd, &conn.to_string(), "2024-01-01")
            .await
            .expect("retire1");
        let err = retire(&wd, &conn.to_string(), "2024-12-31")
            .await
            .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }
}
