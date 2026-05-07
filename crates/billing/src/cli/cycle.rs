// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `accounts cycle ...` -- add/edit/list/show/totals/totals-refresh.

use crate::cli::{CliError, Result};
use crate::dates::parse_ymd;
use crate::domain::cycle_totals;
use crate::lookup::find_one;
use crate::money::Cents;
use crate::schema::{
    BILLING_POLICIES_PATH, BillingPolicyRow, CHART_OF_ACCOUNTS_PATH, CONNECTIONS_PATH, CYCLES_PATH,
    ChartOfAccountsRow, ConnectionRow, CycleRow,
};
use crate::store;
use chrono::{Datelike, Months, NaiveDate};
use tinyfs::WD;

const KIND: &str = "cycle";

#[allow(clippy::too_many_arguments)]
pub async fn add(
    wd: &WD,
    name: String,
    start_str: &str,
    bill_date_str: &str,
    policy_query: &str,
    inactive_str: Option<String>,
    notes: Option<String>,
    allow_irregular: bool,
) -> Result<i32> {
    let period_start = parse_ymd(start_str)?;
    let bill_date = parse_ymd(bill_date_str)?;

    if !is_apr1_or_oct1(period_start) && !allow_irregular {
        log::warn!(
            "cycle period_start {period_start} is not Apr 1 or Oct 1; \
             use --allow-irregular to silence this warning"
        );
    }

    // period_end = period_start + 6 months - 1 day. (e.g. 2024-04-01 -> 2024-09-30)
    let period_end = period_start
        .checked_add_months(Months::new(6))
        .and_then(|d| d.pred_opt())
        .ok_or_else(|| CliError::Invalid("period_end overflow".into()))?;

    if bill_date < period_start {
        return Err(CliError::Invalid(format!(
            "bill_date {bill_date} precedes period_start {period_start}"
        )));
    }

    let policies: Vec<BillingPolicyRow> = store::read_table(wd, BILLING_POLICIES_PATH).await?;
    if policies.is_empty() {
        return Err(CliError::Invalid(
            "no billing policies registered yet -- run `policy add` first".into(),
        ));
    }
    let (_, policy) = find_one(
        "policy",
        policy_query,
        &policies,
        |p| p.policy_id,
        |p| &p.name,
    )?;
    if policy.effective_from > bill_date {
        return Err(CliError::Invalid(format!(
            "policy `{}` is not effective until {} (bill_date={bill_date})",
            policy.name, policy.effective_from
        )));
    }
    if let Some(to) = policy.effective_to
        && bill_date > to
    {
        return Err(CliError::Invalid(format!(
            "policy `{}` expired on {to} (bill_date={bill_date})",
            policy.name
        )));
    }
    let policy_id = policy.policy_id;

    let inactive = parse_inactive_list(wd, inactive_str.as_deref()).await?;

    let mut cycles: Vec<CycleRow> = store::read_table(wd, CYCLES_PATH).await?;
    if cycles.iter().any(|c| c.name == name) {
        return Err(CliError::Invalid(format!(
            "cycle name `{name}` already exists"
        )));
    }
    if let Some(overlap) = cycles
        .iter()
        .find(|c| ranges_overlap(c.period_start, c.period_end, period_start, period_end))
    {
        return Err(CliError::Invalid(format!(
            "cycle `{name}` ({period_start} - {period_end}) overlaps existing cycle `{}` ({} - {})",
            overlap.name, overlap.period_start, overlap.period_end
        )));
    }

    let cycle_id = store::next_id(&cycles, |c| c.cycle_id);
    let row = CycleRow {
        cycle_id,
        name,
        period_start,
        period_end,
        bill_date,
        policy_id,
        inactive,
        notes,
        issued: false,
        bill_txn_id: None,
    };
    log::info!(
        "[OK] cycle add #{} {} ({} - {}) policy={} bill_date={}",
        row.cycle_id,
        row.name,
        row.period_start,
        row.period_end,
        policy_id,
        row.bill_date
    );
    cycles.push(row);
    store::write_table(wd, CYCLES_PATH, &cycles).await?;
    Ok(cycle_id)
}

pub async fn edit(
    wd: &WD,
    target: &str,
    bill_date: Option<&str>,
    inactive_str: Option<String>,
    notes: Option<String>,
) -> Result<()> {
    let mut cycles: Vec<CycleRow> = store::read_table(wd, CYCLES_PATH).await?;
    let (idx, _) = find_one(KIND, target, &cycles, |c| c.cycle_id, |c| &c.name)?;
    if cycles[idx].issued {
        return Err(CliError::Invalid(format!(
            "cycle `{}` is issued; reverse first to edit",
            cycles[idx].name
        )));
    }
    if let Some(b) = bill_date {
        let new = parse_ymd(b)?;
        if new < cycles[idx].period_start {
            return Err(CliError::Invalid(format!(
                "bill_date {new} precedes period_start {}",
                cycles[idx].period_start
            )));
        }
        cycles[idx].bill_date = new;
    }
    if let Some(s) = inactive_str {
        cycles[idx].inactive = parse_inactive_list(wd, Some(&s)).await?;
    }
    if let Some(n) = notes {
        cycles[idx].notes = Some(n);
    }
    log::info!(
        "[OK] cycle edit #{} {}",
        cycles[idx].cycle_id,
        cycles[idx].name
    );
    store::write_table(wd, CYCLES_PATH, &cycles).await?;
    Ok(())
}

pub async fn list(wd: &WD) -> Result<()> {
    let cycles: Vec<CycleRow> = store::read_table(wd, CYCLES_PATH).await?;
    log::info!(
        "{:>4}  {:<10}  {:<12}  {:<12}  {:<12}  {:>6}  {:<6}  inactive",
        "id",
        "name",
        "period start",
        "period end",
        "bill date",
        "policy",
        "issued"
    );
    for c in cycles.iter() {
        log::info!(
            "{:>4}  {:<10}  {:<12}  {:<12}  {:<12}  {:>6}  {:<6}  {}",
            c.cycle_id,
            c.name,
            c.period_start,
            c.period_end,
            c.bill_date,
            c.policy_id,
            if c.issued { "yes" } else { "no" },
            format_inactive(&c.inactive),
        );
    }
    Ok(())
}

pub async fn show(wd: &WD, target: &str) -> Result<()> {
    let cycles: Vec<CycleRow> = store::read_table(wd, CYCLES_PATH).await?;
    let (_, c) = find_one(KIND, target, &cycles, |c| c.cycle_id, |c| &c.name)?;
    log::info!("cycle #{} {}", c.cycle_id, c.name);
    log::info!("  period           : {} - {}", c.period_start, c.period_end);
    log::info!("  bill date        : {}", c.bill_date);
    log::info!("  policy id        : {}", c.policy_id);
    log::info!("  issued           : {}", c.issued);
    log::info!(
        "  bill txn         : {}",
        c.bill_txn_id
            .map(|i| i.to_string())
            .unwrap_or_else(|| "-".into())
    );
    log::info!("  notes            : {}", c.notes.as_deref().unwrap_or("-"));
    log::info!("  inactive (#ids)  : {}", format_inactive(&c.inactive));
    Ok(())
}

/// `cycle totals --cycle=...` -- print the materialized cycle_totals
/// table joined with cycle and account names.
pub async fn totals(wd: &WD, cycle_query: Option<&str>) -> Result<()> {
    let totals = cycle_totals::read(wd).await?;
    if totals.is_empty() {
        log::info!(
            "(no cycle totals materialized; run `cycle totals-refresh` after adding expenses)"
        );
        return Ok(());
    }
    let cycles: Vec<CycleRow> = store::read_table(wd, CYCLES_PATH).await?;
    let chart: Vec<ChartOfAccountsRow> = store::read_table(wd, CHART_OF_ACCOUNTS_PATH).await?;
    let cycle_filter = match cycle_query {
        Some(q) => Some(
            find_one(KIND, q, &cycles, |c| c.cycle_id, |c| &c.name)?
                .1
                .cycle_id,
        ),
        None => None,
    };
    log::info!(
        "{:<10}  {:<6}  {:<24}  {:>14}",
        "cycle",
        "code",
        "account",
        "amount"
    );
    for r in totals.iter() {
        if let Some(cid) = cycle_filter
            && r.cycle_id != cid
        {
            continue;
        }
        let cname = cycles
            .iter()
            .find(|c| c.cycle_id == r.cycle_id)
            .map(|c| c.name.as_str())
            .unwrap_or("(orphan)");
        let aname = chart
            .iter()
            .find(|a| a.code == r.account_code)
            .map(|a| a.name.as_str())
            .unwrap_or("(orphan)");
        log::info!(
            "{:<10}  {:<6}  {:<24}  {:>14}",
            cname,
            r.account_code,
            aname,
            Cents::from_units(r.amount_cents).display()
        );
    }
    Ok(())
}

/// `cycle totals-refresh` -- recompute cycle_totals_materialized.parquet.
pub async fn refresh(wd: &WD) -> Result<()> {
    let (rows, unassigned) = cycle_totals::refresh(wd).await?;
    log::info!("[OK] cycle totals refreshed: {rows} rows ({unassigned} unassigned expenses)");
    Ok(())
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn is_apr1_or_oct1(d: NaiveDate) -> bool {
    (d.month() == 4 || d.month() == 10) && d.day() == 1
}

fn ranges_overlap(
    a_start: NaiveDate,
    a_end: NaiveDate,
    b_start: NaiveDate,
    b_end: NaiveDate,
) -> bool {
    a_start.max(b_start) <= a_end.min(b_end)
}

/// Parse `--inactive=ID,NAME,...` into a list of connection_ids. Each
/// element is resolved through `connections.parquet`.
async fn parse_inactive_list(wd: &WD, s: Option<&str>) -> Result<Vec<i32>> {
    let s = match s {
        Some(s) if !s.trim().is_empty() => s,
        _ => return Ok(Vec::new()),
    };
    let connections: Vec<ConnectionRow> = store::read_table(wd, CONNECTIONS_PATH).await?;
    let mut out = Vec::new();
    for raw in s.split(',') {
        let q = raw.trim();
        if q.is_empty() {
            continue;
        }
        let (_, c) = find_one(
            "connection",
            q,
            &connections,
            |c| c.connection_id,
            |c| &c.name,
        )?;
        out.push(c.connection_id);
    }
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

fn format_inactive(ids: &[i32]) -> String {
    if ids.is_empty() {
        "-".into()
    } else {
        let strs: Vec<String> = ids.iter().map(|i| format!("#{i}")).collect();
        strs.join(",")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chart::seed as seed_chart;
    use crate::cli::policy as policy_cli;

    async fn setup() -> WD {
        let fs = tinyfs::memory::new_fs().await;
        let wd = fs.root().await.unwrap();
        let _ = wd.create_dir_all("/data").await.unwrap();
        seed_chart(&wd).await.expect("chart");
        // Seed a single billing policy so cycle add can reference it.
        let _ = policy_cli::add(
            &wd,
            "test-policy".into(),
            "share-by-weight".into(),
            "margin: 0.0\nweights:\n  commercial: 2.0\n  residential: 1.0\n".into(),
            None,
            None,
        )
        .await
        .expect("policy");
        wd
    }

    #[tokio::test]
    async fn add_assigns_id_and_computes_period_end() {
        let wd = setup().await;
        let id = add(
            &wd,
            "2024H1".into(),
            "2024-04-01",
            "2024-10-01",
            "test-policy",
            None,
            None,
            false,
        )
        .await
        .expect("add");
        assert_eq!(id, 1);
        let cycles: Vec<CycleRow> = store::read_table(&wd, CYCLES_PATH).await.unwrap();
        assert_eq!(
            cycles[0].period_end,
            NaiveDate::from_ymd_opt(2024, 9, 30).unwrap()
        );
    }

    #[tokio::test]
    async fn add_overlapping_period_rejected() {
        let wd = setup().await;
        let _ = add(
            &wd,
            "2024H1".into(),
            "2024-04-01",
            "2024-10-01",
            "test-policy",
            None,
            None,
            false,
        )
        .await
        .unwrap();
        let err = add(
            &wd,
            "overlap".into(),
            "2024-06-01",
            "2024-12-01",
            "test-policy",
            None,
            None,
            true,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn add_unknown_policy_rejected() {
        let wd = setup().await;
        let err = add(
            &wd,
            "C".into(),
            "2024-04-01",
            "2024-10-01",
            "no-such-policy",
            None,
            None,
            false,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CliError::Lookup(_)));
    }

    #[tokio::test]
    async fn edit_refused_after_issue() {
        let wd = setup().await;
        let _ = add(
            &wd,
            "C".into(),
            "2024-04-01",
            "2024-10-01",
            "test-policy",
            None,
            None,
            false,
        )
        .await
        .unwrap();
        // Manually flip issued = true to simulate post-issue state.
        let mut cycles: Vec<CycleRow> = store::read_table(&wd, CYCLES_PATH).await.unwrap();
        cycles[0].issued = true;
        store::write_table(&wd, CYCLES_PATH, &cycles).await.unwrap();

        let err = edit(&wd, "C", Some("2024-11-01"), None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[test]
    fn period_end_helpers() {
        assert!(is_apr1_or_oct1(
            NaiveDate::from_ymd_opt(2024, 4, 1).unwrap()
        ));
        assert!(is_apr1_or_oct1(
            NaiveDate::from_ymd_opt(2024, 10, 1).unwrap()
        ));
        assert!(!is_apr1_or_oct1(
            NaiveDate::from_ymd_opt(2024, 4, 2).unwrap()
        ));
    }
}
