// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `accounts customer ...` -- add/edit/list/show/find/merge.
//!
//! `merge` uses split-not-mutate semantics: it closes any open tenancy
//! on `from` at `merge_date - 1` and creates a new tenancy starting on
//! `merge_date` for the surviving customer. This preserves the
//! historical responsible-customer for any pre-merge bill_date. Refused
//! if any open tenancy on `from` covers an issued cycle's `bill_date`
//! after the merge date (operator must `bills reverse` such cycles
//! first).

use crate::cli::{CliError, Result};
use crate::dates::fmt_ymd_opt;
use crate::lookup::find_one;
use crate::schema::{CUSTOMERS_PATH, CustomerRow, TENANCIES_PATH, TenancyRow};
use crate::store;
use tinyfs::WD;

const KIND: &str = "customer";

pub async fn add(
    wd: &WD,
    name: String,
    billing: String,
    contact: Option<String>,
    notes: Option<String>,
) -> Result<i32> {
    let mut rows: Vec<CustomerRow> = store::read_table(wd, CUSTOMERS_PATH).await?;
    if rows.iter().any(|c| c.name == name) {
        return Err(CliError::Invalid(format!(
            "customer name `{name}` already exists; names are unique"
        )));
    }
    let customer_id = store::next_id(&rows, |c| c.customer_id);
    let row = CustomerRow {
        customer_id,
        name,
        billing_address: billing,
        contact,
        active: true,
        notes,
        merged_into_customer_id: None,
    };
    log::info!("[OK] customer add #{} {}", row.customer_id, row.name);
    rows.push(row);
    store::write_table(wd, CUSTOMERS_PATH, &rows).await?;
    Ok(customer_id)
}

pub async fn edit(
    wd: &WD,
    target: &str,
    name: Option<String>,
    billing: Option<String>,
    contact: Option<String>,
    active: Option<bool>,
) -> Result<()> {
    let mut rows: Vec<CustomerRow> = store::read_table(wd, CUSTOMERS_PATH).await?;
    let (idx, _) = find_one(KIND, target, &rows, |c| c.customer_id, |c| &c.name)?;

    if let Some(new_name) = name.as_deref()
        && rows
            .iter()
            .enumerate()
            .any(|(i, c)| i != idx && c.name == new_name)
    {
        return Err(CliError::Invalid(format!(
            "customer name `{new_name}` already used"
        )));
    }

    let row = &mut rows[idx];
    if let Some(n) = name {
        row.name = n;
    }
    if let Some(b) = billing {
        row.billing_address = b;
    }
    if let Some(c) = contact {
        row.contact = Some(c);
    }
    if let Some(a) = active {
        row.active = a;
    }
    log::info!("[OK] customer edit #{} {}", row.customer_id, row.name);
    store::write_table(wd, CUSTOMERS_PATH, &rows).await?;
    Ok(())
}

pub async fn list(wd: &WD, only_active: bool, with_balance: bool) -> Result<()> {
    let rows: Vec<CustomerRow> = store::read_table(wd, CUSTOMERS_PATH).await?;
    if with_balance {
        // The dedicated `balance` and `who-owes` reports compute balances
        // from the journal. `customer list --with-balance` is not yet
        // wired through to do the same; ignore the flag for now.
        log::warn!(
            "customer list --with-balance is a no-op for now; \
             use `balance` or `who-owes` for AR balances"
        );
    }
    log::info!(
        "{:>4}  {:<24}  {:<6}  {:<8}  {:<32}  contact",
        "id",
        "name",
        "active",
        "merged",
        "billing"
    );
    for c in rows.iter() {
        if only_active && !c.active {
            continue;
        }
        log::info!(
            "{:>4}  {:<24}  {:<6}  {:<8}  {:<32}  {}",
            c.customer_id,
            truncate(&c.name, 24),
            if c.active { "yes" } else { "no" },
            c.merged_into_customer_id
                .map(|i| format!("->#{i}"))
                .unwrap_or_else(|| "-".into()),
            truncate(&c.billing_address, 32),
            c.contact.as_deref().unwrap_or("-"),
        );
    }
    Ok(())
}

pub async fn show(wd: &WD, target: &str) -> Result<()> {
    let rows: Vec<CustomerRow> = store::read_table(wd, CUSTOMERS_PATH).await?;
    let (_, c) = find_one(KIND, target, &rows, |c| c.customer_id, |c| &c.name)?;
    log::info!("customer #{} {}", c.customer_id, c.name);
    log::info!("  active           : {}", c.active);
    log::info!("  billing address  : {}", c.billing_address);
    log::info!(
        "  contact          : {}",
        c.contact.as_deref().unwrap_or("-")
    );
    log::info!("  notes            : {}", c.notes.as_deref().unwrap_or("-"));
    log::info!(
        "  merged into      : {}",
        c.merged_into_customer_id
            .map(|i| format!("#{i}"))
            .unwrap_or_else(|| "(not merged)".into())
    );

    // Phase-7 enrichment: AR balance + last 5 payments + oldest unpaid bill.
    let legs: Vec<crate::schema::JournalLegRow> =
        store::read_series(wd, crate::schema::JOURNAL_PATH).await?;
    let ar_bal: i64 = legs
        .iter()
        .filter(|l| l.customer_id == Some(c.customer_id) && l.account_code == 1100)
        .map(|l| l.debit_cents - l.credit_cents)
        .sum();
    log::info!(
        "  AR balance       : {}",
        crate::money::Cents::from_units(ar_bal).display()
    );
    let mut payments: Vec<&crate::schema::JournalLegRow> = legs
        .iter()
        .filter(|l| {
            l.customer_id == Some(c.customer_id)
                && l.account_code == 1100
                && l.source == "payment"
                && l.credit_cents > 0
        })
        .collect();
    payments.sort_by_key(|l| std::cmp::Reverse(l.txn_date));
    if payments.is_empty() {
        log::info!("  last payments    : (none)");
    } else {
        log::info!("  last payments    :");
        for p in payments.iter().take(5) {
            let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(p.txn_date)
                .map(|dt| dt.date_naive().to_string())
                .unwrap_or_else(|| "?".into());
            log::info!(
                "    {date}  {}  {}",
                crate::money::Cents::from_units(p.credit_cents).display(),
                p.source_ref.as_deref().unwrap_or("-"),
            );
        }
    }

    let tenancies: Vec<TenancyRow> = store::read_table(wd, TENANCIES_PATH).await?;
    let mine: Vec<&TenancyRow> = tenancies
        .iter()
        .filter(|t| t.customer_id == c.customer_id)
        .collect();
    if mine.is_empty() {
        log::info!("  tenancies        : (none)");
    } else {
        log::info!("  tenancies        :");
        for t in mine {
            log::info!(
                "    #{} connection #{}  {} - {}",
                t.tenancy_id,
                t.connection_id,
                t.start_date,
                fmt_ymd_opt(t.end_date)
            );
        }
    }
    Ok(())
}

/// Case-insensitive substring lookup. Distinct from `find_one` which is
/// the strict id-or-exact-name lookup used by every other subcommand.
pub async fn find(wd: &WD, query: &str) -> Result<()> {
    let rows: Vec<CustomerRow> = store::read_table(wd, CUSTOMERS_PATH).await?;
    let needle = query.to_lowercase();
    let matches: Vec<&CustomerRow> = rows
        .iter()
        .filter(|c| c.name.to_lowercase().contains(&needle))
        .collect();
    if matches.is_empty() {
        log::info!("(no matches for `{query}`)");
        return Ok(());
    }
    for c in matches {
        log::info!(
            "  #{:<3}  {:<24}  {}",
            c.customer_id,
            c.name,
            c.contact.as_deref().unwrap_or("-")
        );
    }
    Ok(())
}

/// Merge customer `from` into `into`. Posts an AR transfer transaction
/// (`DR 1100/into  CR 1100/from`, by from's current AR balance), then:
///
/// - For each OPEN tenancy on `from`: SPLIT it at `merge_date`. The
///   existing tenancy is closed at `merge_date - 1` (preserving the
///   historical fact that `from` was responsible until the merge); a NEW
///   tenancy is created for `into` starting on `merge_date`. This
///   preserves the immutability of any already-issued cycle's billing
///   inputs (the historical responsible-customer never changes).
/// - Refuses if any open tenancy on `from` covers an issued cycle's
///   `bill_date` AFTER `merge_date - 1` -- the operator must `bills
///   reverse` such cycles first.
/// - Sets `from.merged_into_customer_id = into`, `from.active = false`.
///
/// Closed tenancies and historical journal legs of `from` are NEVER
/// mutated. Reports JOIN through `merged_into_customer_id` for rollups.
pub async fn merge(wd: &WD, from_query: &str, into_query: &str) -> Result<()> {
    let mut customers: Vec<CustomerRow> = store::read_table(wd, CUSTOMERS_PATH).await?;
    let (from_idx, _) = find_one(KIND, from_query, &customers, |c| c.customer_id, |c| &c.name)?;
    let (into_idx, _) = find_one(KIND, into_query, &customers, |c| c.customer_id, |c| &c.name)?;
    if from_idx == into_idx {
        return Err(CliError::Invalid(
            "cannot merge a customer into itself".into(),
        ));
    }
    let from_id = customers[from_idx].customer_id;
    let into_id = customers[into_idx].customer_id;
    let from_name = customers[from_idx].name.clone();
    let into_name = customers[into_idx].name.clone();

    if customers[from_idx].merged_into_customer_id.is_some() {
        return Err(CliError::Invalid(format!(
            "#{from_id} {from_name} is already merged"
        )));
    }
    if customers[into_idx].merged_into_customer_id.is_some() {
        return Err(CliError::Invalid(format!(
            "#{into_id} {into_name} is itself merged; merge into a non-merged customer"
        )));
    }
    if !customers[into_idx].active {
        return Err(CliError::Invalid(format!(
            "#{into_id} {into_name} is inactive; cannot merge into an inactive customer"
        )));
    }

    let merge_date = chrono::Local::now().date_naive();
    let prev_close = merge_date
        .pred_opt()
        .ok_or_else(|| CliError::Invalid("merge date underflow".into()))?;

    // Refuse if any open tenancy on `from` covers an issued cycle's
    // bill_date AFTER prev_close (i.e., the future portion that we'd
    // hand off to `into` -- which would change billing history for
    // already-issued cycles whose bill_date falls in that future).
    let mut tenancies: Vec<TenancyRow> = store::read_table(wd, TENANCIES_PATH).await?;
    let cycles: Vec<crate::schema::CycleRow> =
        store::read_table(wd, crate::schema::CYCLES_PATH).await?;
    for t in tenancies
        .iter()
        .filter(|t| t.customer_id == from_id && t.end_date.is_none())
    {
        for c in cycles.iter().filter(|c| c.issued) {
            if c.bill_date > prev_close && c.bill_date >= t.start_date {
                return Err(CliError::Invalid(format!(
                    "open tenancy #{} on connection #{} covers bill_date {} of issued cycle `{}` \
                     after the merge date {merge_date}; `bills reverse` that cycle first",
                    t.tenancy_id, t.connection_id, c.bill_date, c.name
                )));
            }
        }
    }

    // Compute from's current AR balance and post a transfer transaction
    // BEFORE mutating reference tables.
    let bal = crate::cli::journal::customer_ar_balance(wd, from_id).await?;
    let mut transfer_txn: Option<i32> = None;
    if bal.units() != 0 {
        let positive = bal.units() > 0;
        let mag = crate::money::Cents::from_units(bal.units().unsigned_abs() as i64);
        let memo = format!("merge #{from_id} {from_name} -> #{into_id} {into_name}");
        let legs = if positive {
            vec![
                crate::cli::journal::LegSpec::debit(1100, mag, "merge")
                    .with_customer(into_id)
                    .with_source_ref(format!("merge:from={from_id},into={into_id}"))
                    .with_memo(memo.clone()),
                crate::cli::journal::LegSpec::credit(1100, mag, "merge")
                    .with_customer(from_id)
                    .with_source_ref(format!("merge:from={from_id},into={into_id}")),
            ]
        } else {
            vec![
                crate::cli::journal::LegSpec::debit(1100, mag, "merge")
                    .with_customer(from_id)
                    .with_source_ref(format!("merge:from={from_id},into={into_id}"))
                    .with_memo(memo.clone()),
                crate::cli::journal::LegSpec::credit(1100, mag, "merge")
                    .with_customer(into_id)
                    .with_source_ref(format!("merge:from={from_id},into={into_id}")),
            ]
        };
        let txn = crate::cli::journal::write_transaction(wd, merge_date, legs).await?;
        transfer_txn = Some(txn);
    }

    // SPLIT each open tenancy at merge_date: close the existing tenancy
    // at prev_close, create a new tenancy for `into` starting on
    // merge_date. Refuse the split if start_date > prev_close (would put
    // a closed-before-started tenancy into the file).
    let mut split_count = 0usize;
    let mut new_tenancies: Vec<TenancyRow> = Vec::new();
    let next_id_start = store::next_id(&tenancies, |t| t.tenancy_id);
    let mut new_tenancy_id = next_id_start;
    for t in tenancies.iter_mut() {
        if t.customer_id == from_id && t.end_date.is_none() {
            if t.start_date > prev_close {
                return Err(CliError::Invalid(format!(
                    "open tenancy #{} starts on {} which is after merge_date - 1 ({prev_close}); \
                     cannot split. Edit the tenancy or pick a later merge date.",
                    t.tenancy_id, t.start_date
                )));
            }
            t.end_date = Some(prev_close);
            new_tenancies.push(TenancyRow {
                tenancy_id: new_tenancy_id,
                connection_id: t.connection_id,
                customer_id: into_id,
                start_date: merge_date,
                end_date: None,
                notes: Some(format!(
                    "split from tenancy #{} at merge {from_name} -> {into_name}",
                    t.tenancy_id
                )),
            });
            new_tenancy_id += 1;
            split_count += 1;
        }
    }
    tenancies.extend(new_tenancies);

    // Mark `from` as merged + inactive.
    customers[from_idx].merged_into_customer_id = Some(into_id);
    customers[from_idx].active = false;

    store::write_table(wd, CUSTOMERS_PATH, &customers).await?;
    if split_count > 0 {
        store::write_table(wd, TENANCIES_PATH, &tenancies).await?;
    }

    log::info!(
        "[OK] customer merge #{from_id} {from_name} -> #{into_id} {into_name} \
         (split {split_count} open tenanc{} at {merge_date}; transfer balance {}{})",
        if split_count == 1 { "y" } else { "ies" },
        bal.display(),
        match transfer_txn {
            Some(t) => format!(" -> txn #{t}"),
            None => " (no journal txn; balance was zero)".to_string(),
        }
    );
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
        let id1 = add(&wd, "Alice".into(), "addr1".into(), None, None)
            .await
            .expect("add1");
        let id2 = add(&wd, "Bob".into(), "addr2".into(), None, None)
            .await
            .expect("add2");
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
    }

    #[tokio::test]
    async fn add_rejects_duplicate_name() {
        let wd = setup().await;
        let _ = add(&wd, "Alice".into(), "x".into(), None, None)
            .await
            .expect("add1");
        let err = add(&wd, "Alice".into(), "y".into(), None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn edit_changes_billing() {
        let wd = setup().await;
        let id = add(&wd, "Alice".into(), "old".into(), None, None)
            .await
            .expect("add");
        edit(
            &wd,
            &id.to_string(),
            None,
            Some("new addr".into()),
            None,
            None,
        )
        .await
        .expect("edit");
        let rows: Vec<CustomerRow> = store::read_table(&wd, CUSTOMERS_PATH).await.expect("read");
        assert_eq!(rows[0].billing_address, "new addr");
    }

    #[tokio::test]
    async fn merge_splits_open_tenancy_at_merge_date() {
        let wd = setup().await;
        let alice = add(&wd, "Alice".into(), "x".into(), None, None)
            .await
            .expect("add alice");
        let bob = add(&wd, "Bob".into(), "y".into(), None, None)
            .await
            .expect("add bob");

        // Two tenancies for Alice: one closed, one open.
        let tenancies = vec![
            TenancyRow {
                tenancy_id: 1,
                connection_id: 10,
                customer_id: alice,
                start_date: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
                end_date: Some(NaiveDate::from_ymd_opt(2022, 1, 1).unwrap()),
                notes: None,
            },
            TenancyRow {
                tenancy_id: 2,
                connection_id: 11,
                customer_id: alice,
                start_date: NaiveDate::from_ymd_opt(2022, 1, 1).unwrap(),
                end_date: None,
                notes: None,
            },
        ];
        store::write_table(&wd, TENANCIES_PATH, &tenancies)
            .await
            .expect("seed tenancies");

        merge(&wd, &alice.to_string(), &bob.to_string())
            .await
            .expect("merge");

        let after: Vec<TenancyRow> = store::read_table(&wd, TENANCIES_PATH).await.expect("read");
        // Closed tenancy untouched.
        let closed = after.iter().find(|t| t.tenancy_id == 1).unwrap();
        assert_eq!(closed.customer_id, alice, "closed tenancy stays w/ Alice");
        assert_eq!(
            closed.end_date,
            Some(NaiveDate::from_ymd_opt(2022, 1, 1).unwrap())
        );
        // The originally-open tenancy on connection 11 now has end_date set
        // (closed at merge_date - 1) and still points to Alice (history).
        let split_old = after.iter().find(|t| t.tenancy_id == 2).unwrap();
        assert_eq!(split_old.customer_id, alice, "history preserved");
        assert!(split_old.end_date.is_some(), "split closed at merge - 1");
        // A NEW tenancy was created for Bob starting at merge_date on
        // the same connection.
        let new_for_bob = after
            .iter()
            .find(|t| t.tenancy_id != 1 && t.tenancy_id != 2)
            .expect("new tenancy created");
        assert_eq!(new_for_bob.customer_id, bob);
        assert_eq!(new_for_bob.connection_id, 11);
        assert!(new_for_bob.end_date.is_none(), "new tenancy is open");
        // Date continuity: split.end_date + 1 day == new.start_date.
        assert_eq!(
            split_old.end_date.unwrap().succ_opt().unwrap(),
            new_for_bob.start_date
        );

        let customers: Vec<CustomerRow> =
            store::read_table(&wd, CUSTOMERS_PATH).await.expect("read");
        assert_eq!(customers[0].merged_into_customer_id, Some(bob));
        assert!(!customers[0].active);
        assert_eq!(customers[1].merged_into_customer_id, None);
        assert!(customers[1].active);
    }

    #[tokio::test]
    async fn merge_into_self_rejected() {
        let wd = setup().await;
        let alice = add(&wd, "Alice".into(), "x".into(), None, None)
            .await
            .expect("add");
        let err = merge(&wd, &alice.to_string(), &alice.to_string())
            .await
            .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn merge_into_inactive_rejected() {
        let wd = setup().await;
        let alice = add(&wd, "Alice".into(), "x".into(), None, None)
            .await
            .expect("alice");
        let bob = add(&wd, "Bob".into(), "y".into(), None, None)
            .await
            .expect("bob");
        edit(&wd, &bob.to_string(), None, None, None, Some(false))
            .await
            .expect("deactivate bob");
        let err = merge(&wd, &alice.to_string(), &bob.to_string())
            .await
            .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn merge_already_merged_rejected() {
        let wd = setup().await;
        let a = add(&wd, "A".into(), "x".into(), None, None).await.unwrap();
        let b = add(&wd, "B".into(), "y".into(), None, None).await.unwrap();
        let c = add(&wd, "C".into(), "z".into(), None, None).await.unwrap();
        merge(&wd, &a.to_string(), &b.to_string()).await.unwrap();
        let err = merge(&wd, &a.to_string(), &c.to_string())
            .await
            .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    /// Per Phase-6 rubber-duck: merge MUST NOT change the responsible
    /// customer for an issued cycle. If an open tenancy on `from` covers
    /// an issued cycle's bill_date that's AFTER `merge_date - 1`, refuse.
    #[tokio::test]
    async fn merge_refused_when_open_tenancy_covers_future_issued_cycle() {
        use crate::schema::CYCLES_PATH;
        let wd = setup().await;
        let alice = add(&wd, "Alice".into(), "x".into(), None, None)
            .await
            .unwrap();
        let bob = add(&wd, "Bob".into(), "y".into(), None, None)
            .await
            .unwrap();
        // Open tenancy for Alice on connection 5 starting 2020-01-01.
        store::write_table(
            &wd,
            TENANCIES_PATH,
            &[TenancyRow {
                tenancy_id: 1,
                connection_id: 5,
                customer_id: alice,
                start_date: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
                end_date: None,
                notes: None,
            }],
        )
        .await
        .unwrap();
        // Issued cycle whose bill_date is FUTURE relative to "today".
        let future_bill = chrono::Local::now()
            .date_naive()
            .succ_opt()
            .unwrap()
            .succ_opt()
            .unwrap();
        store::write_table(
            &wd,
            CYCLES_PATH,
            &[crate::schema::CycleRow {
                cycle_id: 1,
                name: "future-cycle".into(),
                period_start: future_bill,
                period_end: future_bill.succ_opt().unwrap(),
                bill_date: future_bill,
                policy_id: 1,
                inactive: vec![],
                notes: None,
                issued: true,
                bill_txn_id: Some(99),
            }],
        )
        .await
        .unwrap();

        let err = merge(&wd, &alice.to_string(), &bob.to_string())
            .await
            .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)), "got {err:?}");
        let msg = format!("{err}");
        assert!(
            msg.contains("future-cycle"),
            "error should name the issued cycle, got: {msg}"
        );
    }
}
