// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `accounts policy ...` operator subcommands.
//!
//! Supports `policy add/list/show/edit` and the `policy amortize
//! add/list` sub-tree. `policy add` validates the YAML config against
//! the kind's strategy via the `BillingPolicyRegistry` (fails if
//! `kind` is unknown or the YAML doesn't match the strategy's schema).

use crate::cli::{CliError, Result};
use crate::dates::{fmt_ymd_opt, parse_ymd};
use crate::lookup::find_one;
use crate::schema::{
    AMORTIZATION_RULES_PATH, AmortizationRuleRow, BILLING_POLICIES_PATH, BillingPolicyRow,
    CHART_OF_ACCOUNTS_PATH, CYCLES_PATH, ChartOfAccountsRow, CycleRow,
};
use crate::store;
use chrono::NaiveDate;
use tinyfs::WD;

const KIND: &str = "policy";

pub async fn kinds() -> Result<()> {
    log::info!("Available billing policy kinds:");
    let registered = crate::policy::kinds();
    if registered.is_empty() {
        log::info!("  (none registered; build the binary with at least one policy crate)");
        return Ok(());
    }
    for k in registered {
        let s = crate::policy::find(k).expect("present");
        log::info!("  {:<24}  {}", s.kind(), s.description());
        log::info!("  {:>26}  {}", "config:", s.config_help());
    }
    Ok(())
}

pub async fn add(
    wd: &WD,
    name: String,
    kind: String,
    config_yaml: String,
    effective_from: Option<String>,
    description: Option<String>,
) -> Result<i32> {
    if kind.trim().is_empty() {
        return Err(CliError::Invalid("--kind is required".into()));
    }
    if config_yaml.trim().is_empty() {
        return Err(CliError::Invalid(
            "--config-path is required (use `-` for stdin)".into(),
        ));
    }
    // Look up the strategy and validate the YAML against its schema.
    let strategy = crate::policy::require(&kind)?;
    strategy.validate(&config_yaml)?;
    let effective_from = match effective_from {
        Some(s) => parse_ymd(&s)?,
        None => NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch"),
    };
    let mut policies: Vec<BillingPolicyRow> = store::read_table(wd, BILLING_POLICIES_PATH).await?;
    if policies.iter().any(|p| p.name == name) {
        return Err(CliError::Invalid(format!(
            "policy name `{name}` already exists"
        )));
    }
    let policy_id = store::next_id(&policies, |p| p.policy_id);
    let row = BillingPolicyRow {
        policy_id,
        name,
        kind,
        config_yaml,
        effective_from,
        effective_to: None,
        description,
    };
    log::info!(
        "[OK] policy add #{} {} kind={} effective_from={}",
        row.policy_id,
        row.name,
        row.kind,
        row.effective_from
    );
    policies.push(row);
    store::write_table(wd, BILLING_POLICIES_PATH, &policies).await?;
    Ok(policy_id)
}

pub async fn list(wd: &WD, kind_filter: Option<&str>, effective_as_of: Option<&str>) -> Result<()> {
    let policies: Vec<BillingPolicyRow> = store::read_table(wd, BILLING_POLICIES_PATH).await?;
    let as_of = match effective_as_of {
        Some(s) => Some(parse_ymd(s)?),
        None => None,
    };
    log::info!(
        "{:>4}  {:<24}  {:<20}  {:<12}  {:<12}  description",
        "id",
        "name",
        "kind",
        "from",
        "to"
    );
    for p in policies.iter() {
        if let Some(k) = kind_filter
            && p.kind != k
        {
            continue;
        }
        if let Some(d) = as_of {
            let in_range = p.effective_from <= d && p.effective_to.is_none_or(|to| d <= to);
            if !in_range {
                continue;
            }
        }
        log::info!(
            "{:>4}  {:<24}  {:<20}  {:<12}  {:<12}  {}",
            p.policy_id,
            p.name,
            p.kind,
            p.effective_from,
            fmt_ymd_opt(p.effective_to),
            p.description.as_deref().unwrap_or("-"),
        );
    }
    Ok(())
}

pub async fn show(wd: &WD, target: &str) -> Result<()> {
    let policies: Vec<BillingPolicyRow> = store::read_table(wd, BILLING_POLICIES_PATH).await?;
    let (_, p) = find_one(KIND, target, &policies, |p| p.policy_id, |p| &p.name)?;
    log::info!("policy #{} {}", p.policy_id, p.name);
    log::info!("  kind             : {}", p.kind);
    log::info!("  effective from   : {}", p.effective_from);
    log::info!("  effective to     : {}", fmt_ymd_opt(p.effective_to));
    log::info!(
        "  description      : {}",
        p.description.as_deref().unwrap_or("-")
    );
    log::info!("  config yaml:");
    for line in p.config_yaml.lines() {
        log::info!("    {line}");
    }
    let cycles: Vec<CycleRow> = store::read_table(wd, CYCLES_PATH).await?;
    let used_in: Vec<&CycleRow> = cycles
        .iter()
        .filter(|c| c.policy_id == p.policy_id)
        .collect();
    if used_in.is_empty() {
        log::info!("  used in cycles   : (none)");
    } else {
        log::info!("  used in cycles   :");
        for c in used_in {
            log::info!(
                "    #{} {} ({}-{}) issued={}",
                c.cycle_id,
                c.name,
                c.period_start,
                c.period_end,
                c.issued
            );
        }
    }
    Ok(())
}

pub async fn edit(
    wd: &WD,
    target: &str,
    description: Option<String>,
    effective_to: Option<&str>,
) -> Result<()> {
    let mut policies: Vec<BillingPolicyRow> = store::read_table(wd, BILLING_POLICIES_PATH).await?;
    let (idx, _) = find_one(KIND, target, &policies, |p| p.policy_id, |p| &p.name)?;
    if let Some(d) = description {
        policies[idx].description = Some(d);
    }
    if let Some(s) = effective_to {
        let new_to = parse_ymd(s)?;
        // Refuse impossible date range.
        if new_to < policies[idx].effective_from {
            return Err(CliError::Invalid(format!(
                "effective_to {new_to} cannot precede effective_from {}",
                policies[idx].effective_from
            )));
        }
        // Refuse if shrinking would invalidate any issued cycle that uses
        // this policy (bill_date > new_to).
        let cycles: Vec<CycleRow> = store::read_table(wd, CYCLES_PATH).await?;
        if let Some(bad) = cycles
            .iter()
            .find(|c| c.issued && c.policy_id == policies[idx].policy_id && c.bill_date > new_to)
        {
            return Err(CliError::Invalid(format!(
                "cannot set effective_to={new_to}: issued cycle `{}` has bill_date {} which would fall outside",
                bad.name, bad.bill_date
            )));
        }
        policies[idx].effective_to = Some(new_to);
    }
    log::info!(
        "[OK] policy edit #{} {}",
        policies[idx].policy_id,
        policies[idx].name
    );
    store::write_table(wd, BILLING_POLICIES_PATH, &policies).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// `policy amortize add/list` (append-only; no edit subcommand)
// ---------------------------------------------------------------------------

pub async fn amortize_add(
    wd: &WD,
    account: i32,
    this: f64,
    next: f64,
    effective_from: &str,
    description: Option<String>,
) -> Result<i32> {
    let effective_from = parse_ymd(effective_from)?;
    if !(0.0..=1.0).contains(&this) || !(0.0..=1.0).contains(&next) {
        return Err(CliError::Invalid("fractions must be in [0.0, 1.0]".into()));
    }
    let sum = this + next;
    if (sum - 1.0).abs() > 1e-9 {
        return Err(CliError::Invalid(format!(
            "this_cycle + next_cycle must sum to 1.0 (got {sum})"
        )));
    }
    // Validate account exists and is expense kind.
    let chart: Vec<ChartOfAccountsRow> = store::read_table(wd, CHART_OF_ACCOUNTS_PATH).await?;
    let acct = chart
        .iter()
        .find(|c| c.code == account)
        .ok_or_else(|| CliError::Invalid(format!("account {account} not in chart")))?;
    if acct.kind != "expense" {
        return Err(CliError::Invalid(format!(
            "account {account} {} is kind={}; amortization only applies to expenses",
            acct.name, acct.kind
        )));
    }
    let mut rules: Vec<AmortizationRuleRow> =
        store::read_table(wd, AMORTIZATION_RULES_PATH).await?;
    // If there's a currently-open rule on this account (effective_to is
    // None), close it at effective_from - 1 day. This way the new rule
    // supersedes from `effective_from` forward, and the algorithm still
    // picks the right rule for historical dates.
    let prev_close = effective_from
        .pred_opt()
        .ok_or_else(|| CliError::Invalid("effective_from underflow".into()))?;
    for r in rules.iter_mut() {
        if r.account_code == account
            && r.effective_to.is_none()
            && r.effective_from < effective_from
        {
            r.effective_to = Some(prev_close);
        }
    }
    let rule_id = store::next_id(&rules, |r| r.rule_id);
    let row = AmortizationRuleRow {
        rule_id,
        account_code: account,
        this_cycle_fraction: this,
        next_cycle_fraction: next,
        effective_from,
        effective_to: None,
        description,
    };
    log::info!(
        "[OK] policy amortize add #{} account={} this={} next={} from={}",
        row.rule_id,
        row.account_code,
        row.this_cycle_fraction,
        row.next_cycle_fraction,
        row.effective_from
    );
    rules.push(row);
    store::write_table(wd, AMORTIZATION_RULES_PATH, &rules).await?;
    Ok(rule_id)
}

pub async fn amortize_list(wd: &WD, effective_as_of: Option<&str>) -> Result<()> {
    let rules: Vec<AmortizationRuleRow> = store::read_table(wd, AMORTIZATION_RULES_PATH).await?;
    let chart: Vec<ChartOfAccountsRow> = store::read_table(wd, CHART_OF_ACCOUNTS_PATH).await?;
    let as_of = match effective_as_of {
        Some(s) => Some(parse_ymd(s)?),
        None => None,
    };
    log::info!(
        "{:>4}  {:>4}  {:<20}  {:>5}  {:>5}  {:<12}  {:<12}  description",
        "id",
        "acct",
        "name",
        "this",
        "next",
        "from",
        "to"
    );
    for r in rules.iter() {
        if let Some(d) = as_of {
            let in_range = r.effective_from <= d && r.effective_to.is_none_or(|to| d <= to);
            if !in_range {
                continue;
            }
        }
        let aname = chart
            .iter()
            .find(|a| a.code == r.account_code)
            .map(|a| a.name.as_str())
            .unwrap_or("(orphan)");
        log::info!(
            "{:>4}  {:>4}  {:<20}  {:>5.2}  {:>5.2}  {:<12}  {:<12}  {}",
            r.rule_id,
            r.account_code,
            aname,
            r.this_cycle_fraction,
            r.next_cycle_fraction,
            r.effective_from,
            fmt_ymd_opt(r.effective_to),
            r.description.as_deref().unwrap_or("-"),
        );
    }
    Ok(())
}

/// Default amortization seed (run once from `initialize`): 5300 Insurance
/// and 5400 Taxes both 50/50 from epoch.
pub async fn seed_default_amortization_rules(wd: &WD) -> tinyfs::Result<()> {
    let existing: Vec<AmortizationRuleRow> = store::read_table(wd, AMORTIZATION_RULES_PATH).await?;
    if !existing.is_empty() {
        return Ok(());
    }
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    let rows = vec![
        AmortizationRuleRow {
            rule_id: 1,
            account_code: 5300,
            this_cycle_fraction: 0.5,
            next_cycle_fraction: 0.5,
            effective_from: epoch,
            effective_to: None,
            description: Some("Insurance amortized 50/50 across this and next cycle".into()),
        },
        AmortizationRuleRow {
            rule_id: 2,
            account_code: 5400,
            this_cycle_fraction: 0.5,
            next_cycle_fraction: 0.5,
            effective_from: epoch,
            effective_to: None,
            description: Some("Taxes amortized 50/50 across this and next cycle".into()),
        },
    ];
    store::write_table(wd, AMORTIZATION_RULES_PATH, &rows).await
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup() -> WD {
        let fs = tinyfs::memory::new_fs().await;
        let wd = fs.root().await.unwrap();
        let _ = wd.create_dir_all("/data").await.unwrap();
        crate::chart::seed(&wd).await.unwrap();
        wd
    }

    #[tokio::test]
    async fn add_assigns_id() {
        let wd = setup().await;
        let id = add(
            &wd,
            "p1".into(),
            "share-by-weight".into(),
            "margin: 0.2\nweights:\n  commercial: 2.0\n  residential: 1.0\n".into(),
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(id, 1);
    }

    #[tokio::test]
    async fn add_rejects_duplicate_name() {
        let wd = setup().await;
        let _ = add(
            &wd,
            "p1".into(),
            "share-by-weight".into(),
            "margin: 0.2\nweights:\n  commercial: 2.0\n  residential: 1.0\n".into(),
            None,
            None,
        )
        .await
        .unwrap();
        let err = add(
            &wd,
            "p1".into(),
            "share-by-weight".into(),
            "margin: 0.3\nweights:\n  commercial: 2.0\n  residential: 1.0\n".into(),
            None,
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn add_rejects_invalid_yaml() {
        let wd = setup().await;
        let err = add(
            &wd,
            "p1".into(),
            "share-by-weight".into(),
            ": : : not yaml".into(),
            None,
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn amortize_add_validates_sum() {
        let wd = setup().await;
        let err = amortize_add(&wd, 5400, 0.6, 0.5, "1970-01-01", None)
            .await
            .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn amortize_add_validates_account_kind() {
        let wd = setup().await;
        // 1100 is asset (AR), not expense.
        let err = amortize_add(&wd, 1100, 0.5, 0.5, "1970-01-01", None)
            .await
            .unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[tokio::test]
    async fn amortize_add_supersedes_open_prior_rule() {
        let wd = setup().await;
        let _ = amortize_add(&wd, 5400, 0.5, 0.5, "1970-01-01", None)
            .await
            .unwrap();
        let _ = amortize_add(&wd, 5400, 0.6, 0.4, "2025-01-01", None)
            .await
            .unwrap();
        let rules: Vec<AmortizationRuleRow> = store::read_table(&wd, AMORTIZATION_RULES_PATH)
            .await
            .unwrap();
        let prior = rules.iter().find(|r| r.rule_id == 1).unwrap();
        assert_eq!(
            prior.effective_to,
            Some(NaiveDate::from_ymd_opt(2024, 12, 31).unwrap())
        );
        let new = rules.iter().find(|r| r.rule_id == 2).unwrap();
        assert_eq!(new.effective_to, None);
    }

    #[tokio::test]
    async fn seed_default_amortization_rules_idempotent() {
        let wd = setup().await;
        seed_default_amortization_rules(&wd).await.unwrap();
        seed_default_amortization_rules(&wd).await.unwrap();
        let rules: Vec<AmortizationRuleRow> = store::read_table(&wd, AMORTIZATION_RULES_PATH)
            .await
            .unwrap();
        assert_eq!(rules.len(), 2);
        assert!(rules.iter().any(|r| r.account_code == 5300));
        assert!(rules.iter().any(|r| r.account_code == 5400));
    }

    /// Per final rubber-duck (#1 non-blocker): policy edit must reject
    /// effective_to < effective_from.
    #[tokio::test]
    async fn edit_rejects_inverted_date_range() {
        let wd = setup().await;
        let _ = add(
            &wd,
            "p1".into(),
            "share-by-weight".into(),
            "margin: 0.2\nweights:\n  commercial: 2.0\n  residential: 1.0\n".into(),
            Some("2024-01-01".into()),
            None,
        )
        .await
        .unwrap();
        let err = edit(&wd, "p1", None, Some("2023-01-01")).await.unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)), "got {err:?}");
        let msg = format!("{err}");
        assert!(msg.contains("cannot precede effective_from"), "got: {msg}");
    }
}
