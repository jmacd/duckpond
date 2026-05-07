// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `share-by-weight` strategy.
//!
//! ## Required YAML
//!
//! ```yaml
//! margin: 0.2
//! weights:
//!   commercial: 2.0
//!   residential: 1.0
//! ```
//!
//! ## Optional YAML
//!
//! ```yaml
//! denominator: 14   # int|float; defaults to sum of weights across active
//!                   # connections (auto-grows with the system).
//! ```
//!
//! ## Algorithm (matches the design doc; differs slightly from Go on
//! per-customer pennies for commercial customers, see `shuffle.rs`):
//!
//! 1. `total = cycle_cost.scale_f64(1.0 + margin)`  (truncates toward
//!    zero -- matches Go's `int64(f * float64(units))`).
//! 2. For each active connection, `weight[i]` = `commercial` weight if
//!    the connection is commercial, else `residential`.
//! 3. `denominator` = configured value or `sum(weight[i])`.
//! 4. `raw_share[i] = total.scale_f64(weight[i] / denominator)`.
//! 5. **Deterministic-shuffle rounding**: `pennies = total - sum(raw_share)`.
//!    Pick a shuffled order over the active connections seeded by the
//!    cycle's period-close timestamp; bump each of the first
//!    `|pennies|` connections by `sign(pennies)`.
//! 6. `base_cents[i] = raw_share[i].scale_f64(1.0 / (1.0 + margin))`,
//!    `margin_cents[i] = total[i] - base_cents[i]`.
//!
//! For the "introductory" historical period, set `margin: 0.0` and both
//! weights to `1.0` (denominator omitted -- defaults to count of active
//! connections). This is exactly the Go program's `IntroductoryMethod`.

use crate::cli::{CliError, Result};
use crate::money::Cents;
use crate::policy::shuffle::SplitMix64;
use crate::policy::{AllocateContext, BillRow, BillingPolicy};
use chrono::{NaiveDateTime, NaiveTime};
use linkme::distributed_slice;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    margin: f64,
    weights: Weights,
    /// Optional fixed denominator. If `None`, the strategy uses the sum
    /// of weights across active connections.
    #[serde(default)]
    denominator: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Weights {
    commercial: f64,
    residential: f64,
}

impl Config {
    fn parse(yaml: &str) -> Result<Self> {
        let cfg: Self = serde_yaml::from_str(yaml)
            .map_err(|e| CliError::Invalid(format!("invalid share-by-weight config: {e}")))?;
        if !cfg.margin.is_finite() || cfg.margin < 0.0 {
            return Err(CliError::Invalid(format!(
                "margin must be a non-negative finite number (got {})",
                cfg.margin
            )));
        }
        // > 100x markup is almost certainly a typo (e.g., `margin: 100`
        // when the operator meant `margin: 1.0`).
        if cfg.margin > 100.0 {
            return Err(CliError::Invalid(format!(
                "margin {} is implausibly large (> 10000%); likely a typo",
                cfg.margin
            )));
        }
        if !cfg.weights.commercial.is_finite() || cfg.weights.commercial <= 0.0 {
            return Err(CliError::Invalid(format!(
                "weights.commercial must be a positive finite number (got {})",
                cfg.weights.commercial
            )));
        }
        if !cfg.weights.residential.is_finite() || cfg.weights.residential <= 0.0 {
            return Err(CliError::Invalid(format!(
                "weights.residential must be a positive finite number (got {})",
                cfg.weights.residential
            )));
        }
        if let Some(d) = cfg.denominator {
            if !d.is_finite() || d <= 0.0 {
                return Err(CliError::Invalid(format!(
                    "denominator must be a positive finite number (got {d})"
                )));
            }
            // Smallest reasonable weight is 1.0, so denominator < 0.5
            // implies a degenerate partition that produces wrap-around
            // bumps at allocation time.
            if d < 0.5 {
                return Err(CliError::Invalid(format!(
                    "denominator {d} is implausibly small (< 0.5); likely a typo"
                )));
            }
        }
        Ok(cfg)
    }

    fn weight_for(&self, c: &crate::schema::ConnectionRow) -> f64 {
        if c.commercial {
            self.weights.commercial
        } else {
            self.weights.residential
        }
    }
}

pub struct ShareByWeight;

impl BillingPolicy for ShareByWeight {
    fn kind(&self) -> &'static str {
        "share-by-weight"
    }

    fn description(&self) -> &'static str {
        "Distribute cycle cost by per-connection weight (commercial/residential)"
    }

    fn config_help(&self) -> &'static str {
        "required: margin (>=0), weights.commercial (>0), weights.residential (>0); \
         optional: denominator (>0; defaults to sum of weights)"
    }

    fn validate(&self, yaml: &str) -> Result<()> {
        let _ = Config::parse(yaml)?;
        Ok(())
    }

    fn allocate(&self, yaml: &str, ctx: &AllocateContext<'_>) -> Result<Vec<BillRow>> {
        let cfg = Config::parse(yaml)?;
        if ctx.active_connections.is_empty() {
            return Ok(Vec::new());
        }

        // Sort active connections by connection_id so the shuffle is
        // reproducible regardless of caller-provided order.
        let mut sorted: Vec<&crate::schema::ConnectionRow> =
            ctx.active_connections.iter().collect();
        sorted.sort_by_key(|c| c.connection_id);

        // Headline figures: cycle_cost is the "cost before margin"; total is
        // what the operator collects ("cycle_cost * (1 + margin)" truncated).
        let cycle_cost = ctx.cycle_cost;
        let raw_total = cycle_cost.scale_f64(1.0 + cfg.margin);

        // Per-connection weights and denominator.
        let weights: Vec<f64> = sorted.iter().map(|c| cfg.weight_for(c)).collect();
        let denominator = cfg.denominator.unwrap_or_else(|| weights.iter().sum());
        if denominator <= 0.0 {
            return Err(CliError::Invalid(format!(
                "denominator computed as {denominator} (sum of weights); cannot allocate"
            )));
        }

        // Build a single shuffled permutation; both `total` and `base`
        // distribute their rounding pennies in this order so the per-row
        // bumps cancel correctly (preserving margin = total - base >= 0).
        let n = sorted.len();
        let mut order: Vec<usize> = (0..n).collect();
        let mut rng = SplitMix64::new(seed_from_cycle(ctx) as u64);
        rng.shuffle(&mut order);

        let totals = allocate_with_shuffle(raw_total, &weights, denominator, &order)?;
        let bases = allocate_with_shuffle(cycle_cost, &weights, denominator, &order)?;

        let inv_denom = 1.0 / denominator;
        let mut rows = Vec::with_capacity(n);
        for (i, conn) in sorted.iter().enumerate() {
            let total = totals[i];
            let base = bases[i];
            let margin = total - base;
            rows.push(BillRow {
                connection_id: conn.connection_id,
                customer_id: ctx.responsible_customer.get(&conn.connection_id).copied(),
                total_cents: total,
                base_cents: base,
                margin_cents: margin,
                weight: weights[i],
                share_fraction: weights[i] * inv_denom,
            });
        }
        Ok(rows)
    }
}

/// Distribute `total` across `weights.len()` rows. Each row's raw share
/// is `total.scale_f64(weight[i] / denominator)`. The remainder pennies
/// (`total - sum(raw_share)`) are bumped one each to the rows in
/// `shuffled_order` order. Errors if the rounding remainder is too large
/// to be a single-bump distribution (`|pennies| > N`), which indicates a
/// degenerate config (typically a fixed denominator that grossly
/// mismatches the active-connection set).
fn allocate_with_shuffle(
    total: Cents,
    weights: &[f64],
    denominator: f64,
    shuffled_order: &[usize],
) -> Result<Vec<i64>> {
    let n = weights.len();
    let mut totals: Vec<i64> = weights
        .iter()
        .map(|w| total.scale_f64(*w / denominator).units())
        .collect();
    let pennies: i64 = total.units() - totals.iter().sum::<i64>();
    if pennies != 0 {
        let bumps_unsigned: u64 = pennies.unsigned_abs();
        if bumps_unsigned > n as u64 {
            return Err(CliError::Invalid(format!(
                "policy config produces {bumps_unsigned}c of rounding remainder across {n} \
                 connection(s) (>1c each); check that denominator matches the active-connection \
                 weight sum"
            )));
        }
        let bump_sign: i64 = if pennies > 0 { 1 } else { -1 };
        for &idx in shuffled_order.iter().take(bumps_unsigned as usize) {
            totals[idx] += bump_sign;
        }
    }
    Ok(totals)
}

/// Seed = cycle period_end at midnight UTC, in nanoseconds since epoch.
/// Mirrors the Go program's `cycle.PeriodStart.Closing().Date().UnixNano()`
/// (where "closing" means the last day of the period == our period_end).
fn seed_from_cycle(ctx: &AllocateContext<'_>) -> i64 {
    let dt = NaiveDateTime::new(
        ctx.cycle.period_end,
        NaiveTime::from_hms_opt(0, 0, 0).expect("midnight"),
    );
    dt.and_utc()
        .timestamp_nanos_opt()
        .expect("cycle period_end timestamp must fit in i64 nanoseconds")
}

#[allow(unsafe_code)]
#[distributed_slice(crate::policy::BILLING_POLICIES)]
static SHARE_BY_WEIGHT: &dyn BillingPolicy = &ShareByWeight;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::find;
    use crate::schema::{ConnectionRow, CycleRow};
    use chrono::NaiveDate;
    use std::collections::HashMap;

    fn make_conn(id: i32, name: &str, commercial: bool) -> ConnectionRow {
        ConnectionRow {
            connection_id: id,
            name: name.into(),
            service_address: format!("{name} addr"),
            first_active: NaiveDate::from_ymd_opt(2010, 1, 1).unwrap(),
            last_active: None,
            commercial,
            weight_class: None,
            notes: None,
        }
    }

    fn make_cycle() -> CycleRow {
        CycleRow {
            cycle_id: 1,
            name: "2024H1".into(),
            period_start: NaiveDate::from_ymd_opt(2024, 4, 1).unwrap(),
            period_end: NaiveDate::from_ymd_opt(2024, 9, 30).unwrap(),
            bill_date: NaiveDate::from_ymd_opt(2024, 10, 1).unwrap(),
            policy_id: 1,
            inactive: vec![],
            notes: None,
            issued: false,
            bill_txn_id: None,
        }
    }

    fn run(yaml: &str, conns: Vec<ConnectionRow>, cost_cents: i64) -> Vec<BillRow> {
        let cycle = make_cycle();
        let mut customers = HashMap::new();
        for c in &conns {
            let _ = customers.insert(c.connection_id, c.connection_id * 10);
        }
        let ctx = AllocateContext {
            cycle: &cycle,
            active_connections: &conns,
            responsible_customer: &customers,
            cycle_cost: Cents::from_units(cost_cents),
        };
        ShareByWeight.allocate(yaml, &ctx).expect("allocate")
    }

    // ------------------------------------------------------------------
    // Validation
    // ------------------------------------------------------------------

    #[test]
    fn validate_accepts_minimal_config() {
        let yaml = "margin: 0.2\nweights:\n  commercial: 2.0\n  residential: 1.0\n";
        ShareByWeight.validate(yaml).expect("valid");
    }

    #[test]
    fn validate_rejects_unknown_field() {
        let yaml = "margin: 0.2\nweights:\n  commercial: 2.0\n  residential: 1.0\ngarbage: yes\n";
        let err = ShareByWeight.validate(yaml).unwrap_err();
        assert!(matches!(err, CliError::Invalid(_)));
    }

    #[test]
    fn validate_rejects_negative_margin() {
        let yaml = "margin: -0.1\nweights:\n  commercial: 2.0\n  residential: 1.0\n";
        assert!(ShareByWeight.validate(yaml).is_err());
    }

    #[test]
    fn validate_rejects_zero_weight() {
        let yaml = "margin: 0.0\nweights:\n  commercial: 0.0\n  residential: 1.0\n";
        assert!(ShareByWeight.validate(yaml).is_err());
    }

    #[test]
    fn validate_rejects_nan() {
        let yaml = "margin: .nan\nweights:\n  commercial: 2.0\n  residential: 1.0\n";
        assert!(ShareByWeight.validate(yaml).is_err());
    }

    #[test]
    fn validate_accepts_optional_denominator() {
        let yaml =
            "margin: 0.2\nweights:\n  commercial: 2.0\n  residential: 1.0\ndenominator: 14\n";
        ShareByWeight.validate(yaml).expect("valid");
    }

    // ------------------------------------------------------------------
    // Allocation arithmetic
    // ------------------------------------------------------------------

    #[test]
    fn allocates_evenly_when_no_remainder() {
        // 4 residential connections, margin 0, total $400. Each gets $100.
        let yaml = "margin: 0.0\nweights:\n  commercial: 2.0\n  residential: 1.0\n";
        let conns = (1..=4)
            .map(|i| make_conn(i, &format!("c{i}"), false))
            .collect();
        let rows = run(yaml, conns, 40000);
        assert_eq!(rows.len(), 4);
        for r in &rows {
            assert_eq!(r.total_cents, 10000);
            assert_eq!(r.weight, 1.0);
            assert_eq!(r.share_fraction, 0.25);
            assert_eq!(r.margin_cents, 0);
            assert_eq!(r.base_cents, 10000);
        }
    }

    #[test]
    fn distributes_rounding_pennies_deterministically() {
        // 3 connections, total $1.00 (100 cents). Each gets ~33.33.
        // raw_share = 100 * (1/3) = 33 (truncated). Sum = 99. Pennies = 1.
        // So one connection gets +1 cent.
        let yaml = "margin: 0.0\nweights:\n  commercial: 2.0\n  residential: 1.0\n";
        let conns: Vec<_> = (1..=3)
            .map(|i| make_conn(i, &format!("c{i}"), false))
            .collect();
        let rows1 = run(yaml, conns.clone(), 100);
        let rows2 = run(yaml, conns, 100);
        // Sum is exactly the total.
        assert_eq!(rows1.iter().map(|r| r.total_cents).sum::<i64>(), 100);
        // Deterministic: same seed -> same shuffle.
        assert_eq!(
            rows1.iter().map(|r| r.total_cents).collect::<Vec<_>>(),
            rows2.iter().map(|r| r.total_cents).collect::<Vec<_>>()
        );
        // Exactly one row has the +1 bump.
        let bumped = rows1.iter().filter(|r| r.total_cents == 34).count();
        assert_eq!(bumped, 1);
    }

    #[test]
    fn applies_margin() {
        // Total cost $1000, margin 0.2 -> revenue total = $1200.
        // 4 residential connections, each weight 1, denominator = 4.
        // Each gets $300 total = $250 base + $50 margin.
        let yaml = "margin: 0.2\nweights:\n  commercial: 2.0\n  residential: 1.0\n";
        let conns = (1..=4)
            .map(|i| make_conn(i, &format!("c{i}"), false))
            .collect();
        let rows = run(yaml, conns, 100000);
        let total_revenue: i64 = rows.iter().map(|r| r.total_cents).sum();
        assert_eq!(total_revenue, 120000, "$1000 * 1.2 = $1200");
        for r in &rows {
            assert_eq!(r.total_cents, 30000);
            assert_eq!(r.base_cents, 25000);
            assert_eq!(r.margin_cents, 5000);
        }
    }

    #[test]
    fn commercial_doubles_under_default_weight_config() {
        // 13 residential + 1 commercial = denominator 13*1 + 1*2 = 15.
        // (Using the operator-omits-denominator path.)
        let yaml = "margin: 0.0\nweights:\n  commercial: 2.0\n  residential: 1.0\n";
        let mut conns: Vec<_> = (1..=13)
            .map(|i| make_conn(i, &format!("r{i}"), false))
            .collect();
        conns.push(make_conn(14, "comm", true));
        let rows = run(yaml, conns, 150_000); // $1500 total
        // Residential: 1500 * 1/15 = $100 each. Commercial: $200.
        let r1 = &rows.iter().find(|r| r.connection_id == 1).unwrap();
        let comm = &rows.iter().find(|r| r.connection_id == 14).unwrap();
        assert_eq!(r1.total_cents, 10000);
        assert_eq!(comm.total_cents, 20000);
        assert_eq!(comm.weight, 2.0);
        // Sum is exact.
        assert_eq!(rows.iter().map(|r| r.total_cents).sum::<i64>(), 150_000);
    }

    #[test]
    fn fixed_denominator_pinning_old_test_removed() {
        // The old test asserted that 1 connection with denom=14 produced
        // a $15.00 total via penny wraparound. Per the rubber-duck (Phase 5
        // critique), this is now an error -- the new test
        // `fixed_denominator_pinning_rejected_when_pathological` covers it.
    }

    #[test]
    fn customer_id_resolved_from_map() {
        let yaml = "margin: 0.0\nweights:\n  commercial: 2.0\n  residential: 1.0\n";
        let conns = vec![make_conn(1, "a", false), make_conn(2, "b", false)];
        let cycle = make_cycle();
        let mut customers = HashMap::new();
        let _ = customers.insert(1, 100);
        // Connection 2 has NO covering tenancy -> customer_id is None.
        let ctx = AllocateContext {
            cycle: &cycle,
            active_connections: &conns,
            responsible_customer: &customers,
            cycle_cost: Cents::from_units(200),
        };
        let rows = ShareByWeight.allocate(yaml, &ctx).unwrap();
        assert_eq!(rows[0].customer_id, Some(100));
        assert_eq!(rows[1].customer_id, None);
    }

    #[test]
    fn empty_active_connections_yields_empty() {
        let yaml = "margin: 0.0\nweights:\n  commercial: 2.0\n  residential: 1.0\n";
        let rows = run(yaml, vec![], 10000);
        assert!(rows.is_empty());
    }

    #[test]
    fn introductory_period_is_just_a_config_variant() {
        // Go's IntroductoryMethod = equal share with no margin.
        let yaml = "margin: 0.0\nweights:\n  commercial: 1.0\n  residential: 1.0\n";
        let mut conns: Vec<_> = (1..=10)
            .map(|i| make_conn(i, &format!("c{i}"), false))
            .collect();
        conns.push(make_conn(11, "comm", true)); // commercial counts as 1 here
        let rows = run(yaml, conns, 110_000); // $1100 / 11 = $100 each
        for r in &rows {
            assert_eq!(r.total_cents, 10000);
            assert_eq!(r.weight, 1.0);
            assert_eq!(r.margin_cents, 0);
        }
    }

    #[test]
    fn registered_in_distributed_slice() {
        let s = find("share-by-weight").expect("registered");
        assert_eq!(s.kind(), "share-by-weight");
    }

    /// Cycle-level invariants the rubber-duck flagged: for any allocation,
    /// `sum(base) == cycle_cost`, `sum(margin) == raw_total - cycle_cost`,
    /// and per-row `total == base + margin`.
    #[test]
    fn aggregate_base_and_margin_preserved() {
        // 3 equal residential rows, $10.00 cycle cost, margin 20%.
        // raw_total = 1200, raw_base = 1000, raw_margin = 200.
        let yaml = "margin: 0.2\nweights:\n  commercial: 2.0\n  residential: 1.0\n";
        let conns = (1..=3)
            .map(|i| make_conn(i, &format!("c{i}"), false))
            .collect();
        let rows = run(yaml, conns, 1000);
        let sum_total: i64 = rows.iter().map(|r| r.total_cents).sum();
        let sum_base: i64 = rows.iter().map(|r| r.base_cents).sum();
        let sum_margin: i64 = rows.iter().map(|r| r.margin_cents).sum();
        assert_eq!(sum_total, 1200, "raw_total");
        assert_eq!(sum_base, 1000, "sum(base) must equal cycle_cost");
        assert_eq!(
            sum_margin, 200,
            "sum(margin) must equal raw_total - cycle_cost"
        );
        for r in &rows {
            assert_eq!(r.base_cents + r.margin_cents, r.total_cents);
            assert!(r.margin_cents >= 0, "margin must be non-negative");
        }
    }

    #[test]
    fn allocation_independent_of_input_order() {
        // Two callers passing the same connections in different order must
        // produce the same per-connection bills (with rows sorted by id).
        let yaml = "margin: 0.0\nweights:\n  commercial: 2.0\n  residential: 1.0\n";
        let mut order_a: Vec<_> = (1..=5)
            .map(|i| make_conn(i, &format!("c{i}"), false))
            .collect();
        let order_b: Vec<_> = order_a.iter().rev().cloned().collect();
        let rows_a = run(yaml, order_a.clone(), 1000);
        let rows_b = run(yaml, order_b, 1000);
        // Both outputs should have the same per-connection_id totals.
        for ra in &rows_a {
            let rb = rows_b
                .iter()
                .find(|r| r.connection_id == ra.connection_id)
                .unwrap();
            assert_eq!(
                ra.total_cents, rb.total_cents,
                "connection #{}",
                ra.connection_id
            );
        }
        // And rows are returned in connection_id order (sort by id internally).
        assert_eq!(
            rows_a.iter().map(|r| r.connection_id).collect::<Vec<_>>(),
            (1..=5).collect::<Vec<_>>()
        );
        let _ = &mut order_a; // silence unused-mut from the .clone() above
    }

    #[test]
    fn validate_rejects_implausible_margin() {
        let yaml = "margin: 1000.0\nweights:\n  commercial: 2.0\n  residential: 1.0\n";
        assert!(ShareByWeight.validate(yaml).is_err());
    }

    #[test]
    fn validate_rejects_implausible_denominator() {
        let yaml =
            "margin: 0.0\nweights:\n  commercial: 2.0\n  residential: 1.0\ndenominator: 0.001\n";
        assert!(ShareByWeight.validate(yaml).is_err());
    }
}
