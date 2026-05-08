// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! BillingPolicy trait, registry, and value types.
//!
//! Each policy strategy (one per `kind` string in `billing_policies.kind`)
//! implements `BillingPolicy` and registers itself via the linkme
//! distributed slice `BILLING_POLICIES`. Lookup is by kind name.
//!
//! Day-1 ships exactly one strategy: `share-by-weight`. New strategies
//! ship as additional `register!` invocations under
//! `crates/billing/src/policy/`.

pub mod share_by_weight;
pub mod shuffle;

use crate::cli::{CliError, Result};
use crate::money::Cents;
use crate::schema::{ConnectionRow, CycleRow};
use linkme::distributed_slice;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// One row of the `bill-allocator` output. Sum of `total_cents` over all
/// rows for a cycle equals the cycle's pre-margin cost times `(1 + margin)`,
/// truncated -- bit-identical to the Go program at the cycle level.
#[derive(Debug, Clone, PartialEq)]
pub struct BillRow {
    pub connection_id: i32,
    /// `None` if no tenancy covers `cycle.bill_date` for this connection.
    /// `bills issue` refuses to write if any row has a `None` here; the
    /// allocator itself surfaces the gap to the caller.
    pub customer_id: Option<i32>,
    pub total_cents: i64,
    pub base_cents: i64,
    pub margin_cents: i64,
    pub weight: f64,
    pub share_fraction: f64,
}

/// Inputs the strategy needs. Built by `bills preview` / `bills issue` /
/// the bill-allocator factory; the strategy is a pure function of this
/// plus the parsed YAML config.
pub struct AllocateContext<'a> {
    /// The cycle being billed. The strategy reads `cycle.bill_date` (for
    /// computing share fractions and seeding the rounding shuffle from
    /// the period close) and `cycle.policy_id` (to look up its config).
    pub cycle: &'a CycleRow,
    /// Active connections at `cycle.bill_date`, with `cycle.inactive`
    /// already filtered out. Order is significant only in that the
    /// allocator returns one BillRow per entry in the same order; tests
    /// rely on this for deterministic comparisons.
    pub active_connections: &'a [ConnectionRow],
    /// `connection_id -> customer_id` resolved via tenancies on
    /// `cycle.bill_date`. Connections with no covering tenancy are
    /// absent (the allocator emits `customer_id = None` for those).
    pub responsible_customer: &'a HashMap<i32, i32>,
    /// Total cycle cost (sum of `cycle_totals_materialized.amount_cents`
    /// for this cycle) in cents. The strategy applies its margin on top.
    pub cycle_cost: Cents,
}

/// A registered billing strategy.
pub trait BillingPolicy: Sync + Send {
    /// Stable kind identifier (e.g. `"share-by-weight"`). Matches
    /// `billing_policies.kind`.
    fn kind(&self) -> &'static str;

    /// One-line description shown by `policy kinds`.
    fn description(&self) -> &'static str;

    /// One-line summary of required YAML fields (shown by `policy kinds`).
    fn config_help(&self) -> &'static str;

    /// Validate YAML config. Errors point at the offending field.
    /// Called by `policy add` and at allocator dispatch time.
    fn validate(&self, yaml: &str) -> Result<()>;

    /// Compute one BillRow per active connection.
    fn allocate(&self, yaml: &str, ctx: &AllocateContext<'_>) -> Result<Vec<BillRow>>;
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

#[allow(unsafe_code)]
#[allow(clippy::declare_interior_mutable_const)]
#[distributed_slice]
pub static BILLING_POLICIES: [&'static dyn BillingPolicy] = [..];

/// Look up a strategy by `kind`. Returns `None` if no strategy is
/// registered with that kind.
#[must_use]
pub fn find(kind: &str) -> Option<&'static dyn BillingPolicy> {
    BILLING_POLICIES.iter().find(|p| p.kind() == kind).copied()
}

/// All registered kinds, sorted alphabetically. Used by `policy kinds`.
#[must_use]
pub fn kinds() -> Vec<&'static str> {
    let mut v: Vec<&'static str> = BILLING_POLICIES.iter().map(|p| p.kind()).collect();
    v.sort_unstable();
    v
}

/// Helper for `policy add` and `bill-allocator`: look up the strategy or
/// produce a clear error.
pub fn require(kind: &str) -> Result<&'static dyn BillingPolicy> {
    find(kind).ok_or_else(|| {
        CliError::Invalid(format!(
            "no strategy registered for kind `{kind}`; run `policy kinds` to list available kinds"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn share_by_weight_registered() {
        assert!(find("share-by-weight").is_some());
    }

    #[test]
    fn unknown_kind_returns_none() {
        assert!(find("nonexistent-kind").is_none());
    }

    #[test]
    fn kinds_includes_share_by_weight() {
        assert!(kinds().contains(&"share-by-weight"));
    }

    #[test]
    fn require_errors_on_unknown() {
        let result = require("bogus");
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, CliError::Invalid(_)));
        }
    }
}
