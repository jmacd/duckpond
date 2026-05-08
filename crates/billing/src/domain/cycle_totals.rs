// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `cycle totals refresh` -- materialize cycle totals to parquet.
//!
//! Reads the journal of expenses + amortization_rules + cycles and writes
//! `/data/cycle_totals_materialized.parquet` (entry type
//! TablePhysicalVersion). Idempotent: each call fully rewrites the file.

use crate::cli::Result;
use crate::domain::amortization;
use crate::schema::{
    AMORTIZATION_RULES_PATH, AmortizationRuleRow, CYCLE_TOTALS_MATERIALIZED_PATH, CYCLES_PATH,
    CycleRow, CycleTotalRow, EXPENSES_PATH, ExpenseRow,
};
use crate::store;
use tinyfs::WD;

/// Returns `(rows_written, unassigned_count)`.
pub async fn refresh(wd: &WD) -> Result<(usize, usize)> {
    let expenses: Vec<ExpenseRow> = store::read_series(wd, EXPENSES_PATH).await?;
    let cycles: Vec<CycleRow> = store::read_table(wd, CYCLES_PATH).await?;
    let rules: Vec<AmortizationRuleRow> = store::read_table(wd, AMORTIZATION_RULES_PATH).await?;
    let (totals, unassigned) = amortization::compute_totals(&expenses, &cycles, &rules);
    store::write_table(wd, CYCLE_TOTALS_MATERIALIZED_PATH, &totals).await?;
    for (eid, reason) in &unassigned {
        log::warn!("unassigned expense #{eid}: {reason}");
    }
    Ok((totals.len(), unassigned.len()))
}

/// Read the materialized totals (`cycle totals` subcommand reads from
/// here, NOT from raw expenses, so totals reflect the most recent
/// `refresh` call).
pub async fn read(wd: &WD) -> Result<Vec<CycleTotalRow>> {
    let rows: Vec<CycleTotalRow> = store::read_table(wd, CYCLE_TOTALS_MATERIALIZED_PATH).await?;
    Ok(rows)
}
