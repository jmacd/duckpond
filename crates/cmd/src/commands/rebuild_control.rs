// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `pond rebuild-control` (D6.3) -- reconstruct the control table from
//! the data Delta table when the control table is lost or corrupt.
//!
//! See [`steward::rebuild_control_table`] for the recovery semantics
//! (what is and is not recoverable).  This command is a thin wrapper
//! that resolves the pond path, runs the rebuild, and reports the
//! outcome plus the required operator follow-up.

use crate::common::ShipContext;
use anyhow::{Result, anyhow};

/// Rebuild the control table for the pond at `ship_context`.  When
/// `force` is false and a control table already exists, the rebuild is
/// refused (the operator must opt in to moving the existing control
/// directory aside).
pub async fn rebuild_control_command(ship_context: &ShipContext, force: bool) -> Result<()> {
    let pond_path = ship_context.resolve_pond_path()?;

    let report = steward::rebuild_control_table(&pond_path, force)
        .await
        .map_err(|e| anyhow!("rebuild-control failed: {}", e))?;

    log::info!("[OK] rebuilt control table for pond {}", report.pond_id);
    log::info!(
        "     reconstructed {} transaction(s); last_txn_seq={}",
        report.txns_reconstructed,
        report.last_txn_seq
    );
    if let Some(backup) = &report.backup_path {
        log::info!("     previous control table moved to {}", backup.display());
    }
    log::warn!(
        "     settings were NOT recovered: re-attach remotes with \
         `pond remote add` / `pond backup add`, then re-baseline \
         `pond verify` (reconstructed checksums are empty)"
    );

    Ok(())
}
