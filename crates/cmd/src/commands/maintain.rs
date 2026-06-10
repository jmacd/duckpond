// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::common::ShipContext;
use anyhow::{Result, anyhow};
use log::info;

/// Run Delta Lake maintenance on both data and control tables.
///
/// Performs checkpoint creation, log cleanup, and vacuum.
/// When `compact` is true, also merges small parquet files.
pub async fn maintain_command(ship_context: &ShipContext, compact: bool) -> Result<()> {
    let pond_path = ship_context.resolve_pond_path()?;
    info!("Running maintenance on pond: {}", pond_path.display());

    let mut ship = ship_context
        .open_pond()
        .await
        .map_err(|e| anyhow!("Failed to open pond: {}", e))?;

    let report = ship.maintain(true, compact).await;

    // Print results to stdout
    #[allow(clippy::print_stdout)]
    {
        if let Some(ref data) = report.data {
            println!("{}", data);
        }
        if let Some(ref control) = report.control {
            println!("{}", control);
        }
        if compact && report.data.as_ref().map(|d| d.compacted).unwrap_or(false) {
            println!(
                "  data compaction recorded as a transaction; \
                 run `pond push` to replicate it as a Compact bundle"
            );
        }
    }

    info!("[OK] Maintenance completed");
    Ok(())
}
