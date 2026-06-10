// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Result, anyhow};

use crate::common::ShipContext;
use log::info;
use std::path::Path;

/// Initialize a new pond at the specified path.
///
/// This is the only command that doesn't receive a Ship since it creates one.
///
/// Replica bootstrap (formerly `--from-backup` / `--config`) is going
/// away in the D4 redesign: use `pond init` + `pond remote add` +
/// `pond pull` (or the future `pond restart-from-compact`) instead.
pub async fn init_command(ship_context: &ShipContext) -> Result<()> {
    let pond_path = ship_context.resolve_pond_path()?;
    let pond_path_display = pond_path.display().to_string();

    // Check if pond already exists
    let data_path: std::path::PathBuf = Path::new(&pond_path).join("data");
    if data_path.exists() {
        let data_log = data_path.join("_delta_log");
        if data_log.exists() {
            return Err(anyhow!("Pond already exists"));
        }
    }

    info!("Initializing pond at: {pond_path_display}");
    init_normal(ship_context).await
}

/// Normal initialization - creates empty pond with initial transaction
async fn init_normal(ship_context: &ShipContext) -> Result<()> {
    // Pond doesn't exist, so create a new one
    // This creates both the filesystem infrastructure AND the initial /txn/1 transaction
    let _ship = ship_context.create_pond().await?;
    log::debug!("Pond initialized successfully with transaction #1");
    Ok(())
}
