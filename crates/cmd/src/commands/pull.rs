// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `pond pull [<name>]` -- pull new bundles from one or more remotes via
//! the D4 [`sync_remote::Remote`] pipeline.

use crate::commands::remote::{
    RemoteMode, list_remote_names, load_remote_attachment, remote_mode_for,
};
use crate::common::ShipContext;
use anyhow::{Result, anyhow};
use steward::ShipRemoteSteward;
use sync_remote::Remote;

/// Pull from `name`, or from every remote in `pull`/`both` mode when `name`
/// is `None`.  Each remote is processed independently.
pub async fn pull_command(ship_context: &ShipContext, name: Option<String>) -> Result<()> {
    let mut ship = ship_context.open_pond().await?;

    let targets: Vec<String> = if let Some(n) = name {
        vec![n]
    } else {
        let all = list_remote_names(&mut ship).await?;
        let mut filtered = Vec::new();
        for n in all {
            match remote_mode_for(&ship, &n).await? {
                RemoteMode::Pull | RemoteMode::Both => filtered.push(n),
                RemoteMode::Push => {
                    log::debug!("skip {}: mode=push", n);
                }
            }
        }
        filtered
    };

    if targets.is_empty() {
        log::info!("no remotes to pull from");
        return Ok(());
    }

    let mut had_error = false;
    for name in targets {
        if let Err(e) = pull_one(&mut ship, &name).await {
            log::error!("[ERR] pull {}: {}", name, e);
            had_error = true;
        }
    }

    if had_error {
        Err(anyhow!("one or more pulls failed"))
    } else {
        Ok(())
    }
}

async fn pull_one(ship: &mut steward::Steward, name: &str) -> Result<()> {
    let attachment = load_remote_attachment(ship, name).await?;

    if attachment.url.starts_with("s3://") {
        sync_remote::register_s3_handlers();
    }

    let storage_options = attachment.to_storage_options();
    let remote = Remote::open_at_url(&attachment.url, storage_options)
        .await
        .map_err(|e| anyhow!("open remote `{}` ({}): {}", name, attachment.url, e))?;

    let ship_ref = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("pull requires a pond steward (not a host steward)"))?;

    let mut adapter = ShipRemoteSteward::new(ship_ref);
    let report = remote
        .pull(&mut adapter)
        .await
        .map_err(|e| anyhow!("pull from `{}`: {}", attachment.url, e))?;

    log::info!(
        "[OK] pull {}: applied {} bundle(s) from {}",
        name,
        report.bundles_applied.len(),
        attachment.url
    );
    Ok(())
}
