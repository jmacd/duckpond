// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `pond push [<name>]` -- push the pond's content closure to one or more
//! remotes via the content-addressed [`sync_store::ContentRemote`] pipeline.

use crate::commands::remote::{
    RemoteMode, list_remote_names, load_remote_attachment, remote_mode_for,
};
use crate::common::ShipContext;
use anyhow::{Result, anyhow};

/// Push to `name`, or to every remote in `push`/`both` mode when `name` is
/// `None`.  Each remote is processed independently: a failure on one does
/// NOT halt the others.
pub async fn push_command(ship_context: &ShipContext, name: Option<String>) -> Result<()> {
    let mut ship = ship_context.open_pond().await?;

    let targets: Vec<String> = if let Some(n) = name {
        vec![n]
    } else {
        let all = list_remote_names(&mut ship).await?;
        let mut filtered = Vec::new();
        for n in all {
            match remote_mode_for(&ship, &n).await? {
                RemoteMode::Push | RemoteMode::Both => filtered.push(n),
                RemoteMode::Pull => {
                    log::debug!("skip {}: mode=pull", n);
                }
            }
        }
        filtered
    };

    if targets.is_empty() {
        log::info!("no remotes to push to");
        return Ok(());
    }

    let mut had_error = false;
    for name in targets {
        if let Err(e) = push_one(&mut ship, &name).await {
            log::error!("[ERR] push {}: {}", name, e);
            had_error = true;
        }
    }

    if had_error {
        Err(anyhow!("one or more pushes failed"))
    } else {
        Ok(())
    }
}

/// Push the pond's current content closure and tip commit to the named
/// remote under the `main` ref via the content-addressed pipeline.
async fn push_one(ship: &mut steward::Steward, name: &str) -> Result<()> {
    let attachment = load_remote_attachment(ship, name).await?;

    if attachment.url.starts_with("s3://") {
        sync_remote::register_s3_handlers();
    }
    let storage_options = attachment.to_storage_options()?;

    let ship_ref = ship
        .as_pond()
        .ok_or_else(|| anyhow!("push requires a pond steward (not a host steward)"))?;

    let mut remote = sync_store::ContentRemote::open_at_url(&attachment.url, storage_options)
        .await
        .map_err(|e| anyhow!("open remote `{}` ({}): {}", name, attachment.url, e))?;

    let outcome = steward::push_content_to_remote(ship_ref, &mut remote, "main")
        .await
        .map_err(|e| anyhow!("push {} ({}): {}", name, attachment.url, e))?;

    let tip_hex = outcome.tip.to_hex();
    log::info!(
        "[OK] push {} complete (objects_pushed={}, tip={})",
        name,
        outcome.objects_pushed,
        tip_hex
    );

    // Record the per-ref frontier: the single commit hash we last pushed to
    // this remote (the CA3 replacement for the retired per-pond seq watermark).
    ship.control_table_mut()
        .raw_config_set(&format!("last_pushed_tip:{}", attachment.url), &tip_hex)
        .await
        .map_err(|e| anyhow!("record last_pushed_tip for `{}`: {}", name, e))?;
    Ok(())
}
