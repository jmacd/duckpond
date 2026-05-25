// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `pond push [<name>]` -- push pending local txn_seqs to one or more
//! remotes via the D4 [`sync_remote::Remote`] pipeline.

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

/// Push every pending txn_seq from `last_pushed_seq + 1` up through
/// `ship.last_write_seq()` to the named remote.
async fn push_one(ship: &mut steward::Steward, name: &str) -> Result<()> {
    let attachment = load_remote_attachment(ship, name).await?;

    let ship_ref = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("push requires a pond steward (not a host steward)"))?;

    let outcome = steward::push_pending_to_remote(ship_ref, &attachment)
        .await
        .map_err(|e| anyhow!("push {} ({}): {}", name, attachment.url, e))?;

    if outcome.previous_last_pushed >= outcome.upper_seq {
        log::info!(
            "push {}: nothing to push (last_pushed_seq={} >= last_write_seq={})",
            name,
            outcome.previous_last_pushed,
            outcome.upper_seq
        );
    } else {
        log::info!(
            "[OK] push {} complete (pushed={}, skipped={}, range={}..={})",
            name,
            outcome.pushed,
            outcome.skipped,
            outcome.previous_last_pushed + 1,
            outcome.upper_seq
        );
    }
    Ok(())
}
