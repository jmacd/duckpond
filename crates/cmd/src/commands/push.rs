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
use steward::ShipRemoteSteward;
use sync_remote::{Remote, RemoteSteward};

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

    if attachment.url.starts_with("s3://") {
        sync_remote::register_s3_handlers();
    }

    let storage_options = attachment.to_storage_options();
    let mut remote = Remote::open_at_url(&attachment.url, storage_options)
        .await
        .map_err(|e| anyhow!("open remote `{}` ({}): {}", name, attachment.url, e))?;

    let ship_ref = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("push requires a pond steward (not a host steward)"))?;

    // Determine the seq range to push.  Watermark lookup goes through the
    // adapter so it reads the same raw_config key sync_remote::Remote uses.
    let upper = ship_ref.last_write_seq();
    let setting_key = format!("last_pushed_seq:{}", remote.url());
    let lower = {
        let adapter = ShipRemoteSteward::new(ship_ref);
        match adapter.config_get(&setting_key).await {
            Ok(Some(v)) => v.parse::<i64>().unwrap_or(0),
            _ => 0,
        }
    };

    // Seq 1 is always the pond_init bootstrap commit: it appears in the
    // control table as `DataCommitted` with `data_delta_version = 0` (no
    // parquet payload), so it cannot be pushed.  When pushing from a
    // fresh pond (`lower == 0`) we therefore start at seq 2.
    let start = std::cmp::max(lower + 1, 2);

    if start > upper {
        log::info!(
            "push {}: nothing to push (last_pushed_seq={} >= last_write_seq={})",
            name,
            lower,
            upper
        );
        return Ok(());
    }

    log::info!(
        "push {}: pushing seq {}..={} to {}",
        name,
        start,
        upper,
        attachment.url
    );

    for seq in start..=upper {
        let mut adapter = ShipRemoteSteward::new(ship_ref);
        match remote.push(&mut adapter, seq).await {
            Ok(()) => {
                log::debug!("pushed seq={}", seq);
            }
            Err(sync_remote::RemoteError::NoSuchCommit(_)) => {
                // Holes between last_pushed and last_write are expected
                // (e.g. read-only txns claim a seq but write no data).
                log::debug!("skip seq={}: no DataCommitted record", seq);
            }
            Err(e) => return Err(anyhow!("push seq={}: {}", seq, e)),
        }
    }

    log::info!("[OK] push {} complete (through seq={})", name, upper);
    Ok(())
}
