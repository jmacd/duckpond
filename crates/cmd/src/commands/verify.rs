// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `pond verify [<name>]` -- compare this pond's CURRENT live data against
//! one or more remotes' recorded partition checksums via
//! [`sync_remote::verify_against_remote`].
//!
//! Works for both attachment modes:
//! * **Backup (push)**: the local pond is the producer; verify compares
//!   the local data for the local pond_id against the backup's latest
//!   bundle.
//! * **Remote (pull)**: the local pond mirrors a foreign pond's data;
//!   verify compares the foreign-pond data held locally against the
//!   foreign remote's latest bundle.
//!
//! When invoked without `name`, every attached remote is verified.
//! Each remote is processed independently; one failure does not halt
//! the others.

use crate::commands::remote::{list_remote_names, load_remote_attachment};
use crate::common::ShipContext;
use anyhow::{Result, anyhow};
use steward::ShipRemoteSteward;
use sync_remote::{Remote, RemoteVerifyReport, verify_against_remote};

/// Verify against `name`, or against every attached remote when `name` is
/// `None`.
pub async fn verify_command(ship_context: &ShipContext, name: Option<String>) -> Result<()> {
    let mut ship = ship_context.open_pond().await?;

    let targets: Vec<String> = if let Some(n) = name {
        vec![n]
    } else {
        list_remote_names(&mut ship).await?
    };

    if targets.is_empty() {
        log::info!("no remotes attached; nothing to verify");
        return Ok(());
    }

    let mut had_error = false;
    let mut had_mismatch = false;
    for name in targets {
        match verify_one(&mut ship, &name).await {
            Ok(report) => {
                print_report(&name, &report);
                if !report.ok {
                    had_mismatch = true;
                }
            }
            Err(e) => {
                log::error!("[ERR] verify {}: {}", name, e);
                had_error = true;
            }
        }
    }

    if had_error {
        Err(anyhow!("one or more verifications failed"))
    } else if had_mismatch {
        Err(anyhow!(
            "verification completed with mismatches; see report(s) above"
        ))
    } else {
        Ok(())
    }
}

async fn verify_one(ship: &mut steward::Steward, name: &str) -> Result<RemoteVerifyReport> {
    let attachment = load_remote_attachment(ship, name).await?;

    if attachment.url.starts_with("s3://") {
        sync_remote::register_s3_handlers();
    }

    let storage_options = attachment.to_storage_options()?;
    let remote = Remote::open_at_url(&attachment.url, storage_options)
        .await
        .map_err(|e| anyhow!("open remote `{}` ({}): {}", name, attachment.url, e))?;

    let ship_ref = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("verify requires a pond steward (not a host steward)"))?;

    let adapter = ShipRemoteSteward::new(ship_ref);
    let report = verify_against_remote(&remote, &adapter)
        .await
        .map_err(|e| anyhow!("verify against `{}` ({}): {}", name, attachment.url, e))?;

    Ok(report)
}

fn print_report(name: &str, report: &RemoteVerifyReport) {
    let header = match (report.ok, report.remote_latest_seq) {
        (true, None) => format!(
            "[OK] verify {}: remote has no bundles (vacuous match)",
            name
        ),
        (true, Some(seq)) => format!(
            "[OK] verify {}: live data matches remote at seq={}",
            name, seq
        ),
        (false, None) => format!(
            "[MISMATCH] verify {}: remote has no bundles but consumer has data",
            name
        ),
        (false, Some(seq)) => format!(
            "[MISMATCH] verify {}: {} partition(s) diverge from remote at seq={}",
            name,
            report.mismatches.len(),
            seq
        ),
    };
    log::info!("{}", header);

    for m in &report.mismatches {
        match (&m.consumer_live, &m.remote_recorded) {
            (Some(_), None) => log::info!(
                "  partition `{}`: present locally, absent on remote",
                m.partition
            ),
            (None, Some(_)) => log::info!(
                "  partition `{}`: present on remote, absent locally",
                m.partition
            ),
            (Some(_), Some(_)) => log::info!(
                "  partition `{}`: checksums differ between local and remote",
                m.partition
            ),
            (None, None) => unreachable!("a mismatch must have at least one side present"),
        }
    }

    if let Some(boundary) = report.divergence_boundary {
        log::info!(
            "  divergence boundary: consumer last agreed with remote at seq={} (drift began after)",
            boundary
        );
    } else if !report.ok && report.remote_latest_seq.is_some() {
        log::info!("  divergence boundary: no prior bundle in remote history agrees with consumer");
    }
}
