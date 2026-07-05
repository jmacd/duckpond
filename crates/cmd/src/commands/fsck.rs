// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `pond fsck` -- local, offline filesystem-check.
//!
//! Computes a single deterministic **root checksum** that exhaustively
//! commits to every row in the pond's data table (across every pond_id,
//! including cross-pond imports) and -- unless `--quick` is passed --
//! re-reads and re-hashes every file's content to confirm the bytes
//! match their recorded `blake3`.
//!
//! By default only the root checksum is printed, so two replicas can be
//! compared by eye or by script:
//!
//! ```text
//! $ pond fsck
//! a1b2c3...   (64 hex chars)
//! ```
//!
//! `--verbose` additionally prints a per-partition breakdown and content
//! statistics.  The command exits non-zero if any content check fails.
#![allow(clippy::print_stdout)]

use crate::common::ShipContext;
use anyhow::{Result, anyhow};
use steward::{FsckOptions, FsckReport};

/// Run a filesystem check on the pond at `ship_context`.
///
/// When `quick` is `true`, the expensive content-rehash pass is skipped
/// and only the structural root checksum is computed.  When `verbose` is
/// `true`, a per-partition breakdown and content statistics are printed
/// in addition to the root checksum.
pub async fn fsck_command(ship_context: &ShipContext, quick: bool, verbose: bool) -> Result<()> {
    let ship = ship_context.open_pond().await?;
    let pond = ship
        .as_pond()
        .ok_or_else(|| anyhow!("fsck requires a pond steward (not a host steward)"))?;

    let report = steward::fsck(
        pond,
        FsckOptions {
            verify_content: !quick,
        },
    )
    .await
    .map_err(|e| anyhow!("fsck: {}", e))?;

    if verbose {
        print_verbose(&report, quick);
    } else {
        println!("{}", report.root_hex());
    }

    if report.ok() {
        Ok(())
    } else {
        for err in &report.errors {
            log::error!(
                "[ERR] {}/{} node {} v{}: {}",
                err.pond_id,
                err.part_id,
                err.node_id,
                err.version,
                err.detail
            );
        }
        Err(anyhow!(
            "fsck found {} content error(s); pond is NOT intact",
            report.errors.len()
        ))
    }
}

fn print_verbose(report: &FsckReport, quick: bool) {
    println!("Filesystem check");
    println!("================");
    println!();
    println!("Root checksum: {}", report.root_hex());
    println!();
    println!("Partitions ({}):", report.partitions.len());
    for p in &report.partitions {
        println!(
            "  {}/{}  rows={}  {}",
            p.pond_id,
            p.part_id,
            p.rows,
            p.tree_hash.to_hex()
        );
    }
    println!();
    println!("Rows checked:    {}", report.rows_checked);
    if quick {
        println!("Content pass:    skipped (--quick)");
    } else {
        println!("Inline verified: {}", report.inline_checked);
        println!("Blobs verified:  {}", report.blobs_checked);
    }
    if report.ok() {
        println!(
            "Result:          OK ({} partitions intact)",
            report.partitions.len()
        );
    } else {
        println!(
            "Result:          FAILED ({} content error(s))",
            report.errors.len()
        );
    }
}
