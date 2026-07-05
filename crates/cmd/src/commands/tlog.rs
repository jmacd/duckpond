// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `pond tlog` -- inspect and verify the pond's transparency log (design
//! Decision D5).
//!
//! These are read-only, offline commands over the published C2SP `tlog-tiles`
//! under `{POND}/tlog`.  They exercise only the key-free properties of the log:
//!
//! * **Inclusion** -- every published leaf provably sits in the tree the
//!   checkpoint commits to (RFC 6962 inclusion proof).
//! * **Append-only** -- every checkpoint the pond ever published is a prefix of
//!   the current log (RFC 6962 consistency proof against the checkpoint
//!   history).
//! * **Faithfulness** -- the published leaf sequence matches the authoritative
//!   commit spine recorded in the control table, leaf for leaf.
//!
//! Signing (the log's trust root) is deferred; these checks need no key.  They
//! establish tamper-evidence and append-only growth without trusting the
//! operator, and are exactly the properties a future signature will attest to.
#![allow(clippy::print_stdout)]

use anyhow::{Result, anyhow};

use crate::common::ShipContext;
use sync_store::tlog::hash_leaf;
use sync_store::{
    Checkpoint, LogHash, TileLog, TransparencyLog, verify_consistency, verify_inclusion,
};

/// Print the checkpoint, history, and tile leaf count for the pond's log.
pub async fn tlog_show_command(ship_context: &ShipContext) -> Result<()> {
    let pond_path = ship_context.resolve_pond_path()?;
    let dir = steward::get_tlog_path(&pond_path);

    let Some(cp) = Checkpoint::read(&dir).map_err(|e| anyhow!("read checkpoint: {}", e))? else {
        println!("Transparency log");
        println!("================");
        println!();
        println!("  (no checkpoint published yet -- the pond has no committed leaves)");
        return Ok(());
    };

    let history = Checkpoint::read_history(&dir).map_err(|e| anyhow!("read history: {}", e))?;

    println!("Transparency log");
    println!("================");
    println!();
    println!("  Origin:     {}", cp.origin);
    println!("  Tree size:  {} leaf/leaves", cp.size);
    println!("  Root:       {}", cp.root.to_hex());
    println!("  Directory:  {}", dir.display());
    println!();
    println!("Checkpoint history ({} published)", history.len());
    for h in &history {
        println!("  size={:<8} root={}", h.size, h.root.to_hex());
    }
    Ok(())
}

/// Verify the pond's transparency log: inclusion of every leaf, append-only
/// consistency across every published checkpoint, and faithfulness to the
/// control-table commit spine.  Prints a PASS/FAIL line per check and returns an
/// error if any check fails.
pub async fn tlog_verify_command(ship_context: &ShipContext) -> Result<()> {
    let pond_path = ship_context.resolve_pond_path()?;
    let dir = steward::get_tlog_path(&pond_path);

    println!("Verifying transparency log at {}", dir.display());
    println!();

    let Some(cp) = Checkpoint::read(&dir).map_err(|e| anyhow!("read checkpoint: {}", e))? else {
        println!("  (no checkpoint published yet -- nothing to verify)");
        return Ok(());
    };

    // Reconstruct the tree from the published level-0 tiles.
    let tile_log = TileLog::new(&dir, cp.origin.clone());
    let leaves = tile_log
        .load_leaves()
        .map_err(|e| anyhow!("load leaves from tiles: {}", e))?;
    let mut tree = TransparencyLog::new();
    for l in &leaves {
        let _ = tree.append_leaf_hash(*l);
    }
    let n = leaves.len();

    let mut failures = 0usize;

    // Check 1: the tiles reproduce the checkpoint (size and root).
    let root_ok = n as u64 == cp.size && tree.root() == cp.root;
    report(
        &mut failures,
        root_ok,
        &format!(
            "checkpoint root and size ({} leaf/leaves) reproduced from tiles",
            cp.size
        ),
    );

    // Check 2: every leaf proves inclusion against the checkpoint root.
    let mut included = 0usize;
    for (i, leaf) in leaves.iter().enumerate() {
        match tree.inclusion_proof(i) {
            Ok(proof) if verify_inclusion(leaf, i, n, &proof, &cp.root) => included += 1,
            _ => {}
        }
    }
    report(
        &mut failures,
        included == n,
        &format!("inclusion proofs verified ({included}/{n} leaves)"),
    );

    // Check 3: every published checkpoint is a prefix of the current log.
    let history = Checkpoint::read_history(&dir).map_err(|e| anyhow!("read history: {}", e))?;
    let mut consistent = 0usize;
    for h in &history {
        let old_size = h.size as usize;
        let ok = match tree.consistency_proof(old_size) {
            Ok(proof) => verify_consistency(old_size, n, &h.root, &cp.root, &proof),
            Err(_) => false,
        };
        if ok {
            consistent += 1;
        }
    }
    report(
        &mut failures,
        consistent == history.len(),
        &format!(
            "append-only consistency verified ({consistent}/{} published checkpoints)",
            history.len()
        ),
    );

    // Check 4: the published leaves match the authoritative commit spine in the
    // control table, leaf for leaf.  This ties the TIME export back to the
    // source of truth (design Decision D5).
    match control_table_leaves(ship_context).await {
        Ok(expected) => {
            let faithful = expected == leaves;
            let detail = if expected.len() == leaves.len() {
                format!("faithful to control-table commit spine ({n} leaves)")
            } else {
                format!(
                    "faithful to control-table commit spine (tiles={}, control table={})",
                    leaves.len(),
                    expected.len()
                )
            };
            report(&mut failures, faithful, &detail);
        }
        Err(e) => {
            report(
                &mut failures,
                false,
                &format!("faithful to control-table commit spine (unavailable: {e})"),
            );
        }
    }

    println!();
    if failures == 0 {
        println!("Transparency log OK: {n} leaf/leaves, all checks passed.");
        Ok(())
    } else {
        Err(anyhow!(
            "transparency log verification failed: {failures} check(s) did not pass"
        ))
    }
}

/// The authoritative leaf hashes: `hash_leaf(commit_object)` for every
/// spine-bearing `DataCommitted` record, in commit order.
async fn control_table_leaves(ship_context: &ShipContext) -> Result<Vec<LogHash>> {
    let ship = ship_context.open_pond().await?;
    let objects = ship
        .control_table()
        .commit_objects_in_order()
        .await
        .map_err(|e| anyhow!("read commit spine: {}", e))?;
    let mut leaves = Vec::with_capacity(objects.len());
    for hex in objects {
        let bytes = hex::decode(&hex).map_err(|e| anyhow!("decode commit object: {}", e))?;
        leaves.push(hash_leaf(&bytes));
    }
    Ok(leaves)
}

/// Print a PASS/FAIL line for one check, incrementing `failures` on failure.
fn report(failures: &mut usize, ok: bool, label: &str) {
    if ok {
        println!("  [PASS] {label}");
    } else {
        *failures += 1;
        println!("  [FAIL] {label}");
    }
}
