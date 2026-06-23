// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `pond status` (D6.2) -- operator-facing aggregate of the pond's
//! identity, local commit state, recovery health, and per-remote sync
//! watermarks.
//!
//! This is a fast, OFFLINE command: it reads only the local control
//! table and `/sys/remotes/*` attachments.  It never opens a remote or
//! touches the network, so it is safe to run frequently and on a pond
//! whose remotes are unreachable.  Push "lag" is computed purely from
//! local watermarks (`last_write_seq` vs `last_pushed_seq:<url>`); to
//! cross-check against what a remote actually recorded, use
//! `pond verify`.
#![allow(clippy::print_stdout)]

use crate::commands::remote::{list_remote_names, load_remote_attachment};
use crate::common::ShipContext;
use anyhow::{Result, anyhow};
use steward::{REMOTE_MODE_PREFIX, REMOTE_MOUNT_PATH_PREFIX, RemoteMode};

/// Render the operator status report for the pond at `ship_context`.
pub async fn status_command(ship_context: &ShipContext) -> Result<()> {
    let mut ship = ship_context.open_pond().await?;

    let metadata = ship.control_table().get_pond_metadata().clone();
    let last_write_seq = ship
        .control_table()
        .get_last_write_sequence()
        .await
        .map_err(|e| anyhow!("read last write sequence: {}", e))?;
    let incomplete = ship
        .control_table()
        .find_incomplete_transactions()
        .await
        .map_err(|e| anyhow!("scan incomplete transactions: {}", e))?;

    println!("Pond Status");
    println!("===========");
    println!();
    println!("Identity");
    println!("  Pond ID:    {}", metadata.pond_id);
    println!(
        "  Created:    {} by {}",
        format_timestamp(metadata.birth_timestamp),
        metadata.birth_username,
    );
    println!(
        "  Birthplace: {}",
        if metadata.birthplace.is_empty() {
            "(unspecified)"
        } else {
            &metadata.birthplace
        }
    );
    if let Ok(path) = ship_context.resolve_pond_path() {
        println!("  Location:   {}", path.display());
    }
    println!();

    println!("Local state");
    println!("  Last write seq:  {}", last_write_seq);
    if incomplete.is_empty() {
        println!("  Recovery:        OK (no incomplete transactions)");
    } else {
        println!(
            "  Recovery:        [WARN] {} incomplete transaction(s) -- run `pond recover`",
            incomplete.len()
        );
        for (txn_meta, _data_version) in &incomplete {
            println!(
                "                     seq={} txn_id={}",
                txn_meta.txn_seq, txn_meta.user.txn_id
            );
        }
    }
    println!();

    let names = list_remote_names(&mut ship).await?;
    println!("Remotes ({})", names.len());
    if names.is_empty() {
        println!("  (none attached)");
        return Ok(());
    }

    for name in names {
        let attachment = match load_remote_attachment(&mut ship, &name).await {
            Ok(a) => a,
            Err(e) => {
                println!("  {}  [unreadable: {}]", name, e);
                continue;
            }
        };

        let mode_str = ship
            .control_table()
            .raw_config_get(&format!("{REMOTE_MODE_PREFIX}{name}"))
            .await
            .unwrap_or_default()
            .unwrap_or_else(|| "push".to_string());
        let mode = RemoteMode::parse(&mode_str).unwrap_or(RemoteMode::Push);

        let mount = ship
            .control_table()
            .raw_config_get(&format!("{REMOTE_MOUNT_PATH_PREFIX}{name}"))
            .await
            .unwrap_or_default()
            .filter(|s| !s.is_empty());

        let last_pushed = read_seq(&ship, &format!("last_pushed_seq:{}", attachment.url)).await;
        let last_pulled = read_seq(&ship, &format!("last_pulled_seq:{}", attachment.url)).await;

        println!("  {}  [{}]", name, mode_str);
        println!("    url:          {}", attachment.url);
        match &mount {
            Some(p) => println!("    mount:        {}", p),
            None => println!("    mount:        / (mirror)"),
        }

        if mode.pushes() {
            match last_pushed {
                Some(seq) => {
                    let lag = last_write_seq - seq;
                    if lag <= 0 {
                        println!("    last pushed:  {} (up to date)", seq);
                    } else {
                        println!("    last pushed:  {} (behind local by {} txn)", seq, lag);
                    }
                }
                None => println!(
                    "    last pushed:  - (never pushed; {} local txn pending)",
                    last_write_seq
                ),
            }
        }

        if mode.pulls() {
            match last_pulled {
                Some(seq) => println!("    last pulled:  {}", seq),
                None => println!("    last pulled:  - (never pulled)"),
            }
        }
    }

    Ok(())
}

/// Read a watermark setting and parse it as an `i64` sequence.
/// Returns `None` if the key is unset or unparsable.
async fn read_seq(ship: &steward::Steward, key: &str) -> Option<i64> {
    ship.control_table()
        .raw_config_get(key)
        .await
        .ok()
        .flatten()
        .and_then(|v| v.parse::<i64>().ok())
}

/// Format a microsecond timestamp as a human-readable UTC string.
fn format_timestamp(micros: i64) -> String {
    chrono::DateTime::from_timestamp_micros(micros)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| format!("<invalid timestamp: {}>", micros))
}
