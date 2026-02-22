// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Control table query command - Show transaction status and post-commit execution
//!
//! Displays information from the control table including:
//! - Transaction lifecycle (begin, data_committed, completed, failed)
//! - Post-commit factory execution (pending, started, completed, failed)
//! - Error messages and duration metrics
#![allow(clippy::print_stdout)]

use crate::common::ShipContext;
use anyhow::{Context, Result, anyhow};
use provider::FactoryRegistry;
use provider::registry::ExecutionContext;
use serde::Deserialize;
use tokio::io::AsyncReadExt;

/// Recent transaction record from control table query
#[derive(Debug, Deserialize)]
struct RecentTransaction {
    txn_seq: i64,
    txn_id: String,
    transaction_type: String,
    cli_args: Option<String>,
    started_at: Option<i64>,
    final_state: Option<String>,
    ended_at: Option<i64>,
    error_message: Option<String>,
    duration_ms: Option<i64>,
}

/// Transaction detail record from control table query
#[derive(Debug, Deserialize)]
struct TransactionDetail {
    txn_seq: i64,
    txn_id: String,
    record_type: String,
    timestamp: i64,
    transaction_type: String,
    cli_args: Option<String>,
    data_fs_version: Option<i64>,
    error_message: Option<String>,
    duration_ms: Option<i64>,
    parent_txn_seq: Option<i64>,
    execution_seq: Option<i32>,
    factory_name: Option<String>,
    config_path: Option<String>,
}

/// Control command modes
#[derive(Debug, Clone)]
pub enum ControlMode {
    /// Show recent transactions with summary status
    Recent { limit: usize },
    /// Show detailed lifecycle for specific transaction
    Detail { txn_seq: i64 },
    /// Show incomplete operations (for recovery)
    Incomplete,
    /// Sync with remote: retry failed pushes OR pull new bundles (based on factory mode)
    Sync { config: Option<String> },
    /// Show pond configuration (ID, factory modes, metadata, settings)
    ShowConfig,
    /// Set a configuration value (key=value)
    SetConfig { key: String, value: String },
}

/// Show control table information
pub async fn control_command(ship_context: &ShipContext, mode: ControlMode) -> Result<()> {
    // Use the Ship's control table (already open and updated with all commits)
    let mut ship = ship_context.open_pond().await?;
    let control_table = ship.control_table_mut();

    match mode {
        ControlMode::Recent { limit } => {
            show_recent_transactions(control_table, limit).await?;
        }
        ControlMode::Detail { txn_seq } => {
            show_transaction_detail(control_table, txn_seq).await?;
        }
        ControlMode::Incomplete => {
            show_incomplete_operations(control_table).await?;
        }
        ControlMode::Sync { config } => {
            // Execute remote factory sync (push retry or pull new bundles)
            execute_sync(ship_context, control_table, config).await?;
        }
        ControlMode::ShowConfig => {
            show_pond_config(control_table).await?;
        }
        ControlMode::SetConfig { key, value } => {
            set_pond_config(control_table, &key, &value).await?;
        }
    }

    Ok(())
}

/// Show recent transactions with summary status
async fn show_recent_transactions(
    control_table: &mut steward::ControlTable,
    limit: usize,
) -> Result<()> {
    // Control table automatically sees latest Delta commits via DataFusion

    control_table.print_banner();

    // Use control table's SessionContext (following tlogfs pattern)
    // This ensures we see all committed transactions via Delta's latest _delta_log
    let ctx = control_table.session_context();

    // Query recent transactions with their status
    // First get the N most recent sequence numbers, then get all transactions for those sequences
    // Display in chronological order (oldest first) for easier reading
    // Query recent transactions with their status
    // Show BOTH read and write transactions (control table shows all activity)
    let sql = format!(
        r#"
        WITH recent_seqs AS (
            SELECT DISTINCT txn_seq
            FROM transactions
            WHERE transaction_type IN ('read', 'write')
            ORDER BY txn_seq DESC
            LIMIT {}
        )
        SELECT 
            t.txn_seq,
            t.txn_id,
            t.transaction_type,
            MAX(CASE WHEN t.record_type = 'begin' THEN t.cli_args ELSE NULL END) as cli_args,
            MAX(CASE WHEN t.record_type = 'begin' THEN t.timestamp ELSE NULL END) as started_at,
            MAX(CASE WHEN t.record_type IN ('data_committed', 'completed', 'failed') THEN t.record_type ELSE NULL END) as final_state,
            MAX(CASE WHEN t.record_type IN ('data_committed', 'completed', 'failed') THEN t.timestamp ELSE NULL END) as ended_at,
            MAX(CASE WHEN t.record_type = 'failed' THEN t.error_message ELSE NULL END) as error_message,
            MAX(t.duration_ms) as duration_ms
        FROM transactions t
        WHERE t.transaction_type IN ('read', 'write')
          AND t.txn_seq IN (SELECT txn_seq FROM recent_seqs)
        GROUP BY t.txn_seq, t.txn_id, t.transaction_type
        ORDER BY t.txn_seq ASC, started_at ASC
        "#,
        limit
    );

    let df = ctx
        .sql(&sql)
        .await
        .map_err(|e| anyhow!("Failed to query recent transactions: {}", e))?;

    let batches = df
        .collect()
        .await
        .map_err(|e| anyhow!("Failed to collect query results: {}", e))?;

    // Print header
    println!("\n+===========================================================================+");
    println!("|                        RECENT TRANSACTIONS                                 |");
    println!("+===========================================================================+\n");

    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        println!("No transactions found.\n");
        return Ok(());
    }

    // Deserialize batches into structs using serde_arrow
    let mut transactions = Vec::new();
    for batch in &batches {
        let batch_txns: Vec<RecentTransaction> = serde_arrow::from_record_batch(batch)
            .map_err(|e| anyhow!("Failed to deserialize transaction records: {}", e))?;
        transactions.extend(batch_txns);
    }

    // Format output
    for txn in transactions {
        // Extract CLI args from JSON string
        let cli_args = if let Some(json_str) = &txn.cli_args {
            match serde_json::from_str::<Vec<String>>(json_str) {
                Ok(args) => args.join(" "),
                Err(_) => "<invalid JSON>".to_string(),
            }
        } else {
            "<no command>".to_string()
        };

        // Format timestamps
        let started_at = txn
            .started_at
            .map(format_timestamp)
            .unwrap_or_else(|| "<unknown>".to_string());

        let ended_at = txn
            .ended_at
            .map(format_timestamp)
            .unwrap_or_else(|| "incomplete".to_string());

        // Status indicator
        let status = match txn.final_state.as_deref() {
            Some("data_committed") => "COMMITTED",
            Some("completed") => "COMPLETED",
            Some("failed") => "FAILED",
            Some(_) => "UNKNOWN",
            None => "INCOMPLETE",
        };

        // Duration
        let duration_str = txn
            .duration_ms
            .map(|d| format!("{}ms", d))
            .unwrap_or_else(|| "N/A".to_string());

        println!(
            "+- Transaction {} ({}) -----------------------------",
            txn.txn_seq, txn.transaction_type
        );
        println!("|  Status       : {}", status);
        println!("|  UUID         : {}", txn.txn_id);
        println!("|  Started      : {}", started_at);
        println!("|  Ended        : {}", ended_at);
        println!("|  Duration     : {}", duration_str);
        println!("|  Command      : {}", cli_args);

        // Show error if present
        if let Some(error) = &txn.error_message {
            println!("|  Error        : {}", truncate_error(error));
        }

        println!("+----------------------------------------------------------------");
        println!();
    }

    Ok(())
}

/// Show detailed lifecycle for a specific transaction
async fn show_transaction_detail(
    control_table: &mut steward::ControlTable,
    txn_seq: i64,
) -> Result<()> {
    // Control table automatically sees latest Delta commits via DataFusion
    control_table.print_banner();

    // Use control table's SessionContext (following tlogfs pattern)
    let ctx = control_table.session_context();

    // Query all records for this transaction (main transaction + post-commit tasks)
    let sql = format!(
        r#"
        SELECT 
            txn_seq,
            txn_id,
            record_type,
            timestamp,
            transaction_type,
            cli_args,
            data_fs_version,
            error_message,
            duration_ms,
            parent_txn_seq,
            execution_seq,
            factory_name,
            config_path
        FROM transactions
        WHERE txn_seq = {} OR parent_txn_seq = {}
        ORDER BY 
            CASE WHEN parent_txn_seq IS NULL THEN 0 ELSE 1 END,  -- Main txn first
            execution_seq NULLS FIRST,
            timestamp
        "#,
        txn_seq, txn_seq
    );

    let df = ctx
        .sql(&sql)
        .await
        .map_err(|e| anyhow!("Failed to query transaction detail: {}", e))?;

    let batches = df
        .collect()
        .await
        .map_err(|e| anyhow!("Failed to collect query results: {}", e))?;

    println!("\n+===========================================================================+");
    println!(
        "|                     TRANSACTION DETAIL: {}                                   |",
        txn_seq
    );
    println!("+===========================================================================+\n");

    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        println!("Transaction {} not found.\n", txn_seq);
        return Ok(());
    }

    // Deserialize batches into structs using serde_arrow
    let mut details = Vec::new();
    for batch in &batches {
        let batch_details: Vec<TransactionDetail> = serde_arrow::from_record_batch(batch)
            .map_err(|e| anyhow!("Failed to deserialize transaction detail records: {}", e))?;
        details.extend(batch_details);
    }

    // Track whether we're showing main transaction or post-commit tasks
    let mut in_post_commit = false;

    for detail in details {
        let current_txn_seq = detail.txn_seq;
        let txn_id = &detail.txn_id;
        let record_type = detail.record_type.as_str();
        let timestamp = format_timestamp(detail.timestamp);
        let txn_type = &detail.transaction_type;
        let is_post_commit = detail.parent_txn_seq.is_some();

        // Section header for post-commit tasks
        if is_post_commit && !in_post_commit {
            in_post_commit = true;
            println!("\n=== POST-COMMIT TASKS ===============================================\n");
        }

        match record_type {
            "begin" => {
                // Extract CLI args from JSON string
                let cli_args = if let Some(json_str) = &detail.cli_args {
                    match serde_json::from_str::<Vec<String>>(json_str) {
                        Ok(args) => args.join(" "),
                        Err(_) => "<invalid JSON>".to_string(),
                    }
                } else {
                    "<no command>".to_string()
                };

                println!(
                    "+- BEGIN {} Transaction ----------------------------------",
                    txn_type
                );
                println!("|  Sequence     : {}", current_txn_seq);
                println!("|  UUID         : {}", txn_id);
                println!("|  Timestamp    : {}", timestamp);
                println!("|  Command      : {}", cli_args);
                println!("+----------------------------------------------------------------");
            }
            "data_committed" => {
                let version = detail.data_fs_version.unwrap_or(0);
                let duration = detail
                    .duration_ms
                    .map(|d| format!("{}ms", d))
                    .unwrap_or_else(|| "N/A".to_string());
                println!(
                    "|  [OK] DATA COMMITTED at {} (version {}, duration: {})",
                    timestamp, version, duration
                );
            }
            "completed" => {
                let duration = detail
                    .duration_ms
                    .map(|d| format!("{}ms", d))
                    .unwrap_or_else(|| "N/A".to_string());
                println!(
                    "|  [OK] COMPLETED at {} (duration: {})",
                    timestamp, duration
                );
            }
            "failed" => {
                let duration = detail
                    .duration_ms
                    .map(|d| format!("{}ms", d))
                    .unwrap_or_else(|| "N/A".to_string());
                let error = detail
                    .error_message
                    .as_deref()
                    .unwrap_or("<no error message>");
                println!("|  [FAIL] FAILED at {} (duration: {})", timestamp, duration);
                println!("|  Error: {}", error);
            }
            "post_commit_pending" => {
                let _exec_seq = detail.execution_seq.unwrap_or(0);
                let factory = detail.factory_name.as_deref().unwrap_or("<unknown>");
                let config = detail.config_path.as_deref().unwrap_or("<unknown>");
                println!(
                    "+- POST-COMMIT TASK #{} PENDING ------------------------------",
                    _exec_seq
                );
                println!("|  Factory      : {}", factory);
                println!("|  Config       : {}", config);
                println!("|  Timestamp    : {}", timestamp);
            }
            "post_commit_started" => {
                let _exec_seq = detail.execution_seq.unwrap_or(0);
                println!("|  [RUN] STARTED at {}", timestamp);
            }
            "post_commit_completed" => {
                let _exec_seq = detail.execution_seq.unwrap_or(0);
                let duration = detail
                    .duration_ms
                    .map(|d| format!("{}ms", d))
                    .unwrap_or_else(|| "N/A".to_string());
                println!(
                    "|  [OK] COMPLETED at {} (duration: {})",
                    timestamp, duration
                );
                println!("+----------------------------------------------------------------");
            }
            "post_commit_failed" => {
                let _exec_seq = detail.execution_seq.unwrap_or(0);
                let duration = detail
                    .duration_ms
                    .map(|d| format!("{}ms", d))
                    .unwrap_or_else(|| "N/A".to_string());
                let error = detail
                    .error_message
                    .as_deref()
                    .unwrap_or("<no error message>");
                println!("|  [FAIL] FAILED at {} (duration: {})", timestamp, duration);
                println!("|  Error: {}", error);
                println!("+----------------------------------------------------------------");
            }
            _ => {
                println!("|  {} at {}", record_type, timestamp);
            }
        }
    }

    println!();
    Ok(())
}

/// Execute remote factory sync operation
///
/// This finds factory configurations and executes them, regardless of their mode.
/// The factory's config determines behavior:
/// - push mode factory: Retries failed pushes
/// - pull mode factory: Pulls new bundles and applies them
async fn execute_sync(
    ship_context: &ShipContext,
    control_table: &mut steward::ControlTable,
    config: Option<String>,
) -> Result<()> {
    // Reload control table to see latest commits
    // Control table automatically sees latest Delta commits via DataFusion
    control_table.print_banner();

    log::info!("[SYNC] Executing manual sync operation...");

    // Open pond to read factory configuration
    let mut ship = ship_context.open_pond().await?;

    // Start a read transaction to access the factory config
    let mut tx = ship
        .begin_read(&steward::PondUserMetadata::new(vec!["sync".to_string()]))
        .await?;

    match execute_sync_impl(&mut tx, control_table, config).await {
        Ok(()) => {
            _ = tx.commit().await?;
            log::info!("[OK] Sync operation completed");
            Ok(())
        }
        Err(e) => Err(tx.abort(&e).await.into()),
    }
}

/// Implementation of sync operation
async fn execute_sync_impl(
    tx: &mut steward::Transaction<'_>,
    control_table: &mut steward::ControlTable,
    config_base64: Option<String>,
) -> Result<()> {
    // If --config provided, use it directly (recovery case)
    if let Some(encoded) = config_base64 {
        log::info!("Using remote config from --config argument");
        let repl_config = remote::ReplicationConfig::from_base64(&encoded)
            .map_err(|e| anyhow!("Failed to decode base64 config: {}", e))?;

        let config = repl_config.remote;
        let pond_metadata = control_table.get_pond_metadata().clone();

        // Create factory context
        let provider_context = tx.provider_context()?;
        let factory_context = provider::FactoryContext::with_metadata(
            provider_context,
            tinyfs::FileID::root(),
            pond_metadata,
        );

        // Serialize config to bytes
        let config_str = serde_yaml::to_string(&config)
            .map_err(|e| anyhow!("Failed to serialize remote config: {}", e))?;
        let config_bytes = config_str.as_bytes().to_vec();

        // Execute the remote factory in ControlWriter mode with "pull" arg
        let args = vec!["pull".to_string()];
        FactoryRegistry::execute::<tlogfs::TLogFSError>(
            "remote",
            &config_bytes,
            factory_context,
            ExecutionContext::control_writer(args),
        )
        .await
        .map_err(|e| anyhow!("Remote factory execution failed: {}", e))?;

        return Ok(());
    }

    // Normal case: Look for remote factory node at known path
    let remote_path = "/etc/system.d/1-backup"; // Standard location
    log::info!("Looking for remote factory at: {}", remote_path);

    // Get filesystem root (guard derefs to FS)
    let root = tx.root().await?;

    // Resolve the factory config path
    let (_parent_wd, lookup_result) = root
        .resolve_path(&remote_path)
        .await
        .with_context(|| format!("Failed to resolve path: {}", remote_path))?;

    let config_node = match lookup_result {
        tinyfs::Lookup::Found(node) => node,
        tinyfs::Lookup::NotFound(_, _) => {
            return Err(anyhow!("Factory configuration not found: {}", remote_path));
        }
        tinyfs::Lookup::Empty(_) => {
            return Err(anyhow!("Invalid path: {}", remote_path));
        }
    };

    // Get node ID
    let node_id = config_node.id();

    // Get the factory name from the oplog
    let factory_name = tx
        .get_factory_for_node(node_id)
        .await
        .with_context(|| format!("Failed to get factory for: {}", remote_path))?
        .ok_or_else(|| {
            anyhow!(
                "Factory configuration has no associated factory: {}",
                remote_path
            )
        })?;

    // Read the configuration file contents
    let config_bytes = {
        let mut reader = root
            .async_reader_path(&remote_path)
            .await
            .with_context(|| format!("Failed to open file: {}", remote_path))?;

        let mut buffer = Vec::new();
        _ = reader
            .read_to_end(&mut buffer)
            .await
            .with_context(|| format!("Failed to read file: {}", remote_path))?;
        buffer
    };

    // Get factory mode and pond metadata
    let factory_mode = control_table
        .get_factory_mode(&factory_name)
        .with_context(|| format!("Factory mode not set for: {}", factory_name))?;

    let pond_metadata = control_table.get_pond_metadata().clone();

    // Create factory context for ControlWriter mode
    let provider_context = tx.provider_context()?;
    let factory_context =
        provider::FactoryContext::with_metadata(provider_context, node_id, pond_metadata);

    // Pass factory mode as arg
    let args = vec![factory_mode];

    // Execute the factory in ControlWriter mode
    FactoryRegistry::execute::<tlogfs::TLogFSError>(
        &factory_name,
        &config_bytes,
        factory_context,
        ExecutionContext::control_writer(args),
    )
    .await
    .map_err(|e| anyhow!("Factory execution failed: {}", e))?;

    Ok(())
}
/// Show incomplete operations for recovery
async fn show_incomplete_operations(control_table: &mut steward::ControlTable) -> Result<()> {
    // Control table automatically sees latest Delta commits via DataFusion
    control_table.print_banner();

    println!("\n+===========================================================================+");
    println!("|                      INCOMPLETE OPERATIONS                                 |");
    println!("+===========================================================================+\n");

    // Use existing method from control table
    let incomplete = control_table
        .find_incomplete_transactions()
        .await
        .map_err(|e| anyhow!("Failed to find incomplete transactions: {}", e))?;

    if incomplete.is_empty() {
        println!("No incomplete transactions found. All operations completed successfully.\n");
        return Ok(());
    }

    println!("Found {} incomplete transaction(s):\n", incomplete.len());

    for (txn_meta, data_fs_version) in incomplete {
        println!(
            "+- Transaction {} --------------------------------------------",
            txn_meta.txn_seq
        );
        println!("|  UUID         : {}", txn_meta.user.txn_id);
        println!("|  Status       : [WARN]  Incomplete (crashed during execution)");

        if data_fs_version > 0 {
            println!(
                "|  Data Version : {} (data was committed before crash)",
                data_fs_version
            );
        } else {
            println!("|  Data Version : N/A (crashed before data commit)");
        }

        // Display command from metadata
        if !txn_meta.user.args.is_empty() {
            println!("|  Command      : {}", txn_meta.user.args.join(" "));
        }

        println!("+----------------------------------------------------------------");
        println!();
    }

    println!("To recover, you may need to manually inspect or retry these operations.\n");

    Ok(())
}

/// Format microsecond timestamp as human-readable string
fn format_timestamp(micros: i64) -> String {
    use chrono::{TimeZone, Utc};
    let dt = Utc.timestamp_opt(micros / 1_000_000, ((micros % 1_000_000) * 1000) as u32);
    match dt.single() {
        Some(datetime) => datetime.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        None => format!("<invalid timestamp: {}>", micros),
    }
}

/// Truncate error message for display
fn truncate_error(error: &str) -> String {
    const MAX_LEN: usize = 100;
    if error.len() <= MAX_LEN {
        error.to_string()
    } else {
        format!("{}...", &error[..MAX_LEN])
    }
}

/// Show pond configuration (ID, factory modes, metadata, settings)
async fn show_pond_config(control_table: &steward::ControlTable) -> Result<()> {
    let metadata = control_table.get_pond_metadata();

    println!("Pond Configuration");
    println!("==================");
    println!();
    println!("Pond ID:        {}", metadata.pond_id);
    println!(
        "Created:        {}",
        chrono::DateTime::from_timestamp_micros(metadata.birth_timestamp)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| "unknown".to_string())
    );
    println!(
        "Created by:     {}@{}",
        metadata.birth_username, metadata.birth_hostname
    );
    println!();
    println!("Factory Modes:");
    println!("--------------");

    let factory_modes = control_table.factory_modes();
    if factory_modes.is_empty() {
        println!("  (none configured)");
    } else {
        for (factory_name, mode) in factory_modes.iter() {
            println!("  {:20} {}", format!("{}:", factory_name), mode);
        }
    }

    println!();
    println!("Settings:");
    println!("---------");

    let settings = control_table.settings();
    if settings.is_empty() {
        println!("  (none configured)");
    } else {
        for (key, value) in settings.iter() {
            println!("  {:20} {}", format!("{}:", key), value);
        }
    }

    Ok(())
}

/// Set a pond configuration value
async fn set_pond_config(
    control_table: &mut steward::ControlTable,
    key: &str,
    value: &str,
) -> Result<()> {
    control_table
        .set_setting(key, value)
        .await
        .map_err(|e| anyhow!("Failed to set setting: {}", e))?;

    println!("[OK] Set '{}' = '{}'", key, value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate_error() {
        let short = "Short error";
        assert_eq!(truncate_error(short), "Short error");

        let long = "a".repeat(150);
        let truncated = truncate_error(&long);
        assert!(truncated.ends_with("..."));
        assert!(truncated.len() <= 103); // 100 + "..."
    }
}

// Integration tests in separate file
#[cfg(test)]
#[path = "control_test.rs"]
mod control_integration_tests;
