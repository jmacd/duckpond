//! Control table query command - Show transaction status and post-commit execution
//!
//! Displays information from the control table including:
//! - Transaction lifecycle (begin, data_committed, completed, failed)
//! - Post-commit factory execution (pending, started, completed, failed)
//! - Error messages and duration metrics

use anyhow::{anyhow, Result};
use crate::common::ShipContext;
use arrow::array::{Array, Int32Array, Int64Array, ListArray, StringArray, TimestampMicrosecondArray};
use datafusion::prelude::SessionContext;
use std::sync::Arc;

/// Control command modes
#[derive(Debug, Clone)]
pub enum ControlMode {
    /// Show recent transactions with summary status
    Recent { limit: usize },
    /// Show detailed lifecycle for specific transaction
    Detail { txn_seq: i64 },
    /// Show incomplete operations (for recovery)
    Incomplete,
}

impl ControlMode {
    pub fn from_args(mode: &str, txn_seq: Option<i64>, limit: Option<usize>) -> Result<Self> {
        match mode {
            "recent" => Ok(ControlMode::Recent {
                limit: limit.unwrap_or(10),
            }),
            "detail" => {
                let seq = txn_seq.ok_or_else(|| anyhow!("--txn-seq required for detail mode"))?;
                Ok(ControlMode::Detail { txn_seq: seq })
            }
            "incomplete" => Ok(ControlMode::Incomplete),
            _ => Err(anyhow!(
                "Invalid mode '{}'. Use 'recent', 'detail', or 'incomplete'",
                mode
            )),
        }
    }
}

/// Show control table information
pub async fn control_command(
    ship_context: &ShipContext,
    mode: ControlMode,
) -> Result<()> {
    // Open control table directly (no transaction needed for read-only queries)
    let pond_path = ship_context.resolve_pond_path()?;
    let control_table_path = pond_path.join("control");
    
    let control_table = steward::ControlTable::new(
        control_table_path.to_str().unwrap()
    )
    .await
    .map_err(|e| anyhow!("Failed to open control table: {}", e))?;

    match mode {
        ControlMode::Recent { limit } => {
            show_recent_transactions(&control_table, limit).await?;
        }
        ControlMode::Detail { txn_seq } => {
            show_transaction_detail(&control_table, txn_seq).await?;
        }
        ControlMode::Incomplete => {
            show_incomplete_operations(&control_table).await?;
        }
    }

    Ok(())
}

/// Show recent transactions with summary status
async fn show_recent_transactions(
    control_table: &steward::ControlTable,
    limit: usize,
) -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_table("transactions", Arc::new(control_table.table().clone()))
        .map_err(|e| anyhow!("Failed to register control table: {}", e))?;

    // Query recent write transactions with their status
    let sql = format!(
        r#"
        SELECT 
            t.txn_seq,
            t.txn_id,
            t.transaction_type,
            MAX(CASE WHEN t.record_type = 'begin' THEN t.cli_args ELSE NULL END) as cli_args,
            MAX(CASE WHEN t.record_type = 'begin' THEN t.timestamp ELSE NULL END) as started_at,
            MAX(CASE WHEN t.record_type IN ('data_committed', 'completed', 'failed') THEN t.record_type ELSE NULL END) as final_state,
            MAX(CASE WHEN t.record_type IN ('data_committed', 'completed', 'failed') THEN t.timestamp ELSE NULL END) as ended_at,
            MAX(CASE WHEN t.record_type = 'failed' THEN t.error_message ELSE NULL END) as error_message,
            MAX(t.duration_ms) as duration_ms,
            MAX(CASE WHEN t.record_type = 'data_committed' THEN t.data_fs_version ELSE NULL END) as data_fs_version
        FROM transactions t
        WHERE t.transaction_type IN ('read', 'write')
        GROUP BY t.txn_seq, t.txn_id, t.transaction_type
        ORDER BY t.txn_seq DESC
        LIMIT {}
        "#,
        limit
    );

    let df = ctx.sql(&sql).await.map_err(|e| {
        anyhow!("Failed to query recent transactions: {}", e)
    })?;

    let batches = df.collect().await.map_err(|e| {
        anyhow!("Failed to collect query results: {}", e)
    })?;

    // Print header
    println!("\n╔═══════════════════════════════════════════════════════════════════════════╗");
    println!("║                        RECENT TRANSACTIONS                                 ║");
    println!("╚═══════════════════════════════════════════════════════════════════════════╝\n");
    
    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        println!("No transactions found.\n");
        return Ok(());
    }

    // Format output
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }

        let txn_seqs = batch.column_by_name("txn_seq")
            .unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        let txn_ids = batch.column_by_name("txn_id")
            .unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let txn_types = batch.column_by_name("transaction_type")
            .unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let cli_args_col = batch.column_by_name("cli_args")
            .unwrap().as_any().downcast_ref::<ListArray>().unwrap();
        let started_at_col = batch.column_by_name("started_at")
            .unwrap().as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        let final_states = batch.column_by_name("final_state")
            .unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let ended_at_col = batch.column_by_name("ended_at")
            .unwrap().as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        let error_messages = batch.column_by_name("error_message")
            .unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let durations = batch.column_by_name("duration_ms")
            .unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        let data_fs_versions = batch.column_by_name("data_fs_version")
            .unwrap().as_any().downcast_ref::<Int64Array>().unwrap();

        for i in 0..batch.num_rows() {
            let txn_seq = txn_seqs.value(i);
            let txn_id = txn_ids.value(i);
            let txn_type = txn_types.value(i);
            
            // Extract CLI args
            let cli_args = if !cli_args_col.is_null(i) {
                let args_array = cli_args_col.value(i);
                let string_array = args_array.as_any().downcast_ref::<StringArray>().unwrap();
                let mut args = Vec::new();
                for j in 0..string_array.len() {
                    if !string_array.is_null(j) {
                        args.push(string_array.value(j).to_string());
                    }
                }
                args.join(" ")
            } else {
                "<no command>".to_string()
            };

            // Format timestamps
            let started_at = if !started_at_col.is_null(i) {
                format_timestamp(started_at_col.value(i))
            } else {
                "<unknown>".to_string()
            };

            let ended_at = if !ended_at_col.is_null(i) {
                format_timestamp(ended_at_col.value(i))
            } else {
                "incomplete".to_string()
            };

            // Status indicator
            let status = if final_states.is_null(i) {
                "⚠️  INCOMPLETE"
            } else {
                match final_states.value(i) {
                    "data_committed" => "✓  COMMITTED",
                    "completed" => "✓  COMPLETED",
                    "failed" => "✗  FAILED",
                    _ => "?  UNKNOWN"
                }
            };

            // Duration
            let duration_str = if !durations.is_null(i) {
                format!("{}ms", durations.value(i))
            } else {
                "N/A".to_string()
            };

            // Data filesystem version
            let version_str = if !data_fs_versions.is_null(i) {
                format!("v{}", data_fs_versions.value(i))
            } else {
                "".to_string()
            };

            println!("┌─ Transaction {} ({}) ─────────────────────────────", txn_seq, txn_type);
            println!("│  Status       : {} {}", status, version_str);
            println!("│  UUID         : {}", txn_id);
            println!("│  Started      : {}", started_at);
            println!("│  Ended        : {}", ended_at);
            println!("│  Duration     : {}", duration_str);
            println!("│  Command      : {}", cli_args);
            
            // Show error if present
            if !error_messages.is_null(i) {
                let error = error_messages.value(i);
                println!("│  Error        : {}", truncate_error(error));
            }
            
            println!("└────────────────────────────────────────────────────────────────");
            println!();
        }
    }

    Ok(())
}

/// Show detailed lifecycle for a specific transaction
async fn show_transaction_detail(
    control_table: &steward::ControlTable,
    txn_seq: i64,
) -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_table("transactions", Arc::new(control_table.table().clone()))
        .map_err(|e| anyhow!("Failed to register control table: {}", e))?;

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

    let df = ctx.sql(&sql).await.map_err(|e| {
        anyhow!("Failed to query transaction detail: {}", e)
    })?;

    let batches = df.collect().await.map_err(|e| {
        anyhow!("Failed to collect query results: {}", e)
    })?;

    println!("\n╔═══════════════════════════════════════════════════════════════════════════╗");
    println!("║                     TRANSACTION DETAIL: {}                                   ║", txn_seq);
    println!("╚═══════════════════════════════════════════════════════════════════════════╝\n");

    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        println!("Transaction {} not found.\n", txn_seq);
        return Ok(());
    }

    // Track whether we're showing main transaction or post-commit tasks
    let mut in_post_commit = false;

    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }

        let txn_seqs = batch.column_by_name("txn_seq")
            .unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        let txn_ids = batch.column_by_name("txn_id")
            .unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let record_types = batch.column_by_name("record_type")
            .unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let timestamps_col = batch.column_by_name("timestamp")
            .unwrap().as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        let txn_types = batch.column_by_name("transaction_type")
            .unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let cli_args_col = batch.column_by_name("cli_args")
            .unwrap().as_any().downcast_ref::<ListArray>().unwrap();
        let data_fs_versions = batch.column_by_name("data_fs_version")
            .unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        let error_messages = batch.column_by_name("error_message")
            .unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let durations = batch.column_by_name("duration_ms")
            .unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        let parent_txn_seqs = batch.column_by_name("parent_txn_seq")
            .unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        let execution_seqs = batch.column_by_name("execution_seq")
            .unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
        let factory_names = batch.column_by_name("factory_name")
            .unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let config_paths = batch.column_by_name("config_path")
            .unwrap().as_any().downcast_ref::<StringArray>().unwrap();

        for i in 0..batch.num_rows() {
            let current_txn_seq = txn_seqs.value(i);
            let txn_id = txn_ids.value(i);
            let record_type = record_types.value(i);
            let timestamp = format_timestamp(timestamps_col.value(i));
            let txn_type = txn_types.value(i);
            let is_post_commit = !parent_txn_seqs.is_null(i);

            // Section header for post-commit tasks
            if is_post_commit && !in_post_commit {
                in_post_commit = true;
                println!("\n═══ POST-COMMIT TASKS ═══════════════════════════════════════════════\n");
            }

            match record_type {
                "begin" => {
                    // Extract CLI args
                    let cli_args = if !cli_args_col.is_null(i) {
                        let args_array = cli_args_col.value(i);
                        let string_array = args_array.as_any().downcast_ref::<StringArray>().unwrap();
                        let mut args = Vec::new();
                        for j in 0..string_array.len() {
                            if !string_array.is_null(j) {
                                args.push(string_array.value(j).to_string());
                            }
                        }
                        args.join(" ")
                    } else {
                        "<no command>".to_string()
                    };

                    println!("┌─ BEGIN {} Transaction ──────────────────────────────────", txn_type);
                    println!("│  Sequence     : {}", current_txn_seq);
                    println!("│  UUID         : {}", txn_id);
                    println!("│  Timestamp    : {}", timestamp);
                    println!("│  Command      : {}", cli_args);
                    println!("└────────────────────────────────────────────────────────────────");
                }
                "data_committed" => {
                    let version = if !data_fs_versions.is_null(i) {
                        data_fs_versions.value(i)
                    } else {
                        0
                    };
                    let duration = if !durations.is_null(i) {
                        format!("{}ms", durations.value(i))
                    } else {
                        "N/A".to_string()
                    };
                    println!("│  ✓ DATA COMMITTED at {} (version {}, duration: {})", timestamp, version, duration);
                }
                "completed" => {
                    let duration = if !durations.is_null(i) {
                        format!("{}ms", durations.value(i))
                    } else {
                        "N/A".to_string()
                    };
                    println!("│  ✓ COMPLETED at {} (duration: {})", timestamp, duration);
                }
                "failed" => {
                    let duration = if !durations.is_null(i) {
                        format!("{}ms", durations.value(i))
                    } else {
                        "N/A".to_string()
                    };
                    let error = if !error_messages.is_null(i) {
                        error_messages.value(i)
                    } else {
                        "<no error message>"
                    };
                    println!("│  ✗ FAILED at {} (duration: {})", timestamp, duration);
                    println!("│  Error: {}", error);
                }
                "post_commit_pending" => {
                    let _exec_seq = if !execution_seqs.is_null(i) {
                        execution_seqs.value(i)
                    } else {
                        0
                    };
                    let factory = if !factory_names.is_null(i) {
                        factory_names.value(i)
                    } else {
                        "<unknown>"
                    };
                    let config = if !config_paths.is_null(i) {
                        config_paths.value(i)
                    } else {
                        "<unknown>"
                    };
                    println!("┌─ POST-COMMIT TASK #{} PENDING ──────────────────────────────", _exec_seq);
                    println!("│  Factory      : {}", factory);
                    println!("│  Config       : {}", config);
                    println!("│  Timestamp    : {}", timestamp);
                }
                "post_commit_started" => {
                    let _exec_seq = if !execution_seqs.is_null(i) {
                        execution_seqs.value(i)
                    } else {
                        0
                    };
                    println!("│  ▶ STARTED at {}", timestamp);
                }
                "post_commit_completed" => {
                    let _exec_seq = if !execution_seqs.is_null(i) {
                        execution_seqs.value(i)
                    } else {
                        0
                    };
                    let duration = if !durations.is_null(i) {
                        format!("{}ms", durations.value(i))
                    } else {
                        "N/A".to_string()
                    };
                    println!("│  ✓ COMPLETED at {} (duration: {})", timestamp, duration);
                    println!("└────────────────────────────────────────────────────────────────");
                }
                "post_commit_failed" => {
                    let _exec_seq = if !execution_seqs.is_null(i) {
                        execution_seqs.value(i)
                    } else {
                        0
                    };
                    let duration = if !durations.is_null(i) {
                        format!("{}ms", durations.value(i))
                    } else {
                        "N/A".to_string()
                    };
                    let error = if !error_messages.is_null(i) {
                        error_messages.value(i)
                    } else {
                        "<no error message>"
                    };
                    println!("│  ✗ FAILED at {} (duration: {})", timestamp, duration);
                    println!("│  Error: {}", error);
                    println!("└────────────────────────────────────────────────────────────────");
                }
                _ => {
                    println!("│  {} at {}", record_type, timestamp);
                }
            }
        }
    }

    println!();
    Ok(())
}

/// Show incomplete operations for recovery
async fn show_incomplete_operations(
    control_table: &steward::ControlTable,
) -> Result<()> {
    println!("\n╔═══════════════════════════════════════════════════════════════════════════╗");
    println!("║                      INCOMPLETE OPERATIONS                                 ║");
    println!("╚═══════════════════════════════════════════════════════════════════════════╝\n");

    // Use existing method from control table
    let incomplete = control_table.find_incomplete_transactions()
        .await
        .map_err(|e| anyhow!("Failed to find incomplete transactions: {}", e))?;

    if incomplete.is_empty() {
        println!("No incomplete transactions found. All operations completed successfully.\n");
        return Ok(());
    }

    println!("Found {} incomplete transaction(s):\n", incomplete.len());

    for (txn_seq, txn_id, data_fs_version) in incomplete {
        println!("┌─ Transaction {} ────────────────────────────────────────────", txn_seq);
        println!("│  UUID         : {}", txn_id);
        println!("│  Status       : ⚠️  Incomplete (crashed during execution)");
        
        if data_fs_version > 0 {
            println!("│  Data Version : {} (data was committed before crash)", data_fs_version);
        } else {
            println!("│  Data Version : N/A (crashed before data commit)");
        }

        // Get additional details
        if let Ok((cli_args, _)) = control_table.get_incomplete_transaction_details(txn_seq).await {
            if !cli_args.is_empty() {
                println!("│  Command      : {}", cli_args.join(" "));
            }
        }

        println!("└────────────────────────────────────────────────────────────────");
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_control_mode_from_args() {
        // Test recent mode
        let mode = ControlMode::from_args("recent", None, None).unwrap();
        match mode {
            ControlMode::Recent { limit } => assert_eq!(limit, 10),
            _ => panic!("Wrong mode"),
        }

        // Test detail mode
        let mode = ControlMode::from_args("detail", Some(5), None).unwrap();
        match mode {
            ControlMode::Detail { txn_seq } => assert_eq!(txn_seq, 5),
            _ => panic!("Wrong mode"),
        }

        // Test incomplete mode
        let mode = ControlMode::from_args("incomplete", None, None).unwrap();
        matches!(mode, ControlMode::Incomplete);
    }

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
