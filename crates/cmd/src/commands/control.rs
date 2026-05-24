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

/// Recent transaction record from control table query.  Post-D2 the
/// control table no longer carries `cli_args` / `error_message` /
/// `transaction_type` columns; tx kind is inferred from the lifecycle
/// records present and error text lives in `metadata_json`.
#[derive(Debug, Deserialize)]
struct RecentTransaction {
    txn_seq: i64,
    txn_id: String,
    tx_kind: String,
    started_at: Option<i64>,
    final_state: Option<String>,
    ended_at: Option<i64>,
    error_metadata: Option<String>,
    duration_ms: Option<i64>,
}

/// Transaction detail record from control table query.  Post-D2 schema:
/// post-commit record attributes (`parent_seq`, `execution_seq`,
/// `factory_name`, `config_path`, `error_message`) live in
/// `metadata_json`.
#[derive(Debug, Deserialize)]
struct TransactionDetail {
    txn_seq: i64,
    txn_id: String,
    record_kind: String,
    ts_micros: i64,
    has_parent_seq: bool,
    #[allow(dead_code)]
    parent_seq: i64,
    has_commit_kind: bool,
    commit_kind: String,
    has_duration_ms: bool,
    duration_ms: i64,
    metadata_json: String,
}

/// Decoded post-commit attributes packed by `record_post_commit_*` into
/// `metadata_json` (see `steward::control_table::PostCommitMetadata`).
#[derive(Debug, Default, Deserialize)]
struct PostCommitMetadata {
    #[serde(default)]
    execution_seq: Option<i64>,
    #[serde(default)]
    factory_name: Option<String>,
    #[serde(default)]
    config_path: Option<String>,
    #[serde(default)]
    error_message: Option<String>,
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

    // Post-D2 lean schema lives at table "control" with columns:
    //   pond_id, record_kind, txn_seq, txn_id,
    //   commit_kind, has_commit_kind,
    //   parent_seq, has_parent_seq,
    //   duration_ms, has_duration_ms,
    //   ts_micros, metadata_json
    //
    // Transaction kind is inferred from lifecycle records:
    //   * a tx with a `data_committed` record is a "write"
    //   * any other tx (begin -> completed/failed only) is a "read"
    //
    // Setting records (record_kind='setting') and post-commit records
    // (parent_seq present) are excluded from this view.
    let sql = format!(
        r#"
        WITH user_txns AS (
            SELECT *
            FROM control
            WHERE record_kind <> 'setting'
              AND NOT (has_parent_seq = true AND parent_seq <> txn_seq)
        ),
        recent_seqs AS (
            SELECT DISTINCT txn_seq
            FROM user_txns
            WHERE txn_seq > 0
            ORDER BY txn_seq DESC
            LIMIT {limit}
        )
        SELECT
            t.txn_seq,
            t.txn_id,
            CASE
                WHEN MAX(CASE WHEN t.record_kind = 'data_committed' THEN 1 ELSE 0 END) = 1
                    THEN 'write'
                ELSE 'read'
            END AS tx_kind,
            MAX(CASE WHEN t.record_kind = 'begin' THEN t.ts_micros END) AS started_at,
            MAX(CASE
                WHEN t.record_kind IN ('data_committed', 'completed', 'failed')
                    THEN t.record_kind
            END) AS final_state,
            MAX(CASE
                WHEN t.record_kind IN ('data_committed', 'completed', 'failed')
                    THEN t.ts_micros
            END) AS ended_at,
            MAX(CASE WHEN t.record_kind = 'failed' THEN t.metadata_json END) AS error_metadata,
            MAX(CASE WHEN t.has_duration_ms = true THEN t.duration_ms END) AS duration_ms
        FROM user_txns t
        WHERE t.txn_seq IN (SELECT txn_seq FROM recent_seqs)
        GROUP BY t.txn_seq, t.txn_id
        ORDER BY t.txn_seq ASC, started_at ASC
        "#,
        limit = limit
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
            txn.txn_seq, txn.tx_kind
        );
        println!("|  Status       : {}", status);
        println!("|  UUID         : {}", txn.txn_id);
        println!("|  Started      : {}", started_at);
        println!("|  Ended        : {}", ended_at);
        println!("|  Duration     : {}", duration_str);
        // CLI args are no longer captured in the post-D2 lean control
        // table.  Source the original command from data Delta commit
        // metadata (`pond_txn`) if it is needed.

        // Show error if present (parsed from metadata_json on Failed records)
        if let Some(reason) = txn
            .error_metadata
            .as_deref()
            .and_then(extract_failure_reason)
        {
            println!("|  Error        : {}", truncate_error(&reason));
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

    // Post-D2 lean schema: post-commit records carry the parent
    // transaction's seq in `parent_seq` (with `has_parent_seq=true`).
    // Main-transaction records have `has_parent_seq=false`.  Order the
    // result so main records appear first, then post-commit records
    // grouped by their original time ordering.
    let sql = format!(
        r#"
        SELECT
            txn_seq,
            txn_id,
            record_kind,
            ts_micros,
            has_parent_seq,
            parent_seq,
            has_commit_kind,
            commit_kind,
            has_duration_ms,
            duration_ms,
            metadata_json
        FROM control
        WHERE record_kind <> 'setting'
          AND (
              (txn_seq = {seq} AND has_parent_seq = false)
              OR (has_parent_seq = true AND parent_seq = {seq})
          )
        ORDER BY
            CASE WHEN has_parent_seq = false THEN 0 ELSE 1 END,
            ts_micros
        "#,
        seq = txn_seq
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
        let record_kind = detail.record_kind.as_str();
        let timestamp = format_timestamp(detail.ts_micros);
        let is_post_commit = detail.has_parent_seq;

        // Section header for post-commit tasks
        if is_post_commit && !in_post_commit {
            in_post_commit = true;
            println!("\n=== POST-COMMIT TASKS ===============================================\n");
        }

        let duration_str = || -> String {
            if detail.has_duration_ms {
                format!("{}ms", detail.duration_ms)
            } else {
                "N/A".to_string()
            }
        };

        match record_kind {
            "begin" => {
                println!("+- BEGIN Transaction --------------------------------------");
                println!("|  Sequence     : {}", current_txn_seq);
                println!("|  UUID         : {}", txn_id);
                println!("|  Timestamp    : {}", timestamp);
                // CLI args are not persisted in the post-D2 control table;
                // see `pond_txn` in the data Delta commit metadata for
                // the original command.
                println!("+----------------------------------------------------------------");
            }
            "data_committed" => {
                let version = parse_data_delta_version(&detail.metadata_json);
                let commit_kind = if detail.has_commit_kind {
                    detail.commit_kind.as_str()
                } else {
                    "write"
                };
                println!(
                    "|  [OK] DATA COMMITTED at {} ({}, version {}, duration: {})",
                    timestamp,
                    commit_kind,
                    version,
                    duration_str()
                );
            }
            "completed" => {
                println!(
                    "|  [OK] COMPLETED at {} (duration: {})",
                    timestamp,
                    duration_str()
                );
            }
            "failed" => {
                let error = extract_failure_reason(&detail.metadata_json)
                    .unwrap_or_else(|| "<no error message>".to_string());
                println!(
                    "|  [FAIL] FAILED at {} (duration: {})",
                    timestamp,
                    duration_str()
                );
                println!("|  Error: {}", error);
            }
            "post_push_pending" => {
                let pc = parse_post_commit_metadata(&detail.metadata_json);
                let exec_seq = pc.execution_seq.unwrap_or(0);
                let factory = pc.factory_name.as_deref().unwrap_or("<unknown>");
                let config = pc.config_path.as_deref().unwrap_or("<unknown>");
                println!(
                    "+- POST-COMMIT TASK #{} PENDING ------------------------------",
                    exec_seq
                );
                println!("|  Factory      : {}", factory);
                println!("|  Config       : {}", config);
                println!("|  Timestamp    : {}", timestamp);
            }
            "post_push_started" => {
                println!("|  [RUN] STARTED at {}", timestamp);
            }
            "post_push_completed" => {
                println!(
                    "|  [OK] COMPLETED at {} (duration: {})",
                    timestamp,
                    duration_str()
                );
                println!("+----------------------------------------------------------------");
            }
            "post_push_failed" => {
                let pc = parse_post_commit_metadata(&detail.metadata_json);
                let error = pc.error_message.as_deref().unwrap_or("<no error message>");
                println!(
                    "|  [FAIL] FAILED at {} (duration: {})",
                    timestamp,
                    duration_str()
                );
                println!("|  Error: {}", error);
                println!("+----------------------------------------------------------------");
            }
            _ => {
                println!("|  {} at {}", record_kind, timestamp);
            }
        }
    }

    println!();
    Ok(())
}

/// Extract `reason` field from a `Failed` record's `metadata_json` payload.
fn extract_failure_reason(json: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(json)
        .ok()
        .and_then(|v| v.get("reason").and_then(|r| r.as_str().map(String::from)))
}

/// Extract `data_delta_version` from a `DataCommitted` record's
/// `metadata_json`.  Returns 0 if the field is missing (older records).
fn parse_data_delta_version(json: &str) -> i64 {
    serde_json::from_str::<serde_json::Value>(json)
        .ok()
        .and_then(|v| v.get("data_delta_version").and_then(|n| n.as_i64()))
        .unwrap_or(0)
}

/// Decode the post-commit attribute payload from `metadata_json`.
fn parse_post_commit_metadata(json: &str) -> PostCommitMetadata {
    serde_json::from_str(json).unwrap_or_default()
}

/// Execute remote factory sync operation
///
/// This finds factory configurations and executes them, regardless of their mode.
/// The factory's config determines behavior:
/// - push mode factory: Retries failed pushes
/// - pull mode factory: Pulls new bundles and applies them
async fn execute_sync(
    ship_context: &ShipContext,
    _control_table: &mut steward::ControlTable,
    config: Option<String>,
) -> Result<()> {
    // Delegate to sync_command with no name (legacy: syncs hardcoded path)
    sync_command(ship_context, None, config).await
}

/// Resolve a factory short name or path to a full pond path.
///
/// - Full paths (starting with `/`) are returned as-is
/// - Short names are resolved to `/system/run/{name}` (auto-executing factories)
///   or `/system/etc/{name}` (manually-triggered factories)
///
/// For `pond sync`, only `/system/run/` is scanned, so the fallback
/// to `/system/etc/` only matters for `pond run` short names.
fn resolve_factory_path(name: &str) -> String {
    if name.starts_with('/') {
        name.to_string()
    } else {
        // pond sync only looks in /system/run/ anyway;
        // pond run will try both at runtime via resolve_short_factory_name().
        format!("/system/run/{}", name)
    }
}

/// System directories where factory nodes live.
///
/// - `/system/run/`  -- auto-executing on post-commit (remote push/pull)
/// - `/system/etc/`  -- manually triggered or passive (hydrovu, sitegen, column-rename)
pub const SYSTEM_RUN_DIR: &str = "/system/run";
pub const SYSTEM_ETC_DIR: &str = "/system/etc";

/// Resolve a bare factory name against `/system/run/` then `/system/etc/`.
/// Returns the first path that exists, or falls back to `/system/run/{name}`.
pub async fn resolve_short_factory_name(ship_context: &ShipContext, name: &str) -> Result<String> {
    if name.starts_with('/') {
        return Ok(name.to_string());
    }

    let mut ship = ship_context.open_pond().await?;
    let tx = ship
        .begin_read(&steward::PondUserMetadata::new(vec![
            "resolve-factory".to_string(),
        ]))
        .await?;

    let root = tx.root().await?;

    // Try /system/run/ first (auto-executing), then /system/etc/ (manual)
    for dir in &[SYSTEM_RUN_DIR, SYSTEM_ETC_DIR] {
        let candidate = format!("{}/{}", dir, name);
        if root.resolve_path(&candidate).await.is_ok() {
            _ = tx.commit().await?;
            log::info!("Resolved short name '{}' -> '{}'", name, candidate);
            return Ok(candidate);
        }
    }

    _ = tx.commit().await?;
    // Fall back to /system/run/ (will produce a clear error downstream)
    let fallback = format!("{}/{}", SYSTEM_RUN_DIR, name);
    log::info!(
        "Short name '{}' not found, defaulting to '{}'",
        name,
        fallback
    );
    Ok(fallback)
}

/// Sync with remote storage: retry pushes, pull new bundles.
///
/// - If `config` is provided, use it directly (recovery mode, no pond needed).
/// - If `name` is provided, sync just that factory (short name or full path).
/// - If neither, scan `/system/run/` for all remote factories and sync each.
pub async fn sync_command(
    ship_context: &ShipContext,
    name: Option<String>,
    config: Option<String>,
) -> Result<()> {
    // Recovery mode: --config with base64 encoded config
    if let Some(config_value) = config {
        let mut ship = ship_context.open_pond().await?;
        let control_table = ship.control_table_mut();
        control_table.print_banner();

        log::info!("[SYNC] Executing recovery sync with --config...");

        let mut ship2 = ship_context.open_pond().await?;
        let mut tx = ship2
            .begin_read(&steward::PondUserMetadata::new(vec!["sync".to_string()]))
            .await?;

        match execute_sync_with_config(&mut tx, control_table, config_value).await {
            Ok(()) => {
                _ = tx.commit().await?;
                log::info!("[OK] Recovery sync completed");
                return Ok(());
            }
            Err(e) => return Err(tx.abort(&e).await.into()),
        }
    }

    // Determine which factories to sync
    let factory_paths = if let Some(ref n) = name {
        vec![resolve_factory_path(n)]
    } else {
        // Scan /system/run/ for all remote factories
        discover_syncable_factories(ship_context).await?
    };

    if factory_paths.is_empty() {
        log::info!("[SYNC] No remote factories found in /system/run/");
        println!("No remote factories found in /system/run/");
        return Ok(());
    }

    // Sync each factory
    for path in &factory_paths {
        log::info!("[SYNC] Syncing factory: {}", path);
        sync_single_factory(ship_context, path).await?;
    }

    log::info!(
        "[OK] Sync completed for {} factory(ies)",
        factory_paths.len()
    );
    Ok(())
}

/// Discover all syncable (remote) factories in /system/run/
async fn discover_syncable_factories(ship_context: &ShipContext) -> Result<Vec<String>> {
    let mut ship = ship_context.open_pond().await?;
    let tx = ship
        .begin_read(&steward::PondUserMetadata::new(vec![
            "sync-discover".to_string(),
        ]))
        .await?;

    let root = tx.root().await?;

    // Use collect_matches to find all entries in /system/run/*
    let matches = match root.collect_matches("/system/run/*").await {
        Ok(m) => m,
        Err(_) => {
            _ = tx.commit().await?;
            return Ok(vec![]);
        }
    };

    // Check each entry to see if it's a remote factory
    let mut factory_paths = Vec::new();
    for (node_path, _captures) in &matches {
        let path = node_path.path().to_string_lossy().to_string();
        let node_id = node_path.id();
        if let Ok(Some(factory_name)) = tx.get_factory_for_node(node_id).await
            && factory_name == "remote"
        {
            factory_paths.push(path);
        }
    }

    _ = tx.commit().await?;
    Ok(factory_paths)
}

/// Sync a single factory by its full path
async fn sync_single_factory(ship_context: &ShipContext, factory_path: &str) -> Result<()> {
    let mut ship = ship_context.open_pond().await?;
    let control_table = ship.control_table_mut();
    control_table.print_banner();

    let mut ship2 = ship_context.open_pond().await?;
    let mut tx = ship2
        .begin_read(&steward::PondUserMetadata::new(vec!["sync".to_string()]))
        .await?;

    match execute_sync_for_path(&mut tx, control_table, factory_path).await {
        Ok(()) => {
            _ = tx.commit().await?;
            log::info!("[OK] Sync completed for: {}", factory_path);
            Ok(())
        }
        Err(e) => Err(tx.abort(&e).await.into()),
    }
}

/// Execute sync using base64 recovery config
async fn execute_sync_with_config(
    tx: &mut steward::Transaction<'_>,
    control_table: &mut steward::ControlTable,
    encoded: String,
) -> Result<()> {
    log::info!("Using remote config from --config argument");
    let repl_config = remote::ReplicationConfig::from_base64(&encoded)
        .map_err(|e| anyhow!("Failed to decode base64 config: {}", e))?;

    let config = repl_config.remote;
    let pond_metadata = control_table.get_pond_metadata().clone();

    let provider_context = tx.provider_context()?;
    let factory_context = provider::FactoryContext::with_metadata(
        provider_context,
        tinyfs::FileID::root(),
        pond_metadata,
    );

    let config_str = serde_yaml::to_string(&config)
        .map_err(|e| anyhow!("Failed to serialize remote config: {}", e))?;
    let config_bytes = config_str.as_bytes().to_vec();

    let args = vec!["pull".to_string()];
    FactoryRegistry::execute::<tlogfs::TLogFSError>(
        "remote",
        &config_bytes,
        factory_context,
        ExecutionContext::control_writer(args),
    )
    .await
    .map_err(|e| anyhow!("Remote factory execution failed: {}", e))?;

    Ok(())
}

/// Execute sync for a specific factory path
async fn execute_sync_for_path(
    tx: &mut steward::Transaction<'_>,
    control_table: &mut steward::ControlTable,
    factory_path: &str,
) -> Result<()> {
    log::info!("Looking for remote factory at: {}", factory_path);

    let root = tx.root().await?;

    let (_parent_wd, lookup_result) = root
        .resolve_path(factory_path)
        .await
        .with_context(|| format!("Failed to resolve path: {}", factory_path))?;

    let config_node = match lookup_result {
        tinyfs::Lookup::Found(node) => node,
        tinyfs::Lookup::NotFound(_, _) => {
            return Err(anyhow!("Factory configuration not found: {}", factory_path));
        }
        tinyfs::Lookup::Empty(_) => {
            return Err(anyhow!("Invalid path: {}", factory_path));
        }
    };

    let node_id = config_node.id();

    let factory_name = tx
        .get_factory_for_node(node_id)
        .await
        .with_context(|| format!("Failed to get factory for: {}", factory_path))?
        .ok_or_else(|| {
            anyhow!(
                "Factory configuration has no associated factory: {}",
                factory_path
            )
        })?;

    let config_bytes = {
        let mut reader = root
            .async_reader_path(factory_path)
            .await
            .with_context(|| format!("Failed to open file: {}", factory_path))?;

        let mut buffer = Vec::new();
        _ = reader
            .read_to_end(&mut buffer)
            .await
            .with_context(|| format!("Failed to read file: {}", factory_path))?;
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

        // CLI args are no longer captured by the post-D2 control table;
        // see the corresponding `pond_txn` Delta commit metadata if the
        // original command is needed for diagnostics.

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
