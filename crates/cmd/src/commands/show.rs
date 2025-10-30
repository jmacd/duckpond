use anyhow::{Result, anyhow};

use crate::common::{ShipContext, format_node_id};
use utilities::banner;

/// Label for transaction sequence in detailed view headers
const TRANSACTION_LABEL: &str = "Transaction";

/// Show pond contents with a closure for handling output
pub async fn show_command<F>(ship_context: &ShipContext, mode: &str, mut handler: F) -> Result<()>
where
    F: FnMut(String),
{
    let mut ship = ship_context.open_pond().await?;

    // Use read transaction for consistent filesystem access (show is read-only)
    let mut tx = ship
        .begin_transaction(steward::TransactionOptions::read(vec!["show".to_string()]))
        .await
        .map_err(|e| anyhow!("Failed to begin read transaction: {}", e))?;

    let result = show_filesystem_transactions(&mut tx, mode)
        .await
        .map_err(|e| anyhow!("Failed to access data filesystem: {}", e))?;

    tx.commit()
        .await
        .map_err(|e| anyhow!("Failed to commit read transaction: {}", e))?;

    handler(result);
    Ok(())
}

async fn show_filesystem_transactions(
    tx: &mut steward::StewardTransactionGuard<'_>,
    mode: &str,
) -> Result<String, steward::StewardError> {
    // Get data persistence from the transaction guard
    let persistence = tx
        .data_persistence()
        .map_err(|e| steward::StewardError::DataInit(e))?;

    // Get commit history from the filesystem
    let commit_history = persistence
        .get_commit_history(None)
        .await
        .map_err(|e| steward::StewardError::DataInit(e))?;

    if commit_history.is_empty() {
        return Ok("No transactions found in this filesystem.".to_string());
    }

    // Get pond path from persistence for control table access (clone to avoid borrow issues)
    let store_path = persistence.store_path().to_string();
    let pond_path = std::path::Path::new(&store_path)
        .parent()
        .ok_or_else(|| steward::StewardError::Dyn("Invalid store path".into()))?
        .to_path_buf();

    // Route to appropriate display mode
    match mode {
        "brief" => show_brief_mode(&commit_history, &store_path, &pond_path, tx).await,
        "detailed" => show_detailed_mode(&commit_history, &store_path, &pond_path, tx).await,
        _ => Err(steward::StewardError::Dyn(
            format!(
                "Invalid mode '{}'. Use 'brief', 'concise', or 'detailed'.",
                mode
            )
            .into(),
        )),
    }
}

/// Show brief summary statistics about the pond
async fn show_brief_mode(
    commit_history: &[deltalake::kernel::CommitInfo],
    store_path: &str,
    _pond_path: &std::path::Path,
    tx: &mut steward::StewardTransactionGuard<'_>,
) -> Result<String, steward::StewardError> {
    use std::collections::HashMap;

    let mut output = String::new();
    
    // Access control table through transaction guard (uses Ship's cached instance)
    let control_table = tx.control_table();

    control_table.print_banner().await?;

    // Get DataFusion session context from the existing transaction guard
    let session_ctx = tx.session_context().await.map_err(|e| {
        steward::StewardError::Dyn(format!("Failed to get session context: {}", e).into())
    })?;

    // Query partition statistics using SQL against the pre-registered delta_table
    let partition_stats_sql = "
        SELECT 
            part_id,
            COUNT(*) as total_rows,
            SUM(CASE WHEN file_type = 'directory' THEN 1 ELSE 0 END) as directory_rows,
            COUNT(DISTINCT CASE WHEN file_type != 'directory' THEN node_id ELSE NULL END) as distinct_files,
            SUM(CASE WHEN file_type != 'directory' THEN 1 ELSE 0 END) as file_version_rows
        FROM delta_table
        GROUP BY part_id
    ";

    let df = session_ctx.sql(partition_stats_sql).await.map_err(|e| {
        steward::StewardError::Dyn(format!("Failed to query partition stats: {}", e).into())
    })?;

    let batches = df.collect().await.map_err(|e| {
        steward::StewardError::Dyn(format!("Failed to collect partition stats: {}", e).into())
    })?;

    // Parse the results into our PartitionStats structure
    let mut partition_map: HashMap<String, PartitionStats> = HashMap::new();

    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::DataType;
    use arrow_cast::cast;

    for batch in batches {
        // Cast part_id from Dictionary to plain Utf8 if needed
        let part_id_col = batch
            .column_by_name("part_id")
            .ok_or_else(|| steward::StewardError::Dyn("Missing part_id column".into()))?;
        let part_id_string_col = match part_id_col.data_type() {
            DataType::Utf8 => part_id_col.clone(),
            _ => cast(part_id_col.as_ref(), &DataType::Utf8).map_err(|e| {
                steward::StewardError::Dyn(format!("Failed to cast part_id to Utf8: {}", e).into())
            })?,
        };
        let part_ids = part_id_string_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                steward::StewardError::Dyn(
                    format!(
                        "Failed to downcast part_id column. Actual type: {:?}",
                        part_id_col.data_type()
                    )
                    .into(),
                )
            })?;
        let total_rows = batch
            .column_by_name("total_rows")
            .ok_or_else(|| steward::StewardError::Dyn("Missing total_rows column".into()))?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                steward::StewardError::Dyn("Failed to downcast total_rows column".into())
            })?;
        let directory_rows = batch
            .column_by_name("directory_rows")
            .ok_or_else(|| steward::StewardError::Dyn("Missing directory_rows column".into()))?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                steward::StewardError::Dyn("Failed to downcast directory_rows column".into())
            })?;
        let distinct_files = batch
            .column_by_name("distinct_files")
            .ok_or_else(|| steward::StewardError::Dyn("Missing distinct_files column".into()))?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                steward::StewardError::Dyn("Failed to downcast distinct_files column".into())
            })?;
        let file_versions = batch
            .column_by_name("file_version_rows")
            .ok_or_else(|| steward::StewardError::Dyn("Missing file_version_rows column".into()))?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                steward::StewardError::Dyn("Failed to downcast file_versions column".into())
            })?;

        for i in 0..batch.num_rows() {
            let part_id = part_ids.value(i).to_string();
            let stats = PartitionStats {
                total_rows: total_rows.value(i) as usize,
                directory_rows: directory_rows.value(i) as usize,
                distinct_files: distinct_files.value(i) as usize,
                file_versions: file_versions.value(i) as usize,
                path_name: None,
            };
            partition_map.insert(part_id, stats);
        }
    }

    // Get aggregate file statistics from Delta Lake metadata
    let table = deltalake::open_table(store_path).await.map_err(|e| {
        steward::StewardError::Dyn(format!("Failed to open Delta table: {}", e).into())
    })?;

    let snapshot = table
        .snapshot()
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to get snapshot: {}", e).into()))?;

    let log_store = table.log_store();
    let mut file_stream = snapshot.file_actions_iter(&*log_store);

    use futures::stream::StreamExt;

    let mut total_parquet_files = 0;
    let mut total_parquet_bytes: i64 = 0;

    while let Some(add_result) = file_stream.next().await {
        if let Ok(add_action) = add_result {
            total_parquet_files += 1;
            total_parquet_bytes += add_action.size;
        }
    }

    // Resolve partition paths by traversing TinyFS
    log::debug!("Resolving partition paths via TinyFS...");
    let root = tx
        .root()
        .await
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to get root: {}", e).into()))?;

    let matches = root.collect_matches("**/*").await.map_err(|e| {
        steward::StewardError::Dyn(format!("Failed to collect matches: {}", e).into())
    })?;

    // Build path map from all matched entries
    for (node_path, _captured) in matches {
        let node_id = node_path.id().await;
        let path = node_path.path();
        let node_id_str = node_id.to_string();

        if let Some(stats) = partition_map.get_mut(&node_id_str) {
            stats.path_name = Some(path.to_string_lossy().to_string());
        }
    }

    // Also add the root directory
    let root_node_id = root.node_path().id().await;
    if let Some(stats) = partition_map.get_mut(&root_node_id.to_string()) {
        stats.path_name = Some("/".to_string());
    }

    // Format the output
    output.push_str("\n");
    output.push_str(
        "╔════════════════════════════════════════════════════════════════════════════╗\n",
    );
    output.push_str(
        "║                            POND SUMMARY                                    ║\n",
    );
    output.push_str(
        "╚════════════════════════════════════════════════════════════════════════════╝\n",
    );
    output.push_str("\n");

    output.push_str(&format!(
        "  Transactions       : {}\n",
        commit_history.len()
    ));
    output.push_str(&format!(
        "  Delta Lake Version : {}\n",
        table.version().unwrap_or(0)
    ));
    output.push_str("\n");

    output.push_str("  Storage Statistics\n");
    output.push_str("  ──────────────────\n");
    output.push_str(&format!("  Parquet Files      : {}\n", total_parquet_files));
    output.push_str(&format!(
        "  Total Size         : {}\n",
        format_byte_size(total_parquet_bytes)
    ));
    output.push_str(&format!("  Partitions         : {}\n", partition_map.len()));
    output.push_str("\n");

    // Sort partitions by total rows
    let mut partition_vec: Vec<_> = partition_map.into_iter().collect();
    partition_vec.sort_by(|a, b| b.1.total_rows.cmp(&a.1.total_rows));

    // Show all partitions with detailed breakdown
    output.push_str("  Partitions (by row count)\n");
    output.push_str("  ─────────────────────────\n");
    for (part_id, stats) in partition_vec.iter() {
        let path_display = stats.path_name.as_deref().unwrap_or("<unknown>");

        output.push_str(&format!(
            "\n  {} {}\n",
            format_node_id(part_id),
            path_display
        ));
        output.push_str(&format!(
            "    {} rows ({} dir, {} files, {} versions)\n",
            stats.total_rows, stats.directory_rows, stats.distinct_files, stats.file_versions
        ));
    }
    output.push_str("\n");

    Ok(output)
}

#[derive(Default)]
struct PartitionStats {
    total_rows: usize,
    directory_rows: usize,
    distinct_files: usize,
    file_versions: usize,
    path_name: Option<String>,
}

/// Query control table for transaction commands
async fn query_transaction_commands(
    control_table: &steward::ControlTable,
) -> Result<std::collections::HashMap<i64, Vec<String>>, steward::StewardError> {
    use arrow::array::{Array, Int64Array, ListArray, StringArray};
    use std::collections::HashMap;

    // Use control table's SessionContext (following tlogfs pattern)
    let ctx = control_table.session_context();

    // Query for begin records which have the cli_args
    // Only include WRITE transactions - read transactions don't modify pond state
    let df = ctx
        .sql(
            "SELECT txn_seq, cli_args 
         FROM transactions 
         WHERE record_type = 'begin' AND transaction_type = 'write'
         ORDER BY txn_seq",
        )
        .await
        .map_err(|e| {
            steward::StewardError::Dyn(format!("Failed to query commands: {}", e).into())
        })?;

    let batches = df.collect().await.map_err(|e| {
        steward::StewardError::Dyn(format!("Failed to collect command results: {}", e).into())
    })?;

    let mut command_map: HashMap<i64, Vec<String>> = HashMap::new();

    for batch in batches {
        let txn_seq_col = batch
            .column_by_name("txn_seq")
            .ok_or_else(|| steward::StewardError::Dyn("Missing txn_seq column".into()))?;
        let txn_seqs = txn_seq_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| steward::StewardError::Dyn("Failed to downcast txn_seq".into()))?;

        let cli_args_col = batch
            .column_by_name("cli_args")
            .ok_or_else(|| steward::StewardError::Dyn("Missing cli_args column".into()))?;
        let cli_args_list = cli_args_col
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| steward::StewardError::Dyn("Failed to downcast cli_args".into()))?;

        for i in 0..batch.num_rows() {
            let txn_seq = txn_seqs.value(i);

            // Extract the list of strings for this row
            let mut args = Vec::new();
            if !cli_args_list.is_null(i) {
                let args_array = cli_args_list.value(i);
                let string_array = args_array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        steward::StewardError::Dyn("Failed to downcast args array".into())
                    })?;

                for j in 0..string_array.len() {
                    if !string_array.is_null(j) {
                        args.push(string_array.value(j).to_string());
                    }
                }
            }

            command_map.insert(txn_seq, args);
        }
    }

    Ok(command_map)
}

/// Show detailed transaction log using transaction sequences
async fn show_detailed_mode(
    _commit_history: &[deltalake::kernel::CommitInfo],
    _store_path: &str,
    _pond_path: &std::path::Path,
    tx: &mut steward::StewardTransactionGuard<'_>,
) -> Result<String, steward::StewardError> {
    use std::collections::HashMap;

    let mut output = String::new();

    // Access control table through transaction guard (uses Ship's cached instance)
    let control_table = tx.control_table();
    
    control_table.print_banner().await?;

    // Query control table for transaction commands
    // Build a map of txn_seq -> cli_args
    let command_map = query_transaction_commands(control_table).await?;

    // Get session context from the existing transaction guard (no need to create a new one)
    let session_ctx = tx.session_context().await.map_err(|e| {
        steward::StewardError::Dyn(format!("Failed to get session context: {}", e).into())
    })?;

    // Build path map by traversing TinyFS (same as brief mode)
    log::debug!("Resolving partition paths via TinyFS...");
    let mut path_map: HashMap<String, String> = HashMap::new();

    // Access TinyFS root through the Steward transaction guard (it derefs to FS)
    let fs: &tinyfs::FS = &*tx;
    let root = fs
        .root()
        .await
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to get root: {}", e).into()))?;

    let matches = root.collect_matches("**/*").await.map_err(|e| {
        steward::StewardError::Dyn(format!("Failed to collect matches: {}", e).into())
    })?;

    // Build path map from all matched entries
    for (node_path, _captured) in matches {
        let node_id = node_path.id().await;
        let path = node_path.path();
        let node_id_str = node_id.to_string();
        path_map.insert(node_id_str, path.to_string_lossy().to_string());
    }

    // Also add the root directory
    let root_node_id = root.node_path().id().await;
    path_map.insert(root_node_id.to_string(), "/".to_string());

    // Query ALL operations in a single query, ordered by transaction (newest first)
    // This is vastly more efficient than querying each transaction separately (was 79 queries!)
    let all_ops_sql = "
        SELECT txn_seq, part_id, node_id, file_type, version, timestamp, size, content, factory
        FROM delta_table
        ORDER BY txn_seq DESC, part_id, node_id, version
    ";

    let df = session_ctx.sql(all_ops_sql).await.map_err(|e| {
        steward::StewardError::Dyn(format!("Failed to query operations: {}", e).into())
    })?;

    let batches = df.collect().await.map_err(|e| {
        steward::StewardError::Dyn(format!("Failed to collect operations: {}", e).into())
    })?;

    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        return Ok("No transactions found.\n".to_string());
    }

    // Group batches by transaction sequence
    use arrow::array::{Array, Int64Array};
    use std::collections::BTreeMap;

    let mut txn_batches: BTreeMap<i64, Vec<arrow::record_batch::RecordBatch>> = BTreeMap::new();

    for batch in batches {
        let txn_seq_col = batch
            .column_by_name("txn_seq")
            .ok_or_else(|| steward::StewardError::Dyn("Missing txn_seq column".into()))?;
        let txn_seqs = txn_seq_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                steward::StewardError::Dyn("Failed to downcast txn_seq column".into())
            })?;

        // Group rows by txn_seq within this batch
        let mut current_txn_rows: std::collections::HashMap<i64, Vec<usize>> =
            std::collections::HashMap::new();
        for row_idx in 0..batch.num_rows() {
            let txn_seq = txn_seqs.value(row_idx);
            current_txn_rows
                .entry(txn_seq)
                .or_insert_with(Vec::new)
                .push(row_idx);
        }

        // Create a separate batch for each transaction
        for (txn_seq, row_indices) in current_txn_rows {
            let txn_batch = batch.slice(row_indices[0], row_indices.len());
            txn_batches
                .entry(txn_seq)
                .or_insert_with(Vec::new)
                .push(txn_batch);
        }
    }

    // Format output for each transaction (in descending order)
    for (txn_seq, batches_for_txn) in txn_batches.iter().rev() {
        // Extract unique PartIDs and NodeIDs for the header
        use arrow::array::StringArray;
        use arrow::datatypes::DataType;
        use std::collections::BTreeSet;

        let mut part_ids: BTreeSet<String> = BTreeSet::new();
        let mut node_ids: BTreeSet<String> = BTreeSet::new();

        for batch in batches_for_txn.iter() {
            // Extract part_id column
            if let Some(part_id_col) = batch.column_by_name("part_id") {
                let part_id_string_col = match part_id_col.data_type() {
                    DataType::Utf8 => part_id_col.clone(),
                    _ => arrow_cast::cast(part_id_col.as_ref(), &DataType::Utf8).map_err(|e| {
                        steward::StewardError::Dyn(format!("Failed to cast part_id: {}", e).into())
                    })?,
                };
                if let Some(part_id_array) =
                    part_id_string_col.as_any().downcast_ref::<StringArray>()
                {
                    for i in 0..part_id_array.len() {
                        if !part_id_array.is_null(i) {
                            part_ids.insert(part_id_array.value(i).to_string());
                        }
                    }
                }
            }

            // Extract node_id column
            if let Some(node_id_col) = batch.column_by_name("node_id") {
                let node_id_string_col = match node_id_col.data_type() {
                    DataType::Utf8 => node_id_col.clone(),
                    _ => arrow_cast::cast(node_id_col.as_ref(), &DataType::Utf8).map_err(|e| {
                        steward::StewardError::Dyn(format!("Failed to cast node_id: {}", e).into())
                    })?,
                };
                if let Some(node_id_array) =
                    node_id_string_col.as_any().downcast_ref::<StringArray>()
                {
                    for i in 0..node_id_array.len() {
                        if !node_id_array.is_null(i) {
                            node_ids.insert(node_id_array.value(i).to_string());
                        }
                    }
                }
            }
        }

        // Collect unique UUIDs and format with hyphens using uuid7's to_string()
        let mut unique_uuids: BTreeSet<String> = BTreeSet::new();
        for id_hex in part_ids.iter().chain(node_ids.iter()) {
            // Parse hex string as UUID and format with hyphens
            if let Ok(uuid) = id_hex.parse::<uuid7::Uuid>() {
                unique_uuids.insert(uuid.to_string());
            } else {
                // If parsing fails, use the original hex string
                unique_uuids.insert(id_hex.clone());
            }
        }
        let unique_ids: Vec<String> = unique_uuids.into_iter().collect();

        // Format transaction banner using reusable formatter
        output.push_str(&banner::format_transaction_banner(
            TRANSACTION_LABEL,
            *txn_seq,
            unique_ids,
        ));

        // Display command if available
        if let Some(cli_args) = command_map.get(txn_seq) {
            if !cli_args.is_empty() {
                output.push_str(&format!("  Command: {}\n", cli_args.join(" ")));
            }
        }
        output.push_str("\n");

        // Format operations for this transaction
        let formatted_ops = format_operations_from_batches(batches_for_txn.clone(), &path_map)?;

        if formatted_ops.is_empty() {
            output.push_str("  (No operations found - this should not happen)\n");
        } else {
            for op in formatted_ops {
                output.push_str(&format!("  {}\n", op));
            }
        }
    }

    Ok(output)
}

/// Format byte size as human-readable string
fn format_byte_size(bytes: i64) -> String {
    if bytes < 0 {
        return "unknown size".to_string();
    }

    let bytes = bytes as f64;
    if bytes < 1024.0 {
        format!("{} bytes", bytes as i64)
    } else if bytes < 1024.0 * 1024.0 {
        format!("{:.1} KB", bytes / 1024.0)
    } else if bytes < 1024.0 * 1024.0 * 1024.0 {
        format!("{:.1} MB", bytes / (1024.0 * 1024.0))
    } else {
        format!("{:.1} GB", bytes / (1024.0 * 1024.0 * 1024.0))
    }
}

/// Format operations from Arrow batches (used by transaction sequence display)
fn format_operations_from_batches(
    batches: Vec<arrow::record_batch::RecordBatch>,
    path_map: &std::collections::HashMap<String, String>,
) -> Result<Vec<String>, steward::StewardError> {
    use arrow::array::{Array, BinaryArray, Int64Array, StringArray};
    use arrow::datatypes::DataType;
    use arrow_cast::cast;
    use std::collections::HashMap;
    use tinyfs::tree_format::TreeNode;

    // Group operations by partition for better readability
    let mut partition_groups: HashMap<String, Vec<(String, Vec<String>)>> = HashMap::new();

    for batch in batches {
        // Extract columns
        let part_id_col = batch
            .column_by_name("part_id")
            .ok_or_else(|| steward::StewardError::Dyn("Missing part_id column".into()))?;
        let part_id_string_col = match part_id_col.data_type() {
            DataType::Utf8 => part_id_col.clone(),
            _ => cast(part_id_col.as_ref(), &DataType::Utf8).map_err(|e| {
                steward::StewardError::Dyn(format!("Failed to cast part_id: {}", e).into())
            })?,
        };
        let part_ids = part_id_string_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                steward::StewardError::Dyn("Failed to downcast part_id column".into())
            })?;

        let node_id_col = batch
            .column_by_name("node_id")
            .ok_or_else(|| steward::StewardError::Dyn("Missing node_id column".into()))?;
        let node_id_string_col = match node_id_col.data_type() {
            DataType::Utf8 => node_id_col.clone(),
            _ => cast(node_id_col.as_ref(), &DataType::Utf8).map_err(|e| {
                steward::StewardError::Dyn(format!("Failed to cast node_id: {}", e).into())
            })?,
        };
        let node_ids = node_id_string_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                steward::StewardError::Dyn("Failed to downcast node_id column".into())
            })?;

        let file_type_col = batch
            .column_by_name("file_type")
            .ok_or_else(|| steward::StewardError::Dyn("Missing file_type column".into()))?;
        let file_type_string_col = match file_type_col.data_type() {
            DataType::Utf8 => file_type_col.clone(),
            _ => cast(file_type_col.as_ref(), &DataType::Utf8).map_err(|e| {
                steward::StewardError::Dyn(format!("Failed to cast file_type: {}", e).into())
            })?,
        };
        let file_types = file_type_string_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                steward::StewardError::Dyn("Failed to downcast file_type column".into())
            })?;

        let version_col = batch
            .column_by_name("version")
            .ok_or_else(|| steward::StewardError::Dyn("Missing version column".into()))?;
        let versions = version_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                steward::StewardError::Dyn("Failed to downcast version column".into())
            })?;

        let size_col = batch
            .column_by_name("size")
            .ok_or_else(|| steward::StewardError::Dyn("Missing size column".into()))?;
        let sizes = size_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| steward::StewardError::Dyn("Failed to downcast size column".into()))?;

        let content_col = batch
            .column_by_name("content")
            .ok_or_else(|| steward::StewardError::Dyn("Missing content column".into()))?;
        let contents = content_col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                steward::StewardError::Dyn("Failed to downcast content column".into())
            })?;

        let factory_col = batch
            .column_by_name("factory")
            .ok_or_else(|| steward::StewardError::Dyn("Missing factory column".into()))?;
        let factory_string_col = match factory_col.data_type() {
            DataType::Utf8 => factory_col.clone(),
            _ => cast(factory_col.as_ref(), &DataType::Utf8).map_err(|e| {
                steward::StewardError::Dyn(format!("Failed to cast factory: {}", e).into())
            })?,
        };
        let factories = factory_string_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                steward::StewardError::Dyn("Failed to downcast factory column".into())
            })?;

        // Format each row
        for i in 0..batch.num_rows() {
            let part_id = part_ids.value(i).to_string();
            let node_id = node_ids.value(i);
            let file_type = file_types.value(i);
            let version = versions.value(i);
            let size = if sizes.is_null(i) { -1 } else { sizes.value(i) };
            let factory = if factories.is_null(i) {
                None
            } else {
                Some(factories.value(i))
            };

            // For directories, decode content and show entries
            let (detail_display, dir_entries) = if file_type == "directory" {
                // Check if this is a dynamic directory
                let is_dynamic = factory.is_some() && factory != Some("tlogfs");

                if is_dynamic {
                    // Dynamic directories don't have parseable content
                    let factory_name = factory.unwrap_or("unknown");
                    (format!(" (dynamic: {})", factory_name), Vec::new())
                } else if contents.is_null(i) {
                    (" (0 entries)".to_string(), Vec::new())
                } else {
                    let content_bytes = contents.value(i);
                    // Decode directory entries for static directories
                    match decode_directory_entries(content_bytes) {
                        Ok(entries) => {
                            let count = entries.len();
                            let entry_word = if count == 1 { "entry" } else { "entries" };
                            (format!(" ({} {})", count, entry_word), entries)
                        }
                        Err(e) => {
                            // If decode fails, just show that it has content
                            (format!(" (decode error: {})", e), Vec::new())
                        }
                    }
                }
            } else {
                // For files, show size
                (format!(" ({})", format_byte_size(size)), Vec::new())
            };

            let operation = format!(
                "Node {} {} v{}{}",
                format_node_id(node_id),
                file_type,
                version,
                detail_display
            );

            // Convert directory entries to strings
            let entry_strings: Vec<String> = dir_entries
                .iter()
                .map(|entry| format!("{} → {}", entry.name, format_node_id(&entry.child_node_id)))
                .collect();

            let part_entry = partition_groups.entry(part_id).or_insert_with(Vec::new);
            part_entry.push((operation, entry_strings));
        }
    }

    // Format output grouped by partition using tree formatter
    let mut result = Vec::new();
    let mut sorted_partitions: Vec<_> = partition_groups.into_iter().collect();
    sorted_partitions.sort_by_key(|(part_id, _)| part_id.clone());

    for (part_id, ops) in sorted_partitions {
        let path_display = path_map
            .get(&part_id)
            .map(|s| s.as_str())
            .unwrap_or("<unknown>");

        // Add partition label (not indented)
        result.push(format!(
            "  Partition {} {}:",
            format_node_id(&part_id),
            path_display
        ));

        // Create a tree for the operations under this partition
        let mut partition_root = TreeNode::new(""); // Empty root node

        for (operation, entries) in ops {
            // Create a node for this operation
            let mut node = TreeNode::new(operation);

            // Add directory entries as children
            for entry in entries {
                node.add_child(TreeNode::new(entry));
            }

            partition_root.add_child(node);
        }

        // Format the tree and add to result, skipping the empty root line and indenting the rest
        let tree_output = tinyfs::tree_format::format_tree(&partition_root);
        for (i, line) in tree_output.lines().enumerate() {
            if i == 0 {
                continue; // Skip the empty root label
            }
            result.push(format!("    {}", line)); // Indent tree structure relative to partition label
        }
        result.push(String::new()); // Blank line after each partition
    }

    Ok(result)
}

/// Decode directory content and return entries with their names and node IDs
fn decode_directory_entries(
    content_bytes: &[u8],
) -> Result<Vec<tlogfs::schema::VersionedDirectoryEntry>, String> {
    // Use the public helper from tlogfs
    let entries = tlogfs::schema::decode_versioned_directory_entries(content_bytes)
        .map_err(|e| format!("Failed to decode: {}", e))?;

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::init::init_command;
    use crate::common::ShipContext;
    use std::path::PathBuf;
    use tempfile::TempDir;
    struct TestSetup {
        _temp_dir: TempDir,
        ship_context: ShipContext,
        #[allow(dead_code)] // Needed for test infrastructure
        pond_path: PathBuf,
    }

    impl TestSetup {
        async fn new() -> Result<Self> {
            let temp_dir = TempDir::new().expect("Failed to create temp directory");
            let pond_path = temp_dir.path().join("test_pond");

            // Create ship context for initialization
            let init_args = vec!["pond".to_string(), "init".to_string()];
            let ship_context = ShipContext::new(Some(pond_path.clone()), init_args.clone());

            // Initialize pond
            init_command(&ship_context, None, None)
                .await
                .expect("Failed to initialize pond");

            Ok(TestSetup {
                _temp_dir: temp_dir,
                ship_context,
                pond_path,
            })
        }
    }

    #[tokio::test]
    async fn test_show_empty_pond() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");

        let mut results = Vec::new();
        show_command(&setup.ship_context, "detailed", |output| {
            results.push(output);
        })
        .await
        .expect("Show command failed");

        // Should have at least the pond initialization transaction
        assert!(
            results.len() >= 1,
            "Should have at least initialization transaction"
        );

        // Check that output contains transaction information
        let output = results.join("");
        assert!(
            output.contains("Transaction"),
            "Should contain transaction information"
        );
    }

    #[tokio::test]
    async fn test_show_command_format() {
        use crate::common::ShipContext;
        use steward::Ship;
        use tempfile::tempdir;

        // Create a temporary pond using steward (like production)
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        // Initialize pond using steward - this creates the full Delta Lake setup
        let mut ship = Ship::create_pond(&pond_path)
            .await
            .expect("Failed to initialize pond");

        // Add a transaction with some file operations
        let args = vec!["test_command".to_string(), "test_arg".to_string()];
        ship.transact(args, |_tx, fs| {
            Box::pin(async move {
                let data_root = fs
                    .root()
                    .await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                tinyfs::async_helpers::convenience::create_file_path(
                    &data_root,
                    "/example.txt",
                    b"test content for show",
                )
                .await
                .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                Ok(())
            })
        })
        .await
        .expect("Failed to execute test transaction");

        // Create ship context for show command
        let ship_context = ShipContext::new(Some(pond_path.clone()), vec!["test".to_string()]);

        // Capture show command output
        let mut captured_output = String::new();
        show_command(&ship_context, "detailed", |output: String| {
            captured_output.push_str(&output);
        })
        .await
        .expect("Show command should work");

        println!("Show command output:\n{}", captured_output);

        // Test the new transaction sequence based format:

        // 1. Should have transaction sequence headers
        assert!(
            captured_output.contains(TRANSACTION_LABEL),
            "Should contain transaction headers"
        );

        // 2. Should show partition groups with paths
        assert!(
            captured_output.contains("Partition"),
            "Should contain partition information"
        );
        // The root partition should show "/" path (format is "Partition 00000000 /:" - no entry count on partition line)
        assert!(
            captured_output.contains("/:"),
            "Should show root directory path in partition"
        );

        // 3. Should show the file we created
        assert!(
            captured_output.contains("file:data"),
            "Should show the file we created"
        );

        // 4. Should not be empty
        assert!(
            !captured_output.trim().is_empty(),
            "Show output should not be empty"
        );
    }
}
