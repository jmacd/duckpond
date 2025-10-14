use anyhow::{Result, anyhow};
use arrow::array::Array as _;

use crate::common::{format_node_id, ShipContext};

/// Show pond contents with a closure for handling output
pub async fn show_command<F>(ship_context: &ShipContext, mode: &str, mut handler: F) -> Result<()>
where
    F: FnMut(String),
{
    let mut ship = ship_context.open_pond().await?;
    
    // Use read transaction for consistent filesystem access (show is read-only)
    let tx = ship.begin_transaction(steward::TransactionOptions::read(vec!["show".to_string()])).await
        .map_err(|e| anyhow!("Failed to begin read transaction: {}", e))?;
    
    let result = {
        let fs = &*tx;
        show_filesystem_transactions(&tx, fs, mode).await
            .map_err(|e| anyhow!("Failed to access data filesystem: {}", e))?
    };
    
    tx.commit().await
        .map_err(|e| anyhow!("Failed to commit read transaction: {}", e))?;

    handler(result);
    Ok(())
}

async fn show_filesystem_transactions(
    tx: &steward::StewardTransactionGuard<'_>,
    _fs: &tinyfs::FS,
    mode: &str
) -> Result<String, steward::StewardError> {
    // Get data persistence from the transaction guard
    let persistence = tx.data_persistence()
        .map_err(|e| steward::StewardError::DataInit(e))?;
    
    // Get commit history from the filesystem
    let commit_history = persistence.get_commit_history(None).await
        .map_err(|e| steward::StewardError::DataInit(e))?;
    
    if commit_history.is_empty() {
        return Ok("No transactions found in this filesystem.".to_string());
    }
    
    // Get pond path from persistence for control table access
    let store_path = persistence.store_path();
    let pond_path = std::path::Path::new(store_path)
        .parent()
        .ok_or_else(|| steward::StewardError::Dyn("Invalid store path".into()))?
        .to_path_buf();
    
    // Route to appropriate display mode
    match mode {
        "brief" => show_brief_mode(&commit_history, store_path).await,
        "concise" => show_concise_mode(&commit_history).await,
        "detailed" => show_detailed_mode(&commit_history, store_path, &pond_path).await,
        _ => Err(steward::StewardError::Dyn(
            format!("Invalid mode '{}'. Use 'brief', 'concise', or 'detailed'.", mode).into()
        ))
    }
}

/// Show brief summary statistics about the pond
async fn show_brief_mode(
    commit_history: &[deltalake::kernel::CommitInfo],
    store_path: &str
) -> Result<String, steward::StewardError> {
    use std::collections::HashMap;
    
    let mut output = String::new();
    
    // Open the pond and use transaction guard for DataFusion context access
    let mut persistence = tlogfs::OpLogPersistence::open(store_path)
        .await
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to open persistence: {}", e).into()))?;
    
    let mut tx = persistence.begin(1)
        .await
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to begin transaction: {}", e).into()))?;
    
    let session_ctx = tx.session_context()
        .await
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to get session context: {}", e).into()))?;
    
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
    
    let df = session_ctx.sql(partition_stats_sql)
        .await
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to query partition stats: {}", e).into()))?;
    
    let batches = df.collect()
        .await
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to collect partition stats: {}", e).into()))?;
    
    // Parse the results into our PartitionStats structure
    let mut partition_map: HashMap<String, PartitionStats> = HashMap::new();
    
    use arrow::array::{Array, StringArray, Int64Array};
    use arrow::datatypes::DataType;
    use arrow_cast::cast;
    
    for batch in batches {
        // Cast part_id from Dictionary to plain Utf8 if needed
        let part_id_col = batch.column_by_name("part_id")
            .ok_or_else(|| steward::StewardError::Dyn("Missing part_id column".into()))?;
        let part_id_string_col = match part_id_col.data_type() {
            DataType::Utf8 => part_id_col.clone(),
            _ => cast(part_id_col.as_ref(), &DataType::Utf8)
                .map_err(|e| steward::StewardError::Dyn(format!("Failed to cast part_id to Utf8: {}", e).into()))?,
        };
        let part_ids = part_id_string_col.as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| steward::StewardError::Dyn(
                format!("Failed to downcast part_id column. Actual type: {:?}", part_id_col.data_type()).into()
            ))?;
        let total_rows = batch.column_by_name("total_rows")
            .ok_or_else(|| steward::StewardError::Dyn("Missing total_rows column".into()))?
            .as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| steward::StewardError::Dyn("Failed to downcast total_rows column".into()))?;
        let directory_rows = batch.column_by_name("directory_rows")
            .ok_or_else(|| steward::StewardError::Dyn("Missing directory_rows column".into()))?
            .as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| steward::StewardError::Dyn("Failed to downcast directory_rows column".into()))?;
        let distinct_files = batch.column_by_name("distinct_files")
            .ok_or_else(|| steward::StewardError::Dyn("Missing distinct_files column".into()))?
            .as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| steward::StewardError::Dyn("Failed to downcast distinct_files column".into()))?;
        let file_versions = batch.column_by_name("file_version_rows")
            .ok_or_else(|| steward::StewardError::Dyn("Missing file_version_rows column".into()))?
            .as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| steward::StewardError::Dyn("Failed to downcast file_versions column".into()))?;
        
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
    let table = deltalake::open_table(store_path)
        .await
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to open Delta table: {}", e).into()))?;
    
    let snapshot = table.snapshot()
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
    log::info!("Resolving partition paths via TinyFS...");
    let root = tx.root().await
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to get root: {}", e).into()))?;
    
    let matches = root.collect_matches("**/*").await
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to collect matches: {}", e).into()))?;
    
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
    output.push_str("╔════════════════════════════════════════════════════════════════════════════╗\n");
    output.push_str("║                            POND SUMMARY                                    ║\n");
    output.push_str("╚════════════════════════════════════════════════════════════════════════════╝\n");
    output.push_str("\n");
    
    output.push_str(&format!("  Transactions       : {}\n", commit_history.len()));
    output.push_str(&format!("  Delta Lake Version : {}\n", table.version().unwrap_or(0)));
    output.push_str("\n");
    
    output.push_str("  Storage Statistics\n");
    output.push_str("  ──────────────────\n");
    output.push_str(&format!("  Parquet Files      : {}\n", total_parquet_files));
    output.push_str(&format!("  Total Size         : {}\n", format_byte_size(total_parquet_bytes)));
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
        
        output.push_str(&format!("\n  {} {}\n", format_node_id(part_id), path_display));
        output.push_str(&format!("    {} rows ({} dir, {} files, {} versions)\n",
            stats.total_rows,
            stats.directory_rows,
            stats.distinct_files,
            stats.file_versions
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

/// Show concise one-line-per-transaction output
async fn show_concise_mode(
    commit_history: &[deltalake::kernel::CommitInfo]
) -> Result<String, steward::StewardError> {
    // TODO: Implement concise mode
    Ok(format!("Concise mode - {} transactions (not yet implemented)\n", commit_history.len()))
}

/// Query control table for transaction commands
async fn query_transaction_commands(
    control_table: &steward::ControlTable
) -> Result<std::collections::HashMap<i64, Vec<String>>, steward::StewardError> {
    use std::collections::HashMap;
    use arrow::array::{Array, Int64Array, ListArray, StringArray};
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;
    
    // Use DataFusion to query control table
    let ctx = SessionContext::new();
    ctx.register_table("transactions", Arc::new(control_table.table().clone()))
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to register control table: {}", e).into()))?;
    
    // Query for begin records which have the cli_args
    let df = ctx.sql(
        "SELECT txn_seq, cli_args 
         FROM transactions 
         WHERE record_type = 'begin'
         ORDER BY txn_seq"
    ).await
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to query commands: {}", e).into()))?;
    
    let batches = df.collect().await
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to collect command results: {}", e).into()))?;
    
    let mut command_map: HashMap<i64, Vec<String>> = HashMap::new();
    
    for batch in batches {
        let txn_seq_col = batch.column_by_name("txn_seq")
            .ok_or_else(|| steward::StewardError::Dyn("Missing txn_seq column".into()))?;
        let txn_seqs = txn_seq_col.as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| steward::StewardError::Dyn("Failed to downcast txn_seq".into()))?;
        
        let cli_args_col = batch.column_by_name("cli_args")
            .ok_or_else(|| steward::StewardError::Dyn("Missing cli_args column".into()))?;
        let cli_args_list = cli_args_col.as_any().downcast_ref::<ListArray>()
            .ok_or_else(|| steward::StewardError::Dyn("Failed to downcast cli_args".into()))?;
        
        for i in 0..batch.num_rows() {
            let txn_seq = txn_seqs.value(i);
            
            // Extract the list of strings for this row
            let mut args = Vec::new();
            if !cli_args_list.is_null(i) {
                let args_array = cli_args_list.value(i);
                let string_array = args_array.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| steward::StewardError::Dyn("Failed to downcast args array".into()))?;
                
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
    store_path: &str,
    pond_path: &std::path::Path,
) -> Result<String, steward::StewardError> {
    use std::collections::HashMap;
    
    let mut output = String::new();
    
    // Open control table to get command information
    let control_table_path = pond_path.join("control");
    let control_table = steward::ControlTable::new(control_table_path.to_str().unwrap())
        .await
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to open control table: {}", e).into()))?;
    
    // Query control table for transaction commands
    // Build a map of txn_seq -> cli_args
    let command_map = query_transaction_commands(&control_table).await?;
    
    // Open persistence to query OpLog directly
    let mut persistence = tlogfs::OpLogPersistence::open(store_path)
        .await
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to open persistence: {}", e).into()))?;
    
    let mut tx = persistence.begin(0)
        .await
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to begin transaction: {}", e).into()))?;
    
    let session_ctx = tx.session_context()
        .await
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to get session context: {}", e).into()))?;
    
    // Build path map by traversing TinyFS (same as brief mode)
    log::info!("Resolving partition paths via TinyFS...");
    let mut path_map: HashMap<String, String> = HashMap::new();
    
    let root = tx.root().await
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to get root: {}", e).into()))?;
    
    let matches = root.collect_matches("**/*").await
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to collect matches: {}", e).into()))?;
    
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
    
    // Query distinct transaction sequences in descending order (newest first)
    let txn_seq_sql = "
        SELECT DISTINCT txn_seq
        FROM delta_table
        WHERE txn_seq IS NOT NULL
        ORDER BY txn_seq DESC
    ";
    
    let df = session_ctx.sql(txn_seq_sql)
        .await
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to query transaction sequences: {}", e).into()))?;
    
    let batches = df.collect()
        .await
        .map_err(|e| steward::StewardError::Dyn(format!("Failed to collect transaction sequences: {}", e).into()))?;
    
    use arrow::array::Int64Array;
    
    // Extract transaction sequences
    let mut txn_sequences = Vec::new();
    for batch in batches {
        let txn_seq_col = batch.column_by_name("txn_seq")
            .ok_or_else(|| steward::StewardError::Dyn("Missing txn_seq column".into()))?;
        let txn_seq_array = txn_seq_col.as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| steward::StewardError::Dyn("Failed to downcast txn_seq column".into()))?;
        
        for i in 0..batch.num_rows() {
            if !txn_seq_array.is_null(i) {
                txn_sequences.push(txn_seq_array.value(i));
            }
        }
    }
    
    if txn_sequences.is_empty() {
        return Ok("No transactions with sequence numbers found (legacy data uses timestamps only).\n".to_string());
    }
    
    // For each transaction sequence, query all operations in that transaction
    for txn_seq in txn_sequences {
        output.push_str(&format!("╔══════════════════════════════════════════════════════════════╗\n"));
        output.push_str(&format!("║  Transaction Sequence: {:44} ║\n", txn_seq));
        output.push_str(&format!("╚══════════════════════════════════════════════════════════════╝\n"));
        
        // Display command if available
        if let Some(cli_args) = command_map.get(&txn_seq) {
            if !cli_args.is_empty() {
                output.push_str(&format!("  Command: {}\n", cli_args.join(" ")));
            }
        }
        output.push_str("\n");
        
        // Query all operations for this transaction sequence
        let ops_sql = format!(
            "SELECT part_id, node_id, file_type, version, timestamp, size
             FROM delta_table
             WHERE txn_seq = {}
             ORDER BY part_id, node_id, version",
            txn_seq
        );
        
        let df = session_ctx.sql(&ops_sql)
            .await
            .map_err(|e| steward::StewardError::Dyn(format!("Failed to query operations for txn_seq {}: {}", txn_seq, e).into()))?;
        
        let batches = df.collect()
            .await
            .map_err(|e| steward::StewardError::Dyn(format!("Failed to collect operations: {}", e).into()))?;
        
        // Format operations grouped by partition
        let formatted_ops = format_operations_from_batches(batches, &path_map)?;
        
        if formatted_ops.is_empty() {
            output.push_str("  (No operations found - this should not happen)\n");
        } else {
            for op in formatted_ops {
                output.push_str(&format!("  {}\n", op));
            }
        }
        output.push_str("\n");
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
    path_map: &std::collections::HashMap<String, String>
) -> Result<Vec<String>, steward::StewardError> {
    use std::collections::HashMap;
    use arrow::array::{Array, StringArray, Int64Array};
    use arrow::datatypes::DataType;
    use arrow_cast::cast;
    
    // Group operations by partition for better readability
    let mut partition_groups: HashMap<String, Vec<String>> = HashMap::new();
    
    for batch in batches {
        // Extract columns
        let part_id_col = batch.column_by_name("part_id")
            .ok_or_else(|| steward::StewardError::Dyn("Missing part_id column".into()))?;
        let part_id_string_col = match part_id_col.data_type() {
            DataType::Utf8 => part_id_col.clone(),
            _ => cast(part_id_col.as_ref(), &DataType::Utf8)
                .map_err(|e| steward::StewardError::Dyn(format!("Failed to cast part_id: {}", e).into()))?,
        };
        let part_ids = part_id_string_col.as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| steward::StewardError::Dyn("Failed to downcast part_id column".into()))?;
        
        let node_id_col = batch.column_by_name("node_id")
            .ok_or_else(|| steward::StewardError::Dyn("Missing node_id column".into()))?;
        let node_id_string_col = match node_id_col.data_type() {
            DataType::Utf8 => node_id_col.clone(),
            _ => cast(node_id_col.as_ref(), &DataType::Utf8)
                .map_err(|e| steward::StewardError::Dyn(format!("Failed to cast node_id: {}", e).into()))?,
        };
        let node_ids = node_id_string_col.as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| steward::StewardError::Dyn("Failed to downcast node_id column".into()))?;
        
        let file_type_col = batch.column_by_name("file_type")
            .ok_or_else(|| steward::StewardError::Dyn("Missing file_type column".into()))?;
        let file_type_string_col = match file_type_col.data_type() {
            DataType::Utf8 => file_type_col.clone(),
            _ => cast(file_type_col.as_ref(), &DataType::Utf8)
                .map_err(|e| steward::StewardError::Dyn(format!("Failed to cast file_type: {}", e).into()))?,
        };
        let file_types = file_type_string_col.as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| steward::StewardError::Dyn("Failed to downcast file_type column".into()))?;
        
        let version_col = batch.column_by_name("version")
            .ok_or_else(|| steward::StewardError::Dyn("Missing version column".into()))?;
        let versions = version_col.as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| steward::StewardError::Dyn("Failed to downcast version column".into()))?;
        
        let size_col = batch.column_by_name("size")
            .ok_or_else(|| steward::StewardError::Dyn("Missing size column".into()))?;
        let sizes = size_col.as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| steward::StewardError::Dyn("Failed to downcast size column".into()))?;
        
        // Format each row
        for i in 0..batch.num_rows() {
            let part_id = part_ids.value(i).to_string();
            let node_id = node_ids.value(i);
            let file_type = file_types.value(i);
            let version = versions.value(i);
            let size = if sizes.is_null(i) { -1 } else { sizes.value(i) };
            
            // Don't show size for directories - it's not meaningful
            let size_display = if file_type == "directory" {
                String::new()
            } else {
                format!(" ({})", format_byte_size(size))
            };
            
            let operation = format!(
                "Node [{}] {} v{}{}",
                format_node_id(node_id),
                file_type,
                version,
                size_display
            );
            
            partition_groups.entry(part_id).or_insert_with(Vec::new).push(operation);
        }
    }
    
    // Format output grouped by partition
    let mut result = Vec::new();
    let mut sorted_partitions: Vec<_> = partition_groups.into_iter().collect();
    sorted_partitions.sort_by_key(|(part_id, _)| part_id.clone());
    
    for (part_id, ops) in sorted_partitions {
        let entry_word = if ops.len() == 1 { "entry" } else { "entries" };
        let path_display = path_map.get(&part_id).map(|s| s.as_str()).unwrap_or("<unknown>");
        result.push(format!("Partition {} {} ({} {}):", format_node_id(&part_id), path_display, ops.len(), entry_word));
        for op in ops {
            result.push(format!("  └─ {}", op));
        }
    }
    
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use crate::common::ShipContext;
    use crate::commands::init::init_command;
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
            init_command(&ship_context).await
                .expect("Failed to initialize pond");
            
            Ok(TestSetup {
                _temp_dir: temp_dir,
                ship_context,
                pond_path,
            })
        }

        // /// Create a test file in the host filesystem
        // async fn create_host_file(&self, filename: &str, content: &str) -> Result<PathBuf> {
        //     let file_path = self._temp_dir.path().join(filename);
        //     tokio::fs::write(&file_path, content).await?;
        //     Ok(file_path)
        // }

        // /// Copy a file to pond using copy command (creates transactions)
        // async fn copy_to_pond(&self, host_file: &str, pond_path: &str, format: &str) -> Result<()> {
        //     let host_path = self.create_host_file(host_file, "test content").await?;
        //     copy_command(&self.ship_context, &[host_path.to_string_lossy().to_string()], pond_path, format).await
        // }
    }

    #[tokio::test]
    async fn test_show_empty_pond() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");
        
        let mut results = Vec::new();
        show_command(&setup.ship_context, "detailed", |output| {
            results.push(output);
        }).await.expect("Show command failed");
        
        // Should have at least the pond initialization transaction
        assert!(results.len() >= 1, "Should have at least initialization transaction");
        
        // Check that output contains transaction information
        let output = results.join("");
        assert!(output.contains("Transaction"), "Should contain transaction information");
    }

    #[tokio::test]
    async fn test_show_command_format() {
        use tempfile::tempdir;
        use steward::Ship;
        use crate::common::ShipContext;
        
        // Create a temporary pond using steward (like production)
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        // Initialize pond using steward - this creates the full Delta Lake setup
        let mut ship = Ship::create_pond(&pond_path).await.expect("Failed to initialize pond");
        
        // Add a transaction with some file operations 
        let args = vec!["test_command".to_string(), "test_arg".to_string()];
        ship.transact(args, |_tx, fs| Box::pin(async move {
            let data_root = fs.root().await.map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            tinyfs::async_helpers::convenience::create_file_path(&data_root, "/example.txt", b"test content for show").await
                .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            Ok(())
        })).await.expect("Failed to execute test transaction");
        
        // Create ship context for show command
        let ship_context = ShipContext::new(Some(pond_path.clone()), vec!["test".to_string()]);
        
        // Capture show command output
        let mut captured_output = String::new();
        show_command(&ship_context, "detailed", |output: String| {
            captured_output.push_str(&output);
        }).await.expect("Show command should work");
        
        println!("Show command output:\n{}", captured_output);
        
        // Test the new transaction sequence based format:
        
        // 1. Should have transaction sequence headers
        assert!(captured_output.contains("Transaction Sequence:"), "Should contain transaction headers");
        
        // 2. Should show partition groups with paths
        assert!(captured_output.contains("Partition"), "Should contain partition information");
        // The root partition should show "/" path
        assert!(captured_output.contains("/ ("), "Should show root directory path in partition");
        
        // 3. Should show the file we created
        assert!(captured_output.contains("file:data"), "Should show the file we created");
        
        // 4. Should not be empty
        assert!(!captured_output.trim().is_empty(), "Show output should not be empty");
        
        // 5. Should have at least one transaction
        let transaction_count = captured_output.matches("Transaction Sequence:").count();
        assert!(transaction_count >= 1, "Should show at least one transaction");
    }
}
