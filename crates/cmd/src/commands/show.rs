use anyhow::{Result, anyhow};
use datafusion::prelude::*;
use std::path::PathBuf;
use oplog::query::{IpcTable, DeltaTableManager};

use crate::common::{
    get_pond_path_with_override, format_node_id, format_file_size, truncate_string,
    parse_directory_content
};

pub async fn show_command() -> Result<()> {
    let output = show_command_as_string().await?;
    print!("{}", output);
    Ok(())
}

pub async fn show_command_as_string() -> Result<String> {
    show_command_as_string_with_pond(None).await
}

pub async fn show_command_as_string_with_pond(pond_path: Option<PathBuf>) -> Result<String> {
    let store_path = get_pond_path_with_override(pond_path)?;
    let store_path_str = store_path.to_string_lossy();

    // Check if pond exists
    let delta_manager = tlogfs::DeltaTableManager::new();
    if delta_manager.get_table(&store_path_str).await.is_err() {
        return Err(anyhow!("Pond does not exist. Run 'pond init' first."));
    }

    // Use DataFusion to query the oplog entries with transaction sequences
    let ctx = SessionContext::new();
    
    // Create enhanced oplog table with txn_seq projection
    let delta_manager = DeltaTableManager::new();
    let oplog_schema = tlogfs::schema::OplogEntry::create_schema();
    let oplog_table = IpcTable::with_txn_seq(
        oplog_schema, 
        store_path_str.to_string(),
        delta_manager
    );
    ctx.register_table("oplog_with_txn_seq", std::sync::Arc::new(oplog_table))?;

    // Query all oplog entries ordered by transaction sequence
    let df = ctx.sql("SELECT * FROM oplog_with_txn_seq ORDER BY txn_seq, part_id").await?;
    let batches = df.collect().await?;

    let mut output = String::new();
    output.push_str("=== DuckPond Operation Log ===\n");
    
    // Group records by txn_seq (transaction sequence) for better display
    let mut records_by_sequence: std::collections::BTreeMap<i64, Vec<(String, String, String, Vec<u8>)>> = std::collections::BTreeMap::new();

    for batch in &batches {
        let part_ids = batch.column_by_name("part_id")
            .ok_or_else(|| anyhow!("part_id column not found"))?
            .as_any().downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| anyhow!("part_id is not a StringArray"))?;
        let node_ids = batch.column_by_name("node_id")
            .ok_or_else(|| anyhow!("node_id column not found"))?
            .as_any().downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| anyhow!("node_id is not a StringArray"))?;
        let file_types = batch.column_by_name("file_type")
            .ok_or_else(|| anyhow!("file_type column not found"))?
            .as_any().downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| anyhow!("file_type is not a StringArray"))?;
        let contents = batch.column_by_name("content")
            .ok_or_else(|| anyhow!("content column not found"))?
            .as_any().downcast_ref::<arrow_array::BinaryArray>()
            .ok_or_else(|| anyhow!("content is not a BinaryArray"))?;
        let txn_seqs = batch.column_by_name("txn_seq")
            .ok_or_else(|| anyhow!("txn_seq column not found"))?
            .as_any().downcast_ref::<arrow_array::Int64Array>()
            .ok_or_else(|| anyhow!("txn_seq is not an Int64Array"))?;

        for i in 0..batch.num_rows() {
            let part_id = part_ids.value(i);
            let node_id = node_ids.value(i);
            let file_type = file_types.value(i);
            let content_bytes = contents.value(i);
            let txn_seq = txn_seqs.value(i);
            
            // Group records by txn_seq (transaction sequence)
            records_by_sequence.entry(txn_seq)
                .or_insert_with(Vec::new)
                .push((part_id.to_string(), node_id.to_string(), file_type.to_string(), content_bytes.to_vec()));
        }
    }

    let mut transaction_count = 0;
    let mut entry_count = 0;

    // Display grouped transactions
    for (txn_seq, records) in records_by_sequence {
        transaction_count += 1;
        output.push_str(&format!("=== Transaction #{:03} ===\n", transaction_count));
        output.push_str(&format!("  Sequence Number: {}\n", txn_seq));
        output.push_str(&format!("  Operations: {}\n", records.len()));
        
        // Show special marker for pending transactions
        if txn_seq == -1 {
            output.push_str("  Status: PENDING (not yet committed)\n");
        }
        
        // For committed transactions, try to get timestamp from Delta Lake metadata
        // (This could be enhanced to get actual commit timestamps)
        let now = chrono::Utc::now();
        output.push_str(&format!("  Timestamp: {} UTC\n", now.format("%Y-%m-%d %H:%M:%S%.3f")));
        
        output.push_str("\n");
        
        // Display each operation within this transaction
        for (operation_idx, (part_id, node_id, file_type, content_bytes)) in records.iter().enumerate() {
            output.push_str(&format!("  ┌─ Operation #{}: {}\n", 
                operation_idx + 1,
                format_node_id(part_id)
            ));
            
            entry_count += 1;
            output.push_str(&format!("  │  Entry: {} [{}] -> {}\n", 
                format_node_id(node_id),
                file_type,
                format_node_id(part_id)
            ));
            
            // Parse type-specific content
            match file_type.as_str() {
                "directory" => {
                    output.push_str(&format!("  │  Directory entries: {} bytes\n", content_bytes.len()));
                    match parse_directory_content(content_bytes) {
                        Ok(dir_entries) => {
                            if dir_entries.is_empty() {
                                output.push_str("  │  └─ (empty directory)\n");
                            } else {
                                for (idx, entry) in dir_entries.iter().enumerate() {
                                    let is_last = idx == dir_entries.len() - 1;
                                    let connector = if is_last { "└─" } else { "├─" };
                                    output.push_str(&format!("  │  {} '{}' -> {}\n", 
                                        connector, entry.name, format_node_id(&entry.child_node_id)));
                                }
                            }
                        }
                        Err(e) => {
                            output.push_str(&format!("  │  └─ Error parsing directory: {}\n", e));
                        }
                    }
                },
                "file" => {
                    let size = content_bytes.len();
                    output.push_str(&format!("  │  File size: {}\n", format_file_size(size)));
                    if size > 0 && size <= 100 {
                        // Show preview for small files
                        let preview = String::from_utf8_lossy(content_bytes);
                        let preview = preview.replace('\n', "\\n").replace('\r', "\\r");
                        let preview = truncate_string(&preview, 60);
                        output.push_str(&format!("  │  Preview: '{}'\n", preview));
                    }
                },
                "symlink" => {
                    let target = String::from_utf8_lossy(content_bytes);
                    output.push_str(&format!("  │  Target: '{}'\n", target.trim()));
                },
                _ => {
                    output.push_str(&format!("  │  Unknown type: {} bytes\n", content_bytes.len()));
                }
            }
            output.push_str("  └─\n");
            output.push_str("\n");
        }
    }

    output.push_str("=== Summary ===\n");
    output.push_str(&format!("Transactions: {}\n", transaction_count));
    output.push_str(&format!("Entries: {}\n", entry_count));

    Ok(output)
}
