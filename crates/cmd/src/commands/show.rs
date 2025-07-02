use anyhow::{Result, anyhow};
use datafusion::prelude::*;
use std::path::PathBuf;

use crate::common::{
    get_pond_path_with_override, format_node_id, format_file_size, truncate_string,
    parse_oplog_entry_content, parse_directory_content
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
    let delta_manager = tinylogfs::DeltaTableManager::new();
    if delta_manager.get_table(&store_path_str).await.is_err() {
        return Err(anyhow!("Pond does not exist. Run 'pond init' first."));
    }

    // Use DataFusion to query the raw delta records
    let ctx = SessionContext::new();
    
    // Register the delta table directly to get Record schema (part_id, timestamp, version, content)
    let table = delta_manager.get_table(&store_path_str).await?;
    ctx.register_table("raw_records", std::sync::Arc::new(table))?;

    // Query all records ordered by transaction boundaries
    let df = ctx.sql("SELECT * FROM raw_records ORDER BY version").await?;
    let batches = df.collect().await?;

    let mut output = String::new();
    output.push_str("=== DuckPond Operation Log ===\n");
    
    // Group records by version (transaction sequence) for better display
    let mut records_by_sequence: std::collections::BTreeMap<i64, Vec<(String, Vec<u8>, Option<i64>)>> = std::collections::BTreeMap::new();

    for batch in &batches {
        let part_ids = batch.column_by_name("part_id")
            .ok_or_else(|| anyhow!("part_id column not found"))?
            .as_any().downcast_ref::<arrow_array::DictionaryArray<arrow_array::types::UInt16Type>>()
            .ok_or_else(|| anyhow!("part_id is not a DictionaryArray"))?;
        let versions = batch.column_by_name("version")
            .ok_or_else(|| anyhow!("version column not found"))?
            .as_any().downcast_ref::<arrow_array::Int64Array>()
            .ok_or_else(|| anyhow!("version is not an Int64Array"))?;
        let contents = batch.column_by_name("content")
            .ok_or_else(|| anyhow!("content column not found"))?
            .as_any().downcast_ref::<arrow_array::BinaryArray>()
            .ok_or_else(|| anyhow!("content is not a BinaryArray"))?;

        // Try to get timestamp column if it exists
        let timestamps = batch.column_by_name("timestamp")
            .and_then(|col| col.as_any().downcast_ref::<arrow_array::TimestampMicrosecondArray>());

        for i in 0..batch.num_rows() {
            let part_id_key = part_ids.key(i).unwrap();
            let part_id = part_ids.values()
                .as_any().downcast_ref::<arrow_array::StringArray>().unwrap()
                .value(part_id_key as usize);
            let version = versions.value(i);
            let content_bytes = contents.value(i);
            
            let timestamp_us = timestamps.map(|ts_array| ts_array.value(i));
            
            // Group records by version (transaction sequence)
            records_by_sequence.entry(version)
                .or_insert_with(Vec::new)
                .push((part_id.to_string(), content_bytes.to_vec(), timestamp_us));
        }
    }

    let mut transaction_count = 0;
    let mut entry_count = 0;

    // Display grouped transactions
    for (sequence_number, records) in records_by_sequence {
        transaction_count += 1;
        output.push_str(&format!("=== Transaction #{:03} ===\n", transaction_count));
        output.push_str(&format!("  Sequence Number: {}\n", sequence_number));
        output.push_str(&format!("  Operations: {}\n", records.len()));
        
        // Show timestamp from first record in transaction (they should all be similar)
        if let Some(timestamp_us) = records.first().and_then(|(_, _, ts)| *ts) {
            let dt = chrono::DateTime::from_timestamp(
                timestamp_us / 1_000_000, 
                ((timestamp_us % 1_000_000) * 1000) as u32
            ).unwrap_or_default();
            output.push_str(&format!("  Timestamp: {} ({})\n", dt.format("%Y-%m-%d %H:%M:%S%.3f UTC"), timestamp_us));
        }
        
        output.push_str("\n");
        
        // Display each operation within this transaction
        for (operation_idx, (part_id, content_bytes, _)) in records.iter().enumerate() {
            output.push_str(&format!("  ┌─ Operation #{}: {}\n", 
                operation_idx + 1,
                format_node_id(part_id)
            ));
            
            // Parse OplogEntry from content
            match parse_oplog_entry_content(content_bytes) {
                Ok(oplog_entry) => {
                    entry_count += 1;
                    output.push_str(&format!("  │  Entry: {} [{}] -> {}\n", 
                        format_node_id(&oplog_entry.node_id),
                        oplog_entry.file_type,
                        format_node_id(&oplog_entry.part_id)
                    ));
                    
                    // Parse type-specific content
                    match oplog_entry.file_type.as_str() {
                        "directory" => {
                            output.push_str(&format!("  │  Directory entries: {} bytes\n", oplog_entry.content.len()));
                            match parse_directory_content(&oplog_entry.content) {
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
                            let size = oplog_entry.content.len();
                            output.push_str(&format!("  │  File size: {}\n", format_file_size(size)));
                            if size > 0 && size <= 100 {
                                // Show preview for small files
                                let preview = String::from_utf8_lossy(&oplog_entry.content);
                                let preview = preview.replace('\n', "\\n").replace('\r', "\\r");
                                let preview = truncate_string(&preview, 60);
                                output.push_str(&format!("  │  Preview: '{}'\n", preview));
                            }
                        },
                        "symlink" => {
                            let target = String::from_utf8_lossy(&oplog_entry.content);
                            output.push_str(&format!("  │  Target: '{}'\n", target.trim()));
                        },
                        _ => {
                            output.push_str(&format!("  │  Unknown type: {} bytes\n", oplog_entry.content.len()));
                        }
                    }
                    output.push_str("  └─\n");
                }
                Err(e) => {
                    output.push_str(&format!("  └─ Error parsing OplogEntry: {}\n", e));
                }
            }
            output.push_str("\n");
        }
    }

    output.push_str("=== Summary ===\n");
    output.push_str(&format!("Transactions: {}\n", transaction_count));
    output.push_str(&format!("Entries: {}\n", entry_count));

    Ok(output)
}
