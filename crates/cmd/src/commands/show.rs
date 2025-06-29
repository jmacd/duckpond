use anyhow::{Result, anyhow};
use datafusion::prelude::*;

use crate::common::{
    get_pond_path, format_node_id, format_file_size, truncate_string,
    parse_oplog_entry_content, parse_directory_content
};

pub async fn show_command() -> Result<()> {
    let store_path = get_pond_path()?;
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

    println!("=== DuckPond Operation Log ===");
    let mut transaction_count = 0;
    let mut entry_count = 0;

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
            transaction_count += 1;
            let part_id_key = part_ids.key(i).unwrap();
            let part_id = part_ids.values()
                .as_any().downcast_ref::<arrow_array::StringArray>().unwrap()
                .value(part_id_key as usize);
            let version = versions.value(i);
            let content_bytes = contents.value(i);

            println!("=== Transaction #{:03} ===", transaction_count);
            println!("  Partition: {}", format_node_id(part_id));
            
            // Only show timestamp if column exists
            if let Some(ts_array) = timestamps {
                let timestamp_us = ts_array.value(i);
                let dt = chrono::DateTime::from_timestamp(
                    timestamp_us / 1_000_000, 
                    ((timestamp_us % 1_000_000) * 1000) as u32
                ).unwrap_or_default();
                println!("  Timestamp: {} ({})", dt.format("%Y-%m-%d %H:%M:%S%.3f UTC"), timestamp_us);
            }
            
            println!("  Version:   {}", version);
            println!("  Content:   {} bytes", content_bytes.len());

            // Parse OplogEntry from content
            match parse_oplog_entry_content(content_bytes) {
                Ok(oplog_entry) => {
                    entry_count += 1;
                    println!("  ┌─ Entry #{}: {} [{}] -> {}", 
                        entry_count,
                        format_node_id(&oplog_entry.node_id),
                        oplog_entry.file_type,
                        format_node_id(&oplog_entry.part_id)
                    );
                    
                    // Parse type-specific content
                    match oplog_entry.file_type.as_str() {
                        "directory" => {
                            println!("  │  Directory entries: {} bytes", oplog_entry.content.len());
                            match parse_directory_content(&oplog_entry.content) {
                                Ok(dir_entries) => {
                                    if dir_entries.is_empty() {
                                        println!("  │  └─ (empty directory)");
                                    } else {
                                        for (idx, entry) in dir_entries.iter().enumerate() {
                                            let is_last = idx == dir_entries.len() - 1;
                                            let connector = if is_last { "└─" } else { "├─" };
                                            println!("  │  {} '{}' -> {}", 
                                                connector, entry.name, format_node_id(&entry.child_node_id));
                                        }
                                    }
                                }
                                Err(e) => {
                                    println!("  │  └─ Error parsing directory: {}", e);
                                }
                            }
                        },
                        "file" => {
                            let size = oplog_entry.content.len();
                            println!("  │  File size: {}", format_file_size(size));
                            if size > 0 && size <= 100 {
                                // Show preview for small files
                                let preview = String::from_utf8_lossy(&oplog_entry.content);
                                let preview = preview.replace('\n', "\\n").replace('\r', "\\r");
                                let preview = truncate_string(&preview, 60);
                                println!("  │  Preview: '{}'", preview);
                            }
                        },
                        "symlink" => {
                            let target = String::from_utf8_lossy(&oplog_entry.content);
                            println!("  │  Target: '{}'", target.trim());
                        },
                        _ => {
                            println!("  │  Unknown type: {} bytes", oplog_entry.content.len());
                        }
                    }
                    println!("  └─");
                }
                Err(e) => {
                    println!("  └─ Error parsing OplogEntry: {}", e);
                }
            }
            println!();
        }
    }

    println!("=== Summary ===");
    println!("Transactions: {}", transaction_count);
    println!("Entries: {}", entry_count);

    Ok(())
}
