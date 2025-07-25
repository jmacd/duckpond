use anyhow::{Result, anyhow};
use arrow_array::{StringArray, BinaryArray, Array};

use crate::common::{FilesystemChoice, parse_directory_content as parse_directory_entries, format_node_id, ShipContext};
use tinyfs::EntryType;

/// Show pond contents with a closure for handling output
pub async fn show_command<F>(ship_context: &ShipContext, filesystem: FilesystemChoice, mut handler: F) -> Result<()>
where
    F: FnMut(String),
{
    let ship = ship_context.create_ship().await?;
    
    // Get the correct filesystem path
    let store_path_str = match filesystem {
        FilesystemChoice::Data => ship.data_path(),
        FilesystemChoice::Control => {
            let control_path = steward::get_control_path(&std::path::Path::new(&ship.pond_path()));
            control_path.to_string_lossy().to_string()
        }
    };

    // Check if filesystem exists by trying to get the Delta table
    let delta_manager = tlogfs::DeltaTableManager::new();
    if delta_manager.get_table(&store_path_str).await.is_err() {
        return match filesystem {
            FilesystemChoice::Data => Err(anyhow!("Pond does not exist. Run 'pond init' first.")),
            FilesystemChoice::Control => Err(anyhow!("Control filesystem not initialized or pond does not exist.")),
        };
    }

    // For now, implement a simpler approach that counts Delta Lake versions
    // and skips the first one if it appears to be an empty initial commit
    let delta_manager = tlogfs::DeltaTableManager::new();
    let table = delta_manager.get_table(&store_path_str).await
        .map_err(|e| anyhow!("Failed to get Delta table: {}", e))?;
    
    let current_version = table.version();
    
    let mut output = String::new();
    
    // Check if we should skip the first version (table creation)
    // We'll skip version 0 if there are multiple versions and it has no data files
    let start_version = if current_version > 0 && table_version_is_empty(&table, 0).await? {
        1
    } else {
        0
    };
    
    for version in start_version..=current_version {
        let transaction_number = version - start_version + 1;
        
        // Try to read transaction metadata from control filesystem
        let tx_metadata = match read_transaction_metadata(&ship, version as u64).await {
            Ok(Some(tx_desc)) => {
                let command_display = tx_desc.args.join(" ");
                format!(" (Command: {})", command_display)
            }
            Ok(None) => " (No metadata)".to_string(),
            Err(e) => {
                let error_msg = format!("{}", e);
                diagnostics::log_debug!("Failed to read transaction metadata", error: error_msg);
                " (Metadata error)".to_string()
            }
        };
        
        output.push_str(&format!("=== Transaction #{:03}{} ===\n", transaction_number, tx_metadata));
        
        // Load the operations that were added in this specific transaction (delta)
        match load_operations_for_transaction(&store_path_str, version).await {
            Ok(operations) => {
                for op in operations {
                    output.push_str(&format!("    {}\n", op));
                }
            }
            Err(e) => {
                output.push_str(&format!("  Operations: (error reading operations: {})\n", e));
            }
        }
        output.push_str("\n");
    }

    handler(output);
    Ok(())
}

// Helper function to check if a table version has no data files
async fn table_version_is_empty(table: &deltalake::DeltaTable, version: i64) -> Result<bool> {
    // Try to open the table at the specific version and check if it has any files
    let version_table = deltalake::open_table_with_version(
        table.table_uri(), 
        version
    ).await.map_err(|e| anyhow!("Failed to open table at version {}: {}", version, e))?;
    
    Ok(version_table.get_files_count() == 0)
}

// Load operations that were added in a specific transaction by examining commit log
async fn load_operations_for_transaction(store_path: &str, version: i64) -> Result<Vec<String>> {
    // Open the table at the specific version to get files that were part of this commit
    let table = deltalake::open_table_with_version(store_path, version).await
        .map_err(|e| anyhow!("Failed to open table at version {}: {}", version, e))?;

    // Get the previous version to compare what's new
    let previous_version = if version > 0 { version - 1 } else { return Ok(vec![]); };
    
    let current_files: std::collections::HashSet<_> = table.get_file_uris()?.into_iter().collect();
    
    let previous_files: std::collections::HashSet<_> = if previous_version >= 0 {
        let prev_table = deltalake::open_table_with_version(store_path, previous_version).await
            .map_err(|e| anyhow!("Failed to open table at version {}: {}", previous_version, e))?;
        prev_table.get_file_uris()?.into_iter().collect()
    } else {
        std::collections::HashSet::new()
    };

    // Find files that were added in this version (delta)
    let new_files: Vec<_> = current_files.difference(&previous_files).collect();
    
    if new_files.is_empty() {
        return Ok(vec!["(no new operations in this transaction)".to_string()]);
    }

    // Now read only the new files and parse their operations directly
    // This is much more efficient than querying the entire table
    read_parquet_files_directly(&new_files, store_path).await
        .map(format_operations_by_partition)
}

// Read Parquet files directly instead of using SQL queries
async fn read_parquet_files_directly(file_uris: &[&String], store_path: &str) -> Result<Vec<(String, String)>> {
    let mut operations = Vec::new();
    
    for file_uri in file_uris {
        // Convert Delta Lake file URI to actual file path
        let file_path = if file_uri.starts_with("file://") {
            file_uri.strip_prefix("file://").unwrap_or(file_uri)
        } else if file_uri.starts_with('/') {
            file_uri.as_str()
        } else {
            // Relative path - combine with store_path
            &format!("{}/{}", store_path, file_uri)
        };
        
        diagnostics::log_debug!("Reading Parquet file directly", file_path: file_path);
        
        // Read Parquet file directly
        match read_single_parquet_file(file_path).await {
            Ok(file_operations) => {
                operations.extend(file_operations);
            }
            Err(e) => {
                let error_msg = format!("Failed to read Parquet file {}: {}", file_path, e);
                diagnostics::log_debug!("Parquet read error", error: error_msg);
                operations.push(("00000000".to_string(), format!("Error reading file: {}", error_msg)));
            }
        }
    }
    
    Ok(operations)
}

// Read a single Parquet file and extract operations
async fn read_single_parquet_file(file_path: &str) -> Result<Vec<(String, String)>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    
    // Extract part_id from the file path (Delta Lake partitioning)
    // Path format: .../part_id=XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX/part-xxxxx.parquet
    let part_id = extract_part_id_from_path(file_path)?;
    
    // Open the Parquet file
    let file = std::fs::File::open(file_path)
        .map_err(|e| anyhow!("Failed to open Parquet file {}: {}", file_path, e))?;
    
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| anyhow!("Failed to create Parquet reader: {}", e))?;
    
    let reader = builder.build()
        .map_err(|e| anyhow!("Failed to build Parquet reader: {}", e))?;
    
    let mut operations = Vec::new();
    
    // Read all record batches
    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| anyhow!("Failed to read Parquet batch: {}", e))?;
        
        // Get schema - no need to look for part_id since it's in the path
        let schema = batch.schema();
        let column_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let columns_debug = format!("{:?}", column_names);
        diagnostics::log_debug!("Parquet columns: {columns_debug}", columns_debug: &columns_debug);
        
        // Find column indices (part_id comes from path, not schema)
        let node_id_idx = schema.index_of("node_id")
            .map_err(|_| anyhow!("node_id column not found in schema. Available columns: {:?}", column_names))?;
        let file_type_idx = schema.index_of("file_type")
            .map_err(|_| anyhow!("file_type column not found in schema. Available columns: {:?}", column_names))?;
        let content_idx = schema.index_of("content")
            .map_err(|_| anyhow!("content column not found in schema. Available columns: {:?}", column_names))?;
        
        // Get temporal metadata columns (optional for FileSeries)
        let min_event_time_idx = schema.index_of("min_event_time").ok();
        let max_event_time_idx = schema.index_of("max_event_time").ok();
        
        // Get columns using the correct indices
        let node_id_array = batch.column(node_id_idx);
        let file_type_array = batch.column(file_type_idx);
        let content_array = batch.column(content_idx);
        
        // Handle different file_type column types
        let node_ids = if let Some(string_array) = node_id_array.as_any().downcast_ref::<StringArray>() {
            string_array
        } else {
            return Err(anyhow!("node_id column is not a StringArray, actual type: {:?}", node_id_array.data_type()));
        };
        
        let file_types = if let Some(string_array) = file_type_array.as_any().downcast_ref::<StringArray>() {
            string_array
        } else {
            return Err(anyhow!("file_type column is not a StringArray, actual type: {:?}", file_type_array.data_type()));
        };
        
        let contents = if let Some(binary_array) = content_array.as_any().downcast_ref::<BinaryArray>() {
            binary_array
        } else {
            return Err(anyhow!("content column is not a BinaryArray, actual type: {:?}", content_array.data_type()));
        };
        
        // Get temporal metadata arrays if available
        let min_event_times = min_event_time_idx.and_then(|idx| 
            batch.column(idx).as_any().downcast_ref::<arrow_array::Int64Array>()
        );
        let max_event_times = max_event_time_idx.and_then(|idx| 
            batch.column(idx).as_any().downcast_ref::<arrow_array::Int64Array>()
        );
        
        for i in 0..batch.num_rows() {
            // part_id comes from the file path, not the data
            let node_id = node_ids.value(i);
            let file_type_str = file_types.value(i);
            let content_bytes = contents.value(i);
            
            // Extract temporal metadata if available
            let temporal_range = match (min_event_times, max_event_times) {
                (Some(min_arr), Some(max_arr)) if !min_arr.is_null(i) && !max_arr.is_null(i) => {
                    Some((min_arr.value(i), max_arr.value(i)))
                },
                _ => None,
            };
            
            // Parse file_type from string
            let file_type = match file_type_str {
                "directory" => EntryType::Directory,
                "file:data" => EntryType::FileData,
                "file:table" => EntryType::FileTable,
                "file:series" => EntryType::FileSeries,
                "symlink" => EntryType::Symlink,
                _ => return Err(anyhow!("Unknown file_type: {}", file_type_str)),
            };
            
            // Parse content based on file type
            match parse_direct_content(&part_id, node_id, file_type, content_bytes, temporal_range) {
                Ok(description) => {
                    operations.push((part_id.clone(), description));
                },
                Err(e) => {
                    operations.push((part_id.clone(), format!("Error parsing entry {}/{}: {}", format_node_id(&part_id), format_node_id(node_id), e)));
                }
            }
        }
    }
    
    Ok(operations)
}

// Extract part_id from Delta Lake partitioned file path
fn extract_part_id_from_path(file_path: &str) -> Result<String> {
    // Path format: .../part_id=XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX/part-xxxxx.parquet
    if let Some(part_start) = file_path.find("part_id=") {
        let after_equals = &file_path[part_start + 8..]; // Skip "part_id="
        if let Some(slash_pos) = after_equals.find('/') {
            Ok(after_equals[..slash_pos].to_string())
        } else {
            Err(anyhow!("Invalid partitioned path format: {}", file_path))
        }
    } else {
        Err(anyhow!("No part_id found in path: {}", file_path))
    }
}

// Parse oplog content based on entry type  
fn parse_direct_content(_part_id: &str, node_id: &str, file_type: EntryType, content: &[u8], temporal_range: Option<(i64, i64)>) -> Result<String> {
    match file_type {
        EntryType::Directory => {
            // Directory content is still Arrow IPC encoded VersionedDirectoryEntry records
            match parse_directory_entries(content) {
                Ok(entries) => {
                    if entries.is_empty() {
                        Ok(format!("Directory (empty) [{}]", format_node_id(node_id)))
                    } else {
                        let mut descriptions = Vec::new();
                        for entry in entries {
                            descriptions.push(format!("        {} ▸ {}", entry.name, entry.child_node_id));
                        }
                        Ok(format!("Directory [{}] with {} entries:\n{}", format_node_id(node_id), descriptions.len(), descriptions.join("\n")))
                    }
                }
                Err(_) => {
                    let content_preview = format_content_preview(content);
                    Ok(format!("Directory [{}] (parse error): {}", format_node_id(node_id), content_preview))
                }
            }
        }
        EntryType::FileData => {
            // Regular file content - show preview with node ID
            let content_preview = format_content_preview(content);
            Ok(format!("FileData [{}]: {}", format_node_id(node_id), content_preview))
        }
        EntryType::FileTable => {
            // Table file (Parquet) - show as binary data with node ID  
            Ok(format!("FileTable [{}]: Parquet data ({} bytes)", format_node_id(node_id), content.len()))
        }
        EntryType::FileSeries => {
            // Series file content - show preview with node ID and temporal metadata
            let content_preview = format_content_preview(content);
            let temporal_info = match temporal_range {
                Some((min_time, max_time)) => format!(" (temporal: {} to {})", min_time, max_time),
                None => " (temporal: missing)".to_string(),
            };
            Ok(format!("FileSeries [{}]: {}{}", format_node_id(node_id), content_preview, temporal_info))
        }
        EntryType::Symlink => {
            // Symlink content is the target path as UTF-8
            match String::from_utf8(content.to_vec()) {
                Ok(target) => Ok(format!("Symlink [{}] -> {}", format_node_id(node_id), target)),
                Err(_) => {
                    let content_preview = format_content_preview(content);
                    Ok(format!("Symlink [{}] (invalid UTF-8): {}", format_node_id(node_id), content_preview))
                }
            }
        }
    }
}

// Format content preview with proper handling of binary vs text data
fn format_content_preview(content: &[u8]) -> String {
    // Check if content looks like text (mostly printable ASCII with some control chars allowed)
    let is_likely_text = content.iter().take(100).all(|&b| {
        b.is_ascii() && (b.is_ascii_graphic() || b.is_ascii_whitespace())
    });
    
    if is_likely_text {
        // Handle as text content
        if content.len() > 30 {
            let preview = String::from_utf8_lossy(&content[..30]);
            let quoted = quote_newlines(&preview);
            format!("\"{}\"... ({} bytes)", quoted, content.len())
        } else {
            let content_str = String::from_utf8_lossy(content);
            let quoted = quote_newlines(&content_str);
            format!("\"{}\" ({} bytes)", quoted, content.len())
        }
    } else {
        // Handle as binary data
        if content.len() == 0 {
            "Empty file (0 bytes)".to_string()
        } else {
            format!("Binary data ({} bytes)", content.len())
        }
    }
}

// Quote newlines in content preview
fn quote_newlines(s: &str) -> String {
    s.replace('\n', "\\n").replace('\r', "\\r").replace('\t', "\\t")
}

// Format operations grouped by partition with headers and better alignment
fn format_operations_by_partition(operations: Vec<(String, String)>) -> Vec<String> {
    use std::collections::HashMap;
    
    // Group operations by partition
    let mut partition_groups: HashMap<String, Vec<String>> = HashMap::new();
    for (part_id, operation) in operations {
        partition_groups.entry(part_id).or_insert_with(Vec::new).push(operation);
    }
    
    let mut result = Vec::new();
    
    // Sort partitions for consistent output
    let mut sorted_partitions: Vec<_> = partition_groups.into_iter().collect();
    sorted_partitions.sort_by_key(|(part_id, _)| part_id.clone());
    
    for (part_id, ops) in sorted_partitions {
        // More visually distinct partition header
        let entry_word = if ops.len() == 1 { "entry" } else { "entries" };
        result.push(format!("    ▌ Partition {} ({} {}):", format_node_id(&part_id), ops.len(), entry_word));
        for op in ops {
            result.push(format!("      {}", op));
        }
    }
    
    result
}

/// Read transaction metadata from the control filesystem
async fn read_transaction_metadata(ship: &steward::Ship, txn_seq: u64) -> Result<Option<steward::TxDesc>, anyhow::Error> {
    ship.read_transaction_metadata(txn_seq).await
        .map_err(|e| anyhow!("Failed to read transaction metadata: {}", e))
}
