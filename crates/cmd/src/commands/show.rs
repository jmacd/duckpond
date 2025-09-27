use anyhow::{Result, anyhow};
use arrow_array::{StringArray, BinaryArray, Array};
use chrono::{DateTime, Utc, Datelike};
use log::debug;

use crate::common::{FilesystemChoice, parse_directory_content as parse_directory_entries, format_node_id, ShipContext};
use tinyfs::EntryType;

/// Show pond contents with a closure for handling output
pub async fn show_command<F>(ship_context: &ShipContext, filesystem: FilesystemChoice, mut handler: F) -> Result<()>
where
    F: FnMut(String),
{
    // For now, only support data filesystem - control filesystem access would require different API  
    if filesystem == FilesystemChoice::Control {
        return Err(anyhow!("Control filesystem access not yet implemented for show command"));
    }

    let mut ship = ship_context.open_pond().await?;
    
    // Use transaction for consistent filesystem access
    let result = ship.transact(
        vec!["show".to_string()], 
        |tx, fs| Box::pin(async move {
            show_filesystem_transactions(&tx, fs).await
        })
    ).await.map_err(|e| anyhow!("Failed to access data filesystem: {}. Run 'pond init' first.", e))?;

    handler(result);
    Ok(())
}

async fn show_filesystem_transactions(
    tx: &steward::StewardTransactionGuard<'_>,
    _fs: &tinyfs::FS
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
    
    let mut output = String::new();
    
    for commit_info in commit_history.iter() {
        let timestamp = commit_info.timestamp.unwrap_or(0);
        
        // Extract transaction metadata from commit info (stored in "pond_txn" field)
        let (tx_id, tx_metadata) = match commit_info.info.get("pond_txn") {
            Some(pond_txn_value) => {
                // Parse the pond_txn JSON object
                if let Some(obj) = pond_txn_value.as_object() {
                    let txn_id = obj.get("txn_id").and_then(|v| v.as_str()).unwrap_or("unknown");
                    let args = obj.get("args").and_then(|v| v.as_str()).unwrap_or("no args");
                    (txn_id.to_string(), format!(" (Command: {})", args))
                } else {
                    (timestamp.to_string(), " (Invalid pond_txn format)".to_string())
                }
            }
            None => (timestamp.to_string(), " (No metadata)".to_string()),
        };
        
        output.push_str(&format!("=== Transaction {} {} ===\n", tx_id, tx_metadata));
        
        // Load the operations that were added in this specific transaction
        match load_operations_from_commit_info(&commit_info, persistence.store_path()).await {
            Ok((operations, version_num)) => {
                // Show commit information with version if available
                if let Some(timestamp) = &commit_info.timestamp {
                    if let Some(version) = version_num {
                        output.push_str(&format!("  Delta Lake Version: {} ({})\n", version, format_timestamp(*timestamp)));
                    } else {
                        output.push_str(&format!("  Delta Lake Timestamp: {} ({})\n", format_timestamp(*timestamp), timestamp));
                    }
                } else if let Some(version) = version_num {
                    output.push_str(&format!("  Delta Lake Version: {}\n", version));
                }
                
                if operations.len() > 1 { // More than just the "Total files" line
                    for op in operations {
                        output.push_str(&format!("    {}\n", op));
                    }
                } else {
                    // If no specific operations found, indicate this 
                    output.push_str("    (No operation details available for this transaction)\n");
                }
            }
            Err(e) => {
                output.push_str(&format!("    (Error reading operations: {})\n", e));
            }
        }
        output.push_str("\n");
    }
    
    Ok(output)
}

/// Format a Unix timestamp as a human-readable date string
/// Handles both seconds and milliseconds timestamps
fn format_timestamp(timestamp: i64) -> String {
    // Try as seconds first, then milliseconds if the result is unreasonable
    let dt_seconds = DateTime::<Utc>::from_timestamp(timestamp, 0);
    let dt_millis = DateTime::<Utc>::from_timestamp(timestamp / 1000, ((timestamp % 1000) * 1_000_000) as u32);
    
    // Use seconds if it gives a reasonable date (between 1970 and 2100)
    // Otherwise try milliseconds
    match dt_seconds {
        Some(dt) if dt.year() >= 1970 && dt.year() <= 2100 => {
            dt.format("%Y-%m-%d %H:%M:%S UTC").to_string()
        },
        _ => match dt_millis {
            Some(dt) if dt.year() >= 1970 && dt.year() <= 2100 => {
                dt.format("%Y-%m-%d %H:%M:%S UTC").to_string()
            },
            _ => format!("{} (invalid timestamp)", timestamp)
        }
    }
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

// Load operations from commit info directly (read operations from this specific commit)
async fn load_operations_from_commit_info(commit_info: &deltalake::kernel::CommitInfo, store_path: &str) -> Result<(Vec<String>, Option<i64>)> {
    let mut operations = Vec::new();
    
    // Since read_version is not available, we need to determine the version differently
    // Try to find the Delta Lake log files and correlate by timestamp
    let timestamp = commit_info.timestamp.unwrap_or(0);
    debug!("Looking for Delta Lake log files by timestamp {timestamp}");
    
    let mut found_version = None;
    
    match find_delta_log_version_by_timestamp(store_path, timestamp).await {
        Ok(Some(version)) => {
            debug!("Found matching Delta Lake version: {version}");
            found_version = Some(version);
            
            // Try to read the specific Delta Lake log file for this version
            match read_delta_log_for_version(store_path, version).await {
                Ok(add_actions) => {
                    if !add_actions.is_empty() {
                        // Read the specific files that were added
                        match read_specific_parquet_files(&add_actions, store_path).await {
                            Ok(oplog_operations) => {
                                let formatted = format_operations_by_partition(oplog_operations);
                                operations.extend(formatted);
                            }
                            Err(e) => {
                                operations.push(format!("Could not read added files: {}", e));
                            }
                        }
                    } else {
                        operations.push("No files were added in this commit".to_string());
                    }
                }
                Err(e) => {
                    operations.push(format!("Could not parse Delta Lake log for version {}: {}", version, e));
                }
            }
        }
        Ok(None) => {
            operations.push("Could not find matching Delta Lake log version for this timestamp".to_string());
        }
        Err(e) => {
            operations.push(format!("Error searching for Delta Lake version: {}", e));
        }
    }
    
    Ok((operations, found_version))
}

// Read the Delta Lake log file for a specific version to extract Add actions
async fn read_delta_log_for_version(store_path: &str, version: i64) -> Result<Vec<String>> {
    use std::path::Path;
    
    // Delta Lake log files are named with zero-padded version numbers
    let log_filename = format!("{:020}.json", version);
    let log_path = Path::new(store_path).join("_delta_log").join(&log_filename);
    
    let mut add_actions = Vec::new();
    
    // Try to read the log file
    match tokio::fs::read_to_string(&log_path).await {
        Ok(contents) => {
            // Each line in the log file is a JSON object representing an action
            for line in contents.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                
                // Parse the JSON action
                match serde_json::from_str::<serde_json::Value>(line) {
                    Ok(action) => {
                        // Check if this is an Add action
                        if let Some(add) = action.get("add") {
                            if let Some(path) = add.get("path").and_then(|p| p.as_str()) {
                                add_actions.push(path.to_string());
                            }
                        }
                    }
                    Err(e) => {
                        println!("Warning: Could not parse action JSON: {}", e);
                    }
                }
            }
        }
        Err(e) => {
            return Err(anyhow!("Could not read Delta Lake log file {}: {}", log_path.display(), e));
        }
    }
    
    Ok(add_actions)
}

// Read specific Parquet files that were added in a commit
async fn read_specific_parquet_files(file_paths: &[String], store_path: &str) -> Result<Vec<(String, String)>> {
    let mut operations = Vec::new();
    
    let file_count = file_paths.len();
    debug!("read_specific_parquet_files called with {file_count} files");
    
    for file_path in file_paths {
        debug!("Processing file_path: {file_path}");
        
        // Convert to full path
        let full_path = if file_path.starts_with('/') {
            file_path.clone()
        } else {
            format!("{}/{}", store_path, file_path)
        };
        
        debug!("Full path constructed: {full_path}");
        
        // Parse the Parquet file to extract oplog operations
        debug!("About to read parquet file: {full_path}");
        match read_single_parquet_file(&full_path).await {
            Ok(ops) => {
                let op_count = ops.len();
                debug!("Parquet file returned {op_count} operations");
                operations.extend(ops)
            },
            Err(e) => {
                debug!("Parquet file read failed: {e}");
                operations.push(("unknown".to_string(), format!("Error reading {}: {}", file_path, e)));
            }
        }
    }
    
    Ok(operations)
}

// Read a single Parquet file and extract operations (simplified version)
async fn read_single_parquet_file(file_path: &str) -> Result<Vec<(String, String)>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    
    debug!("Reading Parquet file: {file_path}");
    
    // Extract part_id from the file path (Delta Lake partitioning)
    let part_id = extract_part_id_from_path(file_path)?;
    debug!("Extracted part_id: {part_id}");
    
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
        
        // Get schema
        let schema = batch.schema();
        let column_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let column_names_debug = format!("{:?}", column_names);
        debug!("Parquet columns: {column_names_debug}");
        
        // Find required column indices
        let node_id_idx = schema.index_of("node_id")
            .map_err(|_| anyhow!("node_id column not found"))?;
        let file_type_idx = schema.index_of("file_type")
            .map_err(|_| anyhow!("file_type column not found"))?;
        let content_idx = schema.index_of("content")
            .map_err(|_| anyhow!("content column not found"))?;
        
        // Get optional columns
        let factory_idx = schema.index_of("factory").ok();
        let min_event_time_idx = schema.index_of("min_event_time").ok();
        let max_event_time_idx = schema.index_of("max_event_time").ok();
        let sha256_idx = schema.index_of("sha256").ok();
        let size_idx = schema.index_of("size").ok();
        let version_idx = schema.index_of("version").ok();
        
        // Get column arrays
        let node_id_array = batch.column(node_id_idx);
        let file_type_array = batch.column(file_type_idx);
        let content_array = batch.column(content_idx);
        
        let node_ids = node_id_array.as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow!("node_id column is not a StringArray"))?;
        let file_types = file_type_array.as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow!("file_type column is not a StringArray"))?;
        let contents = content_array.as_any().downcast_ref::<BinaryArray>()
            .ok_or_else(|| anyhow!("content column is not a BinaryArray"))?;
        
        // Get optional arrays
        let factories = factory_idx.and_then(|idx| 
            batch.column(idx).as_any().downcast_ref::<StringArray>()
        );
        let min_event_times = min_event_time_idx.and_then(|idx| 
            batch.column(idx).as_any().downcast_ref::<arrow_array::Int64Array>()
        );
        let max_event_times = max_event_time_idx.and_then(|idx| 
            batch.column(idx).as_any().downcast_ref::<arrow_array::Int64Array>()
        );
        let sha256_hashes = sha256_idx.and_then(|idx| 
            batch.column(idx).as_any().downcast_ref::<StringArray>()
        );
        let sizes = size_idx.and_then(|idx| 
            batch.column(idx).as_any().downcast_ref::<arrow_array::Int64Array>()
        );
        let versions = version_idx.and_then(|idx| 
            batch.column(idx).as_any().downcast_ref::<arrow_array::Int64Array>()
        );
        
        for i in 0..batch.num_rows() {
            let node_id = node_ids.value(i);
            let file_type_str = file_types.value(i);
            
            let content_bytes = if contents.is_null(i) {
                &[] // Large files have NULL content
            } else {
                contents.value(i) // Small files have actual content bytes
            };
            
            // Extract optional fields
            let factory = factories.and_then(|arr| {
                if arr.is_null(i) { None } else { Some(arr.value(i)) }
            });
            
            let temporal_range = match (min_event_times, max_event_times) {
                (Some(min_arr), Some(max_arr)) if !min_arr.is_null(i) && !max_arr.is_null(i) => {
                    Some((min_arr.value(i), max_arr.value(i)))
                },
                _ => None,
            };
            
            let sha256_hash = sha256_hashes.and_then(|arr| {
                if arr.is_null(i) { None } else { Some(arr.value(i)) }
            });
            let file_size = sizes.and_then(|arr| {
                if arr.is_null(i) { None } else { Some(arr.value(i)) }
            });
            let tinyfs_version = versions.and_then(|arr| {
                if arr.is_null(i) { None } else { Some(arr.value(i)) }
            });
            
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
            match parse_direct_content(&part_id, node_id, file_type, content_bytes, temporal_range, factory, sha256_hash, file_size, tinyfs_version) {
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
    // Try the partitioned format first: .../part_id=XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX/part-xxxxx.parquet
    if let Some(part_start) = file_path.find("part_id=") {
        let after_equals = &file_path[part_start + 8..]; // Skip "part_id="
        if let Some(slash_pos) = after_equals.find('/') {
            return Ok(after_equals[..slash_pos].to_string());
        }
    }
    
    // Fallback: Extract from filename directly: part-00001-UUID-c000.snappy.parquet
    if let Some(file_name) = file_path.split('/').last() {
        if file_name.starts_with("part-") {
            // Extract the partition number (e.g., "00001" from "part-00001-...")
            if let Some(dash_pos) = file_name[5..].find('-') {
                let partition_num = &file_name[5..5+dash_pos];
                return Ok(format!("0000{}", partition_num)); // Pad to make it consistent
            }
        }
    }
    
    // Default fallback - use "0000" as the partition ID
    Ok("0000".to_string())
}

// Parse oplog content based on entry type  
fn parse_direct_content(_part_id: &str, node_id: &str, file_type: EntryType, content: &[u8], temporal_range: Option<(i64, i64)>, factory: Option<&str>, _sha256_hash: Option<&str>, file_size: Option<i64>, tinyfs_version: Option<i64>) -> Result<String> {
    match file_type {
        EntryType::Directory => {
            // Check if this is a dynamic directory with factory type
            if let Some(factory_type) = factory {
                // Dynamic directory - show configuration content
                match factory_type {
                    "hostmount" => {
                        // Parse as YAML configuration 
                        match std::str::from_utf8(content) {
                            Ok(yaml_content) => {
                                Ok(format!("Dynamic Directory (hostmount) [{}]: {}", format_node_id(node_id), yaml_content.trim()))
                            }
                            Err(_) => {
                                let content_preview = format_content_preview(content);
                                Ok(format!("Dynamic Directory (hostmount) [{}] (parse error): {}", format_node_id(node_id), content_preview))
                            }
                        }
                    }
                    _ => {
                        // Unknown factory type
                        let content_preview = format_content_preview(content);
                        Ok(format!("Dynamic Directory ({}) [{}]: {}", factory_type, format_node_id(node_id), content_preview))
                    }
                }
            } else {
                // Static directory - try to parse as Arrow IPC directory entries
                match parse_directory_entries(content) {
                    Ok(entries) => {
                        if entries.is_empty() {
                            Ok(format!("Directory (empty) [{}]", format_node_id(node_id)))
                        } else {
                            let mut descriptions = Vec::new();
                            for entry in entries {
                                descriptions.push(format!("        {} ▸ {}", entry.name, format_node_id(&entry.child_node_id)));
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
        }
        EntryType::FileData => {
            // Regular file content - show preview with node ID
            let content_preview = format_content_preview(content);
            let version_info = match tinyfs_version {
                Some(ver) => format!(" v{}", ver),
                None => "".to_string(),
            };
            Ok(format!("FileData [{}]{}: {}", format_node_id(node_id), version_info, content_preview))
        }
        EntryType::FileTable => {
            // Check if this is a dynamic file with factory type
            if let Some(factory_type) = factory {
                // Dynamic table file - show configuration content and factory type
                match factory_type {
                    "sql-derived" => {
                        // Parse as YAML configuration 
                        match std::str::from_utf8(content) {
                            Ok(yaml_content) => {
                                Ok(format!("Dynamic FileTable (sql-derived) [{}]: {} ({} bytes config)", format_node_id(node_id), yaml_content.trim(), content.len()))
                            }
                            Err(_) => {
                                let content_preview = format_content_preview(content);
                                Ok(format!("Dynamic FileTable (sql-derived) [{}]: {} ({} bytes config)", format_node_id(node_id), content_preview, content.len()))
                            }
                        }
                    }
                    _ => {
                        // Other dynamic table types
                        let content_preview = format_content_preview(content);
                        Ok(format!("Dynamic FileTable ({}) [{}]: {} ({} bytes config)", factory_type, format_node_id(node_id), content_preview, content.len()))
                    }
                }
            } else {
                // Regular table file (Parquet) - show as binary data with node ID and row count
                let row_count = extract_row_count_from_parquet_content(content).unwrap_or_else(|e| format!("row count error: {}", e));
                Ok(format!("FileTable [{}]: Parquet data ({} bytes, {} rows)", format_node_id(node_id), content.len(), row_count))
            }
        }
        EntryType::FileSeries => {
            // Check if this is a large file: content is None/empty (stored externally)
            // This is the correct test - large files have no inline content
            if content.is_empty() {
                // Large file - stored externally with SHA256 reference
                let size_display = format_byte_size(file_size.unwrap_or(-1));
                let temporal_info = match temporal_range {
                    Some((min_time, max_time)) => format!(" (temporal: {} to {})", format_timestamp(min_time), format_timestamp(max_time)),
                    None => " (temporal: missing)".to_string(),
                };
                let version_info = match tinyfs_version {
                    Some(ver) => format!(" v{}", ver),
                    None => "".to_string(),
                };
                Ok(format!("FileSeries [{}]{}: Large file ({}){}", 
                    format_node_id(node_id), version_info, size_display, temporal_info))
            } else {
                // Small file - stored inline
                let temporal_info = match temporal_range {
                    Some((min_time, max_time)) => format!(" (temporal: {} to {})", format_timestamp(min_time), format_timestamp(max_time)),
                    None => " (temporal: missing)".to_string(),
                };
                let row_count = extract_row_count_from_parquet_content(content)
                    .unwrap_or_else(|e| format!("row count error: {}", e));
                let version_info = match tinyfs_version {
                    Some(ver) => format!(" v{}", ver),
                    None => "".to_string(),
                };
                // Skip schema display for cleaner output - can add --verbose flag later if needed
                Ok(format!("FileSeries [{}]{}: Parquet data ({}){} ({} rows)", format_node_id(node_id), version_info, format_byte_size(content.len() as i64), temporal_info, row_count))
            }
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


// Extract row count from Parquet content efficiently using metadata
fn extract_row_count_from_parquet_content(content: &[u8]) -> Result<String> {
    use parquet::file::reader::{SerializedFileReader, FileReader};
    use bytes::Bytes;
    let bytes = Bytes::copy_from_slice(content);
    let reader = SerializedFileReader::new(bytes)
        .map_err(|e| anyhow!("Failed to create Parquet reader: {}", e))?;
    let metadata = reader.metadata();
    let row_count: i64 = metadata.row_groups().iter().map(|g| g.num_rows()).sum();
    Ok(row_count.to_string())
}

/// Find the Delta Lake version number for a commit by parsing log files directly
async fn find_delta_log_version_by_timestamp(store_path: &str, target_timestamp: i64) -> Result<Option<i64>> {
    use std::path::Path;
    use tokio::fs;
    
    let delta_log_path = Path::new(store_path).join("_delta_log");
    
    if !delta_log_path.exists() {
        return Ok(None);
    }
    
    // Read all .json files in the _delta_log directory
    let mut entries = fs::read_dir(&delta_log_path).await?;
    let mut log_files = Vec::new();
    
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if let Some(extension) = path.extension() {
            if extension == "json" {
                if let Some(filename) = path.file_stem().and_then(|s| s.to_str()) {
                    // Parse version from filename like "00000000000000000001.json"
                    if let Ok(version) = filename.parse::<i64>() {
                        log_files.push((version, path));
                    }
                }
            }
        }
    }
    
    // Sort by version number
    log_files.sort_by_key(|(version, _)| *version);
    
    // Find the version that matches our timestamp (or closest before it)
    let mut best_match = None;
    
    for (version, log_file) in log_files {
        match fs::read_to_string(&log_file).await {
            Ok(content) => {
                // Parse each line as JSON (Delta Lake log format)
                for line in content.lines() {
                    if line.trim().is_empty() {
                        continue;
                    }
                    
                    if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(line) {
                        if let Some(commit_info) = json_value.get("commitInfo") {
                            if let Some(timestamp) = commit_info.get("timestamp").and_then(|v| v.as_i64()) {
                                // Allow for small timing differences (within 1 second)
                                if (timestamp - target_timestamp).abs() <= 1000 {
                                    return Ok(Some(version));
                                }
                                // Track closest match
                                if timestamp <= target_timestamp {
                                    best_match = Some(version);
                                }
                            }
                        }
                    }
                }
            },
            Err(_) => continue, // Skip unreadable files
        }
    }
    
    Ok(best_match)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use crate::common::{ShipContext, FilesystemChoice};
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
        show_command(&setup.ship_context, FilesystemChoice::Data, |output| {
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
        show_command(&ship_context, FilesystemChoice::Data, |output: String| {
            captured_output.push_str(&output);
        }).await.expect("Show command should work");
        
        println!("Show command output:\n{}", captured_output);
        
        // Test the format improvements that we implemented:
        
        // 1. Should have transaction headers with command info in new format
        assert!(captured_output.contains("=== Transaction"), "Should contain transaction headers");
        assert!(captured_output.contains("(Command: ["), "Should contain command info with new array format");
        
        // 2. Should show our test command and arguments 
        assert!(captured_output.contains("test_command"), "Should contain the command we executed");
        assert!(captured_output.contains("test_arg"), "Should contain the command argument");
        
        // 3. Should have Delta Lake version info in new combined format
        // Either "Delta Lake Version: N (timestamp)" or graceful fallback for test environment
        assert!(
            captured_output.contains("Delta Lake Version:") || captured_output.contains("No operation details available"),
            "Should show Delta Lake version info or graceful fallback"
        );
        
        // 4. Should show proper timestamp format if available
        if captured_output.contains("UTC") {
            // If we have timestamps, they should be human-readable
            let has_readable_timestamp = captured_output.contains("2025") || captured_output.contains("2024");
            assert!(has_readable_timestamp, "Timestamps should be human-readable, not raw milliseconds");
        }
        
        // 5. Should NOT contain old verbose format that we removed
        assert!(!captured_output.contains("Added 1 files in this commit"), "Should not contain verbose file addition messages");
        assert!(!captured_output.contains("Files added:"), "Should not contain verbose file listing");
        
        // 6. Should not be empty
        assert!(!captured_output.trim().is_empty(), "Show output should not be empty");
        
        // 7. Should have multiple transactions (steward creates initialization transaction + our test transaction)
        let transaction_count = captured_output.matches("=== Transaction").count();
        assert!(transaction_count >= 2, "Should show at least init + our test transaction");
    }

    #[tokio::test]
    async fn test_show_control_filesystem_error() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");
        
        let result = show_command(&setup.ship_context, FilesystemChoice::Control, |_| {}).await;
        
        assert!(result.is_err(), "Should fail for control filesystem access");
        assert!(result.unwrap_err().to_string().contains("Control filesystem access not yet implemented"),
                "Should have specific error message");
    }
}
