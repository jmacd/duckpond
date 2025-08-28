use anyhow::{Result, anyhow};
use arrow_array::{StringArray, BinaryArray, Array};

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
        let tx_metadata = match commit_info.info.get("pond_txn") {
            Some(pond_txn_value) => {
                // Parse the pond_txn JSON object
                if let Some(obj) = pond_txn_value.as_object() {
                    if let Some(args) = obj.get("args").and_then(|v| v.as_str()) {
                        format!(" (Command: {})", args)
                    } else {
                        " (No args found)".to_string()
                    }
                } else {
                    " (Invalid pond_txn format)".to_string()
                }
            }
            None => " (No metadata)".to_string(),
        };
        
        output.push_str(&format!("=== Transaction {} {} ===\n", timestamp, tx_metadata));
        
        // Show commit information
        if let Some(timestamp) = &commit_info.timestamp {
            output.push_str(&format!("  Timestamp: {}\n", timestamp));
        }
        
        // Load the operations that were added in this specific transaction
        match load_operations_from_commit_info(&commit_info, persistence.store_path()).await {
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
    
    Ok(output)
}

// Load operations from commit info directly (no version numbers needed)
async fn load_operations_from_commit_info(commit_info: &deltalake::kernel::CommitInfo, store_path: &str) -> Result<Vec<String>> {
    // Extract file paths that were added in this commit from the Delta Lake commit info
    let mut operations = Vec::new();
    
    // The commit info should contain information about what files were added
    // Let's check if there are any "add" actions in the commit
    if let Some(operation_parameters) = commit_info.operation_parameters.as_ref() {
        if let Some(files_added) = operation_parameters.get("path") {
            operations.push(format!("Files added: {}", files_added));
        }
    }
    
    // Try to read the actual Parquet files to get oplog information
    // Since we can't use version numbers, we'll try to read from the current state
    match deltalake::open_table(store_path).await {
        Ok(table) => {
            let files: Vec<String> = table.get_file_uris()?.into_iter().collect();
            
            if !files.is_empty() {
                operations.push(format!("Total oplog files in table: {}", files.len()));
                
                // Try to read some oplog entries to show what operations were performed
                let file_refs: Vec<&String> = files[0..files.len().min(3)].iter().collect();
                match read_parquet_files_directly(&file_refs, store_path).await {
                    Ok(oplog_operations) => {
                        let formatted = format_operations_by_partition(oplog_operations);
                        operations.extend(formatted);
                    }
                    Err(e) => {
                        operations.push(format!("Could not read oplog details: {}", e));
                    }
                }
            } else {
                operations.push("No oplog files found".to_string());
            }
        }
        Err(e) => {
            operations.push(format!("Could not access oplog table: {}", e));
        }
    }
    
    if operations.is_empty() {
        operations.push("No operation details available".to_string());
    }
    
    Ok(operations)
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
        
        // Get factory column (optional for dynamic nodes)
        let factory_idx = schema.index_of("factory").ok();
        
        // Get temporal metadata columns (optional for FileSeries)
        let min_event_time_idx = schema.index_of("min_event_time").ok();
        let max_event_time_idx = schema.index_of("max_event_time").ok();
        
        // Get large file metadata columns (optional for large files)
        let sha256_idx = schema.index_of("sha256").ok();
        let size_idx = schema.index_of("size").ok();
        
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
        
        // Get factory column array if available
        let factories = factory_idx.and_then(|idx| 
            batch.column(idx).as_any().downcast_ref::<StringArray>()
        );
        
        // Get temporal metadata arrays if available
        let min_event_times = min_event_time_idx.and_then(|idx| 
            batch.column(idx).as_any().downcast_ref::<arrow_array::Int64Array>()
        );
        let max_event_times = max_event_time_idx.and_then(|idx| 
            batch.column(idx).as_any().downcast_ref::<arrow_array::Int64Array>()
        );
        
        // Get large file metadata arrays if available  
        let sha256_hashes = sha256_idx.and_then(|idx| 
            batch.column(idx).as_any().downcast_ref::<StringArray>()
        );
        
        // Size column uses Int64 to match Delta Lake protocol (Java ecosystem legacy)
        let sizes = size_idx.and_then(|idx| {
            let size_column = batch.column(idx);
            match size_column.data_type() {
                arrow::datatypes::DataType::Int64 => {
                    size_column.as_any().downcast_ref::<arrow_array::Int64Array>()
                },
                unexpected_type => {
                    println!("ERROR: Size column has unexpected type: {:?}, expected Int64", unexpected_type);
                    println!("This indicates a schema inconsistency bug that needs investigation");
                    None
                }
            }
        });
        
        for i in 0..batch.num_rows() {
            // part_id comes from the file path, not the data
            let node_id = node_ids.value(i);
            let file_type_str = file_types.value(i);
            let content_bytes = contents.value(i);
            
            // Extract factory information if available
            let factory = factories.and_then(|arr| {
                if arr.is_null(i) { None } else { Some(arr.value(i)) }
            });
            
            // Extract temporal metadata if available
            let temporal_range = match (min_event_times, max_event_times) {
                (Some(min_arr), Some(max_arr)) if !min_arr.is_null(i) && !max_arr.is_null(i) => {
                    Some((min_arr.value(i), max_arr.value(i)))
                },
                _ => None,
            };
            
            // Extract large file metadata if available
            let sha256_hash = sha256_hashes.and_then(|arr| {
                if arr.is_null(i) { None } else { Some(arr.value(i)) }
            });
            let file_size = sizes.and_then(|arr| {
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
            match parse_direct_content(&part_id, node_id, file_type, content_bytes, temporal_range, factory, sha256_hash, file_size) {
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
fn parse_direct_content(_part_id: &str, node_id: &str, file_type: EntryType, content: &[u8], temporal_range: Option<(i64, i64)>, factory: Option<&str>, sha256_hash: Option<&str>, file_size: Option<i64>) -> Result<String> {
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
        }
        EntryType::FileData => {
            // Regular file content - show preview with node ID
            let content_preview = format_content_preview(content);
            Ok(format!("FileData [{}]: {}", format_node_id(node_id), content_preview))
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
            // Check if this is a large file (stored externally)
            if let (Some(hash), Some(size)) = (sha256_hash, file_size) {
                // Large file - stored externally with SHA256 reference
                let temporal_info = match temporal_range {
                    Some((min_time, max_time)) => format!(" (temporal: {} to {})", min_time, max_time),
                    None => " (temporal: missing)".to_string(),
                };
                Ok(format!("FileSeries [{}]: Large file ({} bytes, sha256={}{})", 
                    format_node_id(node_id), size, hash, temporal_info))
            } else {
                // Small file - stored inline
                let content_preview = format_content_preview(content);
                let temporal_info = match temporal_range {
                    Some((min_time, max_time)) => format!(" (temporal: {} to {})", min_time, max_time),
                    None => " (temporal: missing)".to_string(),
                };
                let row_count = extract_row_count_from_parquet_content(content)
                    .unwrap_or_else(|e| format!("row count error: {}", e));
                // Extract schema information from the Parquet content
                let schema_info = extract_schema_from_parquet_content(content)
                    .unwrap_or_else(|e| format!(" (schema error: {})", e));
                Ok(format!("FileSeries [{}]: {}{} ({} rows){}", format_node_id(node_id), content_preview, temporal_info, row_count, schema_info))
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

// Extract Arrow schema information from Parquet content
fn extract_schema_from_parquet_content(content: &[u8]) -> Result<String> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use bytes::Bytes;
    
    // Create a reader from the binary content using Bytes
    let bytes = Bytes::copy_from_slice(content);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| anyhow!("Failed to create Parquet reader: {}", e))?;
    
    // Get the Arrow schema
    let arrow_schema = builder.schema();
    
    // Format schema fields with types
    let mut field_info = Vec::new();
    for field in arrow_schema.fields() {
        let nullable_marker = if field.is_nullable() { "?" } else { "" };
        field_info.push(format!("{}: {:?}{}", field.name(), field.data_type(), nullable_marker));
    }
    
    let field_count = field_info.len();
    if field_count <= 8 {
        // Show all fields if there aren't too many
        Ok(format!("\n            Schema ({} fields): [{}]", field_count, field_info.join(", ")))
    } else {
        // Show first few fields and indicate there are more
        let shown_fields = &field_info[..5];
        Ok(format!("\n            Schema ({} fields): [{}, ... and {} more]", 
                  field_count, shown_fields.join(", "), field_count - 5))
    }
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
    use crate::commands::copy::copy_command;

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

        /// Create a test file in the host filesystem
        async fn create_host_file(&self, filename: &str, content: &str) -> Result<PathBuf> {
            let file_path = self._temp_dir.path().join(filename);
            tokio::fs::write(&file_path, content).await?;
            Ok(file_path)
        }

        /// Copy a file to pond using copy command (creates transactions)
        async fn copy_to_pond(&self, host_file: &str, pond_path: &str, format: &str) -> Result<()> {
            let host_path = self.create_host_file(host_file, "test content").await?;
            copy_command(&self.ship_context, &[host_path.to_string_lossy().to_string()], pond_path, format).await
        }
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
    async fn test_show_with_copy_transactions() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");
        
        // Create multiple files and copy them to pond
        setup.copy_to_pond("test1.txt", "file1.txt", "data").await
            .expect("Failed to copy first file");
        setup.copy_to_pond("test2.csv", "file2.csv", "table").await
            .expect("Failed to copy second file");
        setup.copy_to_pond("test3.parquet", "file3.parquet", "series").await
            .expect("Failed to copy third file");
        
        let mut results = Vec::new();
        show_command(&setup.ship_context, FilesystemChoice::Data, |output| {
            results.push(output);
        }).await.expect("Show command failed");
        
        let output = results.join("");
        
        // Should contain multiple transactions (init + 3 copy operations)
        assert!(output.contains("Transaction"), "Should contain transaction information");
        
        // Should show command metadata for copy operations
        assert!(output.contains("copy") || output.contains("Command:"), 
                "Should contain copy command information");
        
        // Should show timestamps
        assert!(output.contains("Timestamp:"), "Should contain timestamp information");
    }

    #[tokio::test]
    async fn test_show_transaction_metadata() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");
        
        // Create a file and copy it to pond
        setup.copy_to_pond("test.txt", "test_file.txt", "data").await
            .expect("Failed to copy file");
        
        let mut results = Vec::new();
        show_command(&setup.ship_context, FilesystemChoice::Data, |output| {
            results.push(output);
        }).await.expect("Show command failed");
        
        let output = results.join("");
        
        // Should contain transaction metadata
        assert!(output.contains("Transaction"), "Should contain transaction headers");
        assert!(output.contains("Timestamp:"), "Should contain timestamp");
        
        // The format should be clear and readable
        assert!(output.contains("==="), "Should contain formatted transaction headers");
    }

    #[tokio::test]
    async fn test_show_multiple_operations() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");
        
        // Perform multiple operations to create transaction history
        for i in 0..3 {
            setup.copy_to_pond(&format!("test{}.txt", i), &format!("file{}.txt", i), "data").await
                .expect(&format!("Failed to copy file {}", i));
        }
        
        let mut results = Vec::new();
        show_command(&setup.ship_context, FilesystemChoice::Data, |output| {
            results.push(output);
        }).await.expect("Show command failed");
        
        let output = results.join("");
        
        // Should show multiple transactions in chronological order
        let transaction_count = output.matches("Transaction").count();
        assert!(transaction_count >= 3, "Should show at least 3 copy transactions plus init");
        
        // Should contain timestamps for ordering
        let timestamp_count = output.matches("Timestamp:").count();
        assert!(timestamp_count >= 3, "Should have timestamps for each transaction");
    }

    #[tokio::test]
    async fn test_show_control_filesystem_error() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");
        
        let result = show_command(&setup.ship_context, FilesystemChoice::Control, |_| {}).await;
        
        assert!(result.is_err(), "Should fail for control filesystem access");
        assert!(result.unwrap_err().to_string().contains("Control filesystem access not yet implemented"),
                "Should have specific error message");
    }

    #[tokio::test]
    async fn test_show_transaction_ordering() {
        let setup = TestSetup::new().await.expect("Failed to create test setup");
        
        // Create files with delay to ensure different timestamps
        setup.copy_to_pond("early.txt", "early_file.txt", "data").await
            .expect("Failed to copy early file");
        
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        
        setup.copy_to_pond("late.txt", "late_file.txt", "data").await
            .expect("Failed to copy late file");
        
        let mut results = Vec::new();
        show_command(&setup.ship_context, FilesystemChoice::Data, |output| {
            results.push(output);
        }).await.expect("Show command failed");
        
        let output = results.join("");
        
        // Transactions should be ordered by timestamp
        assert!(output.contains("Transaction"), "Should contain transactions");
        assert!(output.contains("Timestamp:"), "Should contain timestamps for ordering");
        
        // Should show both copy operations
        let transaction_count = output.matches("Transaction").count();
        assert!(transaction_count >= 2, "Should show at least 2 copy transactions");
    }
}
