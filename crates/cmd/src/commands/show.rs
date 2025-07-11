use anyhow::{Result, anyhow};
use std::path::PathBuf;
use arrow_array::{StringArray, BinaryArray, Array};
use arrow::datatypes::DataType;

use crate::common::{FilesystemChoice, parse_directory_content as parse_directory_entries, format_node_id};
use tlogfs::schema::OperationType;
use tinyfs::EntryType;

pub async fn show_command(filesystem: FilesystemChoice) -> Result<()> {
    let output = show_command_as_string(filesystem).await?;
    print!("{}", output);
    Ok(())
}

pub async fn show_command_as_string(filesystem: FilesystemChoice) -> Result<String> {
    show_command_as_string_with_pond(None, filesystem).await
}

pub async fn show_command_as_string_with_pond(pond_path: Option<PathBuf>, filesystem: FilesystemChoice) -> Result<String> {
    // Check if pond exists by checking for data directory (don't create ship yet)
    let pond_path = crate::common::get_pond_path_with_override(pond_path)?;
    let data_path = steward::get_data_path(&pond_path);
    
    if !data_path.exists() {
        return Err(anyhow!("Pond does not exist. Run 'pond init' first."));
    }

    // Now create steward Ship instance to get the correct filesystem path
    let ship = crate::common::create_ship(Some(pond_path)).await?;
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
        output.push_str(&format!("=== Transaction #{:03} ===\n", transaction_number));
        
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

    Ok(output)
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

    // Now read only the new files and parse their operations
    // For now, fall back to the existing approach but only for truly new operations
    // TODO: Implement direct file reading and parsing
    
    // This is a temporary approach - we should eventually read the files directly
    let all_operations = load_operations_for_version(store_path, version).await?;
    let prev_operations = if previous_version >= 0 {
        load_operations_for_version(store_path, previous_version).await.unwrap_or_default()
    } else {
        vec![]
    };
    
    // Return only the operations that weren't in the previous version
    let new_operations: Vec<(String, String)> = all_operations
        .into_iter()
        .skip(prev_operations.len())
        .collect();
    
    if new_operations.is_empty() {
        Ok(vec!["(no new operations in this transaction)".to_string()])
    } else {
        // Group operations by partition and format them properly
        Ok(format_operations_by_partition(new_operations))
    }
}

// Load operations for a specific Delta Lake version, returning (partition_id, operation) pairs
async fn load_operations_for_version(store_path: &str, version: i64) -> Result<Vec<(String, String)>> {
    // Load the table at the specific version
    let table = deltalake::open_table_with_version(store_path, version).await
        .map_err(|e| anyhow!("Failed to open table at version {}: {}", version, e))?;
    
    // Query the table to get all records
    use datafusion::prelude::SessionContext;
    
    let ctx = SessionContext::new();
    ctx.register_table("oplog", std::sync::Arc::new(table))
        .map_err(|e| anyhow!("Failed to register table: {}", e))?;
    
    let df = ctx.sql("SELECT part_id, node_id, timestamp, content FROM oplog ORDER BY timestamp").await
        .map_err(|e| anyhow!("Failed to create query: {}", e))?;
    
    let batches = df.collect().await
        .map_err(|e| anyhow!("Failed to execute query: {}", e))?;
    
    let mut operations = Vec::new();
    
    for batch in batches {
        // Get columns by casting to the appropriate Arrow array types
        let part_id_array = batch.column(0);
        let node_id_array = batch.column(1);
        let content_array = batch.column(3);
        
        // Handle different part_id column types (can be Dictionary or String)
        let part_id_values: Vec<String> = match part_id_array.data_type() {
            DataType::Dictionary(_, _) => {
                // Extract values from dictionary array
                use arrow::compute::kernels::cast;
                let string_array = cast::cast(part_id_array, &DataType::Utf8)
                    .map_err(|e| anyhow!("Failed to cast dictionary to string: {}", e))?;
                let string_array = string_array.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow!("Failed to downcast to StringArray after cast"))?;
                (0..string_array.len()).map(|i| string_array.value(i).to_string()).collect()
            }
            DataType::Utf8 => {
                let string_array = part_id_array.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow!("Failed to downcast part_id to StringArray"))?;
                (0..string_array.len()).map(|i| string_array.value(i).to_string()).collect()
            }
            _ => {
                return Err(anyhow!("Unsupported part_id column type: {:?}", part_id_array.data_type()));
            }
        };
        
        let node_ids = if let Some(string_array) = node_id_array.as_any().downcast_ref::<StringArray>() {
            string_array
        } else {
            return Err(anyhow!("node_id column is not a StringArray, actual type: {:?}", node_id_array.data_type()));
        };
        
        let contents = if let Some(binary_array) = content_array.as_any().downcast_ref::<BinaryArray>() {
            binary_array
        } else {
            return Err(anyhow!("content column is not a BinaryArray, actual type: {:?}", content_array.data_type()));
        };
        
        for i in 0..batch.num_rows() {
            let part_id = &part_id_values[i];
            let node_id = node_ids.value(i);
            let content_bytes = contents.value(i);
            
            // Try to parse the content based on the entry type
            match parse_oplog_content(part_id, node_id, content_bytes) {
                Ok(description) => {
                    operations.push((part_id.clone(), description));
                },
                Err(e) => {
                    operations.push((part_id.clone(), format!("Error parsing entry {}/{}: {}", format_node_id(part_id), format_node_id(node_id), e)));
                }
            }
        }
    }
    
    Ok(operations)
}

// Parse oplog content based on entry type  
fn parse_oplog_content(_part_id: &str, _node_id: &str, content: &[u8]) -> Result<String> {
    // Most oplog entries contain Arrow IPC encoded directory entries
    // Try to parse as directory content first
    match parse_directory_content(content) {
        Ok(description) => {
            Ok(description)
        }
        Err(_) => {
            // If not directory content, treat as file content
            let content_preview = format_content_preview(content);
            Ok(format!("File operation: {}", content_preview))
        }
    }
}

// Format content preview with proper newline quoting
fn format_content_preview(content: &[u8]) -> String {
    if content.len() > 30 {
        let preview = String::from_utf8_lossy(&content[..30]);
        let quoted = quote_newlines(&preview);
        format!("\"{}\"... ({} bytes)", quoted, content.len())
    } else {
        let content_str = String::from_utf8_lossy(content);
        let quoted = quote_newlines(&content_str);
        if content_str.chars().all(|c| c.is_ascii() && (!c.is_control() || c == '\n')) {
            format!("\"{}\" ({} bytes)", quoted, content.len())
        } else {
            format!("Binary data ({} bytes)", content.len())
        }
    }
}

// Quote newlines in content preview
fn quote_newlines(s: &str) -> String {
    s.replace('\n', "\\n").replace('\r', "\\r").replace('\t', "\\t")
}

// Parse directory content from Arrow IPC bytes
fn parse_directory_content(content: &[u8]) -> Result<String> {
    use arrow::ipc::reader::StreamReader;
    use std::io::Cursor;
    
    let cursor = Cursor::new(content);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| anyhow!("Failed to create Arrow IPC reader: {}", e))?;
    
    let mut descriptions = Vec::new();
    for (batch_idx, batch_result) in reader.enumerate() {
        let batch = batch_result.map_err(|e| anyhow!("Failed to read Arrow batch {}: {}", batch_idx, e))?;
        
        // These are OplogEntry records, not VersionedDirectoryEntry
        let oplog_entries: Vec<tlogfs::OplogEntry> = 
            serde_arrow::from_record_batch(&batch)
                .map_err(|e| anyhow!("Failed to deserialize oplog entries from batch {}: {}", batch_idx, e))?;
        
        for entry in oplog_entries {
            // Parse the nested content based on file_type
            let description = match entry.file_type {
                EntryType::File => {
                    // For files, show a tabular format with proper spacing
                    let content_preview = format_content_preview(&entry.content);
                    format!("File      {}  {}", format_node_id(&entry.node_id), content_preview)
                }
                EntryType::Directory => {
                    // For directories, show a tabular format with tree structure
                    match parse_nested_directory_content(&entry.content) {
                        Ok(dir_desc) => {
                            if dir_desc == "empty" {
                                format!("Directory {}  {}", format_node_id(&entry.node_id), dir_desc)
                            } else {
                                format!("Directory {}{}", format_node_id(&entry.node_id), dir_desc)
                            }
                        },
                        Err(_) => format!("Directory {}  ({} bytes)", format_node_id(&entry.node_id), entry.content.len())
                    }
                }
                _ => {
                    format!("{}      {}  ({} bytes)", entry.file_type, format_node_id(&entry.node_id), entry.content.len())
                }
            };
            descriptions.push(description);
        }
    }
    
    if descriptions.is_empty() {
        Err(anyhow!("No entries found"))
    } else {
        Ok(descriptions.join("; "))
    }
}

// Parse nested directory content (VersionedDirectoryEntry records)
fn parse_nested_directory_content(content: &[u8]) -> Result<String> {
    let directory_entries = parse_directory_entries(content)?;
    
    if directory_entries.is_empty() {
        Ok("empty".to_string())
    } else {
        let mut result = String::new();
        for (i, entry) in directory_entries.iter().enumerate() {
            let op_letter = match entry.operation_type {
                OperationType::Insert => "I",
                OperationType::Delete => "D", 
                OperationType::Update => "U",
            };
            let prefix = if i == directory_entries.len() - 1 { "        └─" } else { "        ├─" };
            result.push_str(&format!("\n{} '{}' -> {} ({})", prefix, entry.name, format_node_id(&entry.child_node_id), op_letter));
        }
        Ok(result)
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
        // Always show partition header for clarity
        result.push(format!("  Partition {} ({} entries):", format_node_id(&part_id), ops.len()));
        for op in ops {
            result.push(format!("    {}", op));
        }
    }
    
    result
}
