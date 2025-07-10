use anyhow::{Result, anyhow};
use std::path::PathBuf;
use arrow_array::{StringArray, BinaryArray, Array};
use arrow::datatypes::DataType;

use crate::common::{FilesystemChoice, parse_directory_content as parse_directory_entries, format_node_id};

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
    output.push_str("=== DuckPond Operation Log ===\n");
    
    // Check if we should skip the first version (table creation)
    // We'll skip version 0 if there are multiple versions and it has no data files
    let start_version = if current_version > 0 && table_version_is_empty(&table, 0).await? {
        1
    } else {
        0
    };
    
    let transaction_count = (current_version + 1) - start_version;
    
    for version in start_version..=current_version {
        let transaction_number = version - start_version + 1;
        output.push_str(&format!("=== Transaction #{:03} ===\n", transaction_number));
        
        // Load the table at this specific version and read its data
        match load_operations_for_version(&store_path_str, version).await {
            Ok(operations) => {
                if operations.is_empty() {
                    output.push_str("  Operations: (no operations found)\n");
                } else {
                    output.push_str("  Operations:\n");
                    for op in operations {
                        output.push_str(&format!("    {}\n", op));
                    }
                }
            }
            Err(e) => {
                output.push_str(&format!("  Operations: (error reading operations: {})\n", e));
            }
        }
        output.push_str("\n");
    }

    output.push_str("=== Summary ===\n");
    output.push_str(&format!("Transactions: {}\n", transaction_count));
    output.push_str(&format!("Entries: (not yet calculated)\n"));
    
    // Add final directory state reconstruction
    match reconstruct_final_directory_state(&store_path_str).await {
        Ok(directory_output) => {
            output.push_str("\n");
            output.push_str(&directory_output);
        }
        Err(e) => {
            output.push_str(&format!("\nError reconstructing final directory state: {}\n", e));
        }
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

// Load operations for a specific Delta Lake version
async fn load_operations_for_version(store_path: &str, version: i64) -> Result<Vec<String>> {
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
                Ok(description) => operations.push(description),
                Err(e) => {
                    operations.push(format!("Error parsing entry {}/{}: {}", format_node_id(part_id), format_node_id(node_id), e));
                }
            }
        }
    }
    
    Ok(operations)
}

// Parse oplog content based on entry type  
fn parse_oplog_content(part_id: &str, _node_id: &str, content: &[u8]) -> Result<String> {
    // Most oplog entries contain Arrow IPC encoded directory entries
    // Try to parse as directory content first
    match parse_directory_content(content) {
        Ok(description) => {
            Ok(format!("Directory update for partition {}: {}", format_node_id(part_id), description))
        }
        Err(_) => {
            // If not directory content, treat as file content
            let content_preview = if content.len() > 30 {
                // Try to show some readable content
                let readable_part = String::from_utf8_lossy(&content[..30]);
                if readable_part.chars().all(|c| c.is_ascii() && !c.is_control()) {
                    format!("\"{}\"... ({} bytes)", readable_part, content.len())
                } else {
                    format!("Binary data ({} bytes)", content.len())
                }
            } else {
                let readable_part = String::from_utf8_lossy(content);
                if readable_part.chars().all(|c| c.is_ascii() && !c.is_control()) {
                    format!("\"{}\" ({} bytes)", readable_part, content.len())
                } else {
                    format!("Binary data ({} bytes)", content.len())
                }
            };
            
            Ok(format!("File operation in partition {}: {}", format_node_id(part_id), content_preview))
        }
    }
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
            let description = match entry.file_type.as_str() {
                "file" => {
                    // For files, show a preview of the content
                    let content_preview = if entry.content.len() > 30 {
                        let preview = String::from_utf8_lossy(&entry.content[..30]);
                        format!("\"{}\"... ({} bytes)", preview, entry.content.len())
                    } else {
                        let content_str = String::from_utf8_lossy(&entry.content);
                        format!("\"{}\" ({} bytes)", content_str, entry.content.len())
                    };
                    format!("File {}: {}", format_node_id(&entry.part_id), content_preview)
                }
                "directory" => {
                    // For directories, try to parse the nested directory entries
                    match parse_nested_directory_content(&entry.content) {
                        Ok(dir_desc) => format!("Directory {}: {}", format_node_id(&entry.part_id), dir_desc),
                        Err(_) => format!("Directory {}: ({} bytes)", format_node_id(&entry.part_id), entry.content.len())
                    }
                }
                _ => {
                    format!("{} {}: ({} bytes)", entry.file_type, format_node_id(&entry.part_id), entry.content.len())
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
    
    let entry_descriptions: Vec<String> = directory_entries.into_iter()
        .map(|entry| format!("{} ({:?})", entry.name, entry.operation_type))
        .collect();
    
    if entry_descriptions.is_empty() {
        Ok("empty directory".to_string())
    } else {
        Ok(format!("contains [{}]", entry_descriptions.join(", ")))
    }
}

// Reconstruct the final directory state by reading the current table contents
async fn reconstruct_final_directory_state(store_path: &str) -> Result<String> {
    use datafusion::prelude::SessionContext;
    use std::collections::HashMap;
    
    // Load the current table (latest version)
    let table = deltalake::open_table(store_path).await
        .map_err(|e| anyhow!("Failed to open table: {}", e))?;
    
    let ctx = SessionContext::new();
    ctx.register_table("oplog", std::sync::Arc::new(table))
        .map_err(|e| anyhow!("Failed to register table: {}", e))?;
    
    // Query all current records
    let df = ctx.sql("SELECT part_id, node_id, timestamp, content FROM oplog ORDER BY part_id, timestamp").await
        .map_err(|e| anyhow!("Failed to create query: {}", e))?;
    
    let batches = df.collect().await
        .map_err(|e| anyhow!("Failed to execute query: {}", e))?;
    
    let mut file_entries: HashMap<String, String> = HashMap::new(); // filename -> node_id
    
    // Parse all current entries to build directory structure
    for batch in batches {
        let part_id_array = batch.column(0);
        let content_array = batch.column(3);
        
        // Handle dictionary type for part_id
        let part_id_values: Vec<String> = match part_id_array.data_type() {
            DataType::Dictionary(_, _) => {
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
        
        let contents = content_array.as_any().downcast_ref::<BinaryArray>()
            .ok_or_else(|| anyhow!("Failed to cast content column"))?;
        
        for i in 0..batch.num_rows() {
            let _part_id = &part_id_values[i];  // Keep for potential debug use
            let content_bytes = contents.value(i);
            
            // Try to extract file entries (name -> node_id mapping) from directory content
            if let Ok(entries) = extract_file_entries_from_content(content_bytes) {
                file_entries.extend(entries);
            }
        }
    }
    
    // Format directory output
    let mut output = String::new();
    output.push_str("=== FINAL DIRECTORY SECTION ===\n");
    output.push_str("Directory entries: (reconstructed final state) 0 bytes\n");
    
    if file_entries.is_empty() {
        output.push_str("  (empty)\n");
    } else {
        output.push_str("Directory root:\n");
        let mut sorted_files: Vec<_> = file_entries.iter().collect();
        sorted_files.sort_by_key(|(name, _)| *name);
        
        for (i, (filename, node_id)) in sorted_files.iter().enumerate() {
            let prefix = if i == sorted_files.len() - 1 { "  └─" } else { "  ├─" };
            // Show shortened node ID (last 8 chars) like the list command
            let short_node_id = if node_id.len() >= 8 {
                &node_id[node_id.len()-8..]
            } else {
                node_id
            };
            output.push_str(&format!("{} '{}' -> {}\n", prefix, filename, short_node_id));
        }
    }
    
    Ok(output)
}

// Extract file entries (name -> node_id mappings) from oplog content
fn extract_file_entries_from_content(content: &[u8]) -> Result<std::collections::HashMap<String, String>> {
    use arrow::ipc::reader::StreamReader;
    use std::io::Cursor;
    use std::collections::HashMap;
    
    let cursor = Cursor::new(content);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| anyhow!("Failed to create Arrow IPC reader: {}", e))?;
    
    let mut file_entries = HashMap::new();
    
    for batch_result in reader {
        let batch = batch_result.map_err(|e| anyhow!("Failed to read Arrow batch: {}", e))?;
        
        // Try to parse as OplogEntry first
        if let Ok(oplog_entries) = serde_arrow::from_record_batch::<Vec<tlogfs::OplogEntry>>(&batch) {
            for entry in oplog_entries {
                // Extract file entries from directory entries
                if entry.file_type == "directory" {
                    if let Ok(dir_entries) = extract_directory_file_entries(&entry.content) {
                        file_entries.extend(dir_entries);
                    }
                }
            }
        }
    }
    
    Ok(file_entries)
}

// Extract file entries (name -> child_node_id mappings) from directory content
fn extract_directory_file_entries(content: &[u8]) -> Result<std::collections::HashMap<String, String>> {
    use arrow::ipc::reader::StreamReader;
    use std::io::Cursor;
    use std::collections::HashMap;
    
    let cursor = Cursor::new(content);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| anyhow!("Failed to create directory Arrow IPC reader: {}", e))?;
    
    let mut file_entries = HashMap::new();
    
    for batch_result in reader {
        let batch = batch_result.map_err(|e| anyhow!("Failed to read directory Arrow batch: {}", e))?;
        
        let directory_entries: Vec<tlogfs::VersionedDirectoryEntry> = 
            serde_arrow::from_record_batch(&batch)
                .map_err(|e| anyhow!("Failed to deserialize directory entries: {}", e))?;
        
        for entry in directory_entries {
            // Only include insert operations (not deletes)
            if matches!(entry.operation_type, tlogfs::schema::OperationType::Insert) {
                file_entries.insert(entry.name, entry.child_node_id);
            }
        }
    }
    
    Ok(file_entries)
}
