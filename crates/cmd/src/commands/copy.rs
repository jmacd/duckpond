use diagnostics;
use anyhow::{Result, anyhow};

// Memory-efficient Parquet schema analysis for entry type detection
async fn analyze_parquet_schema_for_entry_type(file_path: &str) -> Result<tinyfs::EntryType> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::fs::File as StdFile;
    
    // Open file for metadata reading only (no data content loading)
    let std_file = StdFile::open(file_path)
        .map_err(|e| anyhow!("Failed to open file for schema analysis: {}", e))?;
    
    let parquet_reader = SerializedFileReader::new(std_file)
        .map_err(|e| anyhow!("Failed to create Parquet reader: {}", e))?;
    
    let metadata = parquet_reader.metadata();
    let schema = metadata.file_metadata().schema_descr();
    
    // Check for timestamp columns using same logic as tlogfs::schema::detect_timestamp_column()
    let entry_type = if has_timestamp_column(schema) {
        tinyfs::EntryType::FileSeries
    } else {
        tinyfs::EntryType::FileTable
    };
    
    let entry_type_str = format!("{:?}", entry_type);
    diagnostics::log_debug!("analyze_parquet_schema_for_entry_type", 
        file_path: file_path, 
        entry_type: entry_type_str
    );
    
    Ok(entry_type)
}

fn has_timestamp_column(schema: &parquet::schema::types::SchemaDescriptor) -> bool {
    // Check for case-insensitive "timestamp" column using same priority as TLogFS
    let candidates = ["timestamp", "Timestamp", "event_time", "time", "ts", "datetime"];
    
    for field in schema.columns() {
        let field_name = field.name();
        if candidates.contains(&field_name) {
            diagnostics::log_debug!("has_timestamp_column found timestamp candidate", 
                field_name: field_name
            );
            return true;
        }
    }
    
    diagnostics::log_debug!("has_timestamp_column no timestamp column found");
    false
}

async fn get_entry_type_for_file(source_path: &str, format: &str) -> Result<tinyfs::EntryType> {
    let entry_type = match format {
        "data" => Ok(tinyfs::EntryType::FileData),
        "table" => {
            // FileTable only applies to actual Parquet files
            let path_lower = source_path.to_lowercase();
            if path_lower.ends_with(".parquet") {
                Ok(tinyfs::EntryType::FileTable)
            } else {
                // Non-Parquet files cannot be FileTable - always FileData
                Ok(tinyfs::EntryType::FileData)
            }
        }
        "series" => {
            // FileSeries only applies to actual Parquet files
            let path_lower = source_path.to_lowercase();
            if path_lower.ends_with(".parquet") {
                Ok(tinyfs::EntryType::FileSeries)
            } else {
                // Non-Parquet files cannot be FileSeries - always FileData
                Ok(tinyfs::EntryType::FileData)
            }
        }
        "auto" => {
            // Automatic detection based on file extension for classification only
            let path_lower = source_path.to_lowercase();
            if path_lower.ends_with(".parquet") {
                // Parquet files: analyze schema to decide FileTable vs FileSeries classification
                analyze_parquet_schema_for_entry_type(source_path).await
            } else {
                // All other files (CSV, TXT, etc.) are stored as FileData in original form
                Ok(tinyfs::EntryType::FileData)
            }
        }
        _ => {
            // Invalid format - return error instead of defaulting
            Err(anyhow!("Invalid format '{}'. Valid options are: auto, data, table, series", format))
        }
    };
    
    match &entry_type {
        Ok(et) => {
            let entry_type_str = format!("{:?}", et);
            diagnostics::log_debug!("get_entry_type_for_file decision", 
                source_path: source_path, 
                format: format, 
                entry_type: entry_type_str
            );
        }
        Err(e) => {
            let error_str = e.to_string();
            diagnostics::log_debug!("get_entry_type_for_file error", 
                source_path: source_path, 
                format: format, 
                error: error_str
            );
        }
    }
    
    entry_type
}

// STREAMING COPY: Copy multiple files to directory using proper context
async fn copy_files_to_directory(
    ship: &steward::Ship,
    sources: &[String],
    dest_wd: &tinyfs::WD,
    format: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    for source in sources {
        // Extract filename from source path
        let source_filename = std::path::Path::new(source)
            .file_name()
            .ok_or("Invalid file path")?
            .to_str()
            .ok_or("Invalid filename")?;
        
        copy_single_file_to_directory_with_name(ship, source, dest_wd, source_filename, format).await?;
    }
    Ok(())
}

// Copy a single file to a directory using the provided working directory context
async fn copy_single_file_to_directory_with_name(
    _ship: &steward::Ship,
    file_path: &str,
    dest_wd: &tinyfs::WD,
    filename: &str,
    format: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::fs::File;
    use tokio::io::AsyncWriteExt;
    
    // Determine entry type based on format flag, NOT filename
    let entry_type = get_entry_type_for_file(file_path, format).await?;
    let entry_type_str = format!("{:?}", entry_type);
    
    diagnostics::log_debug!("copy_single_file_to_directory", 
        source_path: file_path, 
        dest_filename: filename, 
        format: format, 
        entry_type: entry_type_str
    );
    
    // Unified streaming copy for all entry types
    let mut source_file = File::open(file_path).await
        .map_err(|e| format!("Failed to open source file: {}", e))?;
    
    let mut dest_writer = dest_wd.async_writer_path_with_type(filename, entry_type).await
        .map_err(|e| format!("Failed to create destination writer: {}", e))?;
    
    // Stream copy with 64KB buffer for memory efficiency
    tokio::io::copy(&mut source_file, &mut dest_writer).await
        .map_err(|e| format!("Failed to stream file content: {}", e))?;
    
    // TLogFS handles temporal metadata extraction during shutdown for FileSeries
    dest_writer.shutdown().await
        .map_err(|e| format!("Failed to complete file write: {}", e))?;
    
    diagnostics::log_info!("Copied {file_path} to directory as {filename}", file_path: file_path, filename: filename);
    Ok(())
}

/// Copy files into the pond 
/// 
/// This command operates on an existing pond via the provided Ship.
/// The Ship should already have a transaction started.
pub async fn copy_command(mut ship: steward::Ship, sources: &[String], dest: &str, format: &str) -> Result<()> {
    // Add a unique marker to verify we're running the right code
    diagnostics::log_debug!("COPY_VERSION: transaction-control-v1.0");
    
    // Validate arguments
    if sources.is_empty() {
        return Err(anyhow!("At least one source file must be specified"));
    }

    // Get the data filesystem from ship
    let fs = ship.data_fs();

    diagnostics::log_debug!("Checking pending operations after begin_transaction...");
    let has_pending = fs.has_pending_operations().await
        .map_err(|e| anyhow!("Failed to check pending operations: {}", e))?;
    diagnostics::log_debug!("Has pending operations: {has_pending}", has_pending: has_pending);

    let root = fs.root().await?;

    // Use copy-specific destination resolution to handle trailing slashes properly
    let copy_result = root.resolve_copy_destination(dest).await;
    let operation_result = match copy_result {
        Ok((dest_wd, dest_type)) => {
            match dest_type {
                tinyfs::CopyDestination::Directory | tinyfs::CopyDestination::ExistingDirectory => {
                    // Destination is a directory (either explicit with / or existing) - copy files into it
                    copy_files_to_directory(&ship, sources, &dest_wd, format).await
                        .map_err(|e| anyhow!("Copy to directory failed: {}", e))
                }
                tinyfs::CopyDestination::ExistingFile => {
                    // Destination is an existing file - not supported for copy operations
                    if sources.len() == 1 {
                        Err(anyhow!("Destination '{}' exists but is not a directory (cannot copy to existing file)", dest))
                    } else {
                        Err(anyhow!("When copying multiple files, destination '{}' must be a directory", dest))
                    }
                }
                tinyfs::CopyDestination::NewPath(name) => {
                    // Destination doesn't exist
                    if sources.len() == 1 {
                        // Single file to non-existent destination - use format flag only
                        let source = &sources[0];
                        
                        // Use the same logic as directory copying, just with the specific filename
                        copy_single_file_to_directory_with_name(&ship, source, &dest_wd, &name, format).await
                            .map_err(|e| anyhow!("Failed to copy file: {}", e))?;
                        
                        diagnostics::log_info!("Copied {source} to {name}", source: source, name: name);
                        Ok(())
                    } else {
                        Err(anyhow!("When copying multiple files, destination '{}' must be an existing directory", dest))
                    }
                }
            }
        }
        Err(e) => {
            Err(anyhow!("Failed to resolve destination '{}': {}", dest, e))
        }
    };
    
    // Handle the result - rollback on error, commit on success
    match operation_result {
        Ok(()) => {
            diagnostics::log_debug!("Copy operations completed, checking pending operations before commit...");
            let has_pending = fs.has_pending_operations().await
                .map_err(|e| anyhow!("Failed to check pending operations: {}", e))?;
            diagnostics::log_debug!("Has pending operations before commit: {has_pending}", has_pending: has_pending);
            
            // Commit all changes through steward (this will handle both data and control filesystems)
            diagnostics::log_debug!("Committing transaction via steward...");
            ship.commit_transaction().await
                .map_err(|e| anyhow!("Failed to commit transaction: {}", e))?;
            diagnostics::log_info!("✅ File(s) copied successfully");
            Ok(())
        }
        Err(e) => {
            // Rollback on error
            diagnostics::log_debug!("Error occurred, rolling back...");
            fs.rollback().await
                .map_err(|rollback_err| anyhow!("Copy failed and rollback also failed: Copy error: {}, Rollback error: {}", e, rollback_err))?;
            Err(anyhow!("Copy operation failed: {}", e))
        }
    }
}
