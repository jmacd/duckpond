use diagnostics;
use anyhow::{Result, anyhow};

// EXPERIMENTAL PARQUET: Simple detection and conversion functions
fn should_convert_to_parquet(source_path: &str, format: &str) -> bool {
    let result = match format {
        "auto" => false, // Auto mode: never convert, just detect entry type 
        "parquet" => source_path.to_lowercase().ends_with(".csv"), // Only convert CSV to Parquet
        "series" => source_path.to_lowercase().ends_with(".csv"), // Convert CSV to Parquet for FileSeries
        _ => false
    };
    let result_str = format!("{}", result);
    diagnostics::log_debug!("should_convert_to_parquet result", source_path: source_path, format: format, result: result_str);
    result
}

fn get_entry_type_for_file(source_path: &str, format: &str) -> tinyfs::EntryType {
    let entry_type = match format {
        "auto" => {
            // Auto-detect based on file extension
            if source_path.to_lowercase().ends_with(".parquet") {
                tinyfs::EntryType::FileTable
            } else {
                tinyfs::EntryType::FileData
            }
        },
        "parquet" => tinyfs::EntryType::FileTable, // Force FileTable for explicit parquet format
        "series" => tinyfs::EntryType::FileSeries,  // Force FileSeries for explicit series format
        _ => tinyfs::EntryType::FileData
    };
    let entry_type_str = format!("{:?}", entry_type);
    diagnostics::log_debug!("get_entry_type_for_file decision", source_path: source_path, format: format, entry_type: entry_type_str);
    entry_type
}

async fn try_convert_csv_to_parquet(source_path: &str) -> Result<Vec<u8>> {
    use arrow_csv::{ReaderBuilder, reader::Format};
    use parquet::arrow::ArrowWriter;
    use std::io::{Cursor, Seek};
    use std::sync::Arc;
    
    let mut file = std::fs::File::open(source_path)
        .map_err(|e| anyhow!("Failed to open CSV file: {}", e))?;
    
    // Step 1: Infer schema
    let format = Format::default().with_header(true);
    let (schema, _) = format.infer_schema(&mut file, Some(100))
        .map_err(|e| anyhow!("Failed to infer CSV schema: {}", e))?;
    
    // Step 2: Rewind file and read data
    file.rewind()
        .map_err(|e| anyhow!("Failed to rewind CSV file: {}", e))?;
    
    let mut csv_reader = ReaderBuilder::new(Arc::new(schema))
        .with_format(format)
        .build(file)
        .map_err(|e| anyhow!("Failed to create CSV reader: {}", e))?;
    
    let batch = csv_reader.next().transpose()
        .map_err(|e| anyhow!("Failed to read CSV batch: {}", e))?
        .ok_or_else(|| anyhow!("Empty CSV file"))?;
    
    // Step 3: Convert to Parquet
    let mut buffer = Vec::new();
    {
        let cursor = Cursor::new(&mut buffer);
        let mut writer = ArrowWriter::try_new(cursor, batch.schema(), None)
            .map_err(|e| anyhow!("Failed to create Parquet writer: {}", e))?;
        writer.write(&batch)
            .map_err(|e| anyhow!("Failed to write Parquet data: {}", e))?;
        writer.close()
            .map_err(|e| anyhow!("Failed to close Parquet writer: {}", e))?;
    }
    Ok(buffer)
}

async fn copy_file_to_destination(
    ship: &steward::Ship,
    file_path: &str,
    destination: &str,
    format: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::fs::File;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use std::pin::Pin;
    
    // For file:series format, copy TO the destination path directly (append to series)
    // For other formats, copy INTO the destination as a directory 
    let dest_path = if format == "series" {
        destination.to_string() // Direct path for file:series append
    } else {
        // Get destination filename - keep original name regardless of format conversion
        let source_filename = std::path::Path::new(file_path)
            .file_name()
            .ok_or("Invalid file path")?
            .to_str()
            .ok_or("Invalid filename")?;

        // Keep original filename - don't change extension even when converting format
        format!("{}/{}", destination.trim_end_matches('/'), source_filename)
    };
    
    // Determine entry type based on format flag, NOT filename
    let entry_type = if format == "parquet" {
        tinyfs::EntryType::FileTable // Explicit parquet format = table type
    } else {
        get_entry_type_for_file(file_path, format) // Auto-detect from source file
    };
    
    let entry_type_str = format!("{:?}", entry_type);
    let convert_to_parquet = should_convert_to_parquet(file_path, format);
    diagnostics::log_debug!("copy_file_to_destination", 
        source_path: file_path, 
        dest_path: dest_path, 
        format: format, 
        entry_type: entry_type_str,
        convert_to_parquet: convert_to_parquet
    );
    
    // Handle different scenarios with streaming
    if should_convert_to_parquet(file_path, format) {
        // EXPERIMENTAL PARQUET: CSV to Parquet conversion (still uses memory for conversion)
        diagnostics::log_debug!("copy Taking CSV-to-Parquet conversion path for {file_path}", file_path: file_path);
        let parquet_data = try_convert_csv_to_parquet(file_path).await
            .map_err(|e| format!("CSV to Parquet conversion failed: {}", e))?;
        
        // Get TinyFS working directory and create/append to file with conversion data  
        let root = ship.data_fs().root().await
            .map_err(|e| format!("Failed to get root directory: {}", e))?;

        // Use async_writer_path_with_type for both creation and appending
        let mut writer = root.async_writer_path_with_type(&dest_path, entry_type).await
            .map_err(|e| format!("Failed to get writer for {}: {}", dest_path, e))?;
        
        use tokio::io::AsyncWriteExt;
        writer.write_all(&parquet_data).await
            .map_err(|e| format!("Failed to write converted data: {}", e))?;
        writer.shutdown().await
            .map_err(|e| format!("Failed to complete write: {}", e))?;
    } else {
        // STREAMING PATH: Copy file using async streaming to avoid loading into memory
        diagnostics::log_debug!("copy Taking streaming path for {file_path} with entry_type={entry_type}", file_path: file_path, entry_type: entry_type);
        let root = ship.data_fs().root().await
            .map_err(|e| format!("Failed to get root directory: {}", e))?;
        
        // Open source file for streaming read
        let mut source_file = File::open(file_path).await
            .map_err(|e| format!("Failed to open source file {}: {}", file_path, e))?;
        
        // Create destination file with streaming writer
        let mut dest_writer: Pin<Box<dyn tokio::io::AsyncWrite + Send>> = root.async_writer_path_with_type(&dest_path, entry_type).await
            .map_err(|e| format!("Failed to create destination file {}: {}", dest_path, e))?;
        
        // Stream copy with buffered chunks to avoid loading entire file
        const BUFFER_SIZE: usize = 64 * 1024; // 64KB buffer for efficient streaming
        let mut buffer = vec![0; BUFFER_SIZE];
        
        loop {
            let bytes_read = source_file.read(&mut buffer).await
                .map_err(|e| format!("Failed to read from source file: {}", e))?;
            
            if bytes_read == 0 {
                break; // EOF reached
            }
            
            dest_writer.write_all(&buffer[..bytes_read]).await
                .map_err(|e| format!("Failed to write to destination file: {}", e))?;
        }
        
        // Important: Properly close/shutdown the writer to complete the write operation  
        dest_writer.shutdown().await
            .map_err(|e| format!("Failed to complete file write: {}", e))?;
    }

    diagnostics::log_info!("Streamed {file_path} to {dest_path}", file_path: file_path, dest_path: dest_path);
    Ok(())
}

// STREAMING COPY: Copy multiple files to directory using streaming interface
async fn copy_files_to_directory(
    ship: &steward::Ship,
    sources: &[String],
    destination: &str,
    format: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    for source in sources {
        copy_file_to_destination(ship, source, destination, format).await?;
    }
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
                    copy_files_to_directory(&ship, sources, dest, format).await
                        .map_err(|e| anyhow!("Copy to directory failed: {}", e))
                }
                tinyfs::CopyDestination::ExistingFile => {
                    // Destination is an existing file
                    if sources.len() == 1 && format == "series" {
                        // Special case: Allow appending to existing file:series when format is explicitly "series"
                        let source = &sources[0];
                        diagnostics::log_debug!("Appending to existing file:series with --format series: {dest}", dest: dest);
                        
                        copy_file_to_destination(&ship, source, dest, format).await
                            .map_err(|e| anyhow!("Failed to append to file:series: {}", e))
                    } else if sources.len() == 1 {
                        Err(anyhow!("Destination '{}' exists but is not a directory (cannot copy to existing file)", dest))
                    } else {
                        Err(anyhow!("When copying multiple files, destination '{}' must be a directory", dest))
                    }
                }
                tinyfs::CopyDestination::NewPath(name) => {
                    // Destination doesn't exist
                    if sources.len() == 1 {
                        // Single file to non-existent destination - use streaming copy
                        let source = &sources[0];
                        
                        // Determine format - auto-detect .series destinations
                        let effective_format = if name.to_lowercase().ends_with(".series") && format == "auto" {
                            "series" // Auto-detect .series destination as FileSeries
                        } else {
                            format
                        };
                        
                        // EXPERIMENTAL PARQUET: Handle format conversion for single file
                        if effective_format == "parquet" || effective_format == "series" {
                            if source.to_lowercase().ends_with(".csv") {
                                let parquet_content = try_convert_csv_to_parquet(source).await
                                    .map_err(|e| anyhow!("CSV to Parquet conversion failed: {}", e))?;
                                
                                let entry_type = if effective_format == "series" {
                                    tinyfs::EntryType::FileSeries
                                } else {
                                    tinyfs::EntryType::FileTable
                                };
                                
                                tinyfs::async_helpers::convenience::create_file_path_with_type(&dest_wd, &name, &parquet_content, entry_type).await
                                    .map_err(|e| anyhow!("Failed to create {} file '{}': {}", entry_type.as_str(), name, e))?;
                            } else {
                                return Err(anyhow!("EXPERIMENTAL: Only .csv files supported for --format={}, got: {}", effective_format, source));
                            }
                        } else {
                            // STREAMING PATH: Use async streaming for regular file copy
                            use tokio::fs::File;
                            use tokio::io::{AsyncReadExt, AsyncWriteExt};
                            use std::pin::Pin;
                            
                            let entry_type = get_entry_type_for_file(source, effective_format);
                            
                            // Open source file for streaming read
                            let mut source_file = File::open(source).await
                                .map_err(|e| anyhow!("Failed to open source file {}: {}", source, e))?;
                            
                            // Create destination file with streaming writer
                            let mut dest_writer: Pin<Box<dyn tokio::io::AsyncWrite + Send>> = dest_wd.async_writer_path_with_type(&name, entry_type).await
                                .map_err(|e| anyhow!("Failed to create destination file {}: {}", name, e))?;
                            
                            // Stream copy with buffered chunks
                            const BUFFER_SIZE: usize = 64 * 1024; // 64KB buffer
                            let mut buffer = vec![0; BUFFER_SIZE];
                            
                            loop {
                                let bytes_read = source_file.read(&mut buffer).await
                                    .map_err(|e| anyhow!("Failed to read from source file: {}", e))?;
                                
                                if bytes_read == 0 {
                                    break; // EOF reached
                                }
                                
                                dest_writer.write_all(&buffer[..bytes_read]).await
                                    .map_err(|e| anyhow!("Failed to write to destination file: {}", e))?;
                            }
                            
                            // Important: Properly close/shutdown the writer to complete the write operation
                            dest_writer.shutdown().await
                                .map_err(|e| anyhow!("Failed to complete file write: {}", e))?;
                        }
                        
                        diagnostics::log_info!("Streamed {source} to {name}", source: source, name: name);
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
