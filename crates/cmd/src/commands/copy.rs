use diagnostics;
use anyhow::{Result, anyhow};

fn get_entry_type_for_file(source_path: &str, format: &str) -> tinyfs::EntryType {
    let entry_type = match format {
        "parquet" => tinyfs::EntryType::FileTable, // Force FileTable for explicit parquet format
        "series" => tinyfs::EntryType::FileSeries,  // Force FileSeries for explicit series format
        _ => {
            // Default behavior: detect based on file extension only
            if source_path.to_lowercase().ends_with(".parquet") {
                tinyfs::EntryType::FileTable
            } else {
                tinyfs::EntryType::FileData
            }
        }
    };
    let entry_type_str = format!("{:?}", entry_type);
    diagnostics::log_debug!("get_entry_type_for_file decision", source_path: source_path, format: format, entry_type: entry_type_str);
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    
    // Use the provided filename instead of extracting from path
    let source_filename = filename;
    
    // Determine entry type based on format flag, NOT filename
    let entry_type = get_entry_type_for_file(file_path, format);
    let entry_type_str = format!("{:?}", entry_type);
    
    diagnostics::log_debug!("copy_single_file_to_directory", 
        source_path: file_path, 
        dest_filename: source_filename, 
        format: format, 
        entry_type: entry_type_str
    );
    
    // STREAMING PATH: Copy file using async streaming to avoid loading into memory
    diagnostics::log_debug!("copy Taking streaming path for {file_path}", file_path: file_path);
    
    // Special handling for FileSeries format
    if entry_type == tinyfs::EntryType::FileSeries {
        // For FileSeries, we need to read the file and store it properly with temporal metadata
        let mut source_file = File::open(file_path).await
            .map_err(|e| format!("Failed to open source file: {}", e))?;
        let mut file_content = Vec::new();
        source_file.read_to_end(&mut file_content).await
            .map_err(|e| format!("Failed to read source file: {}", e))?;
        
        copy_file_series_with_temporal_metadata(&file_content, dest_wd, source_filename).await?;
    } else {
        // Regular streaming copy for other entry types
        let mut source_file = File::open(file_path).await
            .map_err(|e| format!("Failed to open source file: {}", e))?;
        
        let mut dest_writer = dest_wd.async_writer_path_with_type(source_filename, entry_type).await
            .map_err(|e| format!("Failed to create destination writer: {}", e))?;
        
        // Stream copy with 64KB buffer for memory efficiency
        tokio::io::copy(&mut source_file, &mut dest_writer).await
            .map_err(|e| format!("Failed to stream file content: {}", e))?;
        
        dest_writer.shutdown().await
            .map_err(|e| format!("Failed to complete file write: {}", e))?;
    }
    
    diagnostics::log_info!("Copied {file_path} to directory as {source_filename}", file_path: file_path, source_filename: source_filename);
    Ok(())
}

// FileSeries copy with temporal metadata extraction
async fn copy_file_series_with_temporal_metadata(
    content: &[u8],
    dest_wd: &tinyfs::WD,
    filename: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    diagnostics::log_debug!("copy_file_series_with_temporal_metadata: Processing FileSeries {filename} with temporal metadata extraction", filename: filename);
    
    // For FileSeries, always use the create_series_from_batch path which extracts temporal metadata
    // This applies to both new files and subsequent versions (append-only store handles versioning)
    
    // Parse content as Parquet to extract temporal metadata
    use tokio_util::bytes::Bytes;
    let bytes = Bytes::from(content.to_vec());
    let reader_result = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes);
    
    match reader_result {
        Ok(reader_builder) => {
            let reader = reader_builder.build()
                .map_err(|e| format!("Failed to build Parquet reader: {}", e))?;
            
            // Read all batches
            let mut all_batches = Vec::new();
            for batch_result in reader {
                let batch = batch_result
                    .map_err(|e| format!("Failed to read Parquet batch: {}", e))?;
                all_batches.push(batch);
            }
            
            if all_batches.is_empty() {
                return Err("No data in Parquet file".into());
            }
            
            // Concatenate all batches
            let schema = all_batches[0].schema();
            let batch_refs: Vec<&arrow::record_batch::RecordBatch> = all_batches.iter().collect();
            let combined_batch = arrow::compute::concat_batches(&schema, batch_refs)
                .map_err(|e| format!("Failed to concatenate batches: {}", e))?;
            
            // Extract temporal metadata from the combined batch
            let (min_event_time, max_event_time) = 
                tlogfs::schema::extract_temporal_range_from_batch(&combined_batch, "timestamp")
                    .map_err(|e| format!("Failed to extract temporal metadata: {}", e))?;
            
            // Use WD layer to append to FileSeries with temporal metadata
            // This handles both new files and appends (versioning) automatically
            dest_wd.append_file_series_with_temporal_metadata(filename, content, min_event_time, max_event_time).await
                .map_err(|e| format!("Failed to append to FileSeries with temporal metadata: {}", e))?;
            
            diagnostics::log_info!("✅ FileSeries {filename} created/updated with temporal metadata", filename: filename);
            Ok(())
        }
        Err(e) => {
            Err(format!("Failed to parse Parquet data for temporal metadata extraction: {}", e).into())
        }
    }
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
                    // Destination is an existing file
                    if sources.len() == 1 && format == "series" {
                        // Special case: Allow appending to existing file:series when format is explicitly "series"
                        let source = &sources[0];
                        
                        diagnostics::log_debug!("Appending to existing file:series with --format series: {dest}", dest: dest);
                        
                        // Extract filename from the dest path for the specialized function
                        let dest_filename = std::path::Path::new(dest)
                            .file_name()
                            .ok_or_else(|| anyhow!("Invalid destination path"))?
                            .to_str()
                            .ok_or_else(|| anyhow!("Invalid destination filename"))?;
                        
                        copy_single_file_to_directory_with_name(&ship, source, &dest_wd, dest_filename, format).await
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
                        // Single file to non-existent destination - treat like copying to directory
                        let source = &sources[0];
                        
                        // Determine format - detect .series destinations
                        let effective_format = if name.to_lowercase().ends_with(".series") {
                            "series" // Detect .series destination as FileSeries
                        } else {
                            format
                        };
                        
                        // Use the same logic as directory copying, just with the specific filename
                        copy_single_file_to_directory_with_name(&ship, source, &dest_wd, &name, effective_format).await
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
