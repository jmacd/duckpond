use diagnostics;
use anyhow::{Result, anyhow};

async fn get_entry_type_for_file(format: &str) -> Result<tinyfs::EntryType> {
    let entry_type = match format {
        "data" => Ok(tinyfs::EntryType::FileData),
        "table" => {
            Ok(tinyfs::EntryType::FileTable)
        }
        "series" => {
            Ok(tinyfs::EntryType::FileSeries)
        }
        _ => {
            Err(anyhow!("Invalid format '{}'", format))
        }
    };

    entry_type
}

// STREAMING COPY: Copy multiple files to directory using proper context
async fn copy_files_to_directory(
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
        
        copy_single_file_to_directory_with_name(source, dest_wd, source_filename, format).await?;
    }
    Ok(())
}

// Copy a single file to a directory using the provided working directory context
async fn copy_single_file_to_directory_with_name(
    file_path: &str,
    dest_wd: &tinyfs::WD,
    filename: &str,
    format: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::fs::File;
    use tokio::io::AsyncWriteExt;
    
    // Determine entry type based on format flag
    let entry_type = get_entry_type_for_file(format).await?;
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
/// Uses scoped transactions for automatic commit/rollback handling.
pub async fn copy_command(mut ship: steward::Ship, sources: &[String], dest: &str, format: &str) -> Result<()> {
    // Add a unique marker to verify we're running the right code
    diagnostics::log_debug!("COPY_VERSION: scoped-transaction-v2.0");
    
    // Validate arguments
    if sources.is_empty() {
        return Err(anyhow!("At least one source file must be specified"));
    }

    // Clone data needed inside the closure
    let sources = sources.to_vec();
    let dest = dest.to_string();
    let format = format.to_string();

    // Use scoped transaction for the copy operation
    ship.with_data_transaction(
        vec!["copy".to_string(), dest.clone()],
        |_tx, fs| Box::pin(async move {
            let root = fs.root().await
                .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

            // Use copy-specific destination resolution to handle trailing slashes properly
            let copy_result = root.resolve_copy_destination(&dest).await;
            let operation_result: Result<(), anyhow::Error> = match copy_result {
                Ok((dest_wd, dest_type)) => {
                    match dest_type {
                        tinyfs::CopyDestination::Directory | tinyfs::CopyDestination::ExistingDirectory => {
                            // Destination is a directory (either explicit with / or existing) - copy files into it
                            copy_files_to_directory(&sources, &dest_wd, &format).await
                                .map_err(|e| anyhow!("Copy to directory failed: {}", e))
                        }
                        tinyfs::CopyDestination::ExistingFile => {
                            // Destination is an existing file - not supported for copy operations
                            if sources.len() == 1 {
                                Err(anyhow!("Destination '{}' exists but is not a directory (cannot copy to existing file)", &dest))
                            } else {
                                Err(anyhow!("When copying multiple files, destination '{}' must be a directory", &dest))
                            }
                        }
                        tinyfs::CopyDestination::NewPath(name) => {
                            // Destination doesn't exist
                            if sources.len() == 1 {
                                // Single file to non-existent destination - use format flag only
                                let source = &sources[0];
                                
                                // Use the same logic as directory copying, just with the specific filename
                                if let Err(e) = copy_single_file_to_directory_with_name(&source, &dest_wd, &name, &format).await {
                                    return Err(steward::StewardError::DataInit(
                                        tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(format!("Failed to copy file: {}", e)))
                                    ));
                                }
                                
                                diagnostics::log_info!("Copied {source} to {name}", source: source, name: name);
                                Ok(())
                            } else {
                                Err(anyhow!("When copying multiple files, destination '{}' must be an existing directory", &dest))
                            }
                        }
                    }
                }
                Err(e) => {
                    Err(anyhow!("Failed to resolve destination '{}': {}", &dest, e))
                }
            };
            
            // Convert operation result to StewardError for scoped transaction
            operation_result.map_err(|e| steward::StewardError::DataInit(
                tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string()))
            ))
        })
    ).await.map_err(|e| anyhow!("Copy operation failed: {}", e))?;
    
    diagnostics::log_info!("✅ File(s) copied successfully");
    Ok(())
}