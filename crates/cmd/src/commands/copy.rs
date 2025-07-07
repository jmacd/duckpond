use anyhow::{Result, anyhow};
use std::path::{Path, PathBuf};
use tinyfs::WD;

use crate::common::create_ship;

async fn copy_files_to_directory(sources: &[String], dest: &str, dest_wd: &WD) -> Result<(), tinyfs::Error> {
    // Collect all file operations for batch processing
    let mut file_operations: Vec<(String, Vec<u8>)> = Vec::new();
    let source_count = sources.len();
    diagnostics::log_info!("Copying {source_count} file(s) to pond directory '{dest}'...", source_count: source_count, dest: dest);

    // Read all source files first
    for source in sources.iter() {
        let content = std::fs::read(source)
            .map_err(|e| tinyfs::Error::Other(format!("Failed to read '{}': {}", source, e)))?;

        let source_path = Path::new(source);
        let filename = source_path.file_name()
            .ok_or_else(|| tinyfs::Error::Other(format!("Cannot determine filename from source path: {}", source)))?
            .to_string_lossy()
            .to_string();

        let source_bound = source;
        let filename_bound = &filename;
        diagnostics::log_info!("  Preparing '{source}' as '{filename}'", source: source_bound, filename: filename_bound);
        file_operations.push((filename, content));
    }

    // Create all files in a batch (this should be atomic)
    let op_count = file_operations.len();
    diagnostics::log_info!("Creating {op_count} files atomically...", op_count: op_count);
    for (i, (filename, content)) in file_operations.iter().enumerate() {
        let batch_num = i + 1;
        let total = file_operations.len();
        let filename_bound = filename;
        diagnostics::log_info!("  BATCH: Creating file {batch_num}/{total}: '{filename}'", 
                               batch_num: batch_num, total: total, filename: filename_bound);
        // Create file in the destination directory
        dest_wd.create_file_path(&filename, &content).await?;
        let filename_bound2 = filename;
        diagnostics::log_info!("  BATCH: File '{filename}' created successfully", filename: filename_bound2);
    }
    
    Ok(())
}

pub async fn copy_command(sources: &[String], dest: &str) -> Result<()> {
    copy_command_with_pond(sources, dest, None).await
}

pub async fn copy_command_with_pond(sources: &[String], dest: &str, pond_path: Option<PathBuf>) -> Result<()> {
    // Add a unique marker to verify we're running the right code
    diagnostics::log_debug!("COPY_VERSION: transaction-control-v1.0");
    
    // Validate arguments
    if sources.is_empty() {
        return Err(anyhow!("At least one source file must be specified"));
    }

    // Create steward Ship instance
    diagnostics::log_debug!("Creating steward Ship...");
    let mut ship = create_ship(pond_path).await?;
    
    // Get the data filesystem from ship
    let fs = ship.data_fs();
    
    // Begin explicit transaction
    diagnostics::log_debug!("Beginning transaction...");
    fs.begin_transaction().await
        .map_err(|e| anyhow!("Failed to begin transaction: {}", e))?;

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
                    copy_files_to_directory(sources, dest, &dest_wd).await
                        .map_err(|e| anyhow!("Copy to directory failed: {}", e))
                }
                tinyfs::CopyDestination::ExistingFile => {
                    // Destination is an existing file
                    if sources.len() == 1 {
                        Err(anyhow!("Destination '{}' exists but is not a directory (cannot copy to existing file)", dest))
                    } else {
                        Err(anyhow!("When copying multiple files, destination '{}' must be a directory", dest))
                    }
                }
                tinyfs::CopyDestination::NewPath(name) => {
                    // Destination doesn't exist
                    if sources.len() == 1 {
                        // Single file to non-existent destination - create file with dest name
                        let source = &sources[0];
                        let content = std::fs::read(source)
                            .map_err(|e| anyhow!("Failed to read '{}': {}", source, e))?;

                        dest_wd.create_file_path(&name, &content).await
                            .map_err(|e| anyhow!("Failed to create file '{}': {}", name, e))?;
                        
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
