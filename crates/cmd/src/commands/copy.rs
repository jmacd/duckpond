use anyhow::{Result, anyhow};
use std::path::{Path, PathBuf};
use tinyfs::{Lookup, NodeType, WD};

use crate::common::get_pond_path_with_override;

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

    let store_path = get_pond_path_with_override(pond_path)?;
    let store_path_str = store_path.to_string_lossy();

    // Create filesystem
    diagnostics::log_debug!("Creating filesystem...");
    let fs = tinylogfs::create_oplog_fs(&store_path_str).await?;
    
    // Begin explicit transaction
    diagnostics::log_debug!("Beginning transaction...");
    fs.begin_transaction().await
        .map_err(|e| anyhow!("Failed to begin transaction: {}", e))?;

    diagnostics::log_debug!("Checking pending operations after begin_transaction...");
    let has_pending = fs.has_pending_operations().await
        .map_err(|e| anyhow!("Failed to check pending operations: {}", e))?;
    diagnostics::log_debug!("Has pending operations: {has_pending}", has_pending: has_pending);

    let root = fs.root().await?;

    // Use in_path to resolve destination consistently
    let result = root.in_path(dest, |dest_wd, dest_lookup| async move {
        match dest_lookup {
            Lookup::Found(dest_node) => {
                // Destination exists - check if it's a directory
                let node_guard = dest_node.node.lock().await;
                match &node_guard.node_type {
                    NodeType::Directory(_) => {
                        drop(node_guard);
                        // Destination is an existing directory
                        copy_files_to_directory(sources, dest, &dest_wd).await?;
                    }
                    _ => {
                        drop(node_guard);
                        // Destination exists but is not a directory
                        if sources.len() == 1 {
                            return Err(tinyfs::Error::Other(format!("Destination '{}' exists but is not a directory (cannot copy to existing file)", dest)));
                        } else {
                            return Err(tinyfs::Error::Other(format!("When copying multiple files, destination '{}' must be a directory", dest)));
                        }
                    }
                }
            }
            Lookup::Empty(_dest_node) => {
                // Path ended with empty component (trailing slash) - this is a directory
                copy_files_to_directory(sources, dest, &dest_wd).await?;
            }
            Lookup::NotFound(_, name) => {
                    // Destination doesn't exist
                    if sources.len() == 1 {
                        // Single file to non-existent destination - create file with dest name
                        let source = &sources[0];
                        let content = std::fs::read(source)
                            .map_err(|e| tinyfs::Error::Other(format!("Failed to read '{}': {}", source, e)))?;

                        dest_wd.create_file_path(&name, &content).await?;
                    } else {
                        return Err(tinyfs::Error::Other(format!("When copying multiple files, destination '{}' must be an existing directory", dest)));
                    }
            }
        }
        Ok(())
    }).await;
    
    // Handle the result - rollback on error, commit on success
    match result {
        Ok(()) => {
            diagnostics::log_debug!("Copy operations completed, checking pending operations before commit...");
            let has_pending = fs.has_pending_operations().await
                .map_err(|e| anyhow!("Failed to check pending operations: {}", e))?;
            diagnostics::log_debug!("Has pending operations before commit: {has_pending}", has_pending: has_pending);
            
            // Commit all changes in a single atomic transaction
            diagnostics::log_debug!("Committing transaction...");
            fs.commit().await
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
