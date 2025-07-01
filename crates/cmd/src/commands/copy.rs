use anyhow::{Result, anyhow};
use std::path::Path;
use tinyfs::{Lookup, NodeType, WD};

use crate::common::get_pond_path;

async fn copy_files_to_directory(sources: &[String], dest: &str, dest_wd: &WD) -> Result<(), tinyfs::Error> {
    // Collect all file operations for batch processing
    let mut file_operations: Vec<(String, Vec<u8>)> = Vec::new();
    
    println!("Copying {} file(s) to pond directory '{}'...", sources.len(), dest);

    // Read all source files first
    for source in sources.iter() {
        let content = std::fs::read(source)
            .map_err(|e| tinyfs::Error::Other(format!("Failed to read '{}': {}", source, e)))?;

        let source_path = Path::new(source);
        let filename = source_path.file_name()
            .ok_or_else(|| tinyfs::Error::Other(format!("Cannot determine filename from source path: {}", source)))?
            .to_string_lossy()
            .to_string();

        println!("  Preparing '{}' as '{}'", source, filename);
        file_operations.push((filename, content));
    }

    // Create all files in a batch (this should be atomic)
    println!("Creating {} files atomically...", file_operations.len());
    for (i, (filename, content)) in file_operations.iter().enumerate() {
        println!("  BATCH: Creating file {}/{}: '{}'", i + 1, file_operations.len(), filename);
        // Create file in the destination directory
        dest_wd.create_file_path(&filename, &content).await?;
        println!("  BATCH: File '{}' created successfully", filename);
    }
    
    Ok(())
}

pub async fn copy_command(sources: &[String], dest: &str) -> Result<()> {
    // Add a unique marker to verify we're running the right code
    println!("COPY_VERSION: transaction-control-v1.0");
    
    // Validate arguments
    if sources.is_empty() {
        return Err(anyhow!("At least one source file must be specified"));
    }

    let store_path = get_pond_path()?;
    let store_path_str = store_path.to_string_lossy();

    // Create filesystem
    println!("DEBUG: Creating filesystem...");
    let fs = tinylogfs::create_oplog_fs(&store_path_str).await?;
    
    // Begin explicit transaction
    println!("DEBUG: Beginning transaction...");
    fs.begin_transaction().await
        .map_err(|e| anyhow!("Failed to begin transaction: {}", e))?;

    println!("DEBUG: Checking pending operations after begin_transaction...");
    let has_pending = fs.has_pending_operations().await
        .map_err(|e| anyhow!("Failed to check pending operations: {}", e))?;
    println!("DEBUG: Has pending operations: {}", has_pending);

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
            println!("DEBUG: Copy operations completed, checking pending operations before commit...");
            let has_pending = fs.has_pending_operations().await
                .map_err(|e| anyhow!("Failed to check pending operations: {}", e))?;
            println!("DEBUG: Has pending operations before commit: {}", has_pending);
            
            // Commit all changes in a single atomic transaction
            println!("DEBUG: Committing transaction...");
            fs.commit().await
                .map_err(|e| anyhow!("Failed to commit transaction: {}", e))?;
            println!("✅ File(s) copied successfully");
            Ok(())
        }
        Err(e) => {
            // Rollback on error
            println!("DEBUG: Error occurred, rolling back...");
            fs.rollback().await
                .map_err(|rollback_err| anyhow!("Copy failed and rollback also failed: Copy error: {}, Rollback error: {}", e, rollback_err))?;
            Err(anyhow!("Copy operation failed: {}", e))
        }
    }
}
