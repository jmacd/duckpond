use anyhow::{Result, anyhow};
use std::path::Path;
use tinyfs::{Lookup, NodeType, WD};

use crate::common::get_pond_path;

async fn copy_files_to_directory(sources: &[String], dest: &str, dest_wd: &WD) -> Result<(), tinyfs::Error> {
    if sources.len() == 1 {
        // Single file to existing directory - use source basename
        let source = &sources[0];
        let content = std::fs::read(source)
            .map_err(|e| tinyfs::Error::Other(format!("Failed to read '{}': {}", source, e)))?;

        let source_path = Path::new(source);
        let filename = source_path.file_name()
            .ok_or_else(|| tinyfs::Error::Other(format!("Cannot determine filename from source path: {}", source)))?
            .to_string_lossy();

        println!("Copying '{}' to pond directory '{}' as '{}'...", source, dest, filename);
        
        // Create file in the destination directory
        dest_wd.create_file_path(&*filename, &content).await?;
    } else {
        // Multiple files to existing directory
        println!("Copying {} files to pond directory '{}'...", sources.len(), dest);

        for source in sources {
            let content = std::fs::read(source)
                .map_err(|e| tinyfs::Error::Other(format!("Failed to read '{}': {}", source, e)))?;

            let source_path = Path::new(source);
            let filename = source_path.file_name()
                .ok_or_else(|| tinyfs::Error::Other(format!("Cannot determine filename from source path: {}", source)))?
                .to_string_lossy();

            println!("  Copying '{}' as '{}'", source, filename);
            
            // Create file in the destination directory
            dest_wd.create_file_path(&*filename, &content).await?;
        }
    }
    Ok(())
}

pub async fn copy_command(sources: &[String], dest: &str) -> Result<()> {
    // Validate arguments
    if sources.is_empty() {
        return Err(anyhow!("At least one source file must be specified"));
    }

    println!("DEBUG: copy_command called with sources: {:?}, dest: '{}'", sources, dest);
    println!("DEBUG: checking if dest == '/' : {}", dest == "/");

    let store_path = get_pond_path()?;
    let store_path_str = store_path.to_string_lossy();

    // Create filesystem
    let fs = tinylogfs::create_oplog_fs(&store_path_str).await?;
    let root = fs.root().await?;

    // Use in_path to resolve destination consistently
    println!("DEBUG: About to call in_path with dest: '{}'", dest);
    let result = root.in_path(dest, |dest_wd, dest_lookup| async move {
        println!("DEBUG: Inside in_path callback, lookup = {:?}", std::mem::discriminant(&dest_lookup));
        match dest_lookup {
            Lookup::Found(dest_node) => {
                println!("DEBUG: Found case");
                // Destination exists - check if it's a directory
                match &dest_node.node.lock().await.node_type {
                    NodeType::Directory(_) => {
                        // Destination is an existing directory
                        copy_files_to_directory(sources, dest, &dest_wd).await?;
                    }
                    _ => {
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
                println!("DEBUG: Empty case");
                // Path ended with empty component (trailing slash) - this is a directory
                copy_files_to_directory(sources, dest, &dest_wd).await?;
            }
            Lookup::NotFound(_, name) => {
                println!("DEBUG: NotFound case, name = '{}'", name);
                // Destination doesn't exist
                if sources.len() == 1 {
                    // Single file to non-existent destination - create file with dest name
                    let source = &sources[0];
                    let content = std::fs::read(source)
                        .map_err(|e| tinyfs::Error::Other(format!("Failed to read '{}': {}", source, e)))?;

                    println!("Copying '{}' to pond as '{}'...", source, name);
                    dest_wd.create_file_path(&name, &content).await?;
                } else {
                    return Err(tinyfs::Error::Other(format!("When copying multiple files, destination '{}' must be an existing directory", dest)));
                }
            }
        }
        println!("DEBUG: About to return Ok(()) from in_path callback");
        Ok(())
    }).await;
    println!("DEBUG: in_path call completed");
    // Convert tinyfs::Error to anyhow::Error
    result.map_err(|e| anyhow!("Copy operation failed: {}", e))?;

    // Commit all changes in a single transaction
    fs.commit().await?;

    println!("✅ File(s) copied successfully");
    Ok(())
}
