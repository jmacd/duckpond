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

        for source in sources.iter() {
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
    println!("DEBUG: copy_command ENTRY with sources: {:?}, dest: '{}'", sources, dest);
    
    // Validate arguments
    if sources.is_empty() {
        return Err(anyhow!("At least one source file must be specified"));
    }

    println!("DEBUG: copy_command called with sources: {:?}, dest: '{}'", sources, dest);
    println!("DEBUG: checking if dest == '/' : {}", dest == "/");

    println!("DEBUG: Getting pond path...");
    let store_path = get_pond_path()?;
    let store_path_str = store_path.to_string_lossy();
    println!("DEBUG: Pond path: {}", store_path_str);

    // Create filesystem
    println!("DEBUG: Creating oplog filesystem...");
    let fs = tinylogfs::create_oplog_fs(&store_path_str).await?;
    println!("DEBUG: Filesystem created, getting root...");
    let root = fs.root().await?;
    println!("DEBUG: Got filesystem root successfully");

    // Use in_path to resolve destination consistently
    println!("DEBUG: About to call in_path with dest: '{}'", dest);
    println!("DEBUG: Calling root.in_path() - this is where it might hang...");
    let result = root.in_path(dest, |dest_wd, dest_lookup| async move {
        println!("DEBUG: Inside in_path callback, lookup = {:?}", std::mem::discriminant(&dest_lookup));
        match dest_lookup {
            Lookup::Found(dest_node) => {
                println!("DEBUG: Found case - checking node type");
                // Destination exists - check if it's a directory
                println!("DEBUG: About to lock node for type check");
                let node_guard = dest_node.node.lock().await;
                println!("DEBUG: Node locked, checking type");
                match &node_guard.node_type {
                    NodeType::Directory(_) => {
                        println!("DEBUG: Node is directory, dropping lock");
                        drop(node_guard);
                        println!("DEBUG: Lock dropped, calling copy_files_to_directory");
                        // Destination is an existing directory
                        copy_files_to_directory(sources, dest, &dest_wd).await?;
                        println!("DEBUG: copy_files_to_directory returned");
                    }
                    _ => {
                        println!("DEBUG: Node is not directory");
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
                        println!("DEBUG: Reading source file: {}", source);
                        let content = std::fs::read(source)
                            .map_err(|e| tinyfs::Error::Other(format!("Failed to read '{}': {}", source, e)))?;

                        println!("DEBUG: Creating file '{}' in pond...", name);
                        dest_wd.create_file_path(&name, &content).await?;
                        println!("DEBUG: File '{}' created successfully", name);
                    } else {
                        return Err(tinyfs::Error::Other(format!("When copying multiple files, destination '{}' must be an existing directory", dest)));
                    }
            }
        }
        println!("DEBUG: About to return Ok(()) from in_path callback");
        Ok(())
    }).await;
    println!("DEBUG: in_path call completed with result: {:?}", result.is_ok());
    
    // Convert tinyfs::Error to anyhow::Error
    result.map_err(|e| anyhow!("Copy operation failed: {}", e))?;

    println!("DEBUG: About to commit filesystem changes...");
    // Commit all changes in a single transaction
    fs.commit().await?;
    println!("DEBUG: Filesystem commit completed successfully");

    println!("✅ File(s) copied successfully");
    Ok(())
}
