use anyhow::{Result, anyhow};
use std::path::Path;

use crate::common::get_pond_path;

pub async fn copy_command(sources: &[String], dest: &str) -> Result<()> {
    // Validate arguments
    if sources.is_empty() {
        return Err(anyhow!("At least one source file must be specified"));
    }

    let store_path = get_pond_path()?;
    let store_path_str = store_path.to_string_lossy();

    // Create filesystem
    let fs = tinylogfs::create_oplog_fs(&store_path_str).await?;
    let root = fs.root().await?;

    match sources.len() {
        1 => {
            // Single source file - two cases:
            // (a) dest is a specific file path, or
            // (b) dest is an existing directory
            let source = &sources[0];
            
            // Read source file
            let content = std::fs::read(source)
                .map_err(|e| anyhow!("Failed to read '{}': {}", source, e))?;

            // Try to open dest as directory first
            match root.open_dir_path(dest).await {
                Ok(dest_dir) => {
                    // Case (b): dest is existing directory, use source basename
                    let source_path = Path::new(source);
                    let filename = source_path.file_name()
                        .ok_or_else(|| anyhow!("Cannot determine filename from source path: {}", source))?
                        .to_string_lossy();
                    
                    println!("Copying '{}' to pond directory '{}' as '{}'...", source, dest, filename);
                    dest_dir.create_file_path(&*filename, &content).await?;
                }
                Err(tinyfs::Error::NotFound(_)) => {
                    // Case (a): dest doesn't exist, create file with this name
                    println!("Copying '{}' to pond as '{}'...", source, dest);
                    root.create_file_path(dest, &content).await?;
                }
                Err(tinyfs::Error::NotADirectory(_)) => {
                    // dest exists but is a file, not a directory - this is an error
                    return Err(anyhow!("Destination '{}' exists but is not a directory", dest));
                }
                Err(e) => {
                    // Other errors
                    return Err(anyhow!("Error accessing destination '{}': {}", dest, e));
                }
            }
        }
        _ => {
            // Multiple source files - dest must be an existing directory
            let dest_dir = match root.open_dir_path(dest).await {
                Ok(dir) => dir,
                Err(tinyfs::Error::NotFound(_)) => {
                    return Err(anyhow!("When copying multiple files, destination '{}' must be an existing directory", dest));
                }
                Err(tinyfs::Error::NotADirectory(_)) => {
                    return Err(anyhow!("Destination '{}' exists but is not a directory", dest));
                }
                Err(e) => {
                    return Err(anyhow!("Error accessing destination '{}': {}", dest, e));
                }
            };

            println!("Copying {} files to pond directory '{}'...", sources.len(), dest);

            // Copy each source file
            for source in sources {
                let content = std::fs::read(source)
                    .map_err(|e| anyhow!("Failed to read '{}': {}", source, e))?;

                let source_path = Path::new(source);
                let filename = source_path.file_name()
                    .ok_or_else(|| anyhow!("Cannot determine filename from source path: {}", source))?
                    .to_string_lossy();

                println!("  Copying '{}' as '{}'", source, filename);
                dest_dir.create_file_path(&*filename, &content).await?;
            }
        }
    }

    // Commit all changes in a single transaction
    fs.commit().await?;

    println!("✅ File(s) copied successfully");
    Ok(())
}
