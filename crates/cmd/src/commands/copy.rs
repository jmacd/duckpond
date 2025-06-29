use anyhow::{Result, anyhow};

use crate::common::get_pond_path;

pub async fn copy_command(source: &str, dest: &str) -> Result<()> {
    let store_path = get_pond_path()?;
    let store_path_str = store_path.to_string_lossy();

    // Read source file
    let content = std::fs::read(source)
        .map_err(|e| anyhow!("Failed to read '{}': {}", source, e))?;

    println!("Copying '{}' to pond as '{}'...", source, dest);

    // Create filesystem and copy file
    let fs = tinylogfs::create_oplog_fs(&store_path_str).await?;
    let root = fs.root().await?;
    root.create_file_path(dest, &content).await?;
    fs.commit().await?;

    println!("✅ File copied successfully");
    Ok(())
}
