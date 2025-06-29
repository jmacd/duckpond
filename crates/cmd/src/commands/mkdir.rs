use anyhow::Result;

use crate::common::get_pond_path;

pub async fn mkdir_command(path: &str) -> Result<()> {
    let store_path = get_pond_path()?;
    let store_path_str = store_path.to_string_lossy();

    println!("Creating directory '{}' in pond...", path);

    // Create filesystem and create directory
    let fs = tinylogfs::create_oplog_fs(&store_path_str).await?;
    let root = fs.root().await?;
    root.create_dir_path(path).await?;
    fs.commit().await?;

    println!("✅ Directory created successfully");
    Ok(())
}
