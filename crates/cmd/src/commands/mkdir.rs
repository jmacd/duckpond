use anyhow::Result;

use crate::common::get_pond_path;
use diagnostics::{log_info, log_debug};

pub async fn mkdir_command(path: &str) -> Result<()> {
    let store_path = get_pond_path()?;
    let store_path_str = store_path.to_string_lossy();

    log_debug!("Creating directory in pond: {path}", path: path);

    // Create filesystem and create directory
    let fs = tinylogfs::create_oplog_fs(&store_path_str).await?;
    let root = fs.root().await?;
    root.create_dir_path(path).await?;
    fs.commit().await?;

    log_info!("Directory created successfully: {path}", path: path);
    Ok(())
}
