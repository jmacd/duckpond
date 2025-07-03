use anyhow::{Result, anyhow};
use std::path::PathBuf;

use crate::common::get_pond_path_with_override;
use diagnostics::log_info;

pub async fn init_command() -> Result<()> {
    init_command_with_pond(None).await
}

pub async fn init_command_with_pond(pond_path: Option<PathBuf>) -> Result<()> {
    let store_path = get_pond_path_with_override(pond_path)?;
    let store_path_str = store_path.to_string_lossy();

    let store_path_display = store_path.display().to_string();
    log_info!("Initializing pond at: {store_path}", store_path: store_path_display);

    // Check if pond already exists
    let delta_manager = tinylogfs::DeltaTableManager::new();
    if delta_manager.get_table(&store_path_str).await.is_ok() {
        return Err(anyhow!("Pond already exists"));
    }

    // Create directory and initialize
    std::fs::create_dir_all(&store_path)?;
    tinylogfs::create_oplog_table(&store_path_str).await?;

    log_info!("Pond initialized successfully");
    Ok(())
}
