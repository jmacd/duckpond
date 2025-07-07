use anyhow::{Result, anyhow};
use std::path::PathBuf;

use crate::common::get_pond_path_with_override;
use diagnostics::log_info;

pub async fn init_command() -> Result<()> {
    init_command_with_pond(None).await
}

pub async fn init_command_with_pond(pond_path: Option<PathBuf>) -> Result<()> {
    let pond_path = get_pond_path_with_override(pond_path)?;
    let pond_path_display = pond_path.display().to_string();
    
    log_info!("Initializing pond at: {pond_path}", pond_path: pond_path_display);

    // Check if pond already exists by checking for data directory
    let data_path = steward::get_data_path(&pond_path);
    if data_path.exists() {
        // Additional check: see if there's actually a Delta table there
        let data_path_str = data_path.to_string_lossy().to_string();
        let delta_manager = tlogfs::DeltaTableManager::new();
        if delta_manager.get_table(&data_path_str).await.is_ok() {
            return Err(anyhow!("Pond already exists"));
        }
    }

    // Create steward Ship instance (this will initialize both filesystems)
    let _ship = steward::Ship::new(&pond_path).await
        .map_err(|e| anyhow!("Failed to initialize pond: {}", e))?;

    log_info!("Pond initialized successfully");
    Ok(())
}
