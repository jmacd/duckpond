use anyhow::{Result, anyhow};
use std::path::PathBuf;

use crate::common::get_pond_path_with_override;
use diagnostics::log_info;

pub async fn init_command_with_args(args: Vec<String>) -> Result<()> {
    init_command_with_pond_and_args(None, args).await
}

pub async fn init_command_with_pond_and_args(pond_path: Option<PathBuf>, args: Vec<String>) -> Result<()> {
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

    // Create steward Ship instance with complete initialization
    // This creates both the filesystem infrastructure AND the initial /txn/1 transaction
    let _ship = steward::Ship::initialize_new_pond(&pond_path, args).await
        .map_err(|e| anyhow!("Failed to initialize pond: {}", e))?;

    log_info!("Pond initialized successfully with transaction #1");
    Ok(())
}
