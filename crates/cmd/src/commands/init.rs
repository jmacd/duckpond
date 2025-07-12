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

    // Create steward Ship instance (this will initialize both filesystems)
    let mut ship = steward::Ship::new(&pond_path).await
        .map_err(|e| anyhow!("Failed to initialize pond: {}", e))?;

    // Explicitly create root directory via steward transaction
    // This ensures the root directory creation is recorded as transaction #1
    init_root_directory_via_steward(&mut ship, args).await
        .map_err(|e| anyhow!("Failed to initialize root directory: {}", e))?;

    log_info!("Pond initialized successfully with transaction #1");
    Ok(())
}

/// Initialize the root directory via steward transaction
/// This ensures the root directory creation is recorded as transaction #1
async fn init_root_directory_via_steward(ship: &mut steward::Ship, args: Vec<String>) -> Result<()> {
    log_info!("Creating root directory as transaction #1");
    
    // Begin transaction with command arguments
    ship.begin_transaction_with_args(args).await
        .map_err(|e| anyhow!("Failed to begin transaction: {}", e))?;
    
    // Access root directory - this will trigger its creation within the transaction
    let _root = ship.data_fs().root().await
        .map_err(|e| anyhow!("Failed to create root directory: {}", e))?;
    
    // Commit through steward - this creates both:
    // 1. The root directory operation in the data filesystem (transaction #1)
    // 2. The /txn/1 metadata file in the control filesystem
    ship.commit_transaction().await
        .map_err(|e| anyhow!("Failed to commit transaction: {}", e))?;
    
    log_info!("Root directory created and recorded as transaction #1");
    Ok(())
}
