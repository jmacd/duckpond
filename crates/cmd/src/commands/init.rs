use anyhow::{Result, anyhow};

use crate::common::ShipContext;
use diagnostics::*;

/// Initialize a new pond at the specified path
/// 
/// This is the only command that doesn't receive a Ship since it creates one.
pub async fn init_command(ship_context: &ShipContext) -> Result<()> {
    let pond_path = ship_context.resolve_pond_path()?;
    let pond_path_display = pond_path.display().to_string();
    
    info!("Initializing pond at: {pond_path_display}");

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
    let _ship = ship_context.initialize_new_pond().await?;

    log_info!("Pond initialized successfully with transaction #1");
    Ok(())
}
