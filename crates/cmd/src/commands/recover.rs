use anyhow::{Result, anyhow};

use crate::common::ShipContext;
use diagnostics::log_info;

/// Recover from crash by checking and restoring transaction metadata
pub async fn recover_command(ship_context: &ShipContext) -> Result<()> {
    let pond_path = ship_context.resolve_pond_path()?;
    let pond_path_display = pond_path.display().to_string();
    
    log_info!("Starting crash recovery at pond: {pond_path}", pond_path: pond_path_display);

    // Check if pond exists by checking for data directory
    let data_path = steward::get_data_path(&pond_path);
    if !data_path.exists() {
        return Err(anyhow!("Pond does not exist at path: {}", pond_path_display));
    }

    // Create steward Ship instance for recovery
    let mut ship = steward::Ship::open_existing_pond(&pond_path).await
        .map_err(|e| anyhow!("Failed to open pond for recovery: {}", e))?;

    // Call ship's recovery process
    let recovery_result = ship.recover().await;
    
    match recovery_result {
        Ok(result) => {
            log_info!("🔧 Recovery completed successfully. Recovered count: {recovered_count}, Was needed: {was_needed}", 
                     recovered_count: result.recovered_count, 
                     was_needed: result.was_needed);
            Ok(())
        }
        Err(e) => {
            Err(anyhow!("Recovery failed: {}", e))
        }
    }
}
