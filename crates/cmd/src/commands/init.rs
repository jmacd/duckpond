use anyhow::{Result, anyhow};

use crate::common::ShipContext;
use diagnostics::*;
use steward;

/// Initialize a new pond at the specified path
/// 
/// This is the only command that doesn't receive a Ship since it creates one.
pub async fn init_command(ship_context: &ShipContext) -> Result<()> {
    let pond_path = ship_context.resolve_pond_path()?;
    let pond_path_display = pond_path.display().to_string();
    
    info!("Initializing pond at: {pond_path_display}");

    // Check if pond already exists by trying to open it
    // If it opens successfully, the pond already exists
    if let Ok(_existing_ship) = steward::Ship::open_existing_pond(&pond_path).await {
        return Err(anyhow!("Pond already exists"));
    }

    // Pond doesn't exist, so create a new one
    // This creates both the filesystem infrastructure AND the initial /txn/1 transaction
    let _ship = ship_context.initialize_new_pond().await?;

    log_info!("Pond initialized successfully with transaction #1");
    Ok(())
}
