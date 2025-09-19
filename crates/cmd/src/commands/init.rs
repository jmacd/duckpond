use anyhow::{Result, anyhow};

use crate::common::ShipContext;
use log::info;

/// Initialize a new pond at the specified path
/// 
/// This is the only command that doesn't receive a Ship since it creates one.
pub async fn init_command(ship_context: &ShipContext) -> Result<()> {
    let pond_path = ship_context.resolve_pond_path()?;
    let pond_path_display = pond_path.display().to_string();
    
    info!("Initializing pond at: {pond_path_display}");

    // Check if pond already exists by looking for the control filesystem structure
    // A properly initialized pond should have both data and control directories
    // with Delta table metadata
    let data_path = pond_path.join("data");
    let control_path = pond_path.join("control");
    
    if data_path.exists() && control_path.exists() {
        // Check if these look like properly initialized Delta tables
        let data_log = data_path.join("_delta_log");
        let control_log = control_path.join("_delta_log");
        
        if data_log.exists() && control_log.exists() {
            return Err(anyhow!("Pond already exists"));
        }
    }

    // Pond doesn't exist, so create a new one
    // This creates both the filesystem infrastructure AND the initial /txn/1 transaction
    let _ship = ship_context.create_pond().await?;

    info!("Pond initialized successfully with transaction #1");
    Ok(())
}
