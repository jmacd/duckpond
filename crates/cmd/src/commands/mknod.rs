// CLI command for creating dynamic nodes
use std::fs;
use tlogfs::factory::FactoryRegistry;
use anyhow::{Result, anyhow};
use diagnostics::*;

/// Create a dynamic node in the pond
/// 
/// This command operates on an existing pond via the provided Ship.
/// The Ship should already have a transaction started.
pub async fn mknod_command(mut ship: steward::Ship, factory_type: &str, path: &str, config_path: &str) -> Result<()> {
    log_debug!("Creating dynamic node in pond: {path} with factory: {factory_type}", path: path, factory_type: factory_type);

    // Read config file
    let config_bytes = fs::read(config_path)?;
    
    // Validate the factory and configuration
    FactoryRegistry::validate_config(factory_type, &config_bytes)
        .map_err(|e| anyhow!("Invalid configuration for factory '{}': {}", factory_type, e))?;
    
    mknod_impl(&mut ship, path, factory_type, config_bytes).await
}

async fn mknod_impl(ship: &mut steward::Ship, path: &str, factory_type: &str, config_bytes: Vec<u8>) -> Result<()> {
    // Get the data filesystem from ship
    let fs = ship.data_fs();
    
    // Perform operation
    let operation_result: Result<(), anyhow::Error> = async {
        let root = fs.root().await?;
        let _node_path = root.create_dynamic_directory_path(
            path,
            factory_type,
            config_bytes,
        ).await?;
        Ok(())
    }.await;

    match operation_result {
        Ok(()) => {
	    ship.commit_transaction().await
		.map_err(|e| anyhow!("Failed to commit transaction: {}", e))?;
            Ok(())
        }
        Err(e) => {
            Err(anyhow!("Failed to create dynamic node: {}", e))
        }
    }
}
