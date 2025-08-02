// CLI command for creating dynamic nodes
use std::fs;
use tlogfs::hostmount::HostmountConfig;
use anyhow::{Result, anyhow};
use diagnostics::*;

/// Create a dynamic node in the pond
/// 
/// This command operates on an existing pond via the provided Ship.
/// The Ship should already have a transaction started.
pub async fn mknod_command(mut ship: steward::Ship, factory_type: &str, path: &str, config_path: &str) -> Result<()> {
    log_debug!("Creating dynamic node in pond: {path} with factory: {factory_type}", path: path, factory_type: factory_type);

    match factory_type {
        "hostmount" => {
            mknod_hostmount_impl(&mut ship, path, config_path).await
        }
        _ => {
            Err(anyhow!("Unsupported factory type: {}. Currently supported: hostmount", factory_type))
        }
    }
}

async fn mknod_hostmount_impl(ship: &mut steward::Ship, path: &str, config_path: &str) -> Result<()> {
    // Read and validate config YAML
    let config_bytes = fs::read(config_path)?;
    let config: HostmountConfig = serde_yaml::from_slice(&config_bytes)?;
    
    // Validate host directory exists
    if !config.directory.exists() {
        return Err(anyhow!("Host directory does not exist: {}", config.directory.display()));
    }

    // Get the data filesystem from ship
    let fs = ship.data_fs();
    
    // Perform operation
    let operation_result: Result<(), anyhow::Error> = async {
        let root = fs.root().await?;
        let _node_path = root.create_dynamic_directory_path(
            path,
            "hostmount",
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
            Err(anyhow!("Failed to create hostmount directory: {}", e))
        }
    }
	
}
