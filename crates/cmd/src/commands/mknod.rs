// CLI command for creating dynamic nodes
use std::fs;
use tlogfs::factory::FactoryRegistry;
use anyhow::{Result, anyhow};
use diagnostics::*;

/// Create a dynamic node in the pond
/// 
/// This command operates on an existing pond via the provided Ship.
/// Create dynamic node using scoped transaction pattern
pub async fn mknod_command(mut ship: steward::Ship, factory_type: &str, path: &str, config_path: &str) -> Result<()> {
    log_debug!("Creating dynamic node in pond: {path} with factory: {factory_type}", path: path, factory_type: factory_type);

    // Read config file
    let config_bytes = fs::read(config_path)?;
    
    // Validate the factory and configuration
    FactoryRegistry::validate_config(factory_type, &config_bytes)
        .map_err(|e| anyhow!("Invalid configuration for factory '{}': {}", factory_type, e))?;
    
    // Use scoped transaction for mknod operation
    let factory_type = factory_type.to_string();
    let path = path.to_string();
    
    ship.with_data_transactionrtansac(
        vec!["mknod".to_string(), factory_type.clone()],
        move |_tx, fs| Box::pin(async move {
            mknod_impl(&fs, &path, &factory_type, config_bytes).await
                .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string()))))
        })
    ).await
    .map_err(|e| anyhow::anyhow!("mknod operation failed: {}", e))
}

async fn mknod_impl(fs: &tinyfs::FS, path: &str, factory_type: &str, config_bytes: Vec<u8>) -> Result<()> {
    let root = fs.root().await?;
    
    // Check what the factory supports and use the appropriate creation method
    let factory = tlogfs::factory::FactoryRegistry::get_factory(factory_type)
        .ok_or_else(|| anyhow!("Unknown factory type: {}", factory_type))?;
    
    if factory.create_directory_with_context.is_some() && factory.create_file_with_context.is_none() {
        // Factory only supports directories
        let _node_path = root.create_dynamic_directory_path(
            path,
            factory_type,
            config_bytes,
        ).await?;
    } else if factory.create_file_with_context.is_some() && factory.create_directory_with_context.is_none() {
        // Factory only supports files
        let _node_path = root.create_dynamic_file_path(
            path,
            tinyfs::EntryType::FileTable, // SQL-derived files are table-like
            factory_type,
            config_bytes,
        ).await?;
    } else if factory.create_directory_with_context.is_some() && factory.create_file_with_context.is_some() {
        // Factory supports both - default to directory for backward compatibility
        let _node_path = root.create_dynamic_directory_path(
            path,
            factory_type,
            config_bytes,
        ).await?;
    } else {
        return Err(anyhow!("Factory '{}' does not support creating directories or files", factory_type));
    }
    
    Ok(())
}
