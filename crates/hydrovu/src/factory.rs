//! HydroVu executable factory
//!
//! This factory creates run configuration nodes for HydroVu data collection.
//! Configuration is stored as a file:data:dynamic node that can be:
//! - Read with `pond cat /path/to/config` 
//! - Executed with `pond run /path/to/config`

use tlogfs::factory::{ConfigFile, FactoryContext};
use tlogfs::TLogFSError;
use crate::HydroVuConfig;
use serde_json::Value;
use tinyfs::{FileHandle, Result as TinyFSResult};

/// Validate HydroVu configuration from YAML bytes
fn validate_hydrovu_config(config: &[u8]) -> TinyFSResult<Value> {
    let config_str = std::str::from_utf8(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid UTF-8: {}", e)))?;

    let parsed_config: HydroVuConfig = serde_yaml::from_str(config_str)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid YAML: {}", e)))?;

    serde_json::to_value(parsed_config)
        .map_err(|e| tinyfs::Error::Other(format!("Serialization error: {}", e)))
}

/// Create a HydroVu configuration file
fn create_hydrovu_file(config: Value, _context: FactoryContext) -> TinyFSResult<FileHandle> {
    let parsed_config: HydroVuConfig = serde_json::from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid config: {}", e)))?;

    // Convert back to YAML bytes for storage
    let config_yaml = serde_yaml::to_string(&parsed_config)
        .map_err(|e| tinyfs::Error::Other(format!("YAML serialization error: {}", e)))?
        .into_bytes();

    let file = ConfigFile::new(config_yaml);
    Ok(file.create_handle())
}

/// Execute HydroVu data collection
async fn execute_hydrovu(config: Value, context: FactoryContext) -> Result<(), TLogFSError> {
    // Parse the configuration
    let hydrovu_config: HydroVuConfig = serde_json::from_value(config)
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Invalid config: {}", e))))?;

    log::info!("Executing HydroVu collection with config: {:?}", hydrovu_config);

    // The transaction is already started by the caller (pond run command)
    // We work directly with State which provides DataFusion context and filesystem access
    
    // Create the collector (no Ship needed, just API client setup)
    let mut collector = crate::HydroVuCollector::new(hydrovu_config.clone())
        .await
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Failed to create collector: {}", e))))?;

    // Create filesystem from state
    let fs = tinyfs::FS::new(context.state.clone())
        .await
        .map_err(|e| TLogFSError::TinyFS(e))?;
    
    // Run collection within the existing transaction
    let result = collector
        .collect_data(&context.state, &fs)
        .await
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Collection failed: {}", e))))?;

    log::info!(
        "HydroVu collection complete: {} records collected",
        result.records_collected
    );

    Ok(())
}

// Register the factory
tlogfs::register_executable_factory!(
    name: "hydrovu",
    description: "HydroVu data collector configuration",
    file: create_hydrovu_file,
    validate: validate_hydrovu_config,
    execute: execute_hydrovu
);
