//! HydroVu executable factory
//!
//! This factory creates run configuration nodes for HydroVu data collection.
//! Configuration is stored as a file:data:dynamic node that can be:
//! - Read with `pond cat /path/to/config` 
//! - Executed with `pond run /path/to/config`

use tlogfs::factory::FactoryContext;
use tlogfs::TLogFSError;
use crate::HydroVuConfig;
use serde_json::Value;
use tinyfs::Result as TinyFSResult;
use clap::{Parser, Subcommand};

/// HydroVu command-line interface
#[derive(Debug, Parser)]
#[command(name = "hydrovu", about = "HydroVu data collection operations")]
struct HydroVuCommand {
    #[command(subcommand)]
    command: HydroVuSubcommand,
}

#[derive(Debug, Subcommand)]
enum HydroVuSubcommand {
    /// Collect data from HydroVu API
    /// 
    /// Fetches sensor data from configured devices and stores it in the pond.
    /// This command requires a write transaction and cannot run post-commit.
    /// 
    /// Example: pond run /etc/system.d/20-hydrovu collect
    Collect,
}

impl HydroVuSubcommand {
    /// Returns the allowed execution mode for this command
    fn allowed_mode(&self) -> tlogfs::factory::ExecutionMode {
        match self {
            // Collect MUST run in write transaction (it writes data)
            HydroVuSubcommand::Collect => tlogfs::factory::ExecutionMode::InTransactionWriter,
        }
    }
    
    /// Validates that the command is being executed in the correct mode
    fn validate_execution_mode(&self, actual_mode: tlogfs::factory::ExecutionMode) -> Result<(), TLogFSError> {
        let allowed = self.allowed_mode();
        if actual_mode != allowed {
            return Err(TLogFSError::TinyFS(tinyfs::Error::Other(format!(
                "Command '{:?}' requires execution mode {:?}, but was called in {:?}. \
                HydroVu collect must run in a write transaction via 'pond run'.",
                self, allowed, actual_mode
            ))));
        }
        Ok(())
    }
}

/// Validate HydroVu configuration from YAML bytes
fn validate_hydrovu_config(config_bytes: &[u8]) -> TinyFSResult<Value> {
    let config_str = std::str::from_utf8(config_bytes)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid UTF-8: {}", e)))?;

    let config: HydroVuConfig = serde_yaml::from_str(config_str)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid YAML: {}", e)))?;

    // Use as_declassified() to access actual values for validation
    if config.client_id.as_declassified().is_empty() {
        return Err(tinyfs::Error::InvalidConfig("client_id cannot be empty".into()));
    }

    if config.client_secret.as_declassified().is_empty() {
        return Err(tinyfs::Error::InvalidConfig("client_secret cannot be empty".into()));
    }

    if config.devices.is_empty() {
	return Err(tinyfs::Error::InvalidConfig("At least one device must be configured".into()));
    }

    for device in &config.devices {
        if device.name.is_empty() {
	    return Err(tinyfs::Error::InvalidConfig(format!("Device name cannot be empty for device ID {}", device.id)));
        }
        if device.scope.is_empty() {
            return Err(tinyfs::Error::InvalidConfig(format!("Device scope cannot be empty for device ID {}", device.id)));
        }
    }

    if config.max_points_per_run == 0 {
        return Err(tinyfs::Error::InvalidConfig("max_points_per_run must be greater than 0".into()));
    }

    serde_json::to_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Serialization error: {}", e)))
}

/// Create a HydroVu configuration file (simple, no FS operations)
/// Initialize HydroVu factory after node creation (runs outside lock)
/// This creates the directory structure required for data collection
async fn initialize_hydrovu(config: Value, context: FactoryContext) -> Result<(), TLogFSError> {
    let parsed_config: HydroVuConfig = serde_json::from_value(config)
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Invalid config: {}", e))))?;

    // Create directory structure now that lock is released
    create_directory_structure(&parsed_config, &context).await
        .map_err(|e| TLogFSError::TinyFS(e))?;

    Ok(())
}

/// Create HydroVu directory structure
async fn create_directory_structure(
    config: &HydroVuConfig,
    context: &FactoryContext,
) -> TinyFSResult<()> {
    log::debug!("Creating HydroVu directory structure");

    // Create FS from State - this is SAFE here because create_dynamic_file_node has returned and released its lock
    let fs = tinyfs::FS::new(context.state.clone()).await?;
    
    let root = fs.root().await?;

    // Create base HydroVu directory
    let hydrovu_path = &config.hydrovu_path;
    log::debug!("Creating HydroVu base directory: {}", hydrovu_path);
    root.create_dir_path(hydrovu_path).await?;

    // Create devices directory
    let devices_path = format!("{}/devices", config.hydrovu_path);
    log::debug!("Creating devices directory: {}", devices_path);
    root.create_dir_path(&devices_path).await?;

    // Create directory for each configured device
    for device in &config.devices {
        let device_id = device.id;
        let device_name = &device.name;
        let device_path = format!("{}/{}", devices_path, device_id);
        log::debug!("Creating device directory: {} ({})", device_path, device_name);
        root.create_dir_path(&device_path).await?;
    }

    log::debug!("HydroVu directory structure created successfully");
    Ok(())
}

/// Execute HydroVu data collection
async fn execute_hydrovu(
    config: Value,
    context: FactoryContext,
    mode: tlogfs::factory::ExecutionMode,
    args: Vec<String>,
) -> Result<(), TLogFSError> {
    log::info!("🌊 HYDROVU FACTORY");
    log::info!("   Execution mode: {:?}", mode);
    log::info!("   Args: {:?}", args);
    
    // Parse command with clap - prepend "hydrovu" as program name
    let mut clap_args = vec!["hydrovu".to_string()];
    clap_args.extend(args);
    
    let cmd = match HydroVuCommand::try_parse_from(&clap_args) {
        Ok(cmd) => cmd,
        Err(e) => {
            // Clap already prints nice error messages and help text to stderr
            // We just need to exit cleanly rather than propagating as an error chain
            e.print().ok();
            std::process::exit(e.exit_code());
        }
    };
    
    log::info!("   Command: {:?}", cmd.command);
    
    // SAFETY: Validate that command is running in correct execution mode
    cmd.command.validate_execution_mode(mode)?;
    
    // Parse the configuration
    let hydrovu_config: HydroVuConfig = serde_json::from_value(config)
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Invalid config: {}", e))))?;

    log::info!("Executing HydroVu collection with config: {:?}", hydrovu_config);

    // Dispatch to command handler
    match cmd.command {
        HydroVuSubcommand::Collect => {
            execute_collect(hydrovu_config, context).await
        }
    }
}

/// Execute the collect command - fetch data from HydroVu API
async fn execute_collect(
    hydrovu_config: HydroVuConfig,
    context: FactoryContext,
) -> Result<(), TLogFSError> {
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
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("HydroVu data collection failed: {}", e))))?;

    // Print summary for each device
    for summary in &result.device_summaries {
        let start_date = crate::utc2date(summary.start_timestamp)
            .unwrap_or_else(|_| format!("{}", summary.start_timestamp));
        let final_date = crate::utc2date(summary.final_timestamp)
            .unwrap_or_else(|_| format!("{}", summary.final_timestamp));
        
        log::info!(
            "Device {} collected data from {} ({}) to {} ({}) ({} records)",
            summary.device_id,
            summary.start_timestamp,
            start_date,
            summary.final_timestamp,
            final_date,
            summary.records_collected
        );
    }

    log::info!(
        "HydroVu collection complete: {} total records collected",
        result.records_collected
    );

    Ok(())
}

// Register the factory
// Note: No file parameter - executable factories don't need create_file
// Config bytes ARE the file content (YAML), handled by ConfigFile wrapper
tlogfs::register_executable_factory!(
    name: "hydrovu",
    description: "HydroVu data collector configuration",
    validate: validate_hydrovu_config,
    initialize: initialize_hydrovu,
    execute: execute_hydrovu
);
