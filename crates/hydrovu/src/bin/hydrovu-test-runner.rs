use hydrovu::HydroVuCollector;
use anyhow::{Result, Context};
use std::env;
use std::path::Path;
use diagnostics::*;
use steward::Ship;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    
    if args.len() != 2 {
        print_usage(&args[0]);
        return Ok(());
    }
    
    let config_path = &args[1];
    
    if !Path::new(config_path).exists() {
        return Err(anyhow::anyhow!("Configuration file not found: {config_path}"));
    }
    
    info!("=== HydroVu Comprehensive Data Collection Test ===");
    info!("Loading configuration from: {config_path}");
    
    // Load configuration
    let config = hydrovu::config::load_config(config_path)
        .with_context(|| format!("Failed to load configuration from {config_path}"))?;
    
    let device_count = config.devices.len();
    let max_points = config.max_points_per_run;
    let pond_path = &config.pond_path;
    
    info!("Configuration loaded successfully");
    info!("Target devices: {device_count}");
    info!("Max points per transaction: {max_points}");
    info!("Pond path: {pond_path}");
    
    // Initialize pond (equivalent to "pond init")
    info!("=== Phase 1: Pond Initialization ===");
    let mut ship = initialize_pond(&config.pond_path).await?;
    info!("Pond initialized successfully");
    
    // Create directory structure for HydroVu
    create_hydrovu_directories(&mut ship, &config).await?;
    info!("HydroVu directory structure created");
    
    // Create HydroVu collector
    let mut collector = HydroVuCollector::new(config.clone()).await
        .map_err(|e| anyhow::anyhow!("Failed to create collector: {}", e))?;

    collector.collect_data().await?;
    
    Ok(())
}

fn print_usage(program_name: &str) {
    println!("HydroVu Comprehensive Data Collection Test");
    println!();
    println!("Usage: {program_name} <config-file>");
    println!();
    println!("Example:");
    println!("  {program_name} hydrovu-config.yaml");
}

async fn initialize_pond(pond_path: &str) -> Result<Ship> {
    // Remove existing pond if it exists
    if Path::new(pond_path).exists() {
        info!("Removing existing pond at: {pond_path}");
        std::fs::remove_dir_all(pond_path)
            .with_context(|| format!("Failed to remove existing pond at {pond_path}"))?;
    }
    
    // Initialize new pond
    info!("Creating new pond at: {pond_path}");
    let ship = Ship::create_pond(pond_path).await
        .with_context(|| format!("Failed to initialize pond at {pond_path}"))?;

    Ok(ship)
}

async fn create_hydrovu_directories(ship: &mut Ship, config: &hydrovu::HydroVuConfig) -> Result<()> {
    let hydrovu_path = config.hydrovu_path.clone();
    let devices = config.devices.clone();
    
    ship.transact(
        vec!["test-runner".to_string(), "create_dirs".to_string()],
        move |_tx, fs| {
            let hydrovu_path = hydrovu_path.clone();
            let devices = devices.clone();
            Box::pin(async move {
                // Get filesystem root
                let root = fs.root().await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                
                // Create base HydroVu directory
                root.create_dir_path(&hydrovu_path).await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                
                // Create devices directory
                let devices_path = format!("{}/devices", hydrovu_path);
                root.create_dir_path(&devices_path).await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                
                // Create directory for each device
                for device in &devices {
                    let device_id = device.id;
                    let device_path = format!("{devices_path}/{device_id}");
                    root.create_dir_path(&device_path).await
                        .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                    debug!("Created device directory: {device_path}");
                }
                
                Ok(())
            })
        }
    ).await?;
    
    Ok(())
}
