use hydrovu::HydroVuCollector;
use anyhow::{Result, Context};
use std::env;
use std::path::Path;
use diagnostics::*;
use steward::Ship;
use chrono::DateTime;

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
    let max_rows = config.max_rows_per_run;
    let pond_path = &config.pond_path;
    
    let max_rows_value = max_rows.unwrap_or(1000);
    
    info!("Configuration loaded successfully");
    info!("Target devices: {device_count}");
    info!("Max rows per transaction: {max_rows_value}");
    info!("Pond path: {pond_path}");
    
    // Initialize pond (equivalent to "pond init")
    info!("=== Phase 1: Pond Initialization ===");
    let mut ship = initialize_pond(&config.pond_path).await?;
    info!("Pond initialized successfully");
    
    // Create directory structure for HydroVu
    create_hydrovu_directories(&mut ship, &config).await?;
    info!("HydroVu directory structure created");
    
    // Create HydroVu collector
    let mut collector = HydroVuCollector::new(config.clone()).await?;
    info!("HydroVu collector initialized");
    
    // Phase 2: Complete Historical Data Collection
    info!("=== Phase 2: Complete Historical Data Collection ===");
    info!("Starting full historical collection for all devices...");
    info!("Target: 15,000 records per transaction, continuing until current time");
    
    let current_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    
    let current_time_formatted = DateTime::from_timestamp(current_time, 0).unwrap();
    println!("Current time: {} ({})", current_time, current_time_formatted);
    
    for device in &config.devices {
        let device_id = device.id;
        let device_name = &device.name;
        info!("Starting complete historical collection for device {device_id} ({device_name})");
        
        let mut transaction_count = 0;
        let mut total_records_collected = 0;
        
        loop {
            transaction_count += 1;
            info!("=== Transaction {transaction_count} for device {device_id} ===");
            
            // Collect single device data for this transaction
            println!("Collecting data from stored timestamp to current time...");
            match collector.collect_single_device(device_id).await {
                Ok(records_collected) => {
                    if records_collected == 0 {
                        info!("No more data available for device {device_id}, collection complete");
                        break;
                    }
                    total_records_collected += records_collected;
                    info!("Transaction {transaction_count}: collected {records_collected} records");
                    info!("Total collected so far for device {device_id}: {total_records_collected} records");
                    
                    // If we collected less than the maximum, we probably reached the end
                    if records_collected < max_rows_value {
                        info!("Collected fewer than maximum rows, likely reached current time");
                        break;
                    }
                }
                Err(e) => {
                    error!("Data collection failed for device {device_id} in transaction {transaction_count}: {e}");
                    break;
                }
            }
        }
        
        info!("Complete historical collection finished for device {device_id}:");
        info!("  - Total transactions: {transaction_count}");
        info!("  - Total records collected: {total_records_collected}");
    }
    
    // Phase 3: Data verification
    info!("=== Phase 3: Data Verification ===");
    info!("Verifying collected data...");
    
    for device in &config.devices {
        let device_id = device.id;
        let device_name = &device.name;
        info!("Checking device {device_id} ({device_name})");
        
        // Try to get the youngest timestamp for this device
        match collector.get_youngest_timestamp(device_id as u64).await {
            Ok(timestamp) if timestamp > 0 => {
                let dt = DateTime::from_timestamp(timestamp, 0)
                    .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
                info!("  Device {device_id}: data collected through {dt}");
            }
            Ok(_) => {
                warn!("  Device {device_id}: no data collected");
            }
            Err(e) => {
                error!("  Device {device_id}: error checking data - {e}");
            }
        }
    }
    
    info!("=== TEST COMPLETED ===");
    info!("Basic data collection and verification completed for {device_count} devices");
    info!("");
    info!("This is a simplified implementation of the comprehensive test described in:");
    info!("  memory-bank/hydrovu-test-plan.md");
    info!("");
    info!("Future enhancements will add:");
    info!("  - Complete historical data collection from epoch to present");
    info!("  - Incremental collection with proper transaction batching");
    info!("  - Full data integrity verification with read-back comparison");
    info!("  - Schema evolution testing across full device history");
    
    Ok(())
}

fn print_usage(program_name: &str) {
    println!("HydroVu Comprehensive Data Collection Test");
    println!();
    println!("This program performs an end-to-end test of HydroVu data collection:");
    println!("  1. Initializes a new pond");
    println!("  2. Collects available data for each configured device");
    println!("  3. Verifies data integrity by checking timestamps");
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
    let mut ship = Ship::create_pond(pond_path).await
        .with_context(|| format!("Failed to initialize pond at {pond_path}"))?;
        
    // Initialize with a transaction to set up the pond structure
    ship.transact(
        vec!["test-runner".to_string(), "init".to_string()],
        |_tx, _fs| Box::pin(async {
            debug!("Pond initialization transaction completed");
            Ok(())
        })
    ).await?;
    
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