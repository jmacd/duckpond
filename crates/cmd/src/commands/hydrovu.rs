// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::common::ShipContext;
use anyhow::{Context, Result};
use clap::Subcommand;
use hydrovu::{Client, HydroVuCollector, HydroVuDevice};
use log::{info, warn};
use std::path::Path;

/// HydroVu subcommands for managing water sensor data collection
#[derive(Subcommand)]
pub enum HydroVuCommands {
    /// Create pond directory structure for HydroVu data storage
    Create {
        /// Path to HydroVu configuration file
        config: String,
    },
    /// List available instruments from the HydroVu API
    List {
        /// Path to HydroVu configuration file
        config: String,
    },
    /// Collect new data from all configured instruments
    Run {
        /// Path to HydroVu configuration file
        config: String,
    },
}

/// Execute HydroVu subcommand
pub async fn hydrovu_command(ship_context: &ShipContext, command: &HydroVuCommands) -> Result<()> {
    match command {
        HydroVuCommands::Create { config } => create_command(ship_context, config).await,
        HydroVuCommands::List { config } => list_command(config).await,
        HydroVuCommands::Run { config } => run_command(ship_context, config).await,
    }
}

/// Create pond directory structure for HydroVu data
async fn create_command(ship_context: &ShipContext, config_path: &str) -> Result<()> {
    debug!("Creating HydroVu pond directory structure");

    // Load and validate configuration
    let config = hydrovu::load_config(config_path)
        .with_context(|| format!("Failed to load configuration from {}", config_path))?;

    // Open or create pond
    let mut ship = ship_context
        .open_pond()
        .await
        .with_context(|| "Failed to open pond")?;

    // Clone config for later use after transaction
    let config_for_output = config.clone();

    // Create HydroVu directory structure using transaction
    ship.transact(
        vec![
            "pond".to_string(),
            "hydrovu".to_string(),
            "create".to_string(),
        ],
        move |_tx, fs| {
            let config = config.clone();
            Box::pin(async move {
                // Get filesystem root
                let root = fs
                    .root()
                    .await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

                // Create base HydroVu directory
                let hydrovu_path = &config.hydrovu_path;
                debug!("Creating HydroVu base directory: {hydrovu_path}");
                root.create_dir_path(&config.hydrovu_path)
                    .await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

                // Create devices directory
                let devices_path = format!("{}/devices", config.hydrovu_path);
                debug!("Creating devices directory: {devices_path}");
                root.create_dir_path(&devices_path)
                    .await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

                // Create directory for each configured device
                for device in &config.devices {
                    let device_id = device.id;
                    let device_name = &device.name;
                    let device_path = format!("{}/{}", devices_path, device_id);
                    debug!("Creating device directory: {device_path} ({device_name})");

                    root.create_dir_path(&device_path).await.map_err(|e| {
                        steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e))
                    })?;
                }

                debug!("HydroVu directory structure created successfully");
                Ok(())
            })
        },
    )
    .await?;

    println!("✓ Created HydroVu pond directory structure");
    println!("  Base path: {}", config_for_output.hydrovu_path);
    println!("  Devices configured: {}", config_for_output.devices.len());
    for device in &config_for_output.devices {
        println!(
            "    - Device {} ({}): {}",
            device.id, device.name, device.scope
        );
    }

    Ok(())
}

/// List available instruments from HydroVu API
async fn list_command(config_path: &str) -> Result<()> {
    debug!("Listing available instruments from HydroVu API");

    // Validate config file exists
    if !Path::new(config_path).exists() {
        return Err(anyhow::anyhow!(
            "Configuration file not found: {}",
            config_path
        ));
    }

    // Load configuration
    let config = hydrovu::load_config(config_path)
        .with_context(|| format!("Failed to load configuration from {}", config_path))?;

    let (key_id, key_val) = hydrovu::get_key()?;

    // Create HydroVu client
    let client = Client::new(key_id, key_val)
        .await
        .with_context(|| "Failed to create HydroVu client")?;

    debug!("Connected to HydroVu API, fetching locations...");

    // Fetch all available locations
    let locations = client
        .fetch_locations()
        .await
        .with_context(|| "Failed to fetch locations from HydroVu API")?;

    println!("Available HydroVu Instruments:");
    println!("==============================");

    if locations.is_empty() {
        println!("No instruments found for the configured credentials.");
        return Ok(());
    }

    // Display locations in a formatted table
    println!(
        "{:<10} {:<30} {:<20} {:<30}",
        "ID", "Name", "Description", "GPS"
    );
    println!("{}", "-".repeat(90));

    for location in &locations {
        let gps_str = format!(
            "{:.6}, {:.6}",
            location.gps.latitude, location.gps.longitude
        );
        println!(
            "{:<10} {:<30} {:<20} {:<30}",
            location.id,
            truncate_string(&location.name, 30),
            truncate_string(&location.description, 20),
            gps_str
        );
    }

    println!();
    println!("Total instruments: {}", locations.len());

    // Show which instruments are configured
    let configured_ids: std::collections::HashSet<i64> =
        config.devices.iter().map(|d| d.id).collect();

    let configured_count = locations
        .iter()
        .filter(|loc| configured_ids.contains(&loc.id))
        .count();

    println!("Configured in config file: {}", configured_count);

    if configured_count < config.devices.len() {
        warn!("Some configured devices were not found in the API response");
        let missing_devices: Vec<&HydroVuDevice> = config
            .devices
            .iter()
            .filter(|device| !locations.iter().any(|loc| loc.id == device.id))
            .collect();

        println!("\nMissing devices:");
        for device in missing_devices {
            println!(
                "  - Device {} ({}): not found in API",
                device.id, device.name
            );
        }
    }

    Ok(())
}

/// Run data collection from all configured instruments  
async fn run_command(ship_context: &ShipContext, config_path: &str) -> Result<()> {
    debug!("Running HydroVu data collection");

    // Validate config file exists
    if !Path::new(config_path).exists() {
        return Err(anyhow::anyhow!(
            "Configuration file not found: {}",
            config_path
        ));
    }

    // Load configuration
    let config = hydrovu::load_config(config_path)
        .with_context(|| format!("Failed to load configuration from {}", config_path))?;

    let device_count = config.devices.len();
    debug!("Configuration loaded: {device_count} devices to process");

    // Open pond using ShipContext (single source of truth)
    let mut ship = ship_context
        .open_pond()
        .await
        .with_context(|| "Failed to open pond")?;

    let pond_path = ship_context.resolve_pond_path()?;
    debug!("Using pond path: {}", pond_path.display());

    // Create HydroVu collector with config only
    let mut collector = HydroVuCollector::new(config.clone())
        .await
        .with_context(|| "Failed to create HydroVu collector")?;

    debug!("HydroVu collector created, starting data collection...");

    // Begin transaction for data collection
    let tx = ship
        .begin_write(&steward::PondUserMetadata::new(vec![
            "hydrovu".to_string(),
            "collect".to_string(),
        ]))
        .await
        .with_context(|| "Failed to begin transaction")?;

    // Get filesystem from transaction state
    let state = tx.state()?;
    let fs = tinyfs::FS::new(state.clone())
        .await
        .with_context(|| "Failed to create filesystem")?;

    // Run data collection with State and FS
    let results = collector
        .collect_data(&state, &fs)
        .await
        .with_context(|| "Failed to collect data from HydroVu")?;

    // Commit the transaction
    tx.commit()
        .await
        .with_context(|| "Failed to commit transaction")?;

    println!(
        "✓ HydroVu data collection completed successfully {}",
        results.records_collected
    );
    println!("Results:");

    for (device_id, final_timestamp) in &results.final_timestamps {
        let device_name = config
            .devices
            .iter()
            .find(|d| d.id == *device_id)
            .map(|d| d.name.as_str())
            .unwrap_or("Unknown");

        let date_str =
            hydrovu::utc2date(*final_timestamp).unwrap_or_else(|_| "invalid date".to_string());

        println!(
            "  - Device {} ({}): latest data from {}",
            device_id, device_name, date_str
        );
    }

    println!("\nData stored in pond at: {}", config.hydrovu_path);
    println!(
        "Use 'pond list {}/*' to browse collected data",
        config.hydrovu_path
    );

    Ok(())
}

/// Helper function to truncate strings for table display
fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[0..max_len - 3])
    }
}
