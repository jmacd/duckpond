use hydrovu::{load_config, create_example_config, HydroVuCollector};
use anyhow::{Result, Context};
use std::env;
use std::path::Path;
use diagnostics::*;

#[tokio::main]
async fn main() -> Result<()> {
    init_diagnostics();
    
    // Simple argument parsing
    let args: Vec<String> = env::args().collect();
    
    if args.len() < 2 {
        print_usage(&args[0]);
        return Ok(());
    }
    
    match args[1].as_str() {
        "init" => {
            let default_config = "hydrovu-config.yaml".to_string();
            let config_path = args.get(2).unwrap_or(&default_config);
            init_config(config_path).await
        }
        "test" => {
            let default_config = "hydrovu-config.yaml".to_string();
            let config_path = args.get(2).unwrap_or(&default_config);
            test_auth(config_path).await
        }
        "collect" => {
            let default_config = "hydrovu-config.yaml".to_string();
            let config_path = args.get(2).unwrap_or(&default_config);
            collect_data(config_path).await
        }
        _ => {
            // Default behavior: treat first arg as config file path
            collect_data(&args[1]).await
        }
    }
}

async fn init_config(config_path: &str) -> Result<()> {
    let path = Path::new(config_path);
    
    if path.exists() {
        info!("Configuration file already exists: {config_path}");
        info!("Delete it first if you want to create a new one.");
        return Ok(());
    }
    
    create_example_config(path).await
        .with_context(|| format!("Failed to create configuration file: {}", config_path))?;
    
    info!("Created example configuration file: {config_path}");
    info!("");
    info!("Please edit the configuration file to add your HydroVu credentials and device list:");
    info!("  - client_id: Your OAuth client ID from HydroVu");
    info!("  - client_secret: Your OAuth client secret from HydroVu");
    info!("  - pond_path: Path to your pond data directory");
    info!("  - devices: List of device IDs to collect data from");
    info!("");
    let program_name = env::args().next().unwrap();
    info!("Then run: {program_name} collect {config_path}");
    
    Ok(())
}

async fn test_auth(config_path: &str) -> Result<()> {
    let path = Path::new(config_path);
    
    if !path.exists() {
        println!("Configuration file not found: {}", config_path);
        println!("Run: {} init {} to create an example configuration file", 
                 env::args().next().unwrap(), config_path);
        return Ok(());
    }
    
    println!("Loading configuration from: {}", config_path);
    let config = load_config(path).await
        .with_context(|| format!("Failed to load configuration from: {}", config_path))?;
    
    println!("Testing HydroVu authentication...");
    let client_id_preview = if config.client_id.len() > 8 {
        format!("{}...", &config.client_id[..8])
    } else {
        config.client_id.clone()
    };
    println!("Client ID: {}", client_id_preview);
    
    // Create client and test authentication by creating a collector
    // The authentication happens in the constructor
    let _collector = HydroVuCollector::new(config).await
        .context("Failed to create HydroVu collector (authentication failed)")?;
        
    println!("Authentication test completed successfully!");
    Ok(())
}

async fn collect_data(config_path: &str) -> Result<()> {
    let path = Path::new(config_path);
    
    if !path.exists() {
        println!("Configuration file not found: {}", config_path);
        println!("Run: {} init {} to create an example configuration file", 
                 env::args().next().unwrap(), config_path);
        return Ok(());
    }
    
    debug!("Loading configuration from: {config_path}");
    let config = load_config(path).await
        .with_context(|| format!("Failed to load configuration from: {}", config_path))?;
    
    debug!("Creating HydroVu collector...");
    let mut collector = HydroVuCollector::new(config).await
        .context("Failed to create HydroVu collector")?;
    
    debug!("Starting data collection...");
    collector.collect_data().await
        .context("Data collection failed")?;
    
    info!("Data collection completed successfully.");
    Ok(())
}

fn print_usage(program_name: &str) {
    println!("HydroVu Data Collector");
    println!();
    println!("USAGE:");
    println!("    {} init [config-file]          Create example configuration file", program_name);
    println!("    {} test [config-file]          Test authentication with HydroVu API", program_name);
    println!("    {} collect [config-file]       Collect data using configuration file", program_name);
    println!("    {} <config-file>               Collect data (default action)", program_name);
    println!();
    println!("EXAMPLES:");
    println!("    {} init                        Create hydrovu-config.yaml", program_name);
    println!("    {} test                        Test auth using hydrovu-config.yaml", program_name);
    println!("    {} collect                     Collect using hydrovu-config.yaml", program_name);
    println!("    {} my-config.yaml              Collect using my-config.yaml", program_name);
    println!();
    println!("The configuration file should contain your HydroVu OAuth credentials");
    println!("and the list of devices to collect data from.");
}
