use anyhow::{Result, anyhow};
use std::path::PathBuf;

use crate::common::get_pond_path_with_override;
use diagnostics::log_info;

pub async fn recover_command_with_args(args: Vec<String>) -> Result<()> {
    recover_command_with_pond_and_args(None, args).await
}

pub async fn recover_command_with_pond_and_args(pond_path: Option<PathBuf>, _args: Vec<String>) -> Result<()> {
    let pond_path = get_pond_path_with_override(pond_path)?;
    let pond_path_display = pond_path.display().to_string();
    
    log_info!("Starting crash recovery at pond: {pond_path}", pond_path: pond_path_display);

    // Check if pond exists by checking for data directory
    let data_path = steward::get_data_path(&pond_path);
    if !data_path.exists() {
        return Err(anyhow!("Pond does not exist at path: {}", pond_path_display));
    }

    // Create steward Ship instance
    let mut ship = steward::Ship::open_existing_pond(&pond_path).await
        .map_err(|e| anyhow!("Failed to initialize pond for recovery: {}", e))?;

    // First check if recovery is needed
    match ship.check_recovery_needed().await {
        Ok(()) => {
            log_info!("No recovery needed - pond is in consistent state");
            println!("No recovery needed - pond is in consistent state");
            return Ok(());
        }
        Err(steward::StewardError::RecoveryNeeded { sequence }) => {
            log_info!("Recovery needed for transaction sequence: {sequence}", sequence: sequence);
            println!("Recovery needed for transaction sequence: {}", sequence);
        }
        Err(e) => {
            let error_msg = e.to_string();
            log_info!("Failed to check recovery status: {error}", error: error_msg);
            return Err(anyhow!("Failed to check recovery status: {}", e));
        }
    }

    // Perform the recovery
    match ship.execute_recovery().await {
        Ok(result) => {
            if result.was_needed {
                log_info!("Recovery completed successfully", recovered_count: result.recovered_count);
                println!("Recovery completed successfully!");
                println!("Recovered {} missing transaction metadata files", result.recovered_count);
            } else {
                log_info!("No recovery was needed");
                println!("No recovery was needed");
            }
            Ok(())
        }
        Err(e) => {
            let error_msg = e.to_string();
            log_info!("Recovery failed: {error}", error: error_msg);
            Err(anyhow!("Recovery failed: {}", e))
        }
    }
}
