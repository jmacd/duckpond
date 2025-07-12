use anyhow::Result;
use std::path::PathBuf;

use crate::common::create_ship;
use diagnostics::{log_info, log_debug};

pub async fn mkdir_command_with_args(path: &str, args: Vec<String>) -> Result<()> {
    mkdir_command_with_pond_and_args(path, None, args).await
}

pub async fn mkdir_command_with_pond_and_args(path: &str, pond_path: Option<PathBuf>, args: Vec<String>) -> Result<()> {
    log_debug!("Creating directory in pond: {path}", path: path);

    // Create steward Ship instance
    let mut ship = create_ship(pond_path).await?;
    
    // Begin explicit transaction with command arguments
    ship.begin_transaction_with_args(args).await?;
    
    // Get the data filesystem from ship
    let fs = ship.data_fs();
    
    // Perform mkdir operation
    let operation_result = async {
        let root = fs.root().await?;
        root.create_dir_path(path).await?;
        Ok(())
    }.await;
    
    // Handle result - commit on success, rollback on error
    match operation_result {
        Ok(()) => {
            ship.commit_transaction().await?;
            log_info!("Directory created successfully: {path}", path: path);
            Ok(())
        }
        Err(e) => {
            fs.rollback().await.unwrap_or_else(|rollback_err| {
                let error_msg = format!("{}", rollback_err);
                log_debug!("Rollback error after mkdir failure: {error}", error: error_msg);
            });
            Err(e)
        }
    }
}
