use anyhow::Result;

use diagnostics::{log_info, log_debug};

/// Create a directory in the pond
/// 
/// This command operates on an existing pond via the provided Ship.
/// The Ship should already have a transaction started.
pub async fn mkdir_command(mut ship: steward::Ship, path: &str) -> Result<()> {
    log_debug!("Creating directory in pond: {path}", path: path);

    // Get the data filesystem from ship
    let fs = ship.data_fs();
    
    // Perform mkdir operation
    let operation_result: Result<(), anyhow::Error> = async {
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
            Err(e.into())
        }
    }
}
