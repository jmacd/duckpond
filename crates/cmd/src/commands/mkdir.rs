use anyhow::Result;

use diagnostics::*;

/// Create a directory in the pond using scoped transactions
/// 
/// This command operates on an existing pond via the provided Ship.
/// Uses scoped transactions for automatic commit/rollback handling.
pub async fn mkdir_command(mut ship: steward::Ship, path: &str) -> Result<()> {
    debug!("Creating directory in pond: {path}");

    // Clone path for use in closure and save original for logging
    let path_for_closure = path.to_string();
    let path_display = path.to_string();

    // Use scoped transaction for mkdir operation
    ship.with_data_transaction(
        vec!["mkdir".to_string(), path_for_closure.clone()],
        |_tx, fs| Box::pin(async move {
            let root = fs.root().await
                .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            root.create_dir_path(&path_for_closure).await
                .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            Ok(())
        })
    ).await.map_err(|e| anyhow::anyhow!("Failed to create directory: {}", e))?;

    diagnostics::log_info!("Directory created successfully", path: path_display);
    Ok(())
}
