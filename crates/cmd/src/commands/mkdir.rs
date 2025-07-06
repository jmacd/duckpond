use anyhow::Result;
use std::path::PathBuf;

use crate::common::get_pond_path_with_override;
use diagnostics::{log_info, log_debug};

pub async fn mkdir_command(path: &str) -> Result<()> {
    mkdir_command_with_pond(path, None).await
}

pub async fn mkdir_command_with_pond(path: &str, pond_path: Option<PathBuf>) -> Result<()> {
    let store_path = get_pond_path_with_override(pond_path)?;
    let store_path_str = store_path.to_string_lossy();

    log_debug!("Creating directory in pond: {path}", path: path);

    // Create filesystem
    let fs = tlogfs::create_oplog_fs(&store_path_str).await?;
    
    // Begin explicit transaction
    fs.begin_transaction().await?;
    
    // Perform mkdir operation
    let operation_result = async {
        let root = fs.root().await?;
        root.create_dir_path(path).await?;
        Ok(())
    }.await;
    
    // Handle result - commit on success, rollback on error
    match operation_result {
        Ok(()) => {
            fs.commit().await?;
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
