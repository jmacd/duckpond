use anyhow::{Result, anyhow};

use crate::common::get_pond_path;

pub async fn init_command() -> Result<()> {
    let store_path = get_pond_path()?;
    let store_path_str = store_path.to_string_lossy();

    println!("Initializing pond at: {}", store_path.display());

    // Check if pond already exists
    let delta_manager = tinylogfs::DeltaTableManager::new();
    if delta_manager.get_table(&store_path_str).await.is_ok() {
        return Err(anyhow!("Pond already exists"));
    }

    // Create directory and initialize
    std::fs::create_dir_all(&store_path)?;
    tinylogfs::create_oplog_table(&store_path_str).await?;

    println!("✅ Pond initialized successfully");
    Ok(())
}
