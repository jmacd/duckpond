use anyhow::{Result, anyhow};

use crate::common::ShipContext;
use log::info;
use std::path::Path;

/// Initialize a new pond at the specified path
///
/// This is the only command that doesn't receive a Ship since it creates one.
/// If from_backup is provided, the pond will be initialized by restoring from a remote backup.
pub async fn init_command(ship_context: &ShipContext, from_backup: Option<&Path>) -> Result<()> {
    let pond_path = ship_context.resolve_pond_path()?;
    let pond_path_display = pond_path.display().to_string();

    // Check if pond already exists
    let data_path = pond_path.join("data");
    if data_path.exists() {
        let data_log = data_path.join("_delta_log");
        if data_log.exists() {
            return Err(anyhow!("Pond already exists"));
        }
    }

    match from_backup {
        Some(config_path) => {
            info!("Initializing pond from backup at: {pond_path_display}");
            init_from_backup(ship_context, config_path).await
        }
        None => {
            info!("Initializing pond at: {pond_path_display}");
            init_normal(ship_context).await
        }
    }
}

/// Normal initialization - creates empty pond with initial transaction
async fn init_normal(ship_context: &ShipContext) -> Result<()> {
    // Pond doesn't exist, so create a new one
    // This creates both the filesystem infrastructure AND the initial /txn/1 transaction
    let _ship = ship_context.create_pond().await?;

    log::debug!("Pond initialized successfully with transaction #1");
    Ok(())
}

/// Initialize pond from remote backup - restores all transactions
async fn init_from_backup(ship_context: &ShipContext, config_path: &Path) -> Result<()> {
    info!("Reading restore configuration from: {}", config_path.display());
    
    // Read and parse the restore configuration
    let config_content = std::fs::read_to_string(config_path)
        .map_err(|e| anyhow!("Failed to read config file: {}", e))?;
    
    let config: tlogfs::remote_factory::RemoteConfig = serde_yaml::from_str(&config_content)
        .map_err(|e| anyhow!("Failed to parse config YAML: {}", e))?;
    
    // Validate that mode is init
    if config.mode != tlogfs::remote_factory::RemoteMode::Init {
        return Err(anyhow!(
            "Config must have mode: init for --from-backup (found mode: {:?})",
            config.mode
        ));
    }
    
    info!("✓ Configuration validated (mode: init)");
    
    // Create empty pond structure
    let mut ship = ship_context.create_pond().await?;
    
    info!("✓ Empty pond structure created");
    info!("🔄 Starting restore from backup...");
    
    // Build the object store
    let store = tlogfs::remote_factory::build_object_store(&config)
        .map_err(|e| anyhow!("Failed to create object store: {}", e))?;
    
    // Scan for all available versions
    let versions = tlogfs::remote_factory::scan_remote_versions(&store)
        .await
        .map_err(|e| anyhow!("Failed to scan remote versions: {}", e))?;
    
    if versions.is_empty() {
        info!("   No backup versions found");
        return Ok(());
    }
    
    info!("   Found {} version(s) to restore: {:?}", versions.len(), versions);
    
    // Apply each version in a SEPARATE transaction so Delta can commit between versions
    for version in &versions {
        info!("   Restoring version {}...", version);
        
        let version_clone = *version;
        let store_clone = store.clone();
        
        ship.transact(
            vec![format!("restore-version-{}", version)],
            move |tx: &steward::StewardTransactionGuard<'_>, _fs: &tinyfs::FS| {
                let store_inner = store_clone.clone();
                Box::pin(async move {
                    // Get state from transaction
                    let state = tx.state()
                        .map_err(|e| steward::StewardError::DataInit(
                            tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(format!("Failed to get state: {}", e)))
                        ))?;
                    
                    // Download bundle
                    let bundle_data = tlogfs::remote_factory::download_bundle(&store_inner, version_clone)
                        .await
                        .map_err(|e| steward::StewardError::DataInit(e))?;
                    
                    // Extract files
                    let files = tlogfs::remote_factory::extract_bundle(&bundle_data)
                        .await
                        .map_err(|e| steward::StewardError::DataInit(e))?;
                    
                    if files.is_empty() {
                        log::info!("      Version {} has no files, skipping", version_clone);
                        return Ok(());
                    }
                    
                    // Get Delta table from state
                    let mut table = state.table().await;
                    
                    // Apply files to Delta table
                    tlogfs::remote_factory::apply_parquet_files(&mut table, &files)
                        .await
                        .map_err(|e| steward::StewardError::DataInit(e))?;
                    
                    let delta_version = table.version().unwrap_or(0);
                    log::info!("      ✓ Version {} restored (Delta version: {})", version_clone, delta_version);
                    
                    Ok(())
                })
            },
        )
        .await
        .map_err(|e| anyhow!("Failed to restore version {}: {}", version, e))?;
    }
    
    info!("✓ Pond initialized from backup successfully");
    info!("   All transactions from backup have been restored");
    
    Ok(())
}
