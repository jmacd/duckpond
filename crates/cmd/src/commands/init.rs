use anyhow::{Result, anyhow, Context};

use crate::common::ShipContext;
use log::info;
use std::path::Path;

/// Configuration source for pond initialization
enum InitConfig {
    /// Initialize from YAML file
    FromFile(std::path::PathBuf),
    /// Initialize from base64-encoded replication config
    FromBase64(String),
}

/// Initialize a new pond at the specified path
///
/// This is the only command that doesn't receive a Ship since it creates one.
/// If from_backup or config_base64 is provided, the pond will be initialized by restoring from a remote backup.
pub async fn init_command(
    ship_context: &ShipContext, 
    from_backup: Option<&Path>,
    config_base64: Option<&str>,
) -> Result<()> {
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

    // Determine initialization mode
    match (from_backup, config_base64) {
        (Some(config_path), None) => {
            info!("Initializing pond from backup at: {pond_path_display}");
            init_from_backup(ship_context, InitConfig::FromFile(config_path.to_path_buf())).await
        }
        (None, Some(encoded)) => {
            info!("Initializing replica pond at: {pond_path_display}");
            init_from_backup(ship_context, InitConfig::FromBase64(encoded.to_string())).await
        }
        (None, None) => {
            info!("Initializing pond at: {pond_path_display}");
            init_normal(ship_context).await
        }
        (Some(_), Some(_)) => {
            Err(anyhow!("Cannot specify both --from-backup and --config"))
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
async fn init_from_backup(ship_context: &ShipContext, init_config: InitConfig) -> Result<()> {
    // Parse configuration based on source
    let (config, pond_metadata) = match init_config {
        InitConfig::FromFile(config_path) => {
            info!("Reading restore configuration from: {}", config_path.display());
            
            let config_content = std::fs::read_to_string(&config_path)
                .with_context(|| format!("Failed to read config file: {}", config_path.display()))?;
            
            let config: tlogfs::remote_factory::RemoteConfig = serde_yaml::from_str(&config_content)
                .with_context(|| format!("Failed to parse config YAML from {}", config_path.display()))?;
            
            info!("✓ Configuration validated");
            (config, None) // No pond metadata preservation for file-based init
        }
        InitConfig::FromBase64(encoded) => {
            info!("📦 Decoding replication configuration...");
            
            let repl_config = tlogfs::remote_factory::ReplicationConfig::from_base64(&encoded)
                .with_context(|| "Failed to decode base64 replication config")?;
            
            info!("✓ Configuration decoded successfully");
            info!("   Source Pond ID: {}", repl_config.pond_id);
            info!("   Created: {}", chrono::DateTime::from_timestamp_micros(repl_config.birth_timestamp)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                .unwrap_or_else(|| "unknown".to_string()));
            info!("   Origin: {}@{}", repl_config.birth_username, repl_config.birth_hostname);
            
            // Extract pond metadata for preservation
            let metadata = steward::PondMetadata {
                pond_id: repl_config.pond_id.clone(),
                birth_timestamp: repl_config.birth_timestamp,
                birth_hostname: repl_config.birth_hostname.clone(),
                birth_username: repl_config.birth_username.clone(),
            };
            
            (repl_config.remote, Some(metadata))
        }
    };
    
    // Create empty pond structure
    let mut ship = ship_context.create_pond().await?;
    
    // If we have pond metadata from replication config, preserve the source pond's identity
    if let Some(ref metadata) = pond_metadata {
        info!("🔄 Setting pond identity from source...");
        ship.control_table_mut()
            .set_pond_metadata(metadata)
            .await
            .with_context(|| "Failed to set pond metadata")?;
        info!("   ✓ Pond identity preserved from source");
    }
    
    // Get the pond metadata (either from replication or freshly created)
    let final_pond_metadata = ship.control_table().get_pond_metadata().await
        .with_context(|| "Failed to get pond metadata")?
        .ok_or_else(|| anyhow!("Pond metadata not found after initialization"))?;
    
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
        
        // Clone pond_metadata for use in the async block
        let pond_metadata_clone = final_pond_metadata.clone();
        
        // Extract metadata first to get original cli_args
        let bundle_path = format!("pond-{}-bundle-{:06}.tar.zst", final_pond_metadata.pond_id, version);
        let bundle_metadata = tlogfs::bundle::extract_bundle_metadata(
            store.clone(),
            &object_store::path::Path::from(bundle_path.as_str())
        ).await.map_err(|e| anyhow!("Failed to extract bundle metadata: {}", e))?;
        
        // Require cli_args to be present - fail fast if they're missing
        if bundle_metadata.cli_args.is_empty() {
            return Err(anyhow!(
                "Bundle version {} has no cli_args in metadata. \
                This indicates the bundle was created with an older version of the software. \
                Bundles must contain original command information for proper restoration. \
                Source pond needs to be backed up again with current software version.",
                version
            ));
        }
        
        let cli_args = bundle_metadata.cli_args.clone();
        info!("   Original command: {:?}", cli_args);
        
        ship.transact(
            cli_args,
            move |tx: &steward::StewardTransactionGuard<'_>, _fs: &tinyfs::FS| {
                let store_inner = store_clone.clone();
                Box::pin(async move {
                    // Get state from transaction
                    let state = tx.state()
                        .map_err(|e| steward::StewardError::DataInit(
                            tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(format!("Failed to get state: {}", e)))
                        ))?;
                    
                    // Convert pond metadata to tlogfs format
                    let tlogfs_metadata = tlogfs::factory::PondMetadata {
                        pond_id: pond_metadata_clone.pond_id.clone(),
                        birth_timestamp: pond_metadata_clone.birth_timestamp,
                        birth_hostname: pond_metadata_clone.birth_hostname.clone(),
                        birth_username: pond_metadata_clone.birth_username.clone(),
                    };
                    
                    // Download bundle
                    let bundle_data = tlogfs::remote_factory::download_bundle(&store_inner, &tlogfs_metadata, version_clone)
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
    
    // Set remote factory mode to "pull" for replica
    // This tells Steward to only run the remote factory on manual sync (pond control --mode sync)
    // not automatically after each write transaction
    info!("🔄 Configuring replica pond for pull mode...");
    
    // Set factory mode in control table (outside transaction)
    ship.control_table_mut()
        .set_factory_mode("remote", "pull")
        .await
        .map_err(|e| anyhow!("Failed to set factory mode: {}", e))?;
    
    info!("   ✓ Remote factory mode set to 'pull'");
    
    // Verify it was set correctly
    match ship.control_table().get_factory_mode("remote").await {
        Ok(mode) => info!("   Verified: factory mode is '{}'", mode),
        Err(e) => log::warn!("   Could not verify factory mode: {}", e),
    }
    
    info!("   This prevents automatic post-commit execution");
    info!("   Use 'pond control --mode sync' to manually pull updates");
    
    Ok(())
}
