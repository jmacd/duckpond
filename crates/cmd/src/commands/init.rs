use anyhow::{Result, anyhow, Context};

use crate::common::ShipContext;
use log::{info, warn};
use std::path::Path;
use std::str::FromStr;
use uuid7::Uuid;

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
                pond_id: Uuid::from_str(&repl_config.pond_id)?,
                birth_timestamp: repl_config.birth_timestamp,
                birth_hostname: repl_config.birth_hostname.clone(),
                birth_username: repl_config.birth_username.clone(),
            };
            
            (repl_config.remote, Some(metadata))
        }
    };
    
    // Create empty pond structure for restoration
    // Use create_pond_for_restoration() instead of create_pond() to avoid
    // recording an initial "pond init" transaction. The first bundle will 
    // create txn_seq=1 with the original command from the source pond.
    // 
    // CRITICAL: Pass pond_metadata here so the replica pond is created with
    // the SAME pond_id as the source. This ensures bundle paths match.
    let pond_metadata_for_restore = pond_metadata.unwrap_or_else(|| {
        warn!("No source pond metadata provided - creating new pond identity for file-based restore");
        steward::PondMetadata::new()
    });
    
    let mut ship = ship_context.create_pond_for_restoration(pond_metadata_for_restore.clone()).await?;
    
    info!("Starting restore from backup...");
    
    // Build the object store
    let store = tlogfs::remote_factory::build_object_store(&config)
        .map_err(|e| anyhow!("Failed to create object store: {}", e))?;
    
    // Scan for all available versions (filter by source pond_id)
    let versions = tlogfs::remote_factory::scan_remote_versions(&store, Some(&pond_metadata_for_restore.pond_id))
        .await
        .map_err(|e| anyhow!("Failed to scan remote versions: {}", e))?;
    
    if versions.is_empty() {
        info!("   No backup versions found");
        return Ok(());
    }
    
    info!("   Found {} version(s) to restore: {:?}", versions.len(), versions);
    
    // Apply each version - replaying transactions with their ORIGINAL sequence numbers
    for version in &versions {
        info!("   Restoring version {}...", version);
        
        let version_clone = *version;
        
        // Convert pond metadata to tlogfs format for download
        let tlogfs_metadata = tlogfs::factory::PondMetadata {
            pond_id: pond_metadata_for_restore.pond_id.clone(),
            birth_timestamp: pond_metadata_for_restore.birth_timestamp,
            birth_hostname: pond_metadata_for_restore.birth_hostname.clone(),
            birth_username: pond_metadata_for_restore.birth_username.clone(),
        };
        
        // Download bundle
        let bundle_data = tlogfs::remote_factory::download_bundle(&store, &tlogfs_metadata, version_clone)
            .await
            .map_err(|e| anyhow!("Failed to download bundle for version {}: {}", version, e))?;
        
        // Extract files from bundle (Parquet + Delta commit log)
        let files = tlogfs::remote_factory::extract_bundle(&bundle_data)
            .await
            .map_err(|e| anyhow!("Failed to extract bundle for version {}: {}", version, e))?;
        
        if files.is_empty() {
            info!("      Version {} has no files, skipping", version_clone);
            continue;
        }
        
        // Extract original txn_seq from Delta commit log in the bundle
        let txn_seq = tlogfs::remote_factory::extract_txn_seq_from_bundle(&files)
            .map_err(|e| anyhow!("Failed to extract txn_seq from bundle version {}: {}", version, e))?;
        
        info!("      Original txn_seq: {}", txn_seq);
        
        // Extract metadata for cli_args (for backward compatibility and logging)
        let bundle_path = format!("pond-{}-bundle-{:06}.tar.zst", pond_metadata_for_restore.pond_id, version);
        let bundle_metadata = tlogfs::bundle::extract_bundle_metadata(
            store.clone(),
            &object_store::path::Path::from(bundle_path.as_str())
        ).await.map_err(|e| anyhow!("Failed to extract bundle metadata: {}", e))?;
        
        let cli_args = if bundle_metadata.cli_args.is_empty() {
            // Fallback for old bundles - use generic command
            vec!["<restored>".to_string()]
        } else {
            bundle_metadata.cli_args.clone()
        };
        
        info!("      Original command: {:?}", cli_args);
        
        // Build PondTxnMetadata for replay
        let txn_meta = tlogfs::PondTxnMetadata {
            txn_seq,
            user: tlogfs::PondUserMetadata {
                txn_id: uuid7::uuid7(), // Generate new UUID for restoration
                args: cli_args,
                vars: std::collections::HashMap::new(),
            },
        };
        
        // CRITICAL: Use replay_transaction() instead of transact()
        // This preserves the original txn_seq from the source pond
        ship.replay_transaction(
            &txn_meta,
            move |tx: &steward::StewardTransactionGuard<'_>, _fs: &tinyfs::FS| {
                Box::pin(async move {
                    // Get state from transaction
                    let state = tx.state()
                        .map_err(|e| steward::StewardError::DataInit(
                            tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(format!("Failed to get state: {}", e)))
                        ))?;
                    
                    // Get Delta table from state
                    let mut table = state.table().await;
                    
                    // Apply files to Delta table (includes Parquet + Delta commit log)
                    // This directly copies the commit log, preserving the original txn_seq
                    tlogfs::remote_factory::apply_parquet_files(&mut table, &files)
                        .await
                        .map_err(|e| steward::StewardError::DataInit(e))?;
                    
                    let delta_version = table.version().unwrap_or(0);
                    log::info!("      ✓ Version {} restored (Delta version: {}, txn_seq: {})", 
                        version_clone, delta_version, txn_seq);
                    
                    Ok(())
                })
            },
        )
        .await
        .map_err(|e| anyhow!("Failed to restore version {} (txn_seq={}): {}", version, txn_seq, e))?;
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
    match ship.control_table().get_factory_mode("remote") {
        Some(mode) => info!("   Verified: factory mode is '{}'", mode),
        None => log::warn!("   Could not verify factory mode: not set"),
    }
    
    info!("   This prevents automatic post-commit execution");
    info!("   Use 'pond control --mode sync' to manually pull updates");
    
    Ok(())
}
