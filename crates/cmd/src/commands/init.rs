// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, anyhow};

use crate::common::ShipContext;
use log::{info, warn};
use std::path::Path;
use std::str::FromStr;
use url::Url;
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
            init_from_backup(
                ship_context,
                InitConfig::FromFile(config_path.to_path_buf()),
            )
            .await
        }
        (None, Some(encoded)) => {
            info!("Initializing replica pond at: {pond_path_display}");
            init_from_backup(ship_context, InitConfig::FromBase64(encoded.to_string())).await
        }
        (None, None) => {
            info!("Initializing pond at: {pond_path_display}");
            init_normal(ship_context).await
        }
        (Some(_), Some(_)) => Err(anyhow!("Cannot specify both --from-backup and --config")),
    }
}

/// Normal initialization - creates empty pond with initial transaction
async fn init_normal(ship_context: &ShipContext) -> Result<()> {
    // Pond doesn't exist, so create a new one
    // This creates both the filesystem infrastructure AND the initial /txn/1 transaction
    let mut ship = ship_context.create_pond().await?;

    // Set default factory mode to "push" for new ponds
    // This will be inherited by remote factories unless explicitly changed
    info!("Setting default factory mode to 'push' for new pond");
    ship.control_table_mut()
        .set_factory_mode("remote", "push")
        .await
        .map_err(|e| anyhow!("Failed to set default factory mode: {}", e))?;

    log::debug!("Pond initialized successfully with transaction #1");
    Ok(())
}

/// Initialize pond from remote backup - restores all transactions
async fn init_from_backup(ship_context: &ShipContext, init_config: InitConfig) -> Result<()> {
    // Parse configuration based on source
    let (config, pond_metadata) = match init_config {
        InitConfig::FromFile(config_path) => {
            info!(
                "Reading restore configuration from: {}",
                config_path.display()
            );

            let config_content = std::fs::read_to_string(&config_path).with_context(|| {
                format!("Failed to read config file: {}", config_path.display())
            })?;

            let config: remote::RemoteConfig =
                serde_yaml::from_str(&config_content).with_context(|| {
                    format!("Failed to parse config YAML from {}", config_path.display())
                })?;

            info!("✓ Configuration validated");
            (config, None) // No pond metadata preservation for file-based init
        }
        InitConfig::FromBase64(encoded) => {
            info!("📦 Decoding replication configuration...");

            let repl_config = remote::ReplicationConfig::from_base64(&encoded)
                .with_context(|| "Failed to decode base64 replication config")?;

            info!("✓ Configuration decoded successfully");
            info!("   Source Pond ID: {}", repl_config.pond_id);
            info!(
                "   Created: {}",
                chrono::DateTime::from_timestamp_micros(repl_config.birth_timestamp)
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                    .unwrap_or_else(|| "unknown".to_string())
            );
            info!(
                "   Origin: {}@{}",
                repl_config.birth_username, repl_config.birth_hostname
            );

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
        warn!(
            "No source pond metadata provided - creating new pond identity for file-based restore"
        );
        steward::PondMetadata::default()
    });

    let mut ship = ship_context
        .create_pond_for_restoration(pond_metadata_for_restore.clone())
        .await?;

    info!("Starting restore from backup...");

    // Open the remote table
    let remote_url = &config.url;
    let remote_table = remote::RemoteTable::open(remote_url)
        .await
        .map_err(|e| anyhow!("Failed to open remote table: {}", e))?;

    // List available transactions using efficient object_store listing
    let transactions = remote_table
        .list_transaction_numbers(Some(&pond_metadata_for_restore.pond_id.to_string()))
        .await
        .map_err(|e| anyhow!("Failed to list remote transactions: {}", e))?;

    if transactions.is_empty() {
        info!("   No backup transactions found");
        return Ok(());
    }

    info!(
        "   Found {} transactions to restore: {:?}",
        transactions.len(),
        transactions
    );

    // Get the pond path and open the Delta table directly
    let pond_path = ship_context.resolve_pond_path()?;
    let data_path = pond_path.join("data");
    let data_path_str = data_path.to_string_lossy().to_string();
    let url = Url::from_directory_path(&data_path)
        .or_else(|_| Url::from_file_path(&data_path))
        .map_err(|_| anyhow!("Failed to create URL from path: {}", data_path_str))?;
    let mut local_table = deltalake::open_table(url)
        .await
        .map_err(|e| anyhow!("Failed to open local Delta table: {}", e))?;

    // Restore each transaction sequentially
    for txn_seq in &transactions {
        info!("   Restoring transaction {}...", txn_seq);

        // Use apply_parquet_files_from_remote to download and restore files
        remote::factory::apply_parquet_files_from_remote(&remote_table, &mut local_table, *txn_seq)
            .await
            .map_err(|e| anyhow!("Failed to restore transaction {}: {}", txn_seq, e))?;

        info!("      ✓ Transaction {} restored", txn_seq);
    }

    info!("✓ Pond initialized from backup successfully");
    info!("   Restored {} transactions", transactions.len());

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
