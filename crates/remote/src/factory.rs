// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Remote backup factory using chunked parquet storage
//!
//! This factory backs up pond data to remote storage using the chunked parquet
//! approach. Each file is split into chunks and stored in a Delta Lake table with
//! content-based deduplication (bundle_id = SHA256 hash).

use crate::{FileType, RemoteError, RemoteTable};
use base64::Engine;
use bytes::Bytes;
use clap::Parser;
use provider::FactoryContext;
use provider::registry::{ExecutionContext, ExecutionMode, FactoryCommand};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Remote factory subcommands
#[derive(Debug, Parser)]
enum RemoteCommand {
    /// Push local data to remote backup storage
    ///
    /// Backs up new pond files to remote storage using chunked parquet.
    /// Typically invoked automatically post-commit via Steward.
    Push,

    /// Pull new data from remote backup storage
    ///
    /// Downloads and applies new files from remote.
    /// For replica ponds syncing from a primary.
    Pull,

    /// Generate replication command
    ///
    /// Outputs a command to create a replica pond with this remote config.
    Replicate,

    /// List backed up files
    ///
    /// Shows files available in remote storage.
    ListFiles {
        /// Transaction ID to list files for
        #[arg(long)]
        txn_id: Option<i64>,
    },

    /// Verify backup integrity
    ///
    /// Checks that backed up files are complete and valid.
    Verify {
        /// Specific bundle_id to verify
        #[arg(long)]
        bundle_id: Option<String>,
    },
}

impl FactoryCommand for RemoteCommand {
    fn allowed(&self) -> ExecutionMode {
        match self {
            Self::Push => ExecutionMode::ControlWriter,
            Self::Pull => ExecutionMode::ControlWriter,
            Self::Replicate => ExecutionMode::PondReadWriter,
            Self::ListFiles { .. } => ExecutionMode::PondReadWriter,
            Self::Verify { .. } => ExecutionMode::PondReadWriter,
        }
    }
}

/// Remote storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteConfig {
    /// Remote Delta Lake table URL (e.g., "file:///path/to/remote" or "s3://bucket/remote")
    pub url: String,

    /// AWS region (for S3)
    #[serde(default)]
    pub region: String,

    /// AWS access key
    #[serde(default)]
    pub access_key: String,

    /// AWS secret key
    #[serde(default)]
    pub secret_key: String,

    /// Custom S3 endpoint (for MinIO, R2, etc.)
    #[serde(default)]
    pub endpoint: String,
}

/// Replication configuration for creating replica ponds
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    pub remote: RemoteConfig,
    pub pond_id: String,
    pub birth_timestamp: i64,
    pub birth_hostname: String,
    pub birth_username: String,
}

impl ReplicationConfig {
    pub fn to_base64(&self) -> Result<String, RemoteError> {
        let json = serde_json::to_string(self)?;
        Ok(base64::engine::general_purpose::STANDARD.encode(json.as_bytes()))
    }

    pub fn from_base64(encoded: &str) -> Result<Self, RemoteError> {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|e| RemoteError::TableOperation(format!("Invalid base64: {}", e)))?;
        let json_str = String::from_utf8(decoded)
            .map_err(|e| RemoteError::TableOperation(format!("Invalid UTF-8: {}", e)))?;
        Ok(serde_json::from_str(&json_str)?)
    }
}

fn validate_remote_config(config_bytes: &[u8]) -> tinyfs::Result<Value> {
    let config_str = std::str::from_utf8(config_bytes)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid UTF-8: {}", e)))?;

    let config: RemoteConfig = serde_yaml::from_str(config_str)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid YAML: {}", e)))?;

    if config.url.is_empty() {
        return Err(tinyfs::Error::Other("url field is required".to_string()));
    }

    serde_json::to_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Serialization error: {}", e)))
}

async fn execute_remote(
    config: Value,
    context: FactoryContext,
    ctx: ExecutionContext,
) -> Result<(), RemoteError> {
    let config: RemoteConfig = serde_json::from_value(config)?;

    log::info!("🌐 REMOTE FACTORY (Chunked Parquet)");
    log::info!("   Remote URL: {}", config.url);

    let cmd: RemoteCommand = ctx.to_command::<RemoteCommand, RemoteError>()?;
    log::info!("   Command: {:?}", cmd);

    // Open or create remote table
    // DeltaOps supports both file:// and s3:// URLs through object_store
    let path = config.url.strip_prefix("file://").unwrap_or(&config.url);
    let remote_table = RemoteTable::open_or_create(path, true).await?;

    match cmd {
        RemoteCommand::Push => execute_push(remote_table, &context).await,
        RemoteCommand::Pull => execute_pull(remote_table, &context).await,
        RemoteCommand::Replicate => execute_replicate(config, &context).await,
        RemoteCommand::ListFiles { txn_id } => execute_list_files(remote_table, txn_id).await,
        RemoteCommand::Verify { bundle_id } => execute_verify(remote_table, bundle_id).await,
    }
}

/// Push: Back up local files to remote
async fn execute_push(
    mut remote_table: RemoteTable,
    context: &FactoryContext,
) -> Result<(), RemoteError> {
    log::info!("📤 PUSH: Backing up to remote");

    // Get local Delta table from context
    let state = extract_tlogfs_state(context)?;
    let local_table = state.table().await;
    let current_version = local_table
        .version()
        .ok_or_else(|| RemoteError::TableOperation("No Delta version available".to_string()))?;

    log::info!("   Local Delta version: {}", current_version);

    // Get pond metadata
    let pond_metadata = context
        .pond_metadata
        .as_ref()
        .ok_or_else(|| RemoteError::TableOperation("Push requires pond metadata".to_string()))?;

    let pond_id = pond_metadata.pond_id.to_string();
    let txn_seq = context.txn_seq;
    log::info!("   Pond ID: {}, txn_seq: {}", pond_id, txn_seq);

    // Query remote table to find what's already backed up for this pond
    let remote_files = remote_table.list_files(&pond_id).await?;
    log::info!("   Remote has {} files already", remote_files.len());

    // Get list of local files to back up from current Delta table state
    // This queries the Delta table for all current parquet files
    let local_store = local_table.object_store();
    let local_files = get_current_delta_files(&local_table).await?;

    log::info!("   Local has {} files total", local_files.len());

    // Find files not yet backed up (by comparing paths)
    let remote_paths: std::collections::HashSet<_> =
        remote_files.iter().map(|f| f.1.as_str()).collect();

    let files_to_backup: Vec<_> = local_files
        .into_iter()
        .filter(|(path, _)| !remote_paths.contains(path.as_str()))
        .collect();

    if files_to_backup.is_empty() {
        log::info!("   ✓ All files already backed up");
        return Ok(());
    }

    log::info!("   Backing up {} new files", files_to_backup.len());

    // Back up each file using ChunkedWriter
    for (path, size) in files_to_backup {
        log::info!("   Backing up: {} ({} bytes)", path, size);

        // Read file from local Delta table's object store
        let file_path = object_store::path::Path::from(path.as_str());
        let get_result = local_store
            .get(&file_path)
            .await
            .map_err(|e| RemoteError::TableOperation(format!("Failed to read {}: {}", path, e)))?;

        let bytes = get_result.bytes().await.map_err(|e| {
            RemoteError::TableOperation(format!("Failed to read bytes from {}: {}", path, e))
        })?;

        // Write to remote using ChunkedWriter
        let reader = std::io::Cursor::new(bytes.to_vec());
        let bundle_id = remote_table
            .write_file(
                &pond_id,
                txn_seq,
                &path,
                current_version,
                FileType::PondParquet,
                reader,
                vec!["push".to_string()],
            )
            .await?;

        log::info!("      ✓ Backed up as bundle_id: {}", &bundle_id[..16]);
    }

    log::info!("   ✓ Push complete");
    Ok(())
}

/// Pull: Download files from remote
async fn execute_pull(
    remote_table: RemoteTable,
    context: &FactoryContext,
) -> Result<(), RemoteError> {
    log::info!("🔽 PULL: Syncing from remote");

    let pond_metadata = context
        .pond_metadata
        .as_ref()
        .ok_or_else(|| RemoteError::TableOperation("Pull requires pond metadata".to_string()))?;

    let pond_id = pond_metadata.pond_id.to_string();
    let txn_seq = context.txn_seq;
    log::info!("   Pond ID: {}, txn_seq: {}", pond_id, txn_seq);

    // List files available in remote backup for this pond
    let remote_files = remote_table.list_files(&pond_id).await?;

    if remote_files.is_empty() {
        log::info!("   No remote files found");
        return Ok(());
    }

    log::info!("   Remote has {} files", remote_files.len());

    // Get local Delta table
    let state = extract_tlogfs_state(context)?;
    let local_table = state.table().await;
    let local_store = local_table.object_store();

    // Download each remote file that doesn't exist locally
    for (bundle_id, original_path, _file_type, _size) in remote_files {
        let file_path = object_store::path::Path::from(original_path.as_str());

        // Check if file exists locally
        if local_store.head(&file_path).await.is_ok() {
            log::debug!("   Skip {} (already exists)", original_path);
            continue;
        }

        log::info!("   Pulling: {}", original_path);

        // Download using ChunkedReader
        let mut output = Vec::new();
        remote_table.read_file(&bundle_id, &mut output).await?;

        // Write to local Delta table's object store
        let byte_len = output.len();
        let bytes = Bytes::from(output);
        local_store
            .put(&file_path, bytes.into())
            .await
            .map_err(|e| {
                RemoteError::TableOperation(format!("Failed to write {}: {}", original_path, e))
            })?;

        log::info!("      ✓ Pulled {} bytes", byte_len);
    }

    log::info!("   ✓ Pull complete");
    Ok(())
}

/// Replicate: Generate replication command
#[allow(clippy::print_stdout)]
async fn execute_replicate(
    config: RemoteConfig,
    context: &FactoryContext,
) -> Result<(), RemoteError> {
    log::info!("🔄 REPLICATE: Generate replication config");

    let pond_metadata = context.pond_metadata.as_ref().ok_or_else(|| {
        RemoteError::TableOperation("Replicate requires pond metadata".to_string())
    })?;

    let replication_config = ReplicationConfig {
        remote: config,
        pond_id: pond_metadata.pond_id.to_string(),
        birth_timestamp: pond_metadata.birth_timestamp,
        birth_hostname: pond_metadata.birth_hostname.clone(),
        birth_username: pond_metadata.birth_username.clone(),
    };

    let encoded = replication_config.to_base64()?;
    println!("pond init --config={}", encoded);

    Ok(())
}

/// List files in remote storage
async fn execute_list_files(
    remote_table: RemoteTable,
    txn_id: Option<i64>,
) -> Result<(), RemoteError> {
    log::info!("📋 LIST FILES");

    // List all files - we don't filter by txn_id anymore
    let _ = txn_id; // Unused now
    let files = remote_table.list_files("").await?;

    if files.is_empty() {
        log::info!("   No files found");
        return Ok(());
    }

    log::info!("   Found {} files:", files.len());
    for (bundle_id, original_path, file_type, size) in files {
        log::info!(
            "   - {} | {} | {} | {} bytes",
            &bundle_id[..16],
            file_type.as_str(),
            original_path,
            size
        );
    }

    Ok(())
}

/// Verify backup integrity
async fn execute_verify(
    remote_table: RemoteTable,
    bundle_id: Option<String>,
) -> Result<(), RemoteError> {
    log::info!("✓ VERIFY: Checking backup integrity");

    if let Some(id) = bundle_id {
        // Verify specific bundle
        log::info!("   Verifying bundle: {}", &id[..16]);

        let mut output = Vec::new();
        remote_table.read_file(&id, &mut output).await?;

        log::info!("   ✓ Bundle OK ({} bytes)", output.len());
    } else {
        // Verify all bundles
        log::info!("   Verifying all bundles...");

        let files = remote_table.list_files("").await?;
        let total_files = files.len();
        log::info!("   Found {} files to verify", total_files);

        let mut verified = 0;
        for (bundle_id, _path, _type, _size) in files {
            let mut output = Vec::new();
            match remote_table.read_file(&bundle_id, &mut output).await {
                Ok(_) => {
                    verified += 1;
                }
                Err(e) => {
                    log::error!("   ✗ Failed to verify {}: {}", &bundle_id[..16], e);
                }
            }
        }

        log::info!("   ✓ Verified {}/{} bundles", verified, total_files);
    }

    Ok(())
}

// Public API for restore/replication

/// Build object store from remote config (for S3, MinIO, etc.)
pub fn build_object_store(
    config: &RemoteConfig,
) -> Result<std::sync::Arc<dyn object_store::ObjectStore>, RemoteError> {
    if config.url.starts_with("s3://") {
        // Parse bucket from URL
        let url_path = config.url.strip_prefix("s3://").unwrap();
        let bucket = url_path.split('/').next().unwrap_or("");

        let mut builder = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(&config.region);

        if !config.access_key.is_empty() {
            builder = builder.with_access_key_id(&config.access_key);
        }
        if !config.secret_key.is_empty() {
            builder = builder.with_secret_access_key(&config.secret_key);
        }
        if !config.endpoint.is_empty() {
            builder = builder.with_endpoint(&config.endpoint);
        }

        let store = builder
            .build()
            .map_err(|e| RemoteError::TableOperation(format!("Failed to build S3 store: {}", e)))?;

        Ok(std::sync::Arc::new(store))
    } else {
        // Local file system
        let path = config.url.strip_prefix("file://").unwrap_or(&config.url);
        let store = object_store::local::LocalFileSystem::new_with_prefix(path).map_err(|e| {
            RemoteError::TableOperation(format!("Failed to build local store: {}", e))
        })?;
        Ok(std::sync::Arc::new(store))
    }
}

/// Scan remote storage for available pond versions
///
/// Opens the RemoteTable at the given URL and queries for all backed up transaction IDs.
/// Supports both file:// and s3:// URLs through Delta Lake's object_store integration.
///
/// # Arguments
/// * `remote_url` - URL to the remote backup table (e.g., "file:///path" or "s3://bucket/path")
/// * `pond_id` - Optional pond ID to filter by (currently unused, returns all transactions)
///
/// # Returns
/// Vec of transaction IDs (pond_txn_id values) available in the backup
pub async fn scan_remote_versions(
    remote_url: &str,
    pond_id: Option<&uuid7::Uuid>,
) -> Result<Vec<i64>, RemoteError> {
    let _pond_id_str = pond_id.map(|id| id.to_string()).unwrap_or_default();

    // Open the RemoteTable - Delta Lake handles object_store internally
    let remote_table = crate::RemoteTable::open(remote_url).await?;

    // Query for all distinct pond_txn_id values
    let transactions = remote_table.list_transactions().await?;

    log::info!("Found {} transactions in remote backup", transactions.len());
    Ok(transactions)
}

/// Download files for a specific version
pub async fn download_bundle(
    _store: &std::sync::Arc<dyn object_store::ObjectStore>,
    _metadata: &provider::PondMetadata,
    _version: i64,
) -> Result<Vec<u8>, RemoteError> {
    // In chunked format, we don't download "bundles" - we query RemoteTable
    // and reconstruct files from chunks
    log::warn!("download_bundle not applicable to chunked format");
    Err(RemoteError::TableOperation(
        "Use scan_remote_versions and restore from RemoteTable directly".to_string(),
    ))
}

/// Extract files from bundle data
pub async fn extract_bundle(_data: &[u8]) -> Result<Vec<(String, Vec<u8>)>, RemoteError> {
    // Not applicable to chunked format
    Err(RemoteError::TableOperation(
        "extract_bundle not applicable to chunked format".to_string(),
    ))
}

/// Extract transaction sequence from bundle
pub fn extract_txn_seq_from_bundle(_files: &[(String, Vec<u8>)]) -> Result<i64, RemoteError> {
    // In chunked format, pond_txn_id is stored directly in the schema
    Err(RemoteError::TableOperation(
        "extract_txn_seq_from_bundle not applicable - use pond_txn_id from schema".to_string(),
    ))
}

/// Apply parquet files to Delta table (restore operation)
///
/// For chunked format, this function:
/// 1. Reads metadata to get the list of files backed up for this transaction
/// 2. Uses ChunkedReader to reconstruct each file from chunks
/// 3. Writes files directly to the local Delta table's object store
///
/// # Arguments
/// * `remote_table` - The remote backup table to read from
/// * `local_table` - The local Delta table to write to
/// * `pond_id` - Pond UUID (for locating metadata partition)
/// * `pond_txn_id` - Transaction ID to restore
///
/// # Errors
/// Returns error if:
/// - Cannot read metadata
/// - Cannot reconstruct files
/// - Cannot write to local table
pub async fn apply_parquet_files_from_remote(
    remote_table: &crate::RemoteTable,
    local_table: &mut deltalake::DeltaTable,
    pond_id: &str,
    pond_txn_id: i64,
) -> Result<(), RemoteError> {
    log::info!("Restoring transaction {} from remote backup", pond_txn_id);

    // Read metadata for this transaction
    let metadata = remote_table.read_metadata(pond_id, pond_txn_id).await?;

    log::info!("Found {} files to restore", metadata.files.len());

    // Get the object store from the local table
    let object_store = local_table.object_store();

    // Restore each file
    for file_info in &metadata.files {
        let original_path = &file_info.path;
        let bundle_id = &file_info.sha256;

        log::debug!("Restoring file: {} ({})", original_path, bundle_id);

        // Create a buffer to hold the reconstructed file
        let mut buffer = Vec::new();

        // Read file from remote using ChunkedReader
        remote_table.read_file(bundle_id, &mut buffer).await?;

        // Write to local Delta table's object store
        let object_store_path = object_store::path::Path::from(original_path.as_str());

        // Upload the file to the local table's object store
        object_store
            .put(&object_store_path, buffer.into())
            .await
            .map_err(|e| {
                RemoteError::TableOperation(format!("Failed to write file to local table: {}", e))
            })?;

        log::debug!("  ✓ Restored {}", original_path);
    }

    // Reload the table to pick up the new files
    local_table
        .load()
        .await
        .map_err(|e| RemoteError::TableOperation(format!("Failed to reload local table: {}", e)))?;

    log::info!("  ✓ Transaction {} restored", pond_txn_id);
    Ok(())
}

/// Legacy apply_parquet_files - not used in chunked format
///
/// This function exists for backward compatibility but returns an error
/// directing users to use the new restoration flow.
pub async fn apply_parquet_files(
    _table: &mut deltalake::DeltaTable,
    _files: &[(String, Vec<u8>)],
) -> Result<(), RemoteError> {
    Err(RemoteError::TableOperation(
        "apply_parquet_files not used in chunked format. Use apply_parquet_files_from_remote instead.".to_string(),
    ))
}

// Helper functions

fn extract_tlogfs_state(
    context: &FactoryContext,
) -> Result<tlogfs::persistence::State, RemoteError> {
    let state_any = context.context.persistence.as_any();

    state_any
        .downcast_ref::<tlogfs::persistence::State>()
        .cloned()
        .ok_or_else(|| RemoteError::TableOperation("State is not TLogFS State".to_string()))
}

/// Get current parquet files from Delta table
async fn get_current_delta_files(
    table: &deltalake::DeltaTable,
) -> Result<Vec<(String, i64)>, RemoteError> {
    use futures::stream::StreamExt;

    let snapshot = table
        .snapshot()
        .map_err(|e| RemoteError::TableOperation(format!("Failed to get snapshot: {}", e)))?;

    let log_store = table.log_store();
    let mut file_stream = snapshot.file_actions_iter(log_store.as_ref());

    let mut files = Vec::new();
    while let Some(result) = file_stream.next().await {
        match result {
            Ok(add_action) => {
                files.push((add_action.path.clone(), add_action.size));
            }
            Err(e) => {
                log::warn!("Failed to read file action: {}", e);
            }
        }
    }

    Ok(files)
}

provider::register_executable_factory!(
    name: "remote",
    description: "Remote backup storage using chunked parquet in Delta Lake",
    validate: validate_remote_config,
    initialize: |_config, _context| async { Ok::<(), tinyfs::Error>(()) },
    execute: |config, context, ctx| async move {
        execute_remote(config, context, ctx)
            .await
            .map_err(|e| tinyfs::Error::Other(e.to_string()))
    }
);
