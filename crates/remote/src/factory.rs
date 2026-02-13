// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Remote backup factory using chunked parquet storage
//!
//! This factory backs up pond data to remote storage using the chunked parquet
//! approach. Each file is split into chunks and stored in a Delta Lake table with
//! content-based deduplication (bundle_id = SHA256 hash).

use crate::{RemoteError, RemoteTable};
use base64::Engine;
use bytes::Bytes;
use clap::Parser;
use provider::FactoryContext;
use provider::registry::{ExecutionContext, ExecutionMode, FactoryCommand};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;
use url::Url;

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

    /// Show storage details and generate verification script
    ///
    /// Lists files matching the pattern and shows how to verify them
    /// using external tools (duckdb, b3sum) without using pond.
    Show {
        /// Path or glob pattern to match files (e.g., "/*" or "/data/*.csv")
        #[arg(default_value = "/*")]
        pattern: String,

        /// Show full verification script (default: summary only)
        #[arg(long, short)]
        script: bool,
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
            Self::Show { .. } => ExecutionMode::PondReadWriter,
        }
    }
}

/// Remote storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteConfig {
    /// Remote Delta Lake table URL (e.g., "file:///path/to/remote" or "s3://bucket/remote")
    pub url: String,

    /// Region
    #[serde(default)]
    pub region: String,

    /// Access key (YAML: access_key_id or access_key)
    #[serde(default, alias = "access_key_id")]
    pub access_key: String,

    /// Secret key (YAML: secret_access_key or secret_key)
    #[serde(default, alias = "secret_access_key")]
    pub secret_key: String,

    /// Custom S3 endpoint (non-AWS)
    #[serde(default)]
    pub endpoint: String,

    /// Allow HTTP (non-TLS) connections (required for MinIO and other local S3)
    #[serde(default)]
    pub allow_http: bool,
}

impl RemoteConfig {
    /// Build storage options HashMap for S3/R2 configuration
    ///
    /// Returns a HashMap suitable for passing to Delta Lake storage options.
    /// Only includes non-empty configuration values.
    #[must_use]
    pub fn to_storage_options(&self) -> std::collections::HashMap<String, String> {
        let mut storage_options = std::collections::HashMap::new();
        if self.url.starts_with("s3://") {
            if !self.region.is_empty() {
                storage_options.insert("region".to_string(), self.region.clone());
            }
            if !self.access_key.is_empty() {
                storage_options.insert("access_key_id".to_string(), self.access_key.clone());
            }
            if !self.secret_key.is_empty() {
                storage_options.insert("secret_access_key".to_string(), self.secret_key.clone());
            }
            if !self.endpoint.is_empty() {
                storage_options.insert("endpoint".to_string(), self.endpoint.clone());
                // R2-specific settings - use virtual_hosted_style_request = false for path-style access
                storage_options.insert(
                    "virtual_hosted_style_request".to_string(),
                    "false".to_string(),
                );
            }
            if self.allow_http {
                storage_options
                    .insert("allow_http".to_string(), "true".to_string());
            }
        }
        storage_options
    }

    /// Build the full remote table URL with pond_id appended if needed
    ///
    /// For S3 URLs with just bucket (e.g., s3://bucket), appends pond-{pond_id}
    #[must_use]
    pub fn build_table_url(&self, pond_id: &str) -> String {
        if self.url.starts_with("s3://") && self.url.matches('/').count() == 2 {
            format!("{}/pond-{}", self.url, pond_id)
        } else {
            self.url.clone()
        }
    }
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
    // Register S3-compatible storage handlers for R2/S3
    crate::s3_registration::register_s3_handlers();

    let config: RemoteConfig = serde_json::from_value(config)?;

    log::info!("   Remote URL: {}", config.url);
    log::debug!("   Config region: '{}'", config.region);
    log::debug!("   Config access_key length: {}", config.access_key.len());
    log::debug!("   Config secret_key length: {}", config.secret_key.len());
    log::debug!("   Config endpoint: '{}'", config.endpoint);

    // Parse the command first, without mode check
    let args_with_prog_name: Vec<String> = if ctx.args().is_empty() {
        vec!["factory".to_string()]
    } else {
        std::iter::once("factory".to_string())
            .chain(ctx.args().iter().cloned())
            .collect()
    };

    let cmd = RemoteCommand::try_parse_from(&args_with_prog_name).map_err(|e| {
        eprintln!("{}", e);
        RemoteError::CommandParsing(e.to_string())
    })?;

    // Check execution mode - if mismatch for push/pull, provide helpful message
    let required_mode = cmd.allowed();
    let actual_mode = ctx.mode();
    if required_mode != actual_mode {
        match &cmd {
            RemoteCommand::Push | RemoteCommand::Pull => {
                // Push/Pull require ControlWriter mode but were called with PondReadWriter
                // This happens when 'pond run' is used manually - the push/pull already
                // runs automatically as a post-commit factory
                log::info!(
                    "ℹ️  Remote {} runs automatically after each commit.",
                    if matches!(cmd, RemoteCommand::Push) {
                        "push"
                    } else {
                        "pull"
                    }
                );
                log::info!("   No manual execution needed - your data is already synchronized.");
                log::info!("   To check backup status, use: pond run <path> list-files");
                return Ok(());
            }
            _ => {
                return Err(RemoteError::ExecutionMismatch {
                    required: format!("{:?}", required_mode),
                    actual: format!("{:?}", actual_mode),
                    hint: "This command cannot be run in the current context.".to_string(),
                });
            }
        }
    }

    log::info!("   Command: {:?}", cmd);

    // Get pond UUID for path prefix
    let pond_metadata = context
        .pond_metadata
        .as_ref()
        .ok_or_else(|| RemoteError::Configuration("No pond metadata available".to_string()))?;
    let pond_id = pond_metadata.pond_id.to_string();

    // Open or create remote table
    // DeltaOps supports both file:// and s3:// URLs through object_store
    let path = config.url.strip_prefix("file://").unwrap_or(&config.url);

    // If S3 URL is just bucket without path (e.g., s3://bucket), append pond UUID as table path
    // DeltaLake 0.29+ requires a full table path, not just a bucket
    let path = if path.starts_with("s3://") && path.matches('/').count() == 2 {
        let table_path = format!("{}/pond-{}", path, pond_id);
        log::info!("   Appending pond UUID to path: {}", table_path);
        table_path
    } else {
        path.to_string()
    };

    // Build storage options for S3/R2 configuration
    let storage_options = config.to_storage_options();
    log::debug!(
        "   Final storage_options keys: {:?}",
        storage_options.keys().collect::<Vec<_>>()
    );

    let remote_table =
        RemoteTable::open_or_create_with_storage_options(&path, true, storage_options.clone())
            .await?;

    match cmd {
        RemoteCommand::Push => execute_push(remote_table, &context).await,
        RemoteCommand::Pull => execute_pull(remote_table, &context).await,
        RemoteCommand::Replicate => execute_replicate(config, &context).await,
        RemoteCommand::ListFiles { txn_id } => execute_list_files(remote_table, txn_id).await,
        RemoteCommand::Verify { bundle_id } => execute_verify(remote_table, bundle_id).await,
        RemoteCommand::Show { pattern, script } => {
            execute_show(
                remote_table,
                &config,
                &path,
                storage_options,
                &pattern,
                script,
            )
            .await
        }
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
    log::info!("   Pond ID: {}", pond_id);

    // Find which transactions are already backed up
    let backed_up_txns = remote_table.list_transaction_numbers(None).await?;
    log::info!("   Remote has transactions: {:?}", backed_up_txns);

    // Determine which transactions need to be backed up
    // Assumption: txn_seq matches Delta version (1:1 mapping)
    let backed_up_set: std::collections::HashSet<_> = backed_up_txns.into_iter().collect();
    let mut missing_versions = Vec::new();

    for version in 1..=current_version {
        if !backed_up_set.contains(&version) {
            missing_versions.push(version);
        }
    }

    if missing_versions.is_empty() {
        log::info!("   ✓ All transactions already backed up");
        return Ok(());
    }

    log::info!(
        "   Need to back up {} transactions: {:?}",
        missing_versions.len(),
        missing_versions
    );

    // Get pond path for large files
    let pond_path = state.store_path().await;

    // Back up each missing transaction
    for version in missing_versions {
        log::info!(
            "   📦 Backing up transaction {} (version {})...",
            version,
            version
        );

        // Load Delta table at this specific version
        let store_path = pond_path.to_string_lossy().to_string();
        let url = Url::from_directory_path(&pond_path)
            .map_err(|_| RemoteError::TableOperation(format!("Invalid path: {}", store_path)))?;
        let mut versioned_table = deltalake::open_table(url)
            .await
            .map_err(|e| RemoteError::TableOperation(format!("Failed to open table: {}", e)))?;

        versioned_table.load_version(version).await.map_err(|e| {
            RemoteError::TableOperation(format!("Failed to load version {}: {}", version, e))
        })?;

        let local_store = versioned_table.object_store();

        // Get NEW files added in this specific transaction (incremental delta only)
        // Each Delta transaction has a commit log with 'add' actions for new parquet files
        let new_files = get_delta_commit_files(&versioned_table, version).await?;
        log::info!(
            "      Transaction {} added {} new files",
            version,
            new_files.len()
        );

        // Back up parquet files with transaction bundle_id
        let transaction_bundle_id =
            crate::schema::ChunkedFileRecord::transaction_bundle_id(version);

        for (path, size) in &new_files {
            log::debug!("      Backing up: {} ({} bytes)", path, size);

            let file_path = object_store::path::Path::from(path.as_str());
            let get_result = local_store.get(&file_path).await.map_err(|e| {
                RemoteError::TableOperation(format!("Failed to read {}: {}", path, e))
            })?;

            let bytes = get_result.bytes().await.map_err(|e| {
                RemoteError::TableOperation(format!("Failed to read bytes from {}: {}", path, e))
            })?;

            let reader = std::io::Cursor::new(bytes.to_vec());
            remote_table
                .write_file_with_bundle_id(&transaction_bundle_id, version, path, reader)
                .await?;
        }

        // Back up Delta commit log for this version
        let commit_log_path = format!("_delta_log/{:020}.json", version);
        let log_file_path = object_store::path::Path::from(commit_log_path.as_str());

        match local_store.get(&log_file_path).await {
            Ok(get_result) => {
                let bytes = get_result.bytes().await.map_err(|e| {
                    RemoteError::TableOperation(format!("Failed to read commit log: {}", e))
                })?;

                let reader = std::io::Cursor::new(bytes.to_vec());
                remote_table
                    .write_file_with_bundle_id(
                        &transaction_bundle_id,
                        version,
                        &commit_log_path,
                        reader,
                    )
                    .await?;
            }
            Err(e) => {
                log::warn!("      Could not read commit log (may not exist): {}", e);
            }
        }

        log::info!(
            "      ✓ Transaction {} backed up ({} files)",
            version,
            new_files.len()
        );
    }

    // Back up large files (these are cumulative, not per-transaction)
    // Only back up large files that don't exist remotely yet
    let remote_files = remote_table.list_files(&pond_id).await?;
    let remote_paths: std::collections::HashSet<_> =
        remote_files.iter().map(|f| f.1.as_str()).collect();

    let large_files = get_large_files(pond_path.as_path()).await?;
    // Convert absolute paths to relative paths for comparison with remote
    // Absolute: /tmp/pond/_large_files/sha256=abc -> Relative: _large_files/sha256=abc
    let large_files_to_backup: Vec<_> = large_files
        .into_iter()
        .filter_map(|(abs_path, size)| {
            // Extract the relative path: everything after the pond directory
            // e.g., /tmp/pond/_large_files/sha256=X -> _large_files/sha256=X
            let file_name = std::path::Path::new(&abs_path)
                .file_name()
                .and_then(|s| s.to_str())?;
            let relative_path = format!("_large_files/{}", file_name);

            if remote_paths.contains(relative_path.as_str()) {
                None // Already backed up
            } else {
                Some((abs_path, relative_path, size))
            }
        })
        .collect();

    if !large_files_to_backup.is_empty() {
        log::info!(
            "   📦 Backing up {} large files...",
            large_files_to_backup.len()
        );

        for (abs_path, relative_path, size) in &large_files_to_backup {
            log::debug!(
                "      Backing up large file: {} ({} bytes)",
                relative_path,
                size
            );

            let file_data = tokio::fs::read(&abs_path).await.map_err(|e| {
                RemoteError::TableOperation(format!(
                    "Failed to read large file {}: {}",
                    abs_path, e
                ))
            })?;

            let reader = std::io::Cursor::new(file_data);
            // Use the relative path with _large_files/ prefix so restore knows where to put it
            remote_table
                .write_file(current_version, relative_path, reader)
                .await?;
        }

        log::info!("      ✓ Large files backed up");
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

    log::debug!("   Remote has {} files", remote_files.len());

    // Get local Delta table and pond path
    let state = extract_tlogfs_state(context)?;
    let pond_path = state.store_path().await;
    let local_table = state.table().await;
    let local_store = local_table.object_store();

    // Download each remote file that doesn't exist locally
    for (bundle_id, original_path, pond_txn_id, _size) in remote_files {
        // Check if this is a large file
        if original_path.starts_with("_large_files/") {
            // Large files go to the filesystem
            let large_file_fs_path = pond_path.join(&original_path);

            // Check if file exists on filesystem
            if large_file_fs_path.exists() {
                log::debug!("   Skip {} (already exists on filesystem)", original_path);
                continue;
            }

            log::debug!("   Pulling large file: {}", original_path);

            // Download using ChunkedReader
            let mut output = Vec::new();
            remote_table
                .read_file(&bundle_id, &original_path, pond_txn_id, &mut output)
                .await?;

            // Ensure parent directory exists
            if let Some(parent) = large_file_fs_path.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    RemoteError::TableOperation(format!(
                        "Failed to create _large_files directory: {}",
                        e
                    ))
                })?;
            }

            // Write to filesystem
            let byte_len = output.len();
            tokio::fs::write(&large_file_fs_path, &output)
                .await
                .map_err(|e| {
                    RemoteError::TableOperation(format!(
                        "Failed to write large file to {}: {}",
                        large_file_fs_path.display(),
                        e
                    ))
                })?;

            log::debug!(
                "      ✓ Pulled {} bytes to {}",
                byte_len,
                large_file_fs_path.display()
            );
        } else {
            // Regular files go to the object store
            let file_path = object_store::path::Path::from(original_path.as_str());

            // Check if file exists locally
            if local_store.head(&file_path).await.is_ok() {
                log::debug!("   Skip {} (already exists)", original_path);
                continue;
            }

            log::debug!("   Pulling: {}", original_path);

            // Download using ChunkedReader
            let mut output = Vec::new();
            remote_table
                .read_file(&bundle_id, &original_path, pond_txn_id, &mut output)
                .await?;

            // Write to local Delta table's object store
            let byte_len = output.len();
            let bytes = Bytes::from(output);
            local_store
                .put(&file_path, bytes.into())
                .await
                .map_err(|e| {
                    RemoteError::TableOperation(format!("Failed to write {}: {}", original_path, e))
                })?;

            log::debug!("      ✓ Pulled {} bytes", byte_len);
        }
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
    log::debug!("📋 LIST FILES");

    // List all files - we don't filter by txn_id anymore
    let _ = txn_id; // Unused now
    let files = remote_table.list_files("").await?;

    if files.is_empty() {
        log::debug!("   No files found");
        return Ok(());
    }

    log::debug!("   Found {} files:", files.len());
    for (bundle_id, original_path, pond_txn_id, size) in files {
        log::debug!(
            "   - {} | txn {} | {} | {} bytes",
            &bundle_id[..16.min(bundle_id.len())],
            pond_txn_id,
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
    log::debug!("✓ VERIFY: Checking backup integrity");

    if let Some(id) = bundle_id {
        // Verify specific bundle - need to find files with this bundle_id
        log::debug!("   Verifying bundle: {}", &id[..16.min(id.len())]);

        // Query to find all files with this bundle_id
        let files = remote_table.list_files("").await?;
        let matching_files: Vec<_> = files
            .into_iter()
            .filter(|(bid, _, _, _)| bid == &id)
            .collect();

        if matching_files.is_empty() {
            return Err(RemoteError::FileNotFound(id));
        }

        for (bundle_id, file_path, pond_txn_id, _) in matching_files {
            let mut output = Vec::new();
            remote_table
                .read_file(&bundle_id, &file_path, pond_txn_id, &mut output)
                .await?;
            log::debug!("   ✓ {} OK ({} bytes)", file_path, output.len());
        }
    } else {
        // Verify all bundles
        log::info!("   Verifying all bundles...");

        let files = remote_table.list_files("").await?;
        let total_files = files.len();
        log::debug!("   Found {} files to verify", total_files);

        let mut verified = 0;
        for (bundle_id, file_path, pond_txn_id, _size) in files {
            let mut output = Vec::new();
            match remote_table
                .read_file(&bundle_id, &file_path, pond_txn_id, &mut output)
                .await
            {
                Ok(_) => {
                    verified += 1;
                }
                Err(e) => {
                    log::error!("   ✗ Failed to verify {}: {}", &bundle_id[..16], e);
                }
            }
        }

        log::debug!("   ✓ Verified {}/{} bundles", verified, total_files);
    }

    Ok(())
}

/// Show storage details and generate verification script
///
/// Lists files in remote backup matching the pattern and generates a shell script
/// that can be used to verify the files using external tools (duckdb, b3sum).
/// This provides confidence that backup data is accessible and verifiable without
/// using pond software.
#[allow(clippy::print_stdout)]
async fn execute_show(
    remote_table: RemoteTable,
    config: &RemoteConfig,
    table_path: &str,
    storage_options: std::collections::HashMap<String, String>,
    pattern: &str,
    show_script: bool,
) -> Result<(), RemoteError> {
    log::info!("📋 SHOW: Storage details for pattern '{}'", pattern);

    // List all files from remote
    let files = remote_table.list_files("").await?;

    if files.is_empty() {
        println!("No files found in remote backup.");
        return Ok(());
    }

    // Filter files by pattern (simple glob matching on path)
    let matching_files: Vec<_> = files
        .into_iter()
        .filter(|(_, path, _, _)| path_matches_pattern(path, pattern))
        .collect();

    if matching_files.is_empty() {
        println!("No files match pattern: {}", pattern);
        return Ok(());
    }

    println!("\n=== Files in Remote Backup ===\n");
    println!("{:<50} {:>10}  {:>6}  BUNDLE_ID", "PATH", "SIZE", "TXN",);
    println!("{}", "-".repeat(100));

    for (bundle_id, path, pond_txn_id, size) in &matching_files {
        let bundle_short = if bundle_id.len() > 20 {
            format!("{}...", &bundle_id[..20])
        } else {
            bundle_id.clone()
        };
        println!(
            "{:<50} {:>10}  {:>6}  {}",
            path,
            format_size(*size),
            pond_txn_id,
            bundle_short
        );
    }

    println!("\nTotal: {} files\n", matching_files.len());

    if show_script {
        // Generate verification script
        println!("=== Verification Script ===\n");
        println!("# This script verifies backup data using external tools only.");
        println!("# Requirements: duckdb, b3sum (optional), jq (optional)\n");

        generate_verification_script(&matching_files, config, table_path, &storage_options);
    } else {
        println!("Tip: Use --script to generate a verification script for external tools.\n");
    }

    Ok(())
}

/// Check if a path matches a simple glob pattern
fn path_matches_pattern(path: &str, pattern: &str) -> bool {
    // Handle common patterns
    if pattern == "/*" || pattern == "*" {
        return true;
    }

    // Simple prefix matching for "/data/*" style patterns
    if let Some(prefix) = pattern.strip_suffix("/*") {
        return path.starts_with(prefix) || path.starts_with(&prefix[1..]); // Handle with or without leading /
    }

    if let Some(prefix) = pattern.strip_suffix("*") {
        return path.starts_with(prefix) || path.starts_with(&prefix[1..]);
    }

    // Exact match
    path == pattern || path == &pattern[1..] // Handle with or without leading /
}

/// Format file size for display
fn format_size(bytes: i64) -> String {
    const KB: i64 = 1024;
    const MB: i64 = KB * 1024;
    const GB: i64 = MB * 1024;

    if bytes >= GB {
        format!("{:.1}GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1}MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1}KB", bytes as f64 / KB as f64)
    } else {
        format!("{}B", bytes)
    }
}

/// Generate verification script for external tools
#[allow(clippy::print_stdout)]
fn generate_verification_script(
    files: &[(String, String, i64, i64)],
    config: &RemoteConfig,
    table_path: &str,
    storage_options: &std::collections::HashMap<String, String>,
) {
    // Redact sensitive values for display
    let redacted_access_key = if config.access_key.is_empty() {
        String::new()
    } else {
        "<REDACTED_ACCESS_KEY>".to_string()
    };
    let redacted_secret_key = if config.secret_key.is_empty() {
        String::new()
    } else {
        "<REDACTED_SECRET_KEY>".to_string()
    };

    let is_s3 = config.url.starts_with("s3://");
    let local_path = config.url.strip_prefix("file://").unwrap_or(&config.url);
    let duckdb_table_ref = if is_s3 {
        format!("delta_scan('{}')", table_path)
    } else {
        format!("delta_scan('{}')", local_path)
    };

    println!("╔════════════════════════════════════════════════════════════════════════════════╗");
    println!("║  BACKUP VERIFICATION SCRIPTS                                                   ║");
    println!("║  Each section below is a standalone, copy-pastable script.                     ║");
    println!("╚════════════════════════════════════════════════════════════════════════════════╝");
    println!();

    // ═══════════════════════════════════════════════════════════════════════════════
    // SECTION 1: Environment Setup
    // ═══════════════════════════════════════════════════════════════════════════════
    println!("┌────────────────────────────────────────────────────────────────────────────────┐");
    println!("│ 1. ENVIRONMENT SETUP (run first)                                              │");
    println!("└────────────────────────────────────────────────────────────────────────────────┘");
    println!();

    if is_s3 {
        println!("# Set these environment variables for S3/MinIO access:");
        println!("# (Replace <REDACTED_*> with your actual credentials)");
        println!();
        println!("```bash");
        if !config.endpoint.is_empty() {
            println!("export AWS_ENDPOINT_URL=\"{}\"", config.endpoint);
        }
        println!(
            "export AWS_REGION=\"{}\"",
            if config.region.is_empty() {
                "us-east-1"
            } else {
                &config.region
            }
        );
        if !config.access_key.is_empty() {
            println!("export AWS_ACCESS_KEY_ID=\"{}\"", redacted_access_key);
        }
        if !config.secret_key.is_empty() {
            println!("export AWS_SECRET_ACCESS_KEY=\"{}\"", redacted_secret_key);
        }
        println!("```");
    } else {
        println!("# Local filesystem - no credentials needed");
        println!("# Table path: {}", local_path);
        println!();
        println!("```bash");
        println!("# Verify the backup directory exists:");
        println!("ls -la {}", local_path);
        println!("```");
    }
    println!();

    // ═══════════════════════════════════════════════════════════════════════════════
    // SECTION 2: List all files with DuckDB
    // ═══════════════════════════════════════════════════════════════════════════════
    println!("┌────────────────────────────────────────────────────────────────────────────────┐");
    println!("│ 2. LIST ALL BACKED UP FILES (DuckDB)                                          │");
    println!("└────────────────────────────────────────────────────────────────────────────────┘");
    println!();
    println!("```bash");
    println!("duckdb -c \"");
    println!("INSTALL delta; LOAD delta;");
    if is_s3 {
        print_duckdb_s3_config(storage_options, &redacted_access_key, &redacted_secret_key);
    }
    println!("SELECT path, total_size, root_hash");
    println!("FROM {}", duckdb_table_ref);
    println!("GROUP BY path, total_size, root_hash");
    println!("ORDER BY path;");
    println!("\"");
    println!("```");
    println!();

    // ═══════════════════════════════════════════════════════════════════════════════
    // SECTION 3: Extract and verify specific files
    // ═══════════════════════════════════════════════════════════════════════════════
    println!("┌────────────────────────────────────────────────────────────────────────────────┐");
    println!("│ 3. EXTRACT FILES FROM BACKUP                                                  │");
    println!("│    Reassembles chunked data from storage to local filesystem                  │");
    println!("└────────────────────────────────────────────────────────────────────────────────┘");
    println!();

    // Show examples for first few files
    let example_files: Vec<_> = files.iter().take(3).collect();

    for (bundle_id, path, pond_txn_id, size) in &example_files {
        let safe_filename = path.replace(['/', '='], "_");
        let output_path = format!("/tmp/extracted_{}", safe_filename);

        println!("# ── File: {} ({}) ──", path, format_size(*size));
        println!();
        println!("```bash");
        println!("# Extract to: {}", output_path);
        println!("duckdb -c \"");
        println!("INSTALL delta; LOAD delta;");
        if is_s3 {
            print_duckdb_s3_config(storage_options, &redacted_access_key, &redacted_secret_key);
        }
        println!("COPY (");
        println!("  SELECT chunk_data");
        println!("  FROM {}", duckdb_table_ref);
        println!("  WHERE bundle_id = '{}'", bundle_id);
        println!("    AND path = '{}'", path);
        println!("    AND pond_txn_id = {}", pond_txn_id);
        println!("  ORDER BY chunk_id");
        println!(") TO '{}' (FORMAT 'parquet');", output_path);
        println!("\"");
        println!("```");
        println!();
    }

    if files.len() > 3 {
        println!(
            "# ... and {} more files (adjust bundle_id/path/pond_txn_id as needed)",
            files.len() - 3
        );
        println!();
    }

    // ═══════════════════════════════════════════════════════════════════════════════
    // SECTION 4: Verify with BLAKE3
    // ═══════════════════════════════════════════════════════════════════════════════
    println!("┌────────────────────────────────────────────────────────────────────────────────┐");
    println!("│ 4. VERIFY BLAKE3 CHECKSUMS                                                    │");
    println!("│    Compare extracted file hash against stored root_hash                       │");
    println!("└────────────────────────────────────────────────────────────────────────────────┘");
    println!();

    for (bundle_id, path, pond_txn_id, _size) in &example_files {
        let safe_filename = path.replace(['/', '='], "_");
        let output_path = format!("/tmp/extracted_{}", safe_filename);

        println!("# ── Verify: {} ──", path);
        println!();
        println!("```bash");
        println!("# Step 1: Get the expected root_hash from backup");
        println!("EXPECTED_HASH=$(duckdb -noheader -csv -c \"");
        println!("INSTALL delta; LOAD delta;");
        if is_s3 {
            print_duckdb_s3_config(storage_options, &redacted_access_key, &redacted_secret_key);
        }
        println!("SELECT DISTINCT root_hash FROM {}", duckdb_table_ref);
        println!(
            "WHERE bundle_id = '{}' AND path = '{}' AND pond_txn_id = {};",
            bundle_id, path, pond_txn_id
        );
        println!("\")");
        println!();
        println!("# Step 2: Extract the raw binary data and compute BLAKE3");
        println!("duckdb -c \"");
        println!("INSTALL delta; LOAD delta;");
        if is_s3 {
            print_duckdb_s3_config(storage_options, &redacted_access_key, &redacted_secret_key);
        }
        println!("COPY (");
        println!("  SELECT chunk_data FROM {}", duckdb_table_ref);
        println!(
            "  WHERE bundle_id = '{}' AND path = '{}' AND pond_txn_id = {}",
            bundle_id, path, pond_txn_id
        );
        println!("  ORDER BY chunk_id");
        println!(") TO '{}' WITH (FORMAT 'binary');", output_path);
        println!("\"");
        println!();
        println!("# Step 3: Compute BLAKE3 of extracted file");
        println!("ACTUAL_HASH=$(b3sum {} | cut -d' ' -f1)", output_path);
        println!();
        println!("# Step 4: Compare");
        println!("echo \"Expected: $EXPECTED_HASH\"");
        println!("echo \"Actual:   $ACTUAL_HASH\"");
        println!("if [ \"$EXPECTED_HASH\" = \"$ACTUAL_HASH\" ]; then");
        println!("  echo \"✓ BLAKE3 MATCH - File verified!\"");
        println!("else");
        println!("  echo \"✗ BLAKE3 MISMATCH - File may be corrupted!\"");
        println!("  exit 1");
        println!("fi");
        println!("```");
        println!();
    }

    // ═══════════════════════════════════════════════════════════════════════════════
    // SECTION 5: Alternative verification with SHA256
    // ═══════════════════════════════════════════════════════════════════════════════
    println!("┌────────────────────────────────────────────────────────────────────────────────┐");
    println!("│ 5. ALTERNATIVE: VERIFY WITH SHA256 (if b3sum not available)                   │");
    println!("└────────────────────────────────────────────────────────────────────────────────┘");
    println!();

    if let Some((_, path, _, _)) = example_files.first() {
        let safe_filename = path.replace(['/', '='], "_");
        let output_path = format!("/tmp/extracted_{}", safe_filename);

        println!("```bash");
        println!(
            "# SHA256 verification (note: DuckPond uses BLAKE3, so this is for general integrity)"
        );
        println!("shasum -a 256 {}", output_path);
        println!();
        println!("# Or with openssl:");
        println!("openssl dgst -sha256 {}", output_path);
        println!("```");
        println!();
    }

    // ═══════════════════════════════════════════════════════════════════════════════
    // SECTION 6: Full extraction script
    // ═══════════════════════════════════════════════════════════════════════════════
    println!("┌────────────────────────────────────────────────────────────────────────────────┐");
    println!("│ 6. EXTRACT ALL FILES (complete script)                                        │");
    println!("└────────────────────────────────────────────────────────────────────────────────┘");
    println!();
    println!("```bash");
    println!("#!/bin/bash");
    println!("# Extract all {} files from backup", files.len());
    println!("set -e");
    println!();
    println!("OUTPUT_DIR=\"/tmp/pond_backup_extract\"");
    println!("mkdir -p \"$OUTPUT_DIR\"");
    println!();

    if is_s3 {
        println!("# S3/MinIO credentials (replace <REDACTED_*> values)");
        if !config.endpoint.is_empty() {
            println!("export AWS_ENDPOINT_URL=\"{}\"", config.endpoint);
        }
        println!(
            "export AWS_REGION=\"{}\"",
            if config.region.is_empty() {
                "us-east-1"
            } else {
                &config.region
            }
        );
        println!("export AWS_ACCESS_KEY_ID=\"{}\"", redacted_access_key);
        println!("export AWS_SECRET_ACCESS_KEY=\"{}\"", redacted_secret_key);
        println!();
    }

    println!("# Get list of all files");
    println!("echo \"Extracting files from backup...\"");
    println!();

    // Generate extraction command for each file
    for (bundle_id, path, pond_txn_id, _size) in files.iter().take(5) {
        let safe_filename = path.replace(['/', '='], "_");
        println!("# {}", path);
        println!("duckdb -c \"");
        println!("INSTALL delta; LOAD delta;");
        if is_s3 {
            print_duckdb_s3_config(storage_options, &redacted_access_key, &redacted_secret_key);
        }
        println!(
            "COPY (SELECT list_reduce(list(chunk_data ORDER BY chunk_id), (a, b) -> a || b) AS data FROM {} WHERE bundle_id='{}' AND path='{}' AND pond_txn_id={}) TO '$OUTPUT_DIR/{}' (FORMAT BLOB);",
            duckdb_table_ref, bundle_id, path, pond_txn_id, safe_filename
        );
        println!("\"");
        println!();
    }

    if files.len() > 5 {
        println!("# ... repeat for remaining {} files", files.len() - 5);
    }

    println!("echo \"Extraction complete. Files in $OUTPUT_DIR\"");
    println!("ls -la \"$OUTPUT_DIR\"");
    println!("```");
    println!();

    // ═══════════════════════════════════════════════════════════════════════════════
    // SECTION 7: Tool installation
    // ═══════════════════════════════════════════════════════════════════════════════
    println!("┌────────────────────────────────────────────────────────────────────────────────┐");
    println!("│ 7. TOOL INSTALLATION                                                          │");
    println!("└────────────────────────────────────────────────────────────────────────────────┘");
    println!();
    println!("```bash");
    println!("# Install DuckDB");
    println!("# macOS:");
    println!("brew install duckdb");
    println!();
    println!("# Linux:");
    println!(
        "curl -LO https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip"
    );
    println!("unzip duckdb_cli-linux-amd64.zip");
    println!("chmod +x duckdb && sudo mv duckdb /usr/local/bin/");
    println!();
    println!("# Install b3sum (BLAKE3)");
    println!("# macOS:");
    println!("brew install b3sum");
    println!();
    println!("# Linux (via cargo):");
    println!("cargo install b3sum");
    println!("```");
}

/// Helper to print DuckDB S3 configuration
#[allow(clippy::print_stdout)]
fn print_duckdb_s3_config(
    storage_options: &std::collections::HashMap<String, String>,
    redacted_access_key: &str,
    redacted_secret_key: &str,
) {
    println!("INSTALL httpfs; LOAD httpfs;");
    println!(
        "SET s3_region='{}';",
        storage_options
            .get("region")
            .unwrap_or(&"us-east-1".to_string())
    );
    if let Some(endpoint) = storage_options.get("endpoint") {
        let clean_endpoint = endpoint
            .trim_start_matches("http://")
            .trim_start_matches("https://");
        println!("SET s3_endpoint='{}';", clean_endpoint);
        println!("SET s3_url_style='path';");
        if endpoint.starts_with("http://") {
            println!("SET s3_use_ssl=false;");
        }
    }
    if storage_options.contains_key("access_key_id") {
        println!("SET s3_access_key_id='{}';", redacted_access_key);
    }
    if storage_options.contains_key("secret_access_key") {
        println!("SET s3_secret_access_key='{}';", redacted_secret_key);
    }
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
        if config.allow_http {
            builder = builder.with_allow_http(true);
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

/// Scan remote storage for available transaction sequences
///
/// Uses efficient object_store listing to find all FILE-META-* partitions
/// without querying parquet files. Returns range of available transactions.
///
/// # Arguments
/// * `remote_url` - URL to the remote backup table (e.g., "file:///path" or "s3://bucket/path")
/// * `pond_id` - Optional pond ID to filter by (currently unused, returns all transactions)
///
/// # Returns
/// Vec of transaction IDs (txn_seq values) available in the backup, in order
pub async fn scan_remote_versions(
    remote_url: &str,
    pond_id: Option<&uuid7::Uuid>,
) -> Result<Vec<i64>, RemoteError> {
    // Open the RemoteTable - Delta Lake handles object_store internally
    let remote_table = crate::RemoteTable::open(remote_url).await?;

    // Try new FILE-META approach first
    let max_txn = remote_table
        .find_max_transaction(pond_id.map(|id| id.to_string()).as_deref())
        .await?;

    match max_txn {
        Some(max) => {
            let transactions: Vec<i64> = (1..=max).collect();
            log::debug!(
                "Found {} transactions in remote backup (1..={})",
                transactions.len(),
                max
            );
            Ok(transactions)
        }
        None => {
            // Fallback: Try old metadata-based approach for backward compatibility
            log::debug!("No FILE-META partitions found, trying old metadata approach");

            if let Some(pond_id) = pond_id {
                let transactions = remote_table
                    .list_transactions_from_metadata(&pond_id.to_string())
                    .await?;
                log::debug!(
                    "Found {} transactions using metadata approach",
                    transactions.len()
                );
                Ok(transactions)
            } else {
                log::info!("No transactions found in remote backup");
                Ok(Vec::new())
            }
        }
    }
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
/// 1. Queries for all files in the transaction partition (FILE-META-{txn_seq})
/// 2. Uses ChunkedReader to reconstruct each file from chunks
/// 3. Writes files directly to the local Delta table's object store (for parquet/delta files)
/// 4. Writes large files to the filesystem `_large_files` directory
/// 5. Calls table.load() to refresh Delta table state
///
/// # Arguments
/// * `remote_table` - The remote backup table to read from
/// * `local_table` - The local Delta table to write to
/// * `pond_path` - Path to the pond directory (parent of 'data' directory)
/// * `txn_seq` - Transaction sequence number to restore
///
/// # Errors
/// Returns error if:
/// - Cannot query transaction files
/// - Cannot reconstruct files
/// - Cannot write to local table
pub async fn apply_parquet_files_from_remote(
    remote_table: &crate::RemoteTable,
    local_table: &mut deltalake::DeltaTable,
    pond_path: &std::path::Path,
    txn_seq: i64,
) -> Result<(), RemoteError> {
    log::info!("Restoring transaction {} from remote backup", txn_seq);

    // Phase 1: Query for all files in this transaction (efficient partition query)
    let files = remote_table.list_transaction_files(txn_seq).await?;

    if files.is_empty() {
        log::warn!("No files found for transaction {}", txn_seq);
        return Ok(());
    }

    log::debug!("Found {} files to restore in transaction", files.len());

    // Get the object store from the local table
    let object_store = local_table.object_store();

    // Phase 2: Download and write files
    // - Parquet files and Delta logs go to the object store
    // - Large files (with _large_files/ prefix) go to the filesystem
    for (bundle_id, path, _sha256, size, pond_txn_id) in &files {
        log::debug!("Restoring file: {} ({} bytes)", path, size);

        // Create a buffer to hold the reconstructed file
        let mut buffer = Vec::new();

        // Read file from remote using ChunkedReader
        remote_table
            .read_file(bundle_id, path, *pond_txn_id, &mut buffer)
            .await?;

        // Check if this is a large file (stored in _large_files/ directory)
        if path.starts_with("_large_files/") {
            // Large files go to the filesystem, not the object store
            let large_file_fs_path = pond_path.join(path);

            // Ensure parent directory exists
            if let Some(parent) = large_file_fs_path.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    RemoteError::TableOperation(format!(
                        "Failed to create _large_files directory: {}",
                        e
                    ))
                })?;
            }

            // Write to filesystem
            tokio::fs::write(&large_file_fs_path, &buffer)
                .await
                .map_err(|e| {
                    RemoteError::TableOperation(format!(
                        "Failed to write large file to {}: {}",
                        large_file_fs_path.display(),
                        e
                    ))
                })?;

            log::debug!(
                "  ✓ Restored large file to {}",
                large_file_fs_path.display()
            );
        } else {
            // Regular files (parquet, delta log) go to the object store
            let object_store_path = object_store::path::Path::from(path.as_str());
            object_store
                .put(&object_store_path, buffer.into())
                .await
                .map_err(|e| {
                    RemoteError::TableOperation(format!(
                        "Failed to write file to local table: {}",
                        e
                    ))
                })?;

            log::debug!("  ✓ Restored {}", path);
        }
    }

    // Phase 3: Reload the table to pick up the new files
    local_table
        .load()
        .await
        .map_err(|e| RemoteError::TableOperation(format!("Failed to reload local table: {}", e)))?;

    log::info!("  ✓ Transaction {} restored", txn_seq);
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

/// Restore large files from remote backup to the filesystem
///
/// Large files are stored with bundle_id="POND-FILE-{sha256}" and path="_large_files/sha256=..."
/// This function lists all such files from the remote table and restores them to the pond's
/// _large_files directory.
///
/// # Arguments
/// * `remote_table` - The remote backup table to read from
/// * `pond_path` - Path to the pond directory (parent of 'data' directory)
///
/// # Errors
/// Returns error if cannot read or write files
pub async fn restore_large_files_from_remote(
    remote_table: &crate::RemoteTable,
    pond_path: &std::path::Path,
) -> Result<usize, RemoteError> {
    log::debug!("Scanning for large files in remote backup...");

    // List all files in remote
    let all_files = remote_table.list_files("").await?;

    // Filter to only large files (those with _large_files/ prefix in path)
    let large_files: Vec<_> = all_files
        .into_iter()
        .filter(|(_, path, _, _)| path.starts_with("_large_files/"))
        .collect();

    if large_files.is_empty() {
        log::info!("   No large files found in backup");
        return Ok(0);
    }

    log::debug!("   Found {} large files to restore", large_files.len());

    // Large files are stored within the 'data' subdirectory of the pond
    let data_path = pond_path.join("data");

    let mut restored = 0;
    for (bundle_id, path, pond_txn_id, _size) in large_files {
        let large_file_fs_path = data_path.join(&path);

        // Skip if already exists
        if large_file_fs_path.exists() {
            log::debug!("   Skip {} (already exists)", path);
            continue;
        }

        log::debug!("   Restoring: {}", path);

        // Download file from remote
        let mut buffer = Vec::new();
        remote_table
            .read_file(&bundle_id, &path, pond_txn_id, &mut buffer)
            .await?;

        // Ensure parent directory exists
        if let Some(parent) = large_file_fs_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                RemoteError::TableOperation(format!(
                    "Failed to create _large_files directory: {}",
                    e
                ))
            })?;
        }

        // Write to filesystem
        let byte_len = buffer.len();
        tokio::fs::write(&large_file_fs_path, &buffer)
            .await
            .map_err(|e| {
                RemoteError::TableOperation(format!(
                    "Failed to write large file to {}: {}",
                    large_file_fs_path.display(),
                    e
                ))
            })?;

        log::debug!("      ✓ Restored {} bytes", byte_len);
        restored += 1;
    }

    log::debug!("   ✓ Restored {} large files", restored);
    Ok(restored)
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

/// Get large files from _large_files directory
/// Scans both flat (sha256=X) and hierarchical (sha256_16=XX/sha256=X) structures
/// Returns Vec of (absolute_path, file_size)
async fn get_large_files(pond_path: &Path) -> Result<Vec<(String, i64)>, RemoteError> {
    let large_files_dir = pond_path.join("_large_files");

    if !large_files_dir.exists() {
        return Ok(Vec::new());
    }

    let mut files = Vec::new();
    let mut entries = tokio::fs::read_dir(&large_files_dir).await.map_err(|e| {
        RemoteError::TableOperation(format!("Failed to read _large_files directory: {}", e))
    })?;

    while let Some(entry) = entries.next_entry().await.map_err(|e| {
        RemoteError::TableOperation(format!("Failed to read directory entry: {}", e))
    })? {
        let path = entry.path();
        let file_type = entry
            .file_type()
            .await
            .map_err(|e| RemoteError::TableOperation(format!("Failed to get file type: {}", e)))?;

        if file_type.is_file() {
            let filename = entry.file_name();
            let name = filename.to_string_lossy();
            if name.starts_with("sha256=") {
                // Flat structure: _large_files/sha256=X
                let metadata = tokio::fs::metadata(&path).await.map_err(|e| {
                    RemoteError::TableOperation(format!("Failed to get file metadata: {}", e))
                })?;
                files.push((path.to_string_lossy().to_string(), metadata.len() as i64));
            }
        } else if file_type.is_dir() {
            let dirname = entry.file_name();
            let dir_name = dirname.to_string_lossy();
            if dir_name.starts_with("sha256_16=") {
                // Hierarchical structure: _large_files/sha256_16=XX/sha256=Y
                let mut subentries = tokio::fs::read_dir(&path).await.map_err(|e| {
                    RemoteError::TableOperation(format!("Failed to read subdirectory: {}", e))
                })?;

                while let Some(subentry) = subentries.next_entry().await.map_err(|e| {
                    RemoteError::TableOperation(format!("Failed to read subdirectory entry: {}", e))
                })? {
                    let subpath = subentry.path();
                    let subfile_type = subentry.file_type().await.map_err(|e| {
                        RemoteError::TableOperation(format!("Failed to get file type: {}", e))
                    })?;

                    if subfile_type.is_file() {
                        let subfilename = subentry.file_name();
                        let subname = subfilename.to_string_lossy();
                        if subname.starts_with("sha256=") {
                            let metadata = tokio::fs::metadata(&subpath).await.map_err(|e| {
                                RemoteError::TableOperation(format!(
                                    "Failed to get file metadata: {}",
                                    e
                                ))
                            })?;
                            files.push((
                                subpath.to_string_lossy().to_string(),
                                metadata.len() as i64,
                            ));
                        }
                    }
                }
            }
        }
    }

    Ok(files)
}

/// Get current parquet files from Delta table
/// Get NEW files added in a specific Delta transaction
///
/// Reads _delta_log/{version:020}.json and extracts 'add' actions.
/// Returns only the NEW parquet files added in this transaction, not the cumulative state.
/// This is the incremental delta - exactly what needs to be backed up for this transaction.
async fn get_delta_commit_files(
    table: &deltalake::DeltaTable,
    version: i64,
) -> Result<Vec<(String, i64)>, RemoteError> {
    use object_store::path::Path;

    let log_store = table.log_store();
    let commit_log_path = Path::from(format!("_delta_log/{:020}.json", version));

    // Read the commit log file
    let log_data = log_store
        .object_store(None)
        .get(&commit_log_path)
        .await
        .map_err(|e| {
            RemoteError::TableOperation(format!(
                "Failed to read commit log for version {}: {}",
                version, e
            ))
        })?;

    let log_bytes = log_data.bytes().await.map_err(|e| {
        RemoteError::TableOperation(format!("Failed to read commit log bytes: {}", e))
    })?;

    let log_content = String::from_utf8(log_bytes.to_vec())
        .map_err(|e| RemoteError::TableOperation(format!("Invalid UTF-8 in commit log: {}", e)))?;

    // Parse each line as a JSON action
    let mut files = Vec::new();
    for line in log_content.lines() {
        if line.trim().is_empty() {
            continue;
        }

        let action: serde_json::Value = serde_json::from_str(line).map_err(|e| {
            RemoteError::TableOperation(format!("Failed to parse commit log line: {}", e))
        })?;

        // Look for 'add' actions
        if let Some(add) = action.get("add")
            && let (Some(path), Some(size)) = (add.get("path"), add.get("size"))
            && let (Some(path_str), Some(size_i64)) = (path.as_str(), size.as_i64())
        {
            files.push((path_str.to_string(), size_i64));
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
