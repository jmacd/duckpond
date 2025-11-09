//! Remote storage factory for S3-compatible object stores

use crate::TLogFSError;
use crate::data_taxonomy::{ApiKey, ApiSecret, ServiceEndpoint};
use crate::factory::{ExecutionMode, FactoryCommand, FactoryContext};
use base64::Engine;
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinyfs::{NodeID, Result as TinyFSResult};

/// Remote factory subcommands for explicit user operations
#[derive(Debug, Parser)]
enum RemoteCommand {
    /// Push local data to remote backup storage
    ///
    /// Creates backup bundles of new transaction versions and uploads them.
    /// This is the operation mode for the source/primary pond.
    /// Typically invoked automatically post-commit via Steward.
    ///
    /// Example: pond run /etc/system.d/10-remote push
    Push,

    /// Pull new versions from remote backup storage
    ///
    /// Downloads and applies new backup bundles from the remote.
    /// This is the operation mode for replica ponds that sync from a primary.
    /// Typically invoked automatically post-commit via Steward.
    ///
    /// Example: pond run /etc/system.d/10-remote pull
    Pull,

    /// Generate replication command with pond metadata
    ///
    /// This reads the current pond's remote configuration and identity metadata,
    /// then outputs a YAML configuration that can be used to create a replica pond.
    ///
    /// Example: pond run /etc/system.d/10-remote replicate
    Replicate,

    /// List available backup bundles in remote storage
    ///
    /// Shows all versions available for restore from the remote backup.
    ///
    /// Example: pond run /etc/system.d/10-remote list-bundles
    /// @@@
    ListBundles {
        /// Show detailed information for each bundle
        #[arg(long)]
        verbose: bool,
    },

    /// Verify backup integrity
    ///
    /// Checks that backup bundles are complete and valid.
    ///
    /// Example: pond run /etc/system.d/10-remote verify --version 5
    /// @@@
    Verify {
        /// Specific version to verify (optional, verifies all if not provided)
        #[arg(long)]
        version: Option<i64>,
    },
}

impl FactoryCommand for RemoteCommand {
    fn allowed(&self) -> ExecutionMode {
        match self {
            Self::Push => ExecutionMode::ControlWriter,
            Self::Pull => ExecutionMode::ControlWriter,
            Self::Replicate => ExecutionMode::PondReadWriter,
            Self::ListBundles { .. } => ExecutionMode::PondReadWriter,
            Self::Verify { .. } => ExecutionMode::PondReadWriter,
        }
    }
}

/// Remote storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteConfig {
    /// Storage URL with scheme (e.g., "file:///path/to/backup" or "s3://bucket-name")
    #[serde(default = "default_storage_url")]
    pub url: ServiceEndpoint<String>,

    /// AWS region or compatible region identifier (optional, inferred from URL or credentials)
    #[serde(default)]
    pub region: String,

    /// Access key ID for authentication (S3/cloud storage)
    #[serde(default = "default_api_key")]
    pub key: ApiKey<String>,

    /// Secret access key for authentication (S3/cloud storage)
    #[serde(default = "default_api_secret")]
    pub secret: ApiSecret<String>,

    /// S3-compatible endpoint override (for non-AWS S3 services like MinIO)
    #[serde(default = "default_service_endpoint")]
    pub endpoint: ServiceEndpoint<String>,

    /// Compression level for bundles (0-21, default 3)
    #[serde(default = "default_compression_level")]
    pub compression_level: i32,
}

fn default_storage_url() -> ServiceEndpoint<String> {
    ServiceEndpoint::new(String::new())
}

fn default_compression_level() -> i32 {
    3
}

fn default_api_key() -> ApiKey<String> {
    ApiKey::new(String::new())
}

fn default_api_secret() -> ApiSecret<String> {
    ApiSecret::new(String::new())
}

fn default_service_endpoint() -> ServiceEndpoint<String> {
    ServiceEndpoint::new(String::new())
}

/// Complete replication configuration including remote config and pond metadata
/// This is serialized to JSON and base64-encoded for the `pond init --config=BASE64` command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// Remote storage configuration
    pub remote: RemoteConfig,

    /// Original pond identity metadata (preserved in replica)
    pub pond_id: String,
    pub birth_timestamp: i64,
    pub birth_hostname: String,
    pub birth_username: String,
}

impl ReplicationConfig {
    /// Encode to base64 string for command-line usage
    pub fn to_base64(&self) -> Result<String, TLogFSError> {
        let json = serde_json::to_string(self).map_err(|e| TLogFSError::Transaction {
            message: format!("Failed to serialize config: {}", e),
        })?;
        Ok(base64::engine::general_purpose::STANDARD.encode(json.as_bytes()))
    }

    /// Decode from base64 string
    pub fn from_base64(encoded: &str) -> Result<Self, TLogFSError> {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|e| TLogFSError::Transaction {
                message: format!("Invalid base64: {}", e),
            })?;
        let json_str = String::from_utf8(decoded).map_err(|e| TLogFSError::Transaction {
            message: format!("Invalid UTF-8 in decoded data: {}", e),
        })?;
        serde_json::from_str(&json_str).map_err(|e| TLogFSError::Transaction {
            message: format!("Failed to parse replication config: {}", e),
        })
    }
}

fn validate_remote_config(config_bytes: &[u8]) -> TinyFSResult<Value> {
    let config_str = std::str::from_utf8(config_bytes)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid UTF-8: {}", e)))?;

    let config: RemoteConfig = serde_yaml::from_str(config_str)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid YAML: {}", e)))?;

    // Validate URL is provided
    if config.url.as_declassified().is_empty() {
        return Err(tinyfs::Error::Other("url field is required".to_string()));
    }

    // Parse URL to validate scheme
    let url_str = config.url.as_declassified();
    let url = url::Url::parse(url_str)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid URL '{}': {}", url_str, e)))?;

    let scheme = url.scheme();

    // Validate based on scheme
    match scheme {
        "file" => {
            // Local file storage - path is in URL
            if url.path().is_empty() {
                return Err(tinyfs::Error::Other(
                    "file:// URL must contain a path".to_string(),
                ));
            }
        }
        "s3" => {
            // S3 storage - require credentials
            if config.key.as_declassified().is_empty() {
                return Err(tinyfs::Error::Other(
                    "key field required for s3:// URLs".to_string(),
                ));
            }
            if config.secret.as_declassified().is_empty() {
                return Err(tinyfs::Error::Other(
                    "secret field required for s3:// URLs".to_string(),
                ));
            }
        }
        other => {
            return Err(tinyfs::Error::Other(format!(
                "Unsupported URL scheme '{}'. Supported: file://, s3://",
                other
            )));
        }
    }

    serde_json::to_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Serialization error: {}", e)))
}

async fn execute_remote(
    config: Value,
    context: FactoryContext,
    ctx: crate::factory::ExecutionContext,
) -> Result<(), TLogFSError> {
    let config: RemoteConfig = serde_json::from_value(config)
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Invalid config: {}", e))))?;

    log::info!("🌐 REMOTE FACTORY");
    log::info!("   Storage URL: {}", config.url.as_declassified());
    log::info!("   Context: {:?}", ctx);

    let cmd: RemoteCommand = ctx.to_command()?;

    log::info!("   Command: {:?}", cmd);

    // Build object store
    let store = build_object_store(&config)?;

    // Dispatch to command handler
    match cmd {
        RemoteCommand::Push => execute_push(store, context, config).await,
        RemoteCommand::Pull => execute_pull(store, context, config).await,
        RemoteCommand::Replicate => execute_replicate_subcommand(config, context).await,
        RemoteCommand::ListBundles { verbose } => {
            execute_list_bundles_subcommand(store, config, context.pond_metadata.as_ref(), verbose)
                .await
        }
        RemoteCommand::Verify { version } => {
            execute_verify_subcommand(store, config, context.pond_metadata.as_ref(), version).await
        }
    }
}

/// Subcommand: Generate replication command
#[allow(clippy::print_stdout)]
async fn execute_replicate_subcommand(
    config: RemoteConfig,
    context: FactoryContext,
) -> Result<(), TLogFSError> {
    // Get pond metadata from context
    let pond_metadata = context.pond_metadata.as_ref().ok_or_else(|| {
        TLogFSError::TinyFS(tinyfs::Error::Other(
            "Pond metadata not available in factory context".to_string(),
        ))
    })?;

    // Build the replication config
    let replication_config = ReplicationConfig {
        pond_id: pond_metadata.pond_id.to_string(),
        birth_timestamp: pond_metadata.birth_timestamp,
        birth_hostname: pond_metadata.birth_hostname.clone(),
        birth_username: pond_metadata.birth_username.clone(),
        remote: config.clone(),
    };

    log::info!("Replication config {:?}", replication_config);

    // Encode to base64
    let encoded = replication_config.to_base64()?;

    // Output the actual command to stdout for easy capture
    println!("pond init --config={}", encoded);

    Ok(())
}

/// Subcommand: List available backup bundles
async fn execute_list_bundles_subcommand(
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    _config: RemoteConfig,
    pond_metadata: Option<&crate::factory::PondMetadata>,
    verbose: bool,
) -> Result<(), TLogFSError> {
    use object_store::path::Path as ObjectPath;

    log::info!("📦 LIST BUNDLES SUBCOMMAND");
    log::info!("   Verbose: {}", verbose);

    // Get pond_id for filtering
    let pond_id = pond_metadata.map(|m| m.pond_id).ok_or_else(|| {
        TLogFSError::TinyFS(tinyfs::Error::Other(
            "Pond metadata not available for list-bundles command".to_string(),
        ))
    })?;

    // Scan for available versions (filtered by pond_id)
    let versions = scan_remote_versions(&store, Some(&pond_id)).await?;

    if versions.is_empty() {
        log::info!("   No backup bundles found");
        return Ok(());
    }

    log::info!("   Found {} backup version(s)", versions.len());

    for version in versions {
        let bundle_path =
            ObjectPath::from(format!("pond-{}-bundle-{:06}.tar.zst", pond_id, version));

        if verbose {
            // Get metadata for the bundle
            match store.head(&bundle_path).await {
                Ok(meta) => {
                    log::info!("   Version {}: {} bytes", version, meta.size);
                }
                Err(e) => {
                    log::warn!("   Version {}: metadata error: {}", version, e);
                }
            }
        } else {
            log::info!("   Version {}", version);
        }
    }

    Ok(())
}

/// Subcommand: Verify backup integrity
async fn execute_verify_subcommand(
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    _config: RemoteConfig,
    pond_metadata: Option<&crate::factory::PondMetadata>,
    version: Option<i64>,
) -> Result<(), TLogFSError> {
    log::info!("✓ VERIFY SUBCOMMAND");

    let pond_metadata = pond_metadata.ok_or_else(|| {
        TLogFSError::TinyFS(tinyfs::Error::Other(
            "Verify command requires pond metadata".to_string(),
        ))
    })?;

    let versions_to_check = if let Some(v) = version {
        log::info!("   Verifying version {}", v);
        vec![v]
    } else {
        log::info!("   Verifying all versions");
        scan_remote_versions(&store, Some(&pond_metadata.pond_id)).await?
    };

    if versions_to_check.is_empty() {
        log::info!("   No versions to verify");
        return Ok(());
    }

    let mut verified_count = 0;
    let mut skipped_count = 0;

    for v in versions_to_check {
        log::info!("   Checking version {}...", v);

        // Download bundle - skip if not found (404 error)
        let bundle_data = match download_bundle(&store, pond_metadata, v).await {
            Ok(data) => data,
            Err(e) => {
                // Check if this is a 404 / not found error
                let error_msg = format!("{}", e);
                if error_msg.contains("404")
                    || error_msg.contains("not found")
                    || error_msg.contains("NoSuchKey")
                {
                    log::warn!("   ⚠ Version {} bundle not found - skipping", v);
                    skipped_count += 1;
                    continue;
                } else {
                    // Other errors should fail
                    return Err(e);
                }
            }
        };

        // Extract to verify format
        let files = extract_bundle(&bundle_data).await?;

        log::info!("   ✓ Version {} OK ({} files)", v, files.len());
        verified_count += 1;
    }

    if skipped_count > 0 {
        log::info!(
            "✓ Verified {} versions ({} skipped due to missing bundles)",
            verified_count,
            skipped_count
        );
    } else {
        log::info!("✓ All {} versions verified successfully", verified_count);
    }
    Ok(())
}

/// Build an object store from configuration
pub fn build_object_store(
    config: &RemoteConfig,
) -> Result<std::sync::Arc<dyn object_store::ObjectStore>, TLogFSError> {
    use std::collections::HashMap;

    let url_str = config.url.as_declassified();
    let url = url::Url::parse(url_str).map_err(|e| {
        TLogFSError::TinyFS(tinyfs::Error::Other(format!(
            "Invalid URL '{}': {}",
            url_str, e
        )))
    })?;

    let scheme = url.scheme();

    match scheme {
        "file" => {
            // Local filesystem storage
            let path = url.path();
            log::info!("   Local path: {}", path);

            Ok(std::sync::Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(path).map_err(|e| {
                    TLogFSError::TinyFS(tinyfs::Error::Other(format!(
                        "Failed to create local store: {}",
                        e
                    )))
                })?,
            ))
        }
        "s3" => {
            // S3-compatible storage
            log::info!("   S3 URL: {}", url_str);

            // Extract bucket from URL
            let bucket = url.host_str().unwrap_or("");
            if bucket.is_empty() {
                return Err(TLogFSError::TinyFS(tinyfs::Error::Other(
                    "Bucket must be specified in URL (s3://bucket)".to_string(),
                )));
            };

            log::info!("   Bucket: {}", bucket);

            // If a custom endpoint is provided, use the builder pattern
            // This is necessary for S3-compatible services like Cloudflare R2, MinIO, etc.
            if !config.endpoint.as_declassified().is_empty() {
                log::info!(
                    "   Custom endpoint detected: {}",
                    config.endpoint.as_declassified()
                );

                use object_store::{ClientOptions, aws::AmazonS3Builder};

                let client_options =
                    ClientOptions::new().with_timeout(std::time::Duration::from_secs(30));

                let mut builder = AmazonS3Builder::new()
                    .with_bucket_name(bucket)
                    .with_access_key_id(config.key.as_declassified())
                    .with_secret_access_key(config.secret.as_declassified())
                    .with_endpoint(config.endpoint.as_declassified())
                    .with_client_options(client_options);

                // Region is optional for S3-compatible services
                if !config.region.is_empty() {
                    log::info!("   Region: {}", config.region);
                    builder = builder.with_region(&config.region);
                } else {
                    // Use auto region for S3-compatible services
                    log::info!("   Region: auto");
                    builder = builder.with_region("auto");
                }

                Ok(std::sync::Arc::new(builder.build().map_err(|e| {
                    TLogFSError::TinyFS(tinyfs::Error::Other(format!(
                        "Failed to build S3 client: {}",
                        e
                    )))
                })?))
            } else {
                // No custom endpoint - use parse_url_opts for standard AWS S3
                log::info!("   Using standard AWS S3");

                let mut options = HashMap::new();

                // Credentials
                if !config.key.as_declassified().is_empty() {
                    options.insert(
                        "aws_access_key_id".to_string(),
                        config.key.as_declassified().to_string(),
                    );
                }

                if !config.secret.as_declassified().is_empty() {
                    options.insert(
                        "aws_secret_access_key".to_string(),
                        config.secret.as_declassified().to_string(),
                    );
                }

                // Region (optional, can be inferred from credentials)
                if !config.region.is_empty() {
                    log::info!("   Region: {}", config.region);
                    options.insert("aws_region".to_string(), config.region.clone());
                }

                // Connection timeouts
                options.insert("timeout".to_string(), "30s".to_string());

                let final_url = url::Url::parse(&format!("s3://{}", bucket)).map_err(|e| {
                    TLogFSError::TinyFS(tinyfs::Error::Other(format!(
                        "Failed to construct S3 URL: {}",
                        e
                    )))
                })?;

                log::info!("   Final S3 URL: {}", final_url);

                // Use parse_url_opts to create the object store
                let (store, _path) =
                    object_store::parse_url_opts(&final_url, options).map_err(|e| {
                        TLogFSError::TinyFS(tinyfs::Error::Other(format!(
                            "Failed to create S3 store from URL: {}",
                            e
                        )))
                    })?;

                // parse_url_opts returns Box, we need Arc
                Ok(std::sync::Arc::from(store))
            }
        }
        other => Err(TLogFSError::TinyFS(tinyfs::Error::Other(format!(
            "Unsupported URL scheme '{}'. Supported: file://, s3://",
            other
        )))),
    }
}

/// Push mode: Backup local data to remote storage
async fn execute_push(
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    context: FactoryContext,
    config: RemoteConfig,
) -> Result<(), TLogFSError> {
    log::info!("📤 PUSH MODE: Backing up to remote");

    // Get the Delta table from State (contains transaction-scoped table reference)
    let table = context.state.table().await;
    let current_version = table.version().ok_or_else(|| {
        TLogFSError::TinyFS(tinyfs::Error::Other(
            "No Delta Lake version available".to_string(),
        ))
    })?;

    log::info!("   Current Delta version: {}", current_version);

    // Get pond_id from context (required for push operations)
    let pond_id = context
        .pond_metadata
        .as_ref()
        .ok_or_else(|| {
            TLogFSError::TinyFS(tinyfs::Error::Other(
                "Push operation requires pond metadata but none was provided".to_string(),
            ))
        })?
        .pond_id;

    // Determine which versions need to be backed up (filtered by pond_id)
    let last_backed_up_version = get_last_backed_up_version(&store, &pond_id).await?;

    let versions_to_backup: Vec<i64> = if let Some(last_version) = last_backed_up_version {
        log::info!("   Last backed up version: {}", last_version);
        // Backup all versions from last_version+1 to current_version
        ((last_version + 1)..=current_version).collect()
    } else {
        log::info!(
            "   No previous backups found - backing up all versions from 1 to {}",
            current_version
        );
        // Backup all versions from 1 to current
        (1..=current_version).collect()
    };

    if versions_to_backup.is_empty() {
        log::info!("   All versions already backed up");
        return Ok(());
    }

    log::info!(
        "   Will backup {} version(s): {:?}",
        versions_to_backup.len(),
        versions_to_backup
    );

    let num_versions = versions_to_backup.len();

    // Backup each version sequentially
    for version in versions_to_backup {
        log::info!("   Processing version {}...", version);

        // Detect changes in this version
        let changeset = detect_changes_from_delta_log(&table, version).await?;

        log::info!(
            "      Detected {} added files, {} removed files",
            changeset.added.len(),
            changeset.removed.len()
        );

        if changeset.added.is_empty() {
            log::info!("      No files to backup in version {}", version);
            continue;
        }

        log::info!("      Total size: {} bytes", changeset.total_bytes_added());

        // Create a bundle with the changed files
        create_backup_bundle(
            store.clone(),
            &changeset,
            &table,
            config.compression_level,
            pond_id,
        )
        .await?;

        log::info!("      ✓ Version {} backed up successfully", version);
    }

    log::info!(
        "   ✓ Remote backup complete - {} version(s) processed",
        num_versions
    );
    Ok(())
}

/// Pull mode: Continuously sync new versions from remote backup
async fn execute_pull(
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    context: FactoryContext,
    _config: RemoteConfig,
) -> Result<(), TLogFSError> {
    log::info!("🔽 PULL MODE: Checking for new versions");

    let pond_metadata = context.pond_metadata.as_ref().ok_or_else(|| {
        TLogFSError::TinyFS(tinyfs::Error::Other(
            "Pull command requires pond metadata".to_string(),
        ))
    })?;

    // Step 1: Get current local Delta table version
    let table = context.state.table().await;
    let local_version = table.version().unwrap_or(0);
    log::info!("   Local Delta version: {}", local_version);

    // Step 2: Scan remote for all available versions (filtered by pond_id)
    log::debug!("   Scanning remote storage for versions...");
    let remote_versions = scan_remote_versions(&store, Some(&pond_metadata.pond_id)).await?;

    if remote_versions.is_empty() {
        log::info!("   No remote versions found");
        return Ok(());
    }

    let max_remote_version = *remote_versions.iter().max().unwrap_or(&0);

    // Step 3: Filter for versions newer than local
    let new_versions: Vec<i64> = remote_versions
        .into_iter()
        .filter(|v| *v > local_version)
        .collect();

    if new_versions.is_empty() {
        log::info!(
            "   Already up to date (local: {}, remote max: {})",
            local_version,
            max_remote_version
        );
        return Ok(());
    }

    log::info!(
        "   Found {} new version(s) to pull: {:?}",
        new_versions.len(),
        new_versions
    );

    // Step 4: Download and apply each new version
    let mut table = table; // Make mutable for apply_parquet_files
    for version in &new_versions {
        log::info!("   Pulling version {}...", version);

        // Download bundle
        log::debug!("      Downloading bundle...");
        let bundle_data = download_bundle(&store, pond_metadata, *version).await?;

        // Extract Parquet files
        log::debug!("      Extracting Parquet files...");
        let files = extract_bundle(&bundle_data).await?;

        if files.is_empty() {
            log::info!("      Version {} has no files, skipping", version);
            continue;
        }

        log::debug!("      Applying {} file(s) to Delta table...", files.len());

        // Apply files to Delta table
        apply_parquet_files(&mut table, &files).await?;

        let current_version = table.version().ok_or_else(|| {
            TLogFSError::TinyFS(tinyfs::Error::Other(
                "No version available after applying files".to_string(),
            ))
        })?;

        log::info!(
            "      ✓ Version {} pulled (Delta version: {})",
            version,
            current_version
        );
    }

    log::info!(
        "   ✓ Pull complete - synced {} version(s)",
        new_versions.len()
    );

    Ok(())
}

/// Get the last successfully backed up version by scanning the backup store
///
/// Returns None if no backups exist, otherwise returns the highest version number found.
async fn get_last_backed_up_version(
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    pond_id: &uuid7::Uuid,
) -> Result<Option<i64>, TLogFSError> {
    use futures::stream::StreamExt;

    // List all objects (no prefix needed for flat structure)
    let mut list_stream = store.list(None);
    let mut max_version: Option<i64> = None;

    while let Some(result) = list_stream.next().await {
        match result {
            Ok(meta) => {
                // Extract pond_id and version from path like: pond-{uuid}-bundle-000006.tar.zst
                let path_str = meta.location.as_ref();

                if let Some((bundle_pond_id_str, version)) = extract_bundle_info_from_path(path_str)
                {
                    // Only consider bundles for this pond
                    if let Ok(bundle_pond_id) = bundle_pond_id_str.parse::<uuid7::Uuid>()
                        && &bundle_pond_id == pond_id
                    {
                        max_version = Some(max_version.unwrap_or(0).max(version));
                    }
                }
            }
            Err(e) => {
                log::warn!("Error listing backup objects: {}", e);
            }
        }
    }

    Ok(max_version)
}

/// Extract version number and pond_id from a backup path
///
/// New format: pond-{pond_id}-bundle-{version:06}.tar.zst
/// Example: pond-019a1efb-7a09-7a75-bc76-e9669c915fc2-bundle-000001.tar.zst
///
/// Returns (pond_id, version) if successfully parsed
fn extract_bundle_info_from_path(path: &str) -> Option<(String, i64)> {
    // Get just the filename
    let filename = path.split('/').next_back().unwrap_or(path);

    // Check if it matches pattern: pond-{uuid}-bundle-{version}.tar.zst
    if !filename.starts_with("pond-") || !filename.ends_with(".tar.zst") {
        return None;
    }

    // Remove prefix and suffix
    let middle = filename.strip_prefix("pond-")?.strip_suffix(".tar.zst")?;

    // Split by "-bundle-" to separate pond_id from version
    let parts: Vec<&str> = middle.split("-bundle-").collect();
    if parts.len() != 2 {
        return None;
    }

    let pond_id = parts[0].to_string();
    let version_str = parts[1];

    // Parse version number
    let version = version_str.parse::<i64>().ok()?;

    Some((pond_id, version))
}

/// Create a backup bundle from a changeset and upload to object storage
///
/// Reads Parquet files AND Delta commit logs directly from the Delta table's object store and bundles them.
async fn create_backup_bundle(
    backup_store: std::sync::Arc<dyn object_store::ObjectStore>,
    changeset: &ChangeSet,
    delta_table: &deltalake::DeltaTable,
    compression_level: i32,
    pond_id: uuid7::Uuid,
) -> Result<(), TLogFSError> {
    use crate::bundle::BundleBuilder;
    use object_store::path::Path;

    let mut builder = BundleBuilder::new().compression_level(compression_level);

    // Get the Delta table's object store (where Parquet files are stored)
    let delta_store = delta_table.object_store();

    log::info!(
        "   Creating bundle with {} Parquet files...",
        changeset.added.len()
    );

    // Add each Parquet file to the bundle
    for file_change in &changeset.added {
        log::debug!(
            "   Adding Parquet: {} ({} bytes)",
            file_change.parquet_path,
            file_change.size
        );

        // Read the Parquet file from Delta table's object store
        let parquet_path = Path::from(file_change.parquet_path.as_str());
        let get_result = delta_store.get(&parquet_path).await.map_err(|e| {
            TLogFSError::ArrowMessage(format!(
                "Failed to read {}: {}",
                file_change.parquet_path, e
            ))
        })?;

        // Convert GetResult to a reader that implements AsyncRead
        let bytes = get_result.bytes().await.map_err(|e| {
            TLogFSError::ArrowMessage(format!(
                "Failed to read bytes from {}: {}",
                file_change.parquet_path, e
            ))
        })?;

        // Create a Cursor that implements AsyncRead + AsyncSeek
        let reader = std::io::Cursor::new(bytes.to_vec());

        // Add to bundle with the Parquet path as the logical path
        builder.add_file(
            file_change.parquet_path.clone(),
            file_change.size as u64,
            reader,
        )?;
    }

    // Include the Delta commit log for this version
    let commit_log_path = format!("_delta_log/{:020}.json", changeset.version);
    log::info!("   Adding Delta commit log: {}", commit_log_path);

    let commit_path = Path::from(commit_log_path.as_str());
    match delta_store.get(&commit_path).await {
        Ok(get_result) => {
            let bytes = get_result.bytes().await.map_err(|e| {
                TLogFSError::ArrowMessage(format!("Failed to read commit log: {}", e))
            })?;

            // @@@ The section below can be simplified because steward
            // is recording a pre-flight transaction in the control
            // table with the details extracted below. We can always
            // expect a control table entry with these details IOW.

            // Parse the commit log to extract cli_args from metadata
            // Delta logs are JSONL format (one JSON object per line)
            // commitInfo is on the LAST line (lines 1 to N-1 are add/remove actions)
            let commit_json = std::str::from_utf8(&bytes).map_err(|e| {
                TLogFSError::ArrowMessage(format!("Delta log is not valid UTF-8: {}", e))
            })?;

            log::debug!("   Parsing commit log JSON for cli_args");

            // Parse the last line (which contains the commitInfo)
            let last_line = commit_json
                .lines()
                .last()
                .ok_or_else(|| TLogFSError::ArrowMessage("Delta log is empty".to_string()))?;

            let commit_value =
                serde_json::from_str::<serde_json::Value>(last_line).map_err(|e| {
                    TLogFSError::ArrowMessage(format!("Failed to parse Delta log JSON: {}", e))
                })?;

            // Delta log format: {"commitInfo": {"operation": "...", "operationMetrics": {...}, ... "pond_txn": {...}}}
            let commit_info = commit_value.get("commitInfo").ok_or_else(|| {
                TLogFSError::ArrowMessage(
                    "No commitInfo found in Delta log - this should not happen".to_string(),
                )
            })?;

            log::debug!("   Found commitInfo in Delta log");

            // CRITICAL: pond_txn metadata MUST exist for all transactions
            // Every command (init, mkdir, mknod, etc.) must write this metadata
            let pond_txn = commit_info.get("pond_txn")
                .ok_or_else(|| TLogFSError::ArrowMessage(format!(
                    "Version {} is missing pond_txn metadata in Delta commitInfo. \
                    This indicates a bug in the command implementation - all commands MUST write transaction metadata. \
                    Cannot create backup bundle without original command information. \
                    Command that created this version needs to be fixed to include metadata.",
                    changeset.version
                )))?;

            log::debug!("   Found pond_txn metadata: {:?}", pond_txn);

            // Handle both old format (pond_txn.args) and new format (pond_txn.user.args)
            let args_array = pond_txn
                .get("user")
                .and_then(|user| user.get("args"))
                .and_then(|v| v.as_array())
                .or_else(|| {
                    // Fallback to old format for backward compatibility
                    pond_txn.get("args").and_then(|v| v.as_array())
                })
                .ok_or_else(|| {
                    TLogFSError::ArrowMessage(
                        "pond_txn.user.args (or pond_txn.args for old format) is not an array or is missing. \
                    This indicates corrupted transaction metadata in the Delta log. \
                    Cannot create backup bundle without original command information."
                            .to_string(),
                    )
                })?;

            // Extract strings from the JSON array
            let cli_args: Vec<String> = args_array
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();

            if cli_args.is_empty() {
                return Err(TLogFSError::ArrowMessage(
                    "Extracted empty cli_args from Delta log. \
                    This indicates a problem with the transaction metadata. \
                    Cannot create backup bundle without original command information."
                        .to_string(),
                ));
            }

            log::info!("   ✓ Extracted CLI args from commit: {:?}", cli_args);

            let reader = std::io::Cursor::new(bytes.to_vec());
            builder.add_file(commit_log_path.clone(), bytes.len() as u64, reader)?;
            log::debug!("   ✓ Commit log added ({} bytes)", bytes.len());

            // Set the cli_args in the bundle
            builder = builder.cli_args(cli_args);
        }
        Err(e) => {
            return Err(TLogFSError::ArrowMessage(format!(
                "Failed to read commit log {}: {}. \
                Commit logs are required for backup bundles. \
                This indicates a problem with the Delta Lake state.",
                commit_log_path, e
            )));
        }
    }

    // Create bundle path: pond-{pond_id}-bundle-{version}.tar.zst (flat structure, no subdirectories)
    let bundle_path = format!("pond-{}-bundle-{:06}.tar.zst", pond_id, changeset.version);
    let object_path = Path::from(bundle_path.clone());

    log::info!("   Uploading bundle to: {}", bundle_path);

    // Write the bundle to the backup object storage
    // Note: metadata.json is now embedded as the first entry in the tar archive
    let metadata = builder
        .write_to_store(backup_store.clone(), &object_path)
        .await?;

    log::info!("   ✓ Bundle uploaded successfully");
    log::info!("     Files: {}", metadata.file_count);
    log::info!("     Uncompressed: {} bytes", metadata.uncompressed_size);
    log::info!(
        "     Compressed: {} bytes ({:.1}%)",
        metadata.compressed_size,
        (metadata.compressed_size as f64 / metadata.uncompressed_size as f64) * 100.0
    );
    log::info!("     Note: metadata.json is embedded as first entry in bundle");

    Ok(())
}

crate::register_executable_factory!(
    name: "remote",
    description: "S3-compatible remote storage configuration and validation",
    validate: validate_remote_config,
    initialize: |_config, _context| Box::pin(async { Ok(()) }),
    execute: execute_remote
);

/// Represents a file change in a Delta Lake commit
#[derive(Debug, Clone)]
pub struct FileChange {
    /// Parquet file path (from Delta Lake)
    pub parquet_path: String,
    /// Logical pond path (e.g., /data/sensors/readings.csv)
    pub pond_path: Option<String>,
    /// Node ID in OpLog
    pub node_id: Option<NodeID>,
    /// Part ID in OpLog
    pub part_id: Option<NodeID>,
    /// File size in bytes
    pub size: i64,
    /// Modification time (Unix timestamp milliseconds)
    pub modification_time: i64,
}

/// Represents a set of changes in a single commit
#[derive(Debug, Clone)]
pub struct ChangeSet {
    /// Delta Lake version number
    pub version: i64,
    /// Transaction sequence number
    pub txn_seq: i64,
    /// Files added in this commit
    pub added: Vec<FileChange>,
    /// Files removed in this commit
    pub removed: Vec<FileChange>,
}

impl ChangeSet {
    #[must_use]
    pub fn new(version: i64, txn_seq: i64) -> Self {
        Self {
            version,
            txn_seq,
            added: Vec::new(),
            removed: Vec::new(),
        }
    }

    /// Total number of changed files
    #[must_use]
    pub fn total_changes(&self) -> usize {
        self.added.len() + self.removed.len()
    }

    /// Total bytes added
    #[must_use]
    pub fn total_bytes_added(&self) -> i64 {
        self.added.iter().map(|f| f.size).sum()
    }

    /// Total bytes removed
    #[must_use]
    pub fn total_bytes_removed(&self) -> i64 {
        self.removed.iter().map(|f| f.size).sum()
    }
}

/// Detect file changes from Delta Lake commit log for a specific version
///
/// This function reads the Delta Lake transaction log at the specified version
/// and extracts Add/Remove actions to determine which files were changed.
///
/// # Arguments
/// * `table` - Reference to the DeltaTable
/// * `version` - Delta Lake version number to inspect (None means current version)
///
/// # Returns
/// A ChangeSet containing lists of added and removed files
pub async fn detect_changes_from_delta_log(
    table: &deltalake::DeltaTable,
    version: i64,
) -> Result<ChangeSet, TLogFSError> {
    log::debug!("Detecting changes from Delta Lake version {}", version);

    // Load the table at the specific version
    let mut versioned_table = table.clone();
    versioned_table
        .load_version(version)
        .await
        .map_err(TLogFSError::Delta)?;

    let snapshot = versioned_table.snapshot().map_err(TLogFSError::Delta)?;

    let mut changeset = ChangeSet::new(version, 0); // txn_seq will be set by caller

    // Get the log store to iterate over file actions
    let log_store = versioned_table.log_store();

    // Use file_actions_iter to get Add actions from the snapshot
    use futures::stream::StreamExt;
    let mut file_stream = snapshot.file_actions_iter(log_store.as_ref());

    while let Some(add_result) = file_stream.next().await {
        match add_result {
            Ok(add_action) => {
                let parquet_path = add_action.path.clone();
                let part_id = extract_part_id_from_parquet_path(&parquet_path);

                changeset.added.push(FileChange {
                    parquet_path,
                    pond_path: None, // Will be resolved later by map_parquet_to_pond_paths
                    node_id: None,   // Will be resolved later by map_parquet_to_pond_paths
                    part_id,
                    size: add_action.size,
                    modification_time: add_action.modification_time,
                });
            }
            Err(e) => {
                log::warn!("Error reading file action: {}", e);
            }
        }
    }

    // Note: Remove actions would need special handling
    // For now, we focus on Add actions which represent new/modified files

    log::info!(
        "Detected {} added files in version {}",
        changeset.added.len(),
        version
    );

    Ok(changeset)
}

/// Map Parquet file paths to pond logical paths using OpLog queries
///
/// This function takes a list of Parquet file paths and queries the OpLog
/// to determine the corresponding logical pond paths (e.g., /data/file.csv).
///
/// # Arguments
/// * `state` - TLogFS State for querying OpLog
/// * `changeset` - ChangeSet to enrich with pond path information
///
/// # Returns
/// Updated ChangeSet with pond_path, node_id, and part_id filled in
pub async fn map_parquet_to_pond_paths(
    _state: &crate::persistence::State,
    mut changeset: ChangeSet,
) -> Result<ChangeSet, TLogFSError> {
    log::debug!(
        "Mapping {} Parquet paths to pond paths",
        changeset.added.len()
    );

    // For each added file, try to extract part_id and look up node information
    for file_change in &mut changeset.added {
        // Extract part_id from Parquet filename
        // Parquet files are typically named: part-{part_id}-{uuid}.parquet
        if let Some(part_id) = extract_part_id_from_parquet_path(&file_change.parquet_path) {
            file_change.part_id = Some(part_id);

            // Query OpLog for records with this part_id to get node_id
            // This requires access to State's query methods
            // For now, we'll log the part_id and defer full implementation
            log::debug!(
                "Extracted part_id {} from {}",
                part_id,
                file_change.parquet_path
            );

            // TODO: Query State to get node_id from part_id
            // TODO: Build pond path from node_id by traversing filesystem
        } else {
            log::warn!(
                "Could not extract part_id from Parquet path: {}",
                file_change.parquet_path
            );
        }
    }

    // Same for removed files
    for file_change in &mut changeset.removed {
        if let Some(part_id) = extract_part_id_from_parquet_path(&file_change.parquet_path) {
            file_change.part_id = Some(part_id);
            log::debug!(
                "Extracted part_id {} from removed file {}",
                part_id,
                file_change.parquet_path
            );
        }
    }

    log::info!(
        "Mapped {} files ({}% successful)",
        changeset.added.len(),
        (changeset
            .added
            .iter()
            .filter(|f| f.part_id.is_some())
            .count()
            * 100)
            / changeset.added.len().max(1)
    );

    Ok(changeset)
}

/// Extract part_id from a Parquet file path
///
/// Parquet files generated by TLogFS are typically named with the pattern:
/// `part-{part_id}-{uuid}.parquet` or similar, where part_id is a UUID7 string.
///
/// This function attempts to parse the part_id (as a UUID) from the filename.
fn extract_part_id_from_parquet_path(parquet_path: &str) -> Option<NodeID> {
    // Delta Lake uses partition directories like: part_id=0199ff37-c320-7325-89a7-371572fdceb8/part-00001-...parquet
    // We need to extract the UUID from the partition directory name

    // Look for "part_id=" pattern in the path
    for segment in parquet_path.split('/') {
        if let Some(uuid_str) = segment.strip_prefix("part_id=")
            && let Ok(node_id) = NodeID::from_string(uuid_str)
        {
            return Some(node_id);
        }
    }

    // Fallback: Try to extract from filename itself (for non-partitioned tables)
    let filename = parquet_path.split('/').next_back().unwrap_or(parquet_path);

    // Remove .parquet extension if present
    let filename_no_ext = filename.strip_suffix(".parquet").unwrap_or(filename);

    // Look for pattern: part-{uuid}-...
    if filename_no_ext.starts_with("part-") {
        let parts: Vec<&str> = filename_no_ext.split('-').collect();
        if parts.len() >= 6 {
            // UUID format: 8-4-4-4-12 hex digits with hyphens
            // So we need parts[1] through parts[5]
            let potential_uuid = format!(
                "{}-{}-{}-{}-{}",
                parts[1], parts[2], parts[3], parts[4], parts[5]
            );

            if let Ok(node_id) = NodeID::from_string(&potential_uuid) {
                return Some(node_id);
            }
        }
    }

    // If we can't parse it, log and return None
    log::debug!(
        "Could not extract part_id from Parquet path: {}",
        parquet_path
    );
    None
}

// ============================================================================
// Restore functionality
// ============================================================================

/// Scan remote storage for all available backup versions
///
/// Lists all backup bundles and extracts version numbers
/// from paths like "pond-{uuid}-bundle-000001.tar.zst"
///
/// Returns a sorted vector of version numbers.
pub async fn scan_remote_versions(
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    pond_id: Option<&uuid7::Uuid>,
) -> Result<Vec<i64>, TLogFSError> {
    use futures::stream::TryStreamExt;

    // List all objects (flat structure, no prefix)
    let list_stream = store.list(None);

    let mut versions = Vec::new();

    // Process each object in the listing
    let objects: Vec<_> = list_stream
        .try_collect()
        .await
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to list remote objects: {}", e)))?;

    for meta in objects {
        let path_str = meta.location.to_string();

        // Extract pond_id and version from new format
        if let Some((bundle_pond_id_str, version)) = extract_bundle_info_from_path(&path_str) {
            // Filter by pond_id if provided
            if let Some(target_pond_id) = pond_id {
                // Parse the bundle's pond_id string as UUID and compare
                if let Ok(bundle_pond_id) = bundle_pond_id_str.parse::<uuid7::Uuid>()
                    && &bundle_pond_id == target_pond_id
                {
                    versions.push(version);
                }
            } else {
                // No filter - include all versions
                versions.push(version);
            }
        }
    }

    // Sort versions and deduplicate
    versions.sort_unstable();
    versions.dedup();

    log::debug!("Found {} backup versions: {:?}", versions.len(), versions);

    Ok(versions)
}

/// Download a bundle from remote storage
///
/// Downloads the compressed tar.zst file for a specific pond and version.
/// Uses the pond_id from pond_metadata to locate the correct bundle.
///
/// # Arguments
/// * `store` - Object store containing the backup
/// * `pond_metadata` - Pond metadata containing the pond_id
/// * `version` - Delta Lake version number to download
///
/// # Returns
/// Raw bytes of the compressed bundle
pub async fn download_bundle(
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    pond_metadata: &crate::factory::PondMetadata,
    version: i64,
) -> Result<Vec<u8>, TLogFSError> {
    use object_store::path::Path;

    let pond_id = &pond_metadata.pond_id;
    let bundle_path = Path::from(format!("pond-{}-bundle-{:06}.tar.zst", pond_id, version));

    log::debug!(
        "Downloading bundle for pond {} version {} from {}",
        pond_id,
        version,
        bundle_path
    );

    let get_result = store.get(&bundle_path).await.map_err(|e| {
        TLogFSError::ArrowMessage(format!(
            "Failed to download bundle for pond {} version {}: {}",
            pond_id, version, e
        ))
    })?;

    let bytes = get_result.bytes().await.map_err(|e| {
        TLogFSError::ArrowMessage(format!(
            "Failed to read bundle bytes for pond {} version {}: {}",
            pond_id, version, e
        ))
    })?;

    log::debug!("Downloaded {} bytes for version {}", bytes.len(), version);

    Ok(bytes.to_vec())
}

/// An extracted file from a bundle
#[derive(Debug, Clone)]
pub struct ExtractedFile {
    /// Path within the bundle (e.g., "part_id=<uuid>/part-00001.parquet")
    pub path: String,
    /// File contents
    pub data: Vec<u8>,
    /// File size in bytes
    pub size: u64,
    /// Modification time (Unix timestamp)
    pub modification_time: i64,
}

/// Extract Parquet files from a compressed bundle
///
/// Decompresses the zstd stream and extracts all tar entries except metadata.json.
///
/// # Arguments
/// * `bundle_data` - Raw bytes of the compressed bundle (tar.zst)
///
/// # Returns
/// Vector of extracted files with their paths and contents
pub async fn extract_bundle(bundle_data: &[u8]) -> Result<Vec<ExtractedFile>, TLogFSError> {
    use async_compression::tokio::bufread::ZstdDecoder;
    use futures::stream::StreamExt;
    use tokio::io::{AsyncReadExt, BufReader};

    log::debug!("Extracting bundle ({} compressed bytes)", bundle_data.len());

    // Decompress the zstd stream
    let cursor = std::io::Cursor::new(bundle_data);
    let buf_reader = BufReader::new(cursor);
    let mut zstd_decoder = ZstdDecoder::new(buf_reader);

    // Read the tar archive
    let mut tar_archive = tokio_tar::Archive::new(&mut zstd_decoder);

    let mut entries = tar_archive
        .entries()
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to read tar entries: {}", e)))?;

    let mut extracted_files = Vec::new();
    let mut entry_count = 0;

    // Process each entry in the tar archive
    while let Some(entry_result) = entries.next().await {
        let mut entry = entry_result
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to read tar entry: {}", e)))?;

        let path = entry
            .path()
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to get entry path: {}", e)))?
            .to_string_lossy()
            .to_string();

        // Skip metadata.json (first entry)
        if path == "metadata.json" {
            log::debug!("Skipping metadata.json entry");
            continue;
        }

        // Only extract regular files (skip directories)
        let header = entry.header();
        if !header.entry_type().is_file() {
            log::debug!("Skipping non-file entry: {}", path);
            continue;
        }

        let size = header
            .size()
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to get file size: {}", e)))?;

        let mtime = header.mtime().map_err(|e| {
            TLogFSError::ArrowMessage(format!("Failed to get modification time: {}", e))
        })? as i64;

        // Read file contents
        let mut data = Vec::new();
        entry
            .read_to_end(&mut data)
            .await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to read file data: {}", e)))?;

        log::debug!("Extracted: {} ({} bytes)", path, data.len());

        extracted_files.push(ExtractedFile {
            path: path.clone(),
            data,
            size,
            modification_time: mtime,
        });

        entry_count += 1;
    }

    log::debug!("Extracted {} files from bundle", entry_count);

    Ok(extracted_files)
}

/// Apply extracted files (Parquet data + Delta commit logs) to Delta table location
///
/// Writes both Parquet files AND _delta_log/*.json commit files directly to storage.
/// This creates an identical Delta Lake replica without generating new commits.
///
/// # Key Insight - TRUE REPLICATION
/// When bundles include _delta_log/*.json files:
/// - We copy BOTH data files and commit logs
/// - Delta Lake sees existing commits (no new commits created)
/// - Replica version matches source EXACTLY
/// - True read-only replica semantics
///
/// When bundles only have Parquet files:
/// - We write data files and call delta_table.load()
/// - Delta Lake creates NEW commits for discovered files
/// - Replica version differs from source (legacy behavior)
///
/// # Arguments
/// * `delta_table` - The Delta table to restore files into
/// * `files` - Extracted files from bundle (may include _delta_log files)
///
/// # Returns
/// Updated DeltaTable after load()
pub async fn apply_parquet_files(
    delta_table: &mut deltalake::DeltaTable,
    files: &[ExtractedFile],
) -> Result<(), TLogFSError> {
    use object_store::path::Path as ObjectPath;

    let mut parquet_count = 0;
    let mut commit_log_count = 0;

    log::debug!("Applying {} files to Delta table", files.len());

    // Get the Delta table's object store
    let object_store = delta_table.object_store();

    // Write each file to the Delta table location
    for file in files {
        let dest_path = ObjectPath::from(file.path.as_str());

        // Track what we're writing
        if file.path.starts_with("_delta_log/") {
            log::debug!(
                "Writing commit log: {} ({} bytes)",
                file.path,
                file.data.len()
            );
            commit_log_count += 1;
        } else {
            log::debug!("Writing Parquet: {} ({} bytes)", file.path, file.data.len());
            parquet_count += 1;
        }

        // Write file data to object store
        let bytes = bytes::Bytes::copy_from_slice(&file.data);
        object_store
            .put(&dest_path, bytes.into())
            .await
            .map_err(|e| {
                TLogFSError::ArrowMessage(format!("Failed to write file {}: {}", file.path, e))
            })?;
    }

    log::info!(
        "Files written: {} Parquet, {} commit logs",
        parquet_count,
        commit_log_count
    );

    if commit_log_count > 0 {
        log::info!("TRUE REPLICATION: Commit logs copied - Delta version will match source");
    } else {
        log::warn!("LEGACY MODE: No commit logs - will create new commits");
    }

    log::debug!("Refreshing Delta table to load state...");

    // Refresh the Delta table to discover files
    // If we copied commit logs, this just loads existing state (no new commit)
    // If no commit logs, this creates new commit (legacy behavior)
    delta_table.load().await.map_err(|e| {
        TLogFSError::ArrowMessage(format!(
            "Failed to refresh Delta table after file application: {}",
            e
        ))
    })?;

    let new_version = delta_table
        .version()
        .ok_or_else(|| TLogFSError::ArrowMessage("No version available after load".to_string()))?;

    log::debug!("Delta table state loaded, version: {}", new_version);

    Ok(())
}

/// Extract transaction sequence number from bundle's Delta commit log
///
/// Parses the _delta_log/*.json file from the extracted bundle to get the
/// original txn_seq that this transaction had on the source pond.
///
/// # Arguments
/// * `files` - Extracted files from bundle (must include _delta_log file)
///
/// # Returns
/// The txn_seq from the Delta commit log, or error if not found
pub fn extract_txn_seq_from_bundle(files: &[ExtractedFile]) -> Result<i64, TLogFSError> {
    // Find the Delta commit log file
    let commit_log = files
        .iter()
        .find(|f| f.path.starts_with("_delta_log/") && f.path.ends_with(".json"))
        .ok_or_else(|| {
            TLogFSError::ArrowMessage(
                "Bundle does not contain Delta commit log (_delta_log/*.json). \
            Cannot extract transaction sequence number."
                    .to_string(),
            )
        })?;

    log::debug!("Parsing txn_seq from commit log: {}", commit_log.path);

    // Parse the commit log JSON
    let commit_json = std::str::from_utf8(&commit_log.data).map_err(|e| {
        TLogFSError::ArrowMessage(format!("Delta commit log is not valid UTF-8: {}", e))
    })?;

    // Delta log is JSONL format - last line contains commitInfo
    let last_line = commit_json
        .lines()
        .last()
        .ok_or_else(|| TLogFSError::ArrowMessage("Delta commit log is empty".to_string()))?;

    let commit_value = serde_json::from_str::<serde_json::Value>(last_line).map_err(|e| {
        TLogFSError::ArrowMessage(format!("Failed to parse Delta commit log JSON: {}", e))
    })?;

    // Extract commitInfo.pond_txn.txn_seq
    let commit_info = commit_value.get("commitInfo").ok_or_else(|| {
        TLogFSError::ArrowMessage("No commitInfo found in Delta commit log".to_string())
    })?;

    let pond_txn = commit_info.get("pond_txn").ok_or_else(|| {
        TLogFSError::ArrowMessage(
            "No pond_txn metadata in Delta commitInfo. \
            This bundle was created with older software that didn't include transaction metadata."
                .to_string(),
        )
    })?;

    let txn_seq = pond_txn
        .get("txn_seq")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| {
            TLogFSError::ArrowMessage(
                "pond_txn.txn_seq is missing or not a valid integer".to_string(),
            )
        })?;

    log::debug!("Extracted txn_seq={} from bundle commit log", txn_seq);

    Ok(txn_seq)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStore;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn test_pond_metadata() -> crate::factory::PondMetadata {
        crate::factory::PondMetadata {
            pond_id: "019a37b4-d539-736f-80aa-16952163cc2f"
                .to_string()
                .try_into()
                .unwrap(),
            birth_timestamp: 1234567890,
            birth_hostname: "test-host".into(),
            birth_username: "test-user".into(),
        }
    }

    /// Helper to create a test object store with backup directory structure
    async fn setup_test_backups() -> Result<(TempDir, Arc<dyn ObjectStore>), TLogFSError> {
        let temp_dir = TempDir::new()
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create temp dir: {}", e)))?;

        let store: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(temp_dir.path()).map_err(|e| {
                TLogFSError::ArrowMessage(format!("Failed to create local store: {}", e))
            })?,
        );

        // Create backup bundles using new flat structure:
        // pond-{uuid}-bundle-000001.tar.zst
        // pond-{uuid}-bundle-000002.tar.zst
        // pond-{uuid}-bundle-000004.tar.zst  (gap in sequence)

        let pond_metadata = test_pond_metadata();

        // Create dummy bundle files (empty for now - just testing scanning)
        for version in &[1, 2, 4] {
            let bundle_path = Path::from(format!(
                "pond-{}-bundle-{:06}.tar.zst",
                pond_metadata.pond_id, version
            ));

            // Create an empty file (we're just testing path scanning)
            let empty_data = bytes::Bytes::from_static(b"");
            store
                .put(&bundle_path, empty_data.into())
                .await
                .map_err(|e| {
                    TLogFSError::ArrowMessage(format!("Failed to create test bundle: {}", e))
                })?;
        }

        Ok((temp_dir, store))
    }

    #[tokio::test]
    async fn test_scan_remote_versions_basic() -> Result<(), TLogFSError> {
        let (_temp_dir, store) = setup_test_backups().await?;

        let versions = scan_remote_versions(&store, None).await?;

        // Should find versions 1, 2, and 4 (in sorted order)
        assert_eq!(versions, vec![1, 2, 4]);

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_remote_versions_empty() -> Result<(), TLogFSError> {
        let temp_dir = TempDir::new()
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create temp dir: {}", e)))?;

        let store: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(temp_dir.path()).map_err(|e| {
                TLogFSError::ArrowMessage(format!("Failed to create local store: {}", e))
            })?,
        );

        let versions = scan_remote_versions(&store, None).await?;

        // Empty directory should return empty vec
        assert_eq!(versions, Vec::<i64>::new());

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_remote_versions_with_invalid_paths() -> Result<(), TLogFSError> {
        let temp_dir = TempDir::new()
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create temp dir: {}", e)))?;

        let store: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(temp_dir.path()).map_err(|e| {
                TLogFSError::ArrowMessage(format!("Failed to create local store: {}", e))
            })?,
        );

        let pond_id = "test-pond-uuid";

        // Create valid versions using new flat format
        for version in &[1, 3] {
            let bundle_path = Path::from(format!("pond-{}-bundle-{:06}.tar.zst", pond_id, version));
            let empty_data = bytes::Bytes::from_static(b"");
            store
                .put(&bundle_path, empty_data.into())
                .await
                .map_err(|e| {
                    TLogFSError::ArrowMessage(format!("Failed to create test bundle: {}", e))
                })?;
        }

        // Create invalid paths (should be ignored)
        let invalid_paths = vec![
            "pond-test-pond-uuid-bundle-abc.tar.zst", // Non-numeric version (abc not a number)
            "pond-wrong-uuid-bundle-000005.tar.zst", // Different pond_id (will match but different version)
            "not-a-bundle-000001.tar.zst",           // Wrong prefix
            "pond-test-pond-uuid-bundle-000001.txt", // Wrong extension
        ];

        for path in invalid_paths {
            let empty_data = bytes::Bytes::from_static(b"");
            store
                .put(&Path::from(path), empty_data.into())
                .await
                .map_err(|e| {
                    TLogFSError::ArrowMessage(format!("Failed to create test file: {}", e))
                })?;
        }

        let versions = scan_remote_versions(&store, None).await?;

        // Should find valid versions from all pond_ids when no filter is provided
        // Version 1 appears twice (two different pond_ids) but should be deduplicated
        assert_eq!(versions, vec![1, 3, 5]);
        // Version 1 and 3 from test-pond-uuid, version 5 from wrong-uuid
        assert_eq!(versions, vec![1, 3, 5]);

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_remote_versions_large_numbers() -> Result<(), TLogFSError> {
        let temp_dir = TempDir::new()
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create temp dir: {}", e)))?;

        let store: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(temp_dir.path()).map_err(|e| {
                TLogFSError::ArrowMessage(format!("Failed to create local store: {}", e))
            })?,
        );

        let pond_id = "test-pond-uuid";

        // Test with larger version numbers using new flat format
        for version in &[100, 999, 1000] {
            let bundle_path = Path::from(format!("pond-{}-bundle-{:06}.tar.zst", pond_id, version));
            let empty_data = bytes::Bytes::from_static(b"");
            store
                .put(&bundle_path, empty_data.into())
                .await
                .map_err(|e| {
                    TLogFSError::ArrowMessage(format!("Failed to create test bundle: {}", e))
                })?;
        }

        let versions = scan_remote_versions(&store, None).await?;

        // Should handle large version numbers correctly
        assert_eq!(versions, vec![100, 999, 1000]);

        Ok(())
    }

    // ========================================================================
    // Download & Extract Tests
    // ========================================================================

    /// Helper to create a real bundle with test files
    async fn create_test_bundle(
        store: &Arc<dyn ObjectStore>,
        version: i64,
        files: Vec<(&str, &[u8])>,
    ) -> Result<(), TLogFSError> {
        use crate::bundle::BundleBuilder;
        use std::io::Cursor;

        let mut builder = BundleBuilder::new();

        // Add test files
        for (path, content) in files {
            builder.add_file(path, content.len() as u64, Cursor::new(content.to_vec()))?;
        }

        // Write bundle to store using new flat path format
        let pond_metadata = test_pond_metadata();
        let bundle_path = Path::from(format!(
            "pond-{}-bundle-{:06}.tar.zst",
            pond_metadata.pond_id, version
        ));
        builder.write_to_store(store.clone(), &bundle_path).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_download_bundle() -> Result<(), TLogFSError> {
        let temp_dir = TempDir::new()
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create temp dir: {}", e)))?;

        let store: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(temp_dir.path()).map_err(|e| {
                TLogFSError::ArrowMessage(format!("Failed to create local store: {}", e))
            })?,
        );

        // Create a test bundle with some files
        let test_files = vec![
            (
                "part_id=test-uuid/part-00001.parquet",
                b"parquet data 1" as &[u8],
            ),
            ("part_id=test-uuid/part-00002.parquet", b"parquet data 2"),
        ];

        create_test_bundle(&store, 1, test_files).await?;

        // Download the bundle
        let bundle_data = download_bundle(&store, &test_pond_metadata(), 1).await?;

        // Verify we got some data
        assert!(bundle_data.len() > 0, "Bundle should not be empty");

        Ok(())
    }

    #[tokio::test]
    async fn test_download_missing_bundle() -> Result<(), TLogFSError> {
        let temp_dir = TempDir::new()
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create temp dir: {}", e)))?;

        let store: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(temp_dir.path()).map_err(|e| {
                TLogFSError::ArrowMessage(format!("Failed to create local store: {}", e))
            })?,
        );

        // Try to download non-existent bundle
        let result = download_bundle(&store, &test_pond_metadata(), 999).await;

        // Should return error
        assert!(result.is_err(), "Should fail to download missing bundle");

        Ok(())
    }

    #[tokio::test]
    async fn test_extract_bundle() -> Result<(), TLogFSError> {
        let temp_dir = TempDir::new()
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create temp dir: {}", e)))?;

        let store: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(temp_dir.path()).map_err(|e| {
                TLogFSError::ArrowMessage(format!("Failed to create local store: {}", e))
            })?,
        );

        // Create a test bundle with multiple files
        let test_content_1 = b"This is test Parquet file 1";
        let test_content_2 = b"This is test Parquet file 2";
        let test_files = vec![
            ("part_id=abc123/part-00001.parquet", test_content_1 as &[u8]),
            ("part_id=abc123/part-00002.parquet", test_content_2 as &[u8]),
        ];

        create_test_bundle(&store, 1, test_files).await?;

        // Download and extract
        let bundle_data = download_bundle(&store, &test_pond_metadata(), 1).await?;
        let extracted = extract_bundle(&bundle_data).await?;

        // Verify extraction
        assert_eq!(extracted.len(), 2, "Should extract 2 files");

        // Check first file
        assert_eq!(extracted[0].path, "part_id=abc123/part-00001.parquet");
        assert_eq!(extracted[0].data, test_content_1);
        assert_eq!(extracted[0].size, test_content_1.len() as u64);

        // Check second file
        assert_eq!(extracted[1].path, "part_id=abc123/part-00002.parquet");
        assert_eq!(extracted[1].data, test_content_2);
        assert_eq!(extracted[1].size, test_content_2.len() as u64);

        Ok(())
    }

    #[tokio::test]
    async fn test_extract_bundle_skips_metadata() -> Result<(), TLogFSError> {
        let temp_dir = TempDir::new()
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create temp dir: {}", e)))?;

        let store: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(temp_dir.path()).map_err(|e| {
                TLogFSError::ArrowMessage(format!("Failed to create local store: {}", e))
            })?,
        );

        // Create bundle with just one Parquet file
        let test_files = vec![(
            "part_id=xyz789/part-00001.parquet",
            b"parquet content" as &[u8],
        )];

        create_test_bundle(&store, 1, test_files).await?;

        // Extract bundle
        let bundle_data = download_bundle(&store, &test_pond_metadata(), 1).await?;
        let extracted = extract_bundle(&bundle_data).await?;

        // Should only get the Parquet file, not metadata.json
        assert_eq!(extracted.len(), 1, "Should only extract Parquet files");
        assert_eq!(extracted[0].path, "part_id=xyz789/part-00001.parquet");

        // Verify metadata.json is not in the extracted files
        assert!(!extracted.iter().any(|f| f.path == "metadata.json"));

        Ok(())
    }

    #[tokio::test]
    async fn test_download_and_extract_empty_bundle() -> Result<(), TLogFSError> {
        let temp_dir = TempDir::new()
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create temp dir: {}", e)))?;

        let store: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(temp_dir.path()).map_err(|e| {
                TLogFSError::ArrowMessage(format!("Failed to create local store: {}", e))
            })?,
        );

        // Create empty bundle (no Parquet files)
        create_test_bundle(&store, 1, vec![]).await?;

        // Extract bundle
        let bundle_data = download_bundle(&store, &test_pond_metadata(), 1).await?;
        let extracted = extract_bundle(&bundle_data).await?;

        // Should return empty vec (metadata.json is skipped)
        assert_eq!(extracted.len(), 0, "Empty bundle should extract no files");

        Ok(())
    }

    // ========================================================================
    // File Application Tests
    // ========================================================================

    #[tokio::test]
    async fn test_apply_parquet_files_basic() -> Result<(), TLogFSError> {
        use deltalake::DeltaOps;
        use deltalake::kernel::{DataType, PrimitiveType, StructField, StructType};
        use object_store::path::Path as ObjectPath;

        let temp_dir = TempDir::new()
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create temp dir: {}", e)))?;

        let table_path = temp_dir.path().join("test_table");
        std::fs::create_dir(&table_path)
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create table dir: {}", e)))?;

        // Create a Delta table with Delta schema
        let delta_schema = StructType::try_new(vec![
            Ok(StructField::new(
                "id".to_string(),
                DataType::Primitive(PrimitiveType::Integer),
                false,
            )),
            Ok(StructField::new(
                "value".to_string(),
                DataType::Primitive(PrimitiveType::String),
                true,
            )),
        ])
        .map_err(|e: std::convert::Infallible| {
            TLogFSError::ArrowMessage(format!("Failed to create schema: {:?}", e))
        })?;

        let mut table = DeltaOps::try_from_uri(table_path.to_str().unwrap())
            .await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create DeltaOps: {}", e)))?
            .create()
            .with_columns(delta_schema.fields().cloned())
            .await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create table: {}", e)))?;

        // Create some mock Parquet file data
        // In reality, these would be actual Parquet files, but for the test we'll use dummy data
        let mock_parquet_data = b"mock parquet file content";

        let extracted_files = vec![ExtractedFile {
            path: "part_id=test-uuid-1/part-00001.parquet".to_string(),
            data: mock_parquet_data.to_vec(),
            size: mock_parquet_data.len() as u64,
            modification_time: 1234567890,
        }];

        // Apply files
        apply_parquet_files(&mut table, &extracted_files).await?;

        // Verify the file was written to the object store
        let object_store = table.object_store();
        let file_path = ObjectPath::from("part_id=test-uuid-1/part-00001.parquet");

        let result = object_store.get(&file_path).await;
        assert!(result.is_ok(), "File should exist in object store");

        let bytes = result
            .unwrap()
            .bytes()
            .await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to read bytes: {}", e)))?;
        assert_eq!(bytes.as_ref(), mock_parquet_data);

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_parquet_files_multiple() -> Result<(), TLogFSError> {
        use deltalake::DeltaOps;
        use deltalake::kernel::{DataType, PrimitiveType, StructField, StructType};
        use object_store::path::Path as ObjectPath;

        let temp_dir = TempDir::new()
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create temp dir: {}", e)))?;

        let table_path = temp_dir.path().join("test_table");
        std::fs::create_dir(&table_path)
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create table dir: {}", e)))?;

        // Create a Delta table with Delta schema
        let delta_schema = StructType::try_new(vec![Ok(StructField::new(
            "id".to_string(),
            DataType::Primitive(PrimitiveType::Integer),
            false,
        ))])
        .map_err(|e: std::convert::Infallible| {
            TLogFSError::ArrowMessage(format!("Failed to create schema: {:?}", e))
        })?;

        let mut table = DeltaOps::try_from_uri(table_path.to_str().unwrap())
            .await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create DeltaOps: {}", e)))?
            .create()
            .with_columns(delta_schema.fields().cloned())
            .await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create table: {}", e)))?;

        // Create multiple mock files
        let files = vec![
            ExtractedFile {
                path: "part_id=uuid-1/part-00001.parquet".to_string(),
                data: b"file1".to_vec(),
                size: 5,
                modification_time: 1234567890,
            },
            ExtractedFile {
                path: "part_id=uuid-1/part-00002.parquet".to_string(),
                data: b"file2".to_vec(),
                size: 5,
                modification_time: 1234567891,
            },
            ExtractedFile {
                path: "part_id=uuid-2/part-00001.parquet".to_string(),
                data: b"file3".to_vec(),
                size: 5,
                modification_time: 1234567892,
            },
        ];

        // Apply files
        apply_parquet_files(&mut table, &files).await?;

        // Verify all files were written
        let object_store = table.object_store();

        for file in &files {
            let file_path = ObjectPath::from(file.path.as_str());
            let result = object_store.get(&file_path).await;
            assert!(result.is_ok(), "File {} should exist", file.path);

            let bytes =
                result.unwrap().bytes().await.map_err(|e| {
                    TLogFSError::ArrowMessage(format!("Failed to read bytes: {}", e))
                })?;
            assert_eq!(bytes.as_ref(), file.data.as_slice());
        }

        Ok(())
    }
}
