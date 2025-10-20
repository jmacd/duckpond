//! Remote storage factory for S3-compatible object stores
//!
//! This factory configures and validates access to remote object storage.
//! It can be used as a post-commit factory to backup pond data to remote storage.
//!
//! Configuration fields match the S3Fields structure from original backup.rs:
//! - bucket: S3 bucket name
//! - region: AWS region or compatible
//! - key: Access key ID
//! - secret: Secret access key
//! - endpoint: S3-compatible endpoint URL

use crate::factory::FactoryContext;
use crate::TLogFSError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinyfs::{NodeID, Result as TinyFSResult};

/// Remote storage configuration matching S3Fields from original
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteConfig {
    /// Storage type: "s3" or "local"
    #[serde(default = "default_storage_type")]
    pub storage_type: String,
    
    /// S3 bucket name (required for s3)
    #[serde(default)]
    pub bucket: String,
    
    /// AWS region or compatible region identifier (required for s3)
    #[serde(default)]
    pub region: String,
    
    /// Access key ID for authentication (required for s3)
    #[serde(default)]
    pub key: String,
    
    /// Secret access key for authentication (required for s3)
    #[serde(default)]
    pub secret: String,
    
    /// S3-compatible endpoint URL (optional for s3)
    #[serde(default)]
    pub endpoint: String,
    
    /// Local filesystem path (required for local)
    #[serde(default)]
    pub path: String,
    
    /// Compression level for bundles (0-21, default 3)
    #[serde(default = "default_compression_level")]
    pub compression_level: i32,
}

fn default_storage_type() -> String {
    "s3".to_string()
}

fn default_compression_level() -> i32 {
    3
}

fn validate_remote_config(config_bytes: &[u8]) -> TinyFSResult<Value> {
    let config_str = std::str::from_utf8(config_bytes)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid UTF-8: {}", e)))?;
    
    let config: RemoteConfig = serde_yaml::from_str(config_str)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid YAML: {}", e)))?;
    
    // Validate based on storage type
    match config.storage_type.as_str() {
        "local" => {
            if config.path.is_empty() {
                return Err(tinyfs::Error::Other("path field required for local storage".to_string()));
            }
        }
        "s3" => {
            if config.bucket.is_empty() {
                return Err(tinyfs::Error::Other("bucket field cannot be empty".to_string()));
            }
            if config.region.is_empty() {
                return Err(tinyfs::Error::Other("region field cannot be empty".to_string()));
            }
            if config.key.is_empty() {
                return Err(tinyfs::Error::Other("key field cannot be empty".to_string()));
            }
            if config.secret.is_empty() {
                return Err(tinyfs::Error::Other("secret field cannot be empty".to_string()));
            }
        }
        other => {
            return Err(tinyfs::Error::Other(format!("Invalid storage_type: {}. Must be 'local' or 's3'", other)));
        }
    }
    
    serde_json::to_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Serialization error: {}", e)))
}

async fn execute_remote(
    config: Value,
    context: FactoryContext,
    mode: crate::factory::ExecutionMode,
) -> Result<(), TLogFSError> {
    let config: RemoteConfig = serde_json::from_value(config)
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Invalid config: {}", e))))?;
    
    log::info!("🌐 REMOTE BACKUP FACTORY");
    log::info!("   Mode: {:?}", mode);
    log::info!("   Storage: {}", config.storage_type);
    
    // Build the appropriate object store
    let store: std::sync::Arc<dyn object_store::ObjectStore> = match config.storage_type.as_str() {
        "local" => {
            log::info!("   Local path: {}", config.path);
            std::sync::Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(&config.path)
                    .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Failed to create local store: {}", e))))?
            )
        }
        "s3" => {
            log::info!("   Bucket: {}", config.bucket);
            log::info!("   Region: {}", config.region);
            log::info!("   Endpoint: {}", config.endpoint);
            log::info!("   Key: {}...", &config.key.chars().take(8).collect::<String>());
            
            use object_store::{ClientOptions, aws::AmazonS3Builder};
            
            let client_options = ClientOptions::new()
                .with_timeout(std::time::Duration::from_secs(30));
            
            let mut builder = AmazonS3Builder::new()
                .with_bucket_name(&config.bucket)
                .with_region(&config.region)
                .with_access_key_id(&config.key)
                .with_secret_access_key(&config.secret)
                .with_client_options(client_options);
            
            if !config.endpoint.is_empty() {
                builder = builder.with_endpoint(&config.endpoint);
            }
            
            std::sync::Arc::new(
                builder.build()
                    .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Failed to build S3 client: {}", e))))?
            )
        }
        _ => {
            return Err(TLogFSError::TinyFS(tinyfs::Error::Other(format!("Invalid storage_type: {}", config.storage_type))));
        }
    };
    
    // Get the Delta table from State (contains transaction-scoped table reference)
    let table = context.state.table().await;
    let version = table.version().ok_or_else(|| {
        TLogFSError::TinyFS(tinyfs::Error::Other("No Delta Lake version available".to_string()))
    })?;
    
    log::info!("   Current Delta version: {}", version);
    
    // Detect changes in current version
    let changeset = detect_changes_from_delta_log(&table, version).await?;
    
    log::info!("   Detected {} added files, {} removed files",
        changeset.added.len(),
        changeset.removed.len()
    );
    log::info!("   Total size: {} bytes", changeset.total_bytes_added());
    
    if changeset.added.is_empty() {
        log::info!("   No files to backup");
        return Ok(());
    }
    
    // Create a bundle with the changed files
    create_backup_bundle(
        store,
        &changeset,
        &table,
        config.compression_level,
    ).await?;
    
    log::info!("   ✓ Remote backup complete");
    Ok(())
}

/// Create a backup bundle from a changeset and upload to object storage
///
/// Reads Parquet files directly from the Delta table's object store and bundles them.
async fn create_backup_bundle(
    backup_store: std::sync::Arc<dyn object_store::ObjectStore>,
    changeset: &ChangeSet,
    delta_table: &deltalake::DeltaTable,
    compression_level: i32,
) -> Result<(), TLogFSError> {
    use crate::bundle::BundleBuilder;
    use object_store::path::Path;
    
    let mut builder = BundleBuilder::new().compression_level(compression_level);
    
    // Get the Delta table's object store (where Parquet files are stored)
    let delta_store = delta_table.object_store();
    
    log::info!("   Creating bundle with {} files...", changeset.added.len());
    
    // Add each Parquet file to the bundle
    for file_change in &changeset.added {
        log::debug!("   Adding: {} ({} bytes)", file_change.parquet_path, file_change.size);
        
        // Read the Parquet file from Delta table's object store
        let parquet_path = Path::from(file_change.parquet_path.as_str());
        let get_result = delta_store.get(&parquet_path).await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to read {}: {}", file_change.parquet_path, e)))?;
        
        // Convert GetResult to a reader that implements AsyncRead
        let bytes = get_result.bytes().await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to read bytes from {}: {}", file_change.parquet_path, e)))?;
        
        // Create a Cursor that implements AsyncRead + AsyncSeek
        let reader = std::io::Cursor::new(bytes.to_vec());
        
        // Add to bundle with the Parquet path as the logical path
        builder.add_file(
            file_change.parquet_path.clone(),
            file_change.size as u64,
            reader,
        )?;
    }
    
    // Create bundle path: backups/version-{version}/bundle.tar.zst
    let bundle_path = format!("backups/version-{:06}/bundle.tar.zst", changeset.version);
    let object_path = Path::from(bundle_path.clone());
    
    log::info!("   Uploading bundle to: {}", bundle_path);
    
    // Write the bundle to the backup object storage
    let metadata = builder.write_to_store(backup_store.clone(), &object_path).await?;
    
    log::info!("   ✓ Bundle uploaded successfully");
    log::info!("     Files: {}", metadata.file_count);
    log::info!("     Uncompressed: {} bytes", metadata.uncompressed_size);
    log::info!("     Compressed: {} bytes ({:.1}%)",
        metadata.compressed_size,
        (metadata.compressed_size as f64 / metadata.uncompressed_size as f64) * 100.0
    );
    
    // Also write the metadata as a separate JSON file
    let metadata_path = format!("backups/version-{:06}/metadata.json", changeset.version);
    let metadata_json = serde_json::to_string_pretty(&metadata)
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to serialize metadata: {}", e)))?;
    
    let put_result = backup_store.put(
        &Path::from(metadata_path.clone()),
        bytes::Bytes::from(metadata_json).into()
    ).await;
    
    put_result.map_err(|e| TLogFSError::ArrowMessage(format!("Failed to upload metadata: {}", e)))?;
    
    log::info!("   ✓ Metadata uploaded to: {}", metadata_path);
    
    Ok(())
}

// Register factory supporting BOTH execution modes
// (though it's primarily designed for PostCommitReader)
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
    pub fn new(version: i64, txn_seq: i64) -> Self {
        Self {
            version,
            txn_seq,
            added: Vec::new(),
            removed: Vec::new(),
        }
    }

    /// Total number of changed files
    pub fn total_changes(&self) -> usize {
        self.added.len() + self.removed.len()
    }

    /// Total bytes added
    pub fn total_bytes_added(&self) -> i64 {
        self.added.iter().map(|f| f.size).sum()
    }

    /// Total bytes removed
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
            .count() * 100)
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
        if let Some(uuid_str) = segment.strip_prefix("part_id=") {
            if let Ok(node_id) = NodeID::from_string(uuid_str) {
                return Some(node_id);
            }
        }
    }
    
    // Fallback: Try to extract from filename itself (for non-partitioned tables)
    let filename = parquet_path
        .split('/')
        .last()
        .unwrap_or(parquet_path);

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

