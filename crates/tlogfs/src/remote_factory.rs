//! Remote storage factory for S3-compatible object stores
//!
//! This factory configures and validates access to remote object storage.
//! It can be used as a post-commit factory to verify connectivity and access
//! to remote backup/export destinations.
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
use tinyfs::Result as TinyFSResult;

/// Remote storage configuration matching S3Fields from original
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteConfig {
    /// S3 bucket name
    pub bucket: String,
    
    /// AWS region or compatible region identifier
    pub region: String,
    
    /// Access key ID for authentication
    pub key: String,
    
    /// Secret access key for authentication
    pub secret: String,
    
    /// S3-compatible endpoint URL (e.g., https://s3.amazonaws.com)
    pub endpoint: String,
}

fn validate_remote_config(config_bytes: &[u8]) -> TinyFSResult<Value> {
    let config_str = std::str::from_utf8(config_bytes)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid UTF-8: {}", e)))?;
    
    let config: RemoteConfig = serde_yaml::from_str(config_str)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid YAML: {}", e)))?;
    
    // Basic validation
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
    if config.endpoint.is_empty() {
        return Err(tinyfs::Error::Other("endpoint field cannot be empty".to_string()));
    }
    
    serde_json::to_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Serialization error: {}", e)))
}

async fn execute_remote(
    config: Value,
    _context: FactoryContext,
    mode: crate::factory::ExecutionMode,
) -> Result<(), TLogFSError> {
    let config: RemoteConfig = serde_json::from_value(config)
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Invalid config: {}", e))))?;
    
    log::info!("🌐 REMOTE STORAGE FACTORY");
    log::info!("   Mode: {:?}", mode);
    log::info!("   Bucket: {}", config.bucket);
    log::info!("   Region: {}", config.region);
    log::info!("   Endpoint: {}", config.endpoint);
    log::info!("   Key: {}...", &config.key.chars().take(8).collect::<String>());
    
    // Use object_store to verify access
    use object_store::{ClientOptions, ObjectStore};
    use object_store::aws::AmazonS3Builder;
    use futures::TryStreamExt;
    
    log::debug!("   Building S3 client...");
    
    let client_options = ClientOptions::new()
        .with_timeout(std::time::Duration::from_secs(10));
    
    let store = AmazonS3Builder::new()
        .with_bucket_name(&config.bucket)
        .with_region(&config.region)
        .with_endpoint(&config.endpoint)
        .with_access_key_id(&config.key)
        .with_secret_access_key(&config.secret)
        .with_client_options(client_options)
        .build()
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Failed to build S3 client: {}", e))))?;
    
    log::debug!("   Testing connectivity...");
    
    // Try to list objects (empty prefix to just test access)
    let list_result = store
        .list(None)
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Failed to list bucket: {}", e))))?;
    
    log::info!("   ✓ Successfully connected to remote storage");
    log::info!("   ✓ Bucket contains {} objects (sample listing)", list_result.len());
    
    // Log first few objects if any exist
    if !list_result.is_empty() {
        log::info!("   Sample objects:");
        for (i, meta) in list_result.iter().take(5).enumerate() {
            log::info!("     {}. {} ({} bytes)", i + 1, meta.location, meta.size);
        }
        if list_result.len() > 5 {
            log::info!("     ... and {} more", list_result.len() - 5);
        }
    }
    
    log::info!("   ✓ Remote storage validation complete");
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
