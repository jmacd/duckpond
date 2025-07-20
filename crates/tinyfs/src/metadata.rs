use async_trait::async_trait;
use crate::error::Result;
use crate::EntryType;

/// Consolidated metadata for filesystem nodes
#[derive(Debug, Clone, PartialEq)]
pub struct NodeMetadata {
    /// Node version (incremented on each modification)
    pub version: u64,
    
    /// File size in bytes (None for directories and symlinks)
    pub size: Option<u64>,
    
    /// SHA256 checksum (Some for all files, None for directories/symlinks)
    pub sha256: Option<String>,
    
    /// Entry type (file, directory, symlink with file format variants)
    pub entry_type: EntryType,
}

/// Common metadata interface for all filesystem nodes
/// This trait provides a uniform way to access metadata across different node types
#[async_trait]
pub trait Metadata: Send + Sync {
    /// Get consolidated metadata for this node
    async fn metadata(&self) -> Result<NodeMetadata>;
    
    /// Get a u64 metadata value by name (legacy compatibility)
    /// 
    /// Supported names: "version", "size"
    /// Common metadata names:
    /// - "timestamp": Node modification time (microseconds since Unix epoch)
    /// - "version": Per-node modification counter (starts at 1, increments on each change)
    /// - "size": File size in bytes (files only)
    /// 
    /// Returns None if the metadata field doesn't exist or the node doesn't support it
    async fn metadata_u64(&self, name: &str) -> Result<Option<u64>> {
        let metadata = self.metadata().await?;
        match name {
            "version" => Ok(Some(metadata.version)),
            "size" => Ok(metadata.size),
            // Keep existing behavior for timestamp - let implementations handle it
            _ => self.metadata_u64_impl(name).await,
        }
    }
    
    /// Implementation-specific metadata_u64 for backward compatibility
    /// Default implementations can override this for custom metadata fields
    async fn metadata_u64_impl(&self, _name: &str) -> Result<Option<u64>> {
        Ok(None)
    }
}