use crate::EntryType;
use crate::error::Result;
use async_trait::async_trait;

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

    /// Entry timestamp
    pub timestamp: i64,
}

/// Common metadata interface for all filesystem nodes
/// This trait provides a uniform way to access metadata across different node types
#[async_trait]
pub trait Metadata: Send + Sync {
    /// Get consolidated metadata for this node
    async fn metadata(&self) -> Result<NodeMetadata>;
}
