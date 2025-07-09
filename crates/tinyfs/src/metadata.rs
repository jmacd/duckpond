use async_trait::async_trait;
use crate::error::Result;

/// Common metadata interface for all filesystem nodes
/// This trait provides a uniform way to access metadata across different node types
#[async_trait]
pub trait Metadata: Send + Sync {
    /// Get a u64 metadata value by name
    /// 
    /// Common metadata names:
    /// - "timestamp": Node modification time (microseconds since Unix epoch)
    /// - "version": Per-node modification counter (starts at 1, increments on each change)
    /// 
    /// Returns None if the metadata field doesn't exist or the node doesn't support it
    async fn metadata_u64(&self, name: &str) -> Result<Option<u64>>;
}