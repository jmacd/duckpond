//! Filesystem backend trait.

use crate::dir;
use crate::file;
use crate::symlink;
use crate::error::Result;
use async_trait::async_trait;
use std::path::Path;

/// Trait for filesystem backend.
#[async_trait]
pub trait FilesystemBackend: Send + Sync {
    async fn create_file(&self, content: &[u8], parent_node_id: Option<&str>) -> Result<file::Handle>;
    async fn create_directory(&self) -> Result<dir::Handle>;
    async fn create_symlink(&self, target: &str, parent_node_id: Option<&str>) -> Result<symlink::Handle>;
    
    /// Commit any pending operations to persistent storage
    /// Returns the number of operations committed
    async fn commit(&self) -> Result<usize> {
        // Default implementation for backends that don't need explicit commits
        Ok(0)
    }
}
