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
    async fn create_file(&self, node_id: crate::node::NodeID, content: &[u8], parent_node_id: Option<&str>) -> Result<file::Handle>;
    async fn create_directory(&self, node_id: crate::node::NodeID) -> Result<dir::Handle>;
    async fn create_symlink(&self, node_id: crate::node::NodeID, target: &str, parent_node_id: Option<&str>) -> Result<symlink::Handle>;
    
    /// Commit any pending operations to persistent storage
    /// Returns the number of operations committed
    async fn commit(&self) -> Result<()>;
    
    /// Get the root directory handle for this backend
    /// Each backend implementation should handle its own initialization logic
    /// (restore existing vs create new, etc.) and provide a ready-to-use root directory
    async fn root_directory(&self) -> Result<dir::Handle>;
}
