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
    async fn create_root_directory(&self) -> Result<dir::Handle> {
        // Default implementation: create a regular directory (for non-persistent backends)
        // Root directory always has NodeID(0)
        self.create_directory(crate::node::NodeID::new(0)).await
    }
    async fn create_symlink(&self, node_id: crate::node::NodeID, target: &str, parent_node_id: Option<&str>) -> Result<symlink::Handle>;
    
    /// Commit any pending operations to persistent storage
    /// Returns the number of operations committed
    async fn commit(&self) -> Result<usize> {
        // Default implementation for backends that don't need explicit commits
        Ok(0)
    }
    
    /// Check if this backend has an existing root directory that should be restored
    /// Returns None for non-persistent backends (like memory), Some(handle) for persistent backends
    async fn restore_root_directory(&self) -> Result<Option<dir::Handle>> {
        // Default implementation for non-persistent backends
        Ok(None)
    }
    
    /// Initialize restored nodes in the filesystem
    /// This method is called after the filesystem is created to register any existing nodes
    /// that need to be restored from persistent storage
    async fn initialize_restored_nodes(&self, _fs: &crate::fs::FS) -> Result<()> {
        // Default implementation for non-persistent backends
        Ok(())
    }
    
    /// Restore a specific node by its ID from persistent storage
    /// This method is called on-demand when a node is requested but not found in memory
    /// Returns None if the node doesn't exist in persistent storage
    async fn restore_node_by_id(&self, _fs: &crate::fs::FS, _node_id: crate::node::NodeID) -> Result<Option<crate::node::NodeRef>> {
        // Default implementation for non-persistent backends
        Ok(None)
    }
    
    /// Restore a specific node by partition ID and node ID from persistent storage
    /// This method supports backends that use partitioned storage where nodes are stored
    /// in partitions with their own node ID system (like OpLog with random hex node IDs)
    /// Returns None if the node doesn't exist in persistent storage
    async fn restore_node_by_partition_and_id(&self, _fs: &crate::fs::FS, _partition_id: &str, _node_id: &str) -> Result<Option<crate::node::NodeRef>> {
        // Default implementation for non-persistent backends
        Ok(None)
    }
}
