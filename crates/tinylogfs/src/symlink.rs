// Clean architecture Symlink implementation for TinyFS
use tinyfs::{Symlink, persistence::PersistenceLayer, NodeID};
use std::sync::Arc;
use async_trait::async_trait;

/// Clean architecture symlink implementation - COMPLETELY STATELESS
/// - NO local state or caching (persistence layer is single source of truth)
/// - Simple delegation to persistence layer for all operations
/// - Proper separation of concerns
pub struct OpLogSymlink {
    /// Unique node identifier for this symlink
    node_id: NodeID,
    
    /// Parent directory node ID (for persistence operations)
    parent_node_id: NodeID,
    
    /// Reference to persistence layer (single source of truth)
    persistence: Arc<dyn PersistenceLayer>,
}

impl OpLogSymlink {
    /// Create new symlink instance with persistence layer dependency injection
    pub fn new(
        node_id: NodeID,
        parent_node_id: NodeID,
        persistence: Arc<dyn PersistenceLayer>
    ) -> Self {
        println!("OpLogSymlink::new() - creating symlink with node_id: {:?}, parent: {:?}", 
                 node_id, parent_node_id);
        
        Self {
            node_id,
            parent_node_id,
            persistence,
        }
    }
    
    /// Create a symlink handle for TinyFS integration
    pub fn create_handle(oplog_symlink: OpLogSymlink) -> tinyfs::SymlinkHandle {
        tinyfs::SymlinkHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(oplog_symlink))))
    }
}

#[async_trait]
impl Symlink for OpLogSymlink {
    async fn readlink(&self) -> tinyfs::Result<std::path::PathBuf> {
        println!("OpLogSymlink::readlink() - loading target via persistence layer");
        
        // Load symlink target directly from persistence layer (avoids recursion)
        let target = self.persistence.load_symlink_target(self.node_id, self.parent_node_id).await?;
        println!("OpLogSymlink::readlink() - loaded target: {:?}", target);
        Ok(target)
    }
}
