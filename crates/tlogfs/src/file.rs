// Clean architecture File implementation for TinyFS
use tinyfs::{File, persistence::PersistenceLayer, NodeID};
use std::sync::Arc;
use async_trait::async_trait;

/// Clean architecture file implementation - COMPLETELY STATELESS
/// - NO local state or caching (persistence layer is single source of truth)
/// - Simple delegation to persistence layer for all operations
/// - Proper separation of concerns
pub struct OpLogFile {
    /// Unique node identifier for this file
    node_id: NodeID,
    
    /// Parent directory node ID (for persistence operations)
    parent_node_id: NodeID,
    
    /// Reference to persistence layer (single source of truth)
    persistence: Arc<dyn PersistenceLayer>,
}

impl OpLogFile {
    /// Create new file instance with persistence layer dependency injection
    pub fn new(
        node_id: NodeID,
        parent_node_id: NodeID,
        persistence: Arc<dyn PersistenceLayer>
    ) -> Self {
        let node_id_debug = format!("{:?}", node_id);
        let parent_node_id_debug = format!("{:?}", parent_node_id);
        diagnostics::log_debug!("OpLogFile::new() - creating file with node_id: {node_id}, parent: {parent_node_id}", 
                                node_id: node_id_debug, parent_node_id: parent_node_id_debug);
        
        Self {
            node_id,
            parent_node_id,
            persistence,
        }
    }
    
    /// Create a file handle for TinyFS integration
    pub fn create_handle(oplog_file: OpLogFile) -> tinyfs::FileHandle {
        tinyfs::FileHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(oplog_file))))
    }
}

#[async_trait]
impl File for OpLogFile {
    async fn read_to_vec(&self) -> tinyfs::Result<Vec<u8>> {
        diagnostics::log_debug!("OpLogFile::read_to_vec() - loading content via persistence layer");
        
        // Load file content directly from persistence layer (avoids recursion)
        let content = self.persistence.load_file_content(self.node_id, self.parent_node_id).await?;
        let content_len = content.len();
        diagnostics::log_debug!("OpLogFile::read_to_vec() - loaded {content_len} bytes", content_len: content_len);
        Ok(content)
    }
    
    async fn write_from_slice(&mut self, content: &[u8]) -> tinyfs::Result<()> {
        let content_len = content.len();
        diagnostics::log_debug!("OpLogFile::write_from_slice() - storing {content_len} bytes via persistence layer", content_len: content_len);
        
        // Store content directly via persistence layer
        self.persistence.store_file_content(self.node_id, self.parent_node_id, content).await?;
        
        diagnostics::log_debug!("OpLogFile::write_from_slice() - content stored via persistence layer");
        Ok(())
    }
}
