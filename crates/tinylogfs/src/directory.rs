// Clean architecture implementation of Directory for OpLog persistence
use super::TinyLogFSError;
use tinyfs::{DirHandle, Directory, NodeRef, NodeID, persistence::{PersistenceLayer, DirectoryOperation}};
use std::sync::Arc;
use async_trait::async_trait;
use tokio::sync::Mutex;
use futures::Stream;
use std::pin::Pin;

/// Clean architecture directory implementation
/// - NO local state (all data comes from persistence layer)
/// - Simple delegation to persistence layer
/// - Proper separation of concerns
pub struct OpLogDirectory {
    /// Unique node identifier for this directory
    node_id: String,
    
    /// Reference to persistence layer (single source of truth)
    persistence: Arc<dyn PersistenceLayer>,
}

impl OpLogDirectory {
    /// Create new directory instance with persistence layer dependency injection
    pub fn new(
        node_id: String,
        persistence: Arc<dyn PersistenceLayer>
    ) -> Self {
        let node_id_bound = &node_id;
        diagnostics::log_debug!("OpLogDirectory::new() - creating directory with node_id: {node_id}", node_id: node_id_bound);
        
        Self {
            node_id,
            persistence,
        }
    }
    
    /// Create a DirHandle from this directory
    pub fn create_handle(self) -> DirHandle {
        DirHandle::new(Arc::new(Mutex::new(Box::new(self))))
    }
    
    /// Convert hex node_id string to NodeID
    fn parse_node_id(&self) -> Result<NodeID, TinyLogFSError> {
        NodeID::from_hex_string(&self.node_id)
            .map_err(|e| TinyLogFSError::TinyFS(tinyfs::Error::Other(format!("Invalid node ID: {}", e))))
    }
}

#[async_trait]
impl Directory for OpLogDirectory {
    async fn get(&self, name: &str) -> tinyfs::Result<Option<NodeRef>> {
        let name_bound = name;
        diagnostics::log_debug!("OpLogDirectory::get('{name}') - querying single entry via optimized persistence layer", name: name_bound);
        
        // Get current directory node ID
        let node_id = self.parse_node_id()
            .map_err(|e| tinyfs::Error::Other(e.to_string()))?;
        
        // Use optimized single entry query instead of loading all entries
        if let Some(child_node_id) = self.persistence.query_directory_entry_by_name(node_id, name).await? {
            // Load the child node to create proper NodeRef
            let child_node_type = self.persistence.load_node(child_node_id, node_id).await?;
            
            // Create Node and wrap in NodeRef
            let node = tinyfs::Node {
                id: child_node_id,
                node_type: child_node_type,
            };
            let node_ref = NodeRef::new(Arc::new(tokio::sync::Mutex::new(node)));
            
            let name_bound = name;
            let child_node_id_bound = format!("{:?}", child_node_id);
            diagnostics::log_debug!("OpLogDirectory::get('{name}') - found child with node_id: {child_node_id}", 
                                    name: name_bound, child_node_id: child_node_id_bound);
            Ok(Some(node_ref))
        } else {
            let name_bound = name;
            diagnostics::log_debug!("OpLogDirectory::get('{name}') - not found", name: name_bound);
            Ok(None)
        }
    }
    
    async fn insert(&mut self, name: String, node_ref: NodeRef) -> tinyfs::Result<()> {
        let name_bound = &name;
        diagnostics::log_debug!("OpLogDirectory::insert('{name}') - delegating to persistence layer", name: name_bound);
        
        // Get current directory node ID
        let node_id = self.parse_node_id()
            .map_err(|e| tinyfs::Error::Other(e.to_string()))?;
        
        // Get child node ID from NodeRef
        let child_node_id = node_ref.id().await;
        
        // Get child node type by accessing the node directly
        let child_node_type = {
            let node = node_ref.lock().await;
            node.node_type.clone()
        };
        
        // Store the child node first (if not already stored)
        let already_exists = self.persistence.exists_node(child_node_id, node_id).await?;
        if !already_exists {
            self.persistence.store_node(child_node_id, node_id, &child_node_type).await?;
        }
        
        // Update directory entry through persistence layer
        self.persistence.update_directory_entry(
            node_id,
            &name,
            DirectoryOperation::Insert(child_node_id)
        ).await?;
        
        let name_bound = &name;
        diagnostics::log_debug!("OpLogDirectory::insert('{name}') - completed via persistence layer", name: name_bound);
        Ok(())
    }
    
    async fn entries(&self) -> tinyfs::Result<Pin<Box<dyn Stream<Item = tinyfs::Result<(String, NodeRef)>> + Send>>> {
        diagnostics::log_debug!("OpLogDirectory::entries() - querying via persistence layer");
        
        // Get current directory node ID
        let node_id = self.parse_node_id()
            .map_err(|e| tinyfs::Error::Other(e.to_string()))?;
        
        // Load directory entries from persistence layer
        let entries = self.persistence.load_directory_entries(node_id).await?;
        
        let entry_count = entries.len();
        diagnostics::log_debug!("OpLogDirectory::entries() - found {entry_count} entries", entry_count: entry_count);
        
        // Convert to stream of NodeRef instances
        let mut entry_results = Vec::new();
        
        for (name, child_node_id) in entries {
            // Load each child node to create proper NodeRef
            match self.persistence.load_node(child_node_id, node_id).await {
                Ok(child_node_type) => {
                    // Create Node and wrap in NodeRef
                    let node = tinyfs::Node {
                        id: child_node_id,
                        node_type: child_node_type,
                    };
                    let node_ref = NodeRef::new(Arc::new(tokio::sync::Mutex::new(node)));
                    entry_results.push(Ok((name, node_ref)));
                }
                Err(e) => {
                    let child_node_hex = child_node_id.to_hex_string();
                    let error_msg = format!("{}", e);
                    diagnostics::log_debug!("  Warning: Failed to load child node {child_node_hex}: {error_msg}", 
                                           child_node_hex: child_node_hex, error_msg: error_msg);
                    entry_results.push(Err(e));
                }
            }
        }
        
        // Create stream from results
        let stream = futures::stream::iter(entry_results);
        Ok(Box::pin(stream))
    }
}
