// Clean architecture implementation of Directory for OpLog persistence
use super::{TinyLogFSError, OplogEntry, DirectoryEntry};
use tinyfs::{DirHandle, Directory, NodeRef, NodeID, persistence::{PersistenceLayer, DirectoryOperation}};
use std::sync::Arc;
use async_trait::async_trait;
use tokio::sync::Mutex;
use futures::{Stream, StreamExt};
use std::pin::Pin;

/// Clean architecture directory implementation
/// - NO local state (all data comes from persistence layer)
/// - Simple delegation to persistence layer
/// - Proper separation of concerns
pub struct OpLogDirectory {
    /// Unique node identifier for this directory
    node_id: String,
    
    /// Parent directory node ID (for persistence operations)
    parent_node_id: NodeID,
    
    /// Reference to persistence layer (single source of truth)
    persistence: Arc<dyn PersistenceLayer>,
}

impl OpLogDirectory {
    /// Create new directory instance with persistence layer dependency injection
    pub fn new(
        node_id: String,
        parent_node_id: NodeID,
        persistence: Arc<dyn PersistenceLayer>
    ) -> Self {
        println!("OpLogDirectory::new() - creating directory with node_id: {}, parent: {:?}", 
                 node_id, parent_node_id);
        
        Self {
            node_id,
            parent_node_id,
            persistence,
        }
    }
    
    /// Create a DirHandle from this directory
    pub fn create_handle(self) -> DirHandle {
        DirHandle::new(Arc::new(Mutex::new(Box::new(self))))
    }
    
    /// Convert hex node_id string to NodeID
    fn parse_node_id(&self) -> Result<NodeID, TinyLogFSError> {
        NodeID::from_hex(&self.node_id)
            .map_err(|e| TinyLogFSError::Other(format!("Invalid node ID: {}", e)))
    }
}

#[async_trait]
impl Directory for OpLogDirectory {
    async fn get(&self, name: &str) -> tinyfs::Result<Option<NodeRef>> {
        println!("OpLogDirectory::get('{}') - querying via persistence layer", name);
        
        // Get current directory node ID
        let node_id = self.parse_node_id()
            .map_err(|e| tinyfs::Error::Other(e.to_string()))?;
        
        // Load directory entries from persistence layer
        let entries = self.persistence.load_directory_entries(node_id).await?;
        
        // Look up the specific entry
        if let Some(child_node_id) = entries.get(name) {
            // Load the child node to create proper NodeRef
            let child_node = self.persistence.load_node(*child_node_id, node_id).await?;
            let node_ref = NodeRef::new(*child_node_id, child_node);
            
            println!("OpLogDirectory::get('{}') - found child with node_id: {:?}", name, child_node_id);
            Ok(Some(node_ref))
        } else {
            println!("OpLogDirectory::get('{}') - not found", name);
            Ok(None)
        }
    }
    
    async fn insert(&mut self, name: String, node_ref: NodeRef) -> tinyfs::Result<()> {
        println!("OpLogDirectory::insert('{}') - delegating to persistence layer", name);
        
        // Get current directory node ID
        let node_id = self.parse_node_id()
            .map_err(|e| tinyfs::Error::Other(e.to_string()))?;
        
        // Get child node ID from NodeRef
        let child_node_id = node_ref.node_id();
        
        // Store the child node first (if not already stored)
        let child_node_type = node_ref.into_node_type();
        self.persistence.store_node(child_node_id, node_id, &child_node_type).await?;
        
        // Update directory entry through persistence layer
        self.persistence.update_directory_entry(
            node_id,
            &name,
            DirectoryOperation::Insert(child_node_id)
        ).await?;
        
        println!("OpLogDirectory::insert('{}') - completed via persistence layer", name);
        Ok(())
    }
    
    async fn entries(&self) -> tinyfs::Result<Pin<Box<dyn Stream<Item = tinyfs::Result<(String, NodeRef)>> + Send>>> {
        println!("OpLogDirectory::entries() - querying via persistence layer");
        
        // Get current directory node ID
        let node_id = self.parse_node_id()
            .map_err(|e| tinyfs::Error::Other(e.to_string()))?;
        
        // Load directory entries from persistence layer
        let entries = self.persistence.load_directory_entries(node_id).await?;
        
        println!("OpLogDirectory::entries() - found {} entries", entries.len());
        
        // Convert to stream of NodeRef instances
        let mut entry_results = Vec::new();
        
        for (name, child_node_id) in entries {
            // Load each child node to create proper NodeRef
            match self.persistence.load_node(child_node_id, node_id).await {
                Ok(child_node) => {
                    let node_ref = NodeRef::new(child_node_id, child_node);
                    entry_results.push(Ok((name, node_ref)));
                }
                Err(e) => {
                    println!("  Warning: Failed to load child node {}: {}", child_node_id.to_hex_string(), e);
                    entry_results.push(Err(e));
                }
            }
        }
        
        // Create stream from results
        let stream = futures::stream::iter(entry_results);
        Ok(Box::pin(stream))
    }
}
