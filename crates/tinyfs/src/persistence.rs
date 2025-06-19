use crate::node::{NodeID, NodeType};
use crate::error::Result;
use async_trait::async_trait;
use std::collections::HashMap;

/// Pure persistence layer - no caching, no NodeRef management
#[async_trait]
pub trait PersistenceLayer: Send + Sync {
    // Node operations (with part_id for containing directory)
    async fn load_node(&self, node_id: NodeID, part_id: NodeID) -> Result<NodeType>;
    async fn store_node(&self, node_id: NodeID, part_id: NodeID, node_type: &NodeType) -> Result<()>;
    async fn exists_node(&self, node_id: NodeID, part_id: NodeID) -> Result<bool>;
    
    // Directory operations with versioning
    async fn load_directory_entries(&self, parent_node_id: NodeID) -> Result<HashMap<String, NodeID>>;
    async fn update_directory_entry(&self, parent_node_id: NodeID, entry_name: &str, operation: DirectoryOperation) -> Result<()>;
    
    // Transaction management
    async fn commit(&self) -> Result<()>;
    async fn rollback(&self) -> Result<()>;
}

#[derive(Debug, Clone)]
pub enum DirectoryOperation {
    Insert(NodeID),
    Delete,
    Rename(String, NodeID),
}
