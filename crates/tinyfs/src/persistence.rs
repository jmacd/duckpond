use crate::node::{NodeID, NodeType};
use crate::error::Result;
use crate::EntryType;
use async_trait::async_trait;
use std::collections::HashMap;

/// Pure persistence layer - no caching, no NodeRef management
#[async_trait]
pub trait PersistenceLayer: Send + Sync {
    // Node operations (with part_id for containing directory)
    async fn load_node(&self, node_id: NodeID, part_id: NodeID) -> Result<NodeType>;
    async fn store_node(&self, node_id: NodeID, part_id: NodeID, node_type: &NodeType) -> Result<()>;
    async fn exists_node(&self, node_id: NodeID, part_id: NodeID) -> Result<bool>;
    
    // Raw content operations (for files to avoid recursion)
    async fn load_file_content(&self, node_id: NodeID, part_id: NodeID) -> Result<Vec<u8>>;
    async fn store_file_content(&self, node_id: NodeID, part_id: NodeID, content: &[u8]) -> Result<()>;
    
    // Symlink operations (for symlinks to avoid local state)
    async fn load_symlink_target(&self, node_id: NodeID, part_id: NodeID) -> Result<std::path::PathBuf>;
    async fn store_symlink_target(&self, node_id: NodeID, part_id: NodeID, target: &std::path::Path) -> Result<()>;
    
    // Factory methods for creating nodes directly with persistence
    async fn create_file_node(&self, node_id: NodeID, part_id: NodeID, content: &[u8], entry_type: EntryType) -> Result<NodeType>;
    async fn create_directory_node(&self, node_id: NodeID, parent_node_id: NodeID) -> Result<NodeType>;
    async fn create_symlink_node(&self, node_id: NodeID, part_id: NodeID, target: &std::path::Path) -> Result<NodeType>;
    
    // Directory operations with versioning
    async fn load_directory_entries(&self, parent_node_id: NodeID) -> Result<HashMap<String, NodeID>>;
    /// Optimized query for a single directory entry by name
    async fn query_directory_entry_by_name(&self, parent_node_id: NodeID, entry_name: &str) -> Result<Option<NodeID>>;
    /// Enhanced query for a single directory entry by name that returns node type
    async fn query_directory_entry_with_type_by_name(&self, parent_node_id: NodeID, entry_name: &str) -> Result<Option<(NodeID, crate::EntryType)>>;
    /// Enhanced directory entries loading that returns node types
    async fn load_directory_entries_with_types(&self, parent_node_id: NodeID) -> Result<HashMap<String, (NodeID, crate::EntryType)>>;
    /// Directory entry update that stores node type (only supported operation)
    async fn update_directory_entry_with_type(&self, parent_node_id: NodeID, entry_name: &str, operation: DirectoryOperation, node_type: &crate::EntryType) -> Result<()>;
    
    // Transaction management
    async fn begin_transaction(&self) -> Result<()>;
    async fn commit(&self) -> Result<()>;
    async fn rollback(&self) -> Result<()>;
    
    // Transaction operations
    /// Get the current transaction ID, if any transaction is active
    async fn current_transaction_id(&self) -> Result<Option<i64>>;
    
    // Metadata operations
    /// Get a u64 metadata value for a node by name
    /// Common metadata names: "timestamp", "version"
    /// Requires both node_id and part_id for efficient querying
    async fn metadata_u64(&self, node_id: NodeID, part_id: NodeID, name: &str) -> Result<Option<u64>>;
    
    /// Check if there are pending operations that need to be committed
    async fn has_pending_operations(&self) -> Result<bool>;
}

#[derive(Debug, Clone)]
pub enum DirectoryOperation {
    /// Insert operation that includes node type (only supported operation)
    InsertWithType(NodeID, crate::EntryType),
    /// Delete operation with node type for consistency
    DeleteWithType(crate::EntryType),
    /// Rename operation with node type
    RenameWithType(String, NodeID, crate::EntryType), // old_name, new_node_id, node_type
}
