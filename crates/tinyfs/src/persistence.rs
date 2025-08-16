use crate::node::{NodeID, NodeType};
use crate::error::Result;
use crate::EntryType;
use async_trait::async_trait;
use std::collections::HashMap;

/// Information about a specific version of a file
#[derive(Debug, Clone)]
pub struct FileVersionInfo {
    /// Version number (monotonically increasing)
    pub version: u64,
    /// Timestamp when this version was created (Unix microseconds)
    pub timestamp: i64,
    /// Size of the file content in bytes
    pub size: u64,
    /// SHA256 hash of the content (for integrity checking)
    pub sha256: Option<String>,
    /// Entry type for this version
    pub entry_type: EntryType,
    /// Extended metadata for this version (e.g., temporal range for file:series)
    pub extended_metadata: Option<std::collections::HashMap<String, String>>,
}

/// Pure persistence layer - no caching, no NodeRef management
#[async_trait]
pub trait PersistenceLayer: Send + Sync {
    /// Downcast support for accessing concrete implementation methods
    fn as_any(&self) -> &dyn std::any::Any;
    // Node operations (with part_id for containing directory)
    async fn load_node(&self, node_id: NodeID, part_id: NodeID) -> Result<NodeType>;
    async fn store_node(&self, node_id: NodeID, part_id: NodeID, node_type: &NodeType) -> Result<()>;
    async fn exists_node(&self, node_id: NodeID, part_id: NodeID) -> Result<bool>;
    
    // Raw content operations (for files to avoid recursion)
    async fn load_file_content(&self, node_id: NodeID, part_id: NodeID) -> Result<Vec<u8>>;
    /// Store file content - INTERNAL USE ONLY - called by streaming writers
    // async fn store_file_content(&mut self, node_id: NodeID, part_id: NodeID, content: &[u8]) -> Result<()>;
    /// Store file content with specific entry type - INTERNAL USE ONLY - called by streaming writers  
    async fn store_file_content_with_type(&mut self, node_id: NodeID, part_id: NodeID, content: &[u8], entry_type: EntryType) -> Result<()>;
    /// Update existing file content within same transaction (replaces rather than appends)
    async fn update_file_content_with_type(&mut self, node_id: NodeID, part_id: NodeID, content: &[u8], entry_type: EntryType) -> Result<()> {
        // Default implementation falls back to store (for persistence layers that don't support updates)
        self.store_file_content_with_type(node_id, part_id, content, entry_type).await
    }
    /// Store FileSeries with pre-computed temporal metadata
    /// Use this when you already know the min/max event times from Parquet metadata
    async fn store_file_series_with_metadata(
        &mut self,
        node_id: NodeID,
        part_id: NodeID,
        content: &[u8],
        _min_event_time: i64,
        _max_event_time: i64,
        _timestamp_column: &str,
    ) -> Result<()> {
        // Default implementation falls back to regular file storage
        // Concrete implementations (like OpLogPersistence) can override this
        self.store_file_content_with_type(node_id, part_id, content, EntryType::FileSeries).await
    }
    
    // Symlink operations (for symlinks to avoid local state)
    async fn load_symlink_target(&self, node_id: NodeID, part_id: NodeID) -> Result<std::path::PathBuf>;
    async fn store_symlink_target(&self, node_id: NodeID, part_id: NodeID, target: &std::path::Path) -> Result<()>;
    
    // Factory methods for creating nodes directly with persistence
    /// Create file node with content - INTERNAL USE ONLY - called by streaming writers
    async fn create_file_node(&self, node_id: NodeID, part_id: NodeID, content: &[u8], entry_type: EntryType) -> Result<NodeType>;
    /// Create file node in memory only (for streaming operations) - no immediate persistence
    async fn create_file_node_memory_only(&self, node_id: NodeID, part_id: NodeID, entry_type: EntryType) -> Result<NodeType>;
    async fn create_directory_node(&self, node_id: NodeID, parent_node_id: NodeID) -> Result<NodeType>;
    async fn create_symlink_node(&self, node_id: NodeID, part_id: NodeID, target: &std::path::Path) -> Result<NodeType>;
    
    // Dynamic node factory methods
    /// Create dynamic directory node with factory type and configuration
    async fn create_dynamic_directory_node(&self, parent_node_id: NodeID, name: String, factory_type: &str, config_content: Vec<u8>) -> Result<NodeID>;
    /// Create dynamic file node with factory type and configuration  
    async fn create_dynamic_file_node(&self, parent_node_id: NodeID, name: String, file_type: EntryType, factory_type: &str, config_content: Vec<u8>) -> Result<NodeID>;
    /// Check if a node is dynamic and return its factory configuration
    async fn get_dynamic_node_config(&self, node_id: NodeID, part_id: NodeID) -> Result<Option<(String, Vec<u8>)>>; // (factory_type, config)
    
    // Directory operations with versioning
    async fn load_directory_entries(&self, parent_node_id: NodeID) -> Result<HashMap<String, NodeID>>;
    /// Optimized query for a single directory entry by name
    async fn query_directory_entry_by_name(&self, parent_node_id: NodeID, entry_name: &str) -> Result<Option<NodeID>>;
    /// Enhanced query for a single directory entry by name that returns node type
    async fn query_directory_entry_with_type_by_name(&self, parent_node_id: NodeID, entry_name: &str) -> Result<Option<(NodeID, crate::EntryType)>>;
    /// Enhanced directory entries loading that returns node types
    async fn load_directory_entries_with_types(&self, parent_node_id: NodeID) -> Result<HashMap<String, (NodeID, crate::EntryType)>>;
    /// Directory entry update that stores node type (only supported operation)
    async fn update_directory_entry_with_type(&self, parent_node_id: NodeID, entry_name: &str, operation: DirectoryOperation) -> Result<()>;
    
    // Metadata operations
    /// Get consolidated metadata for a node
    /// Requires both node_id and part_id for efficient querying
    async fn metadata(&self, node_id: NodeID, part_id: NodeID) -> Result<crate::NodeMetadata>;
    
    /// Get a u64 metadata value for a node by name
    /// Common metadata names: "timestamp", "version"
    /// Requires both node_id and part_id for efficient querying
    async fn metadata_u64(&self, node_id: NodeID, part_id: NodeID, name: &str) -> Result<Option<u64>>;
    
    /// Check if there are pending operations that need to be committed
    async fn has_pending_operations(&self) -> Result<bool>;
    
    // Versioning operations (for file:series support)
    /// List all versions of a file, returning metadata for each version
    /// Returns versions in chronological order (oldest to newest)
    async fn list_file_versions(&self, node_id: NodeID, part_id: NodeID) -> Result<Vec<FileVersionInfo>>;
    
    /// Read content of a specific version of a file
    /// If version is None, reads the latest version
    async fn read_file_version(&self, node_id: NodeID, part_id: NodeID, version: Option<u64>) -> Result<Vec<u8>>;
    
    /// Check if a file has multiple versions (is a versioned file/series)
    async fn is_versioned_file(&self, node_id: NodeID, part_id: NodeID) -> Result<bool>;
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
