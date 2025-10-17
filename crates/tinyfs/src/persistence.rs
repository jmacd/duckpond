use crate::EntryType;
use crate::error::Result;
use crate::node::{NodeID, NodeType};
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
    /// Extended metadata for this version
    pub extended_metadata: Option<std::collections::HashMap<String, String>>,
}

/// Pure persistence layer - no caching, no NodeRef management
#[async_trait]
pub trait PersistenceLayer: Send + Sync {
    /// Downcast support for accessing concrete implementation methods
    fn as_any(&self) -> &dyn std::any::Any;
    // Node operations (with part_id for containing directory)
    async fn load_node(&self, node_id: NodeID, part_id: NodeID) -> Result<NodeType>;
    async fn store_node(
        &self,
        node_id: NodeID,
        part_id: NodeID,
        node_type: &NodeType,
    ) -> Result<()>;
    async fn exists_node(&self, node_id: NodeID, part_id: NodeID) -> Result<bool>;

    // Symlink operations (for symlinks to avoid local state)
    async fn load_symlink_target(
        &self,
        node_id: NodeID,
        part_id: NodeID,
    ) -> Result<std::path::PathBuf>;
    async fn store_symlink_target(
        &self,
        node_id: NodeID,
        part_id: NodeID,
        target: &std::path::Path,
    ) -> Result<()>;

    // Factory methods for creating nodes directly with persistence
    async fn create_file_node(
        &self,
        node_id: NodeID,
        part_id: NodeID,
        entry_type: EntryType,
    ) -> Result<NodeType>;
    async fn create_directory_node(&self, node_id: NodeID) -> Result<NodeType>;
    async fn create_symlink_node(
        &self,
        node_id: NodeID,
        part_id: NodeID,
        target: &std::path::Path,
    ) -> Result<NodeType>;

    // Dynamic node factory methods
    async fn create_dynamic_directory_node(
        &self,
        parent_node_id: NodeID,
        name: String,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<NodeID>;
    async fn create_dynamic_file_node(
        &self,
        parent_node_id: NodeID,
        name: String,
        file_type: EntryType,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<NodeID>;
    async fn get_dynamic_node_config(
        &self,
        node_id: NodeID,
        part_id: NodeID,
    ) -> Result<Option<(String, Vec<u8>)>>; // (factory_type, config)
    async fn update_dynamic_node_config(
        &self,
        node_id: NodeID,
        part_id: NodeID,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<()>;

    // Directory operations with versioning
    async fn load_directory_entries(
        &self,
        parent_node_id: NodeID,
    ) -> Result<HashMap<String, (NodeID, EntryType)>>;
    /// Optimized query for a single directory entry by name
    async fn query_directory_entry(
        &self,
        parent_node_id: NodeID,
        entry_name: &str,
    ) -> Result<Option<(NodeID, EntryType)>>;
    /// Directory entry update that stores node type (only supported operation)
    async fn update_directory_entry(
        &self,
        parent_node_id: NodeID,
        entry_name: &str,
        operation: DirectoryOperation,
    ) -> Result<()>;

    // Metadata operations
    /// Get consolidated metadata for a node
    /// Requires both node_id and part_id for efficient querying
    async fn metadata(&self, node_id: NodeID, part_id: NodeID) -> Result<crate::NodeMetadata>;

    /// Get a u64 metadata value for a node by name
    /// Common metadata names: "timestamp", "version"
    /// Requires both node_id and part_id for efficient querying
    async fn metadata_u64(
        &self,
        node_id: NodeID,
        part_id: NodeID,
        name: &str,
    ) -> Result<Option<u64>>;

    // Versioning operations (for file:series support)
    /// List all versions of a file, returning metadata for each version
    /// Returns versions in chronological order (oldest to newest)
    async fn list_file_versions(
        &self,
        node_id: NodeID,
        part_id: NodeID,
    ) -> Result<Vec<FileVersionInfo>>;

    /// Read content of a specific version of a file
    /// If version is None, reads the latest version
    async fn read_file_version(
        &self,
        node_id: NodeID,
        part_id: NodeID,
        version: Option<u64>,
    ) -> Result<Vec<u8>>;

    /// Set extended attributes on an existing node
    /// This should modify the pending version of the node in the current transaction
    async fn set_extended_attributes(
        &self,
        node_id: NodeID,
        part_id: NodeID,
        attributes: std::collections::HashMap<String, String>,
    ) -> Result<()>;
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
