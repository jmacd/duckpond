use crate::EntryType;
use crate::dir::DirectoryEntry;
use crate::error::Result;
use crate::node::{NodeID, FileID, Node};
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
    pub extended_metadata: Option<HashMap<String, String>>,
}

/// Pure persistence layer - no caching, no NodeRef management
#[async_trait]
pub trait PersistenceLayer: Send + Sync {
    /// Downcast support for accessing concrete implementation methods
    fn as_any(&self) -> &dyn std::any::Any;

    // Node operations
    async fn load_node(&self, file_id: FileID) -> Result<Node>;
    async fn store_node(
        &self,
        node: &Node,
    ) -> Result<()>;
    async fn exists_node(&self, id: FileID) -> Result<bool>;

    // Symlink operations
    async fn load_symlink_target(
        &self,
	id: FileID,
    ) -> Result<std::path::PathBuf>;

    // Factory methods for creating nodes directly with persistence
    async fn create_file_node(
        &self,
	file_id: FileID,
    ) -> Result<Node>;

    async fn create_directory_node(&self,
	id: FileID,
    ) -> Result<Node>;
    async fn create_symlink_node(
        &self,
        id: FileID,
        target: &std::path::Path,
    ) -> Result<Node>;

    // Dynamic node factory methods
    async fn create_dynamic_node(
        &self,
        id: FileID,
        name: String,
	entry_type: EntryType,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<Node>;

    async fn get_dynamic_node_config(
        &self,
        id: FileID,
    ) -> Result<Option<(String, Vec<u8>)>>; // (factory_type, config)

    async fn update_dynamic_node_config(
        &self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<()>;

    // Directory operations with versioning
    /// Load all directory entries with full metadata (without loading nodes)
    async fn load_directory_entries(
        &self,
        parent_id: FileID,
    ) -> Result<HashMap<String, DirectoryEntry>>;
    
    /// Batch load multiple nodes grouped by partition for efficiency.
    /// Default implementation returns empty map; tlogfs overrides with optimized batch loading.
    async fn batch_load_nodes(
        &self,
	parent_id: FileID,
        requests: Vec<DirectoryEntry>,
    ) -> Result<HashMap<String, Node>>;
    
    /// Optimized query for a single directory entry by name
    async fn query_directory_entry(
        &self,
        parent_id: FileID,
        entry_name: &str,
    ) -> Result<Option<DirectoryEntry>>;
    /// Directory entry update that stores node type (only supported operation)
    async fn update_directory_entry(
        &self,
        id: FileID,
        entry_name: &str,
        operation: DirectoryOperation,
    ) -> Result<()>;

    // Metadata operations
    /// Get consolidated metadata for a node
    /// Requires both node_id and part_id for efficient querying
    async fn metadata(&self, id: FileID) -> Result<crate::NodeMetadata>;

    // Versioning operations (for file:series support)
    /// List all versions of a file, returning metadata for each version
    /// Returns versions in chronological order (oldest to newest)
    async fn list_file_versions(
        &self,
        id: FileID,
    ) -> Result<Vec<FileVersionInfo>>;

    /// Read content of a specific version of a file
    /// If version is None, reads the latest version
    async fn read_file_version(
        &self,
        id: FileID,
        version: Option<u64>,
    ) -> Result<Vec<u8>>;

    /// Set extended attributes on an existing node
    /// This should modify the pending version of the node in the current transaction
    async fn set_extended_attributes(
        &self,
        id: FileID,
        attributes: HashMap<String, String>,
    ) -> Result<()>;
}

#[derive(Clone)]
pub enum DirectoryOperation {
    /// Insert operation that includes node type (only supported operation)
    InsertWithType(NodeID, EntryType),
    /// Delete operation with node type for consistency
    DeleteWithType(EntryType),
    /// Rename operation with node type
    RenameWithType(String, NodeID, EntryType), // old_name, new_node_id, node_type
}
