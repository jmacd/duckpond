use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::DirectoryEntry;
use crate::EntryType;
use crate::error::*;
use crate::node::*;
use crate::persistence::{PersistenceLayer, FileVersionInfo};
use crate::wd::WD;

/// Main filesystem structure - pure persistence layer architecture (Phase 5)
#[derive(Clone)]
pub struct FS {
    pub(crate) persistence: Arc<dyn PersistenceLayer>,

    /// Coordination state for loop detection    
    busy: Arc<Mutex<HashSet<FileID>>>,
}

impl FS {
    /// Creates a filesystem with a PersistenceLayer
    pub async fn new<P: PersistenceLayer + 'static>(persistence: P) -> Result<Self> {
        Ok(FS {
            persistence: Arc::new(persistence),
            busy: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    /// Returns a working directory context for the root directory
    /// The root directory must be explicitly initialized before calling this method
    pub async fn root(&self) -> Result<WD> {
        let root_node = self.get_existing_node(FileID::root()).await?;
        let node = NodePath {
            node: root_node,
            path: "/".into(),
        };
        self.wd(&node).await
    }

    pub(crate) async fn wd(&self, np: &NodePath) -> Result<WD> {
        WD::new(np.clone(), self.clone()).await
    }

    /// Get an existing node - does NOT create if missing
    pub async fn get_existing_node(&self, id: FileID) -> Result<NodeRef> {
        let node_type = self.persistence.load_node(id).await?;
        let node = NodeRef::new(Arc::new(Mutex::new(Node {
            node_type,
            id,
        })));
        Ok(node)
    }

    // /// Get or create a node - uses persistence layer directly
    // pub async fn get_or_create_node(&self, node_id: NodeID, part_id: NodeID) -> Result<NodeRef> {
    //     // Try to load from persistence layer
    //     match self.persistence.load_node(node_id, part_id).await {
    //         Ok(node_type) => {
    //             let node = NodeRef::new(Arc::new(Mutex::new(Node {
    //                 node_type,
    //                 id: node_id,
    //             })));
    //             Ok(node)
    //         }
    //         Err(Error::NotFound(_)) => {
    //             // Node doesn't exist - return error instead of auto-creating
    //             // Nodes should be explicitly created through transactions
    //             Err(Error::NotFound(PathBuf::from(format!(
    //                 "Node {}/{} not found and on-demand creation disabled",
    //                 node_id, part_id
    //             ))))
    //         }
    //         Err(e) => Err(e),
    //     }
    // }

    // /// Get a node by its ID
    // pub async fn get_node(&self, node_id: NodeID, part_id: NodeID) -> Result<NodeRef> {
    //     self.get_or_create_node(node_id, part_id).await
    // }

    /// Create a new node with persistence
    pub async fn create_node(&self, parent_id: FileID, node_type: NodeType) -> Result<NodeRef> {
        let id = parent_id.new_child_id(node_type.entry_type().await?);
        self.persistence
            .store_node(id, &node_type)
            .await?;
        let node = NodeRef::new(Arc::new(Mutex::new(Node {
            node_type,
            id,
        })));
        Ok(node)
    }

    /// Load directory entries with full metadata (without loading nodes)
    pub(crate) async fn load_directory_entries(
        &self,
        parent_id: FileID,
    ) -> Result<HashMap<String, DirectoryEntry>> {
        self.persistence
            .load_directory_entries(parent_id)
            .await
    }

    /// Batch load multiple nodes grouped by partition for efficiency.
    /// Issues one SQL query per partition instead of one query per node.
    pub(crate) async fn batch_load_nodes(
        &self,
        requests: Vec<FileID>,
    ) -> Result<HashMap<FileID, NodeType>> {
        self.persistence.batch_load_nodes(requests).await
    }

    /// Get a metadata value for a node by name (numeric values only)
    /// Common names: "timestamp", "version", "size"
    pub async fn metadata_u64(
        &self,
        id: FileID,
        name: &str,
    ) -> Result<Option<u64>> {
        self.persistence.metadata_u64(id, name).await
    }

    /// Get a working directory context from a NodePath
    pub async fn working_dir_from_node(&self, node_path: &NodePath) -> Result<WD> {
        self.wd(node_path).await
    }

    // Loop detection methods - these work the same regardless of persistence vs backend
    pub(crate) async fn enter_node(&self, node: &NodePath) -> Result<()> {
        let mut busy = self.busy.lock().await;
        let id = node.id().await;
        if busy.contains(&id) {
            return Err(Error::visit_loop(node.path()));
        }
        _ = busy.insert(id);
        Ok(())
    }

    pub(crate) async fn exit_node(&self, node: &NodePath) {
        let mut busy = self.busy.lock().await;
        _ = busy.remove(&node.id().await);
    }

    pub(crate) async fn create_file_node_pending_write(
        &self,
        parent_id: FileID,
        entry_type: EntryType,
    ) -> Result<NodeRef> {
        // Generate a new node ID
        let id = parent_id.new_child_id(entry_type);

        // Create the file node in memory only - no immediate persistence
        let node_type = self
            .persistence
            .create_file_node(id)
            .await?;

        let node = NodeRef::new(Arc::new(Mutex::new(Node {
            node_type,
            id,
        })));
        Ok(node)
    }

    /// Create a new symlink node and return its NodeRef
    pub(crate) async fn create_symlink_node(
        &self,
        parent_id: FileID,
        target: &str,
    ) -> Result<NodeRef> {
        let id = parent_id.new_child_id(EntryType::Symlink);

        let target_path = std::path::Path::new(target);
        let node_type = self
            .persistence
            .create_symlink_node(id, target_path)
            .await?;

        let node = NodeRef::new(Arc::new(Mutex::new(Node {
            node_type,
            id,
        })));
        Ok(node)
    }

    /// List all versions of a file
    pub(crate) async fn list_file_versions(
        &self,
        id: FileID,
    ) -> Result<Vec<FileVersionInfo>> {
        self.persistence.list_file_versions(id).await
    }

    /// Read a specific version of a file
    /// @@@ BAD
    pub(crate) async fn read_file_version(
        &self,
        id: FileID,
        version: Option<u64>,
    ) -> Result<Vec<u8>> {
        self.persistence
            .read_file_version(id, version)
            .await
    }

    /// Create a dynamic directory node with factory type and configuration
    pub(crate) async fn create_dynamic_node(
        &self,
        id: FileID,
        name: String,
	entry_type: EntryType,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<NodeType> {
        self.persistence
            .create_dynamic_node(id, name, entry_type, factory_type, config_content)
            .await
    }

    /// Check if a node is dynamic and return its factory configuration
    pub(crate) async fn get_dynamic_node_config(
        &self,
        id: FileID,
    ) -> Result<Option<(String, Vec<u8>)>> {
        self.persistence
            .get_dynamic_node_config(id)
            .await
    }

    /// Update the configuration of an existing dynamic node
    pub(crate) async fn update_dynamic_node_config(
        &self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<()> {
        self.persistence
            .update_dynamic_node_config(id, factory_type, config_content)
            .await
    }

    /// Set extended attributes on an existing node
    pub async fn set_extended_attributes(
        &self,
        id: FileID,
        attributes: HashMap<String, String>,
    ) -> Result<()> {
        self.persistence
            .set_extended_attributes(id, attributes)
            .await
    }
}

impl std::fmt::Debug for FS {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FS{{}}")
    }
}

impl PartialEq<FS> for FS {
    fn eq(&self, other: &FS) -> bool {
        std::ptr::eq(self, other)
    }
}
