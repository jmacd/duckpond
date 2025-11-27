use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::DirectoryEntry;
use crate::EntryType;
use crate::error::*;
use crate::node::*;
use crate::persistence::{FileVersionInfo, PersistenceLayer};
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
        let root_node = self.persistence.load_node(FileID::root()).await?;
        let node = NodePath {
            node: root_node,
            path: "/".into(),
        };
        self.wd(&node).await
    }

    pub(crate) async fn wd(&self, np: &NodePath) -> Result<WD> {
        WD::new(np.clone(), self.clone()).await
    }

    /// Create a new node with persistence
    pub(crate) async fn create_node(&self, parent_id: FileID, node_type: NodeType) -> Result<Node> {
        let id = parent_id.new_child_id(node_type.entry_type().await?);
	let node = Node::new(id, node_type);
        self.persistence.store_node(&node).await?;
        Ok(node)
    }

    /// Batch load multiple nodes grouped by partition for efficiency.
    pub(crate) async fn batch_load_nodes(
        &self,
	parent_id: FileID,
        requests: Vec<DirectoryEntry>,
    ) -> Result<HashMap<String, Node>> {
        self.persistence.batch_load_nodes(parent_id, requests).await
    }

    // Loop detection methods - these work the same regardless of persistence vs backend
    pub(crate) async fn enter_node(&self, node: &NodePath) -> Result<()> {
        let mut busy = self.busy.lock().await;
        let id = node.id();
        if busy.contains(&id) {
            return Err(Error::visit_loop(node.path()));
        }
        _ = busy.insert(id);
        Ok(())
    }

    pub(crate) async fn exit_node(&self, node: &NodePath) {
        let mut busy = self.busy.lock().await;
        _ = busy.remove(&node.id());
    }

    pub(crate) async fn create_file_node_pending_write(
        &self,
        parent_id: FileID,
        entry_type: EntryType,
    ) -> Result<Node> {
        // Generate a new node ID
        let id = parent_id.new_child_id(entry_type);

        // Create the file node in memory only - no immediate persistence
        self.persistence.create_file_node(id).await
    }

    /// Create a new symlink node and return its Node
    pub(crate) async fn create_symlink_node(
        &self,
        parent_id: FileID,
        target: &str,
    ) -> Result<Node> {
        let id = parent_id.new_child_id(EntryType::Symlink);

        let target_path = std::path::Path::new(target);
        self
            .persistence
            .create_symlink_node(id, target_path)
            .await
    }

    /// List all versions of a file
    pub(crate) async fn list_file_versions(&self, id: FileID) -> Result<Vec<FileVersionInfo>> {
        self.persistence.list_file_versions(id).await
    }

    /// Read a specific version of a file
    /// @@@ BAD
    pub(crate) async fn read_file_version(
        &self,
        id: FileID,
        version: Option<u64>,
    ) -> Result<Vec<u8>> {
        self.persistence.read_file_version(id, version).await
    }

    /// Create a dynamic directory node with factory type and configuration
    pub(crate) async fn create_dynamic_node(
        &self,
        id: FileID,
        name: String,
        entry_type: EntryType,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<Node> {
        self.persistence
            .create_dynamic_node(id, name, entry_type, factory_type, config_content)
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
    pub(crate) async fn set_extended_attributes(
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
