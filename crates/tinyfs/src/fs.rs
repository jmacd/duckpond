use std::collections::{HashSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::persistence::{PersistenceLayer, DirectoryOperation};
use crate::dir::*;
use crate::error::*;
use crate::node::*;
use crate::wd::WD;

/// Main filesystem structure - pure persistence layer architecture (Phase 5)
#[derive(Clone)]
pub struct FS {
    persistence: Arc<dyn PersistenceLayer>,
    busy: Arc<Mutex<HashSet<NodeID>>>, // Only coordination state for loop detection
}

impl FS {
    /// Creates a filesystem with a PersistenceLayer (Phase 5 approach)
    pub async fn with_persistence_layer<P: PersistenceLayer + 'static>(
        persistence: P,
    ) -> Result<Self> {
        Ok(FS {
            persistence: Arc::new(persistence),
            busy: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    /// Returns a working directory context for the root directory
    pub async fn root(&self) -> Result<WD> {
        // For now, create a basic root node - this will be enhanced later
        let root_node_id = crate::node::ROOT_ID;
        let root_node = self.get_or_create_node(root_node_id, root_node_id).await?;
        let node = NodePath {
            node: root_node,
            path: "/".into(),
        };
        self.wd(&node).await
    }
    
    pub(crate) async fn wd(&self, np: &NodePath) -> Result<WD> {
        WD::new(np.clone(), self.clone()).await
    }

    /// Get or create a node - uses persistence layer directly
    pub async fn get_or_create_node(&self, node_id: NodeID, part_id: NodeID) -> Result<NodeRef> {
        // Try to load from persistence layer
        match self.persistence.load_node(node_id, part_id).await {
            Ok(node_type) => {
                let node = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node { 
                    node_type, 
                    id: node_id 
                })));
                Ok(node)
            }
            Err(Error::NotFound(_)) => {
                // Node doesn't exist - create a basic directory for root
                if node_id == crate::node::ROOT_ID {
                    let dir_handle = crate::memory::MemoryDirectory::new_handle();
                    let node_type = NodeType::Directory(dir_handle);
                    let node = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node { 
                        node_type, 
                        id: node_id 
                    })));
                    Ok(node)
                } else {
                    Err(Error::NotFound(PathBuf::from(format!("Node {} not found", node_id))))
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Create a new node with persistence
    pub async fn create_node(&self, part_id: NodeID, node_type: NodeType) -> Result<NodeRef> {
        let node_id = NodeID::new_sequential();
        self.persistence.store_node(node_id, part_id, &node_type).await?;
        let node = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node { 
            node_type, 
            id: node_id 
        })));
        Ok(node)
    }

    /// Update directory entry
    pub async fn update_directory(
        &self,
        parent_node_id: NodeID,
        entry_name: &str,
        operation: DirectoryOperation,
    ) -> Result<()> {
        self.persistence.update_directory_entry(parent_node_id, entry_name, operation).await
    }

    /// Load directory entries
    pub async fn load_directory_entries(&self, parent_node_id: NodeID) -> Result<HashMap<String, NodeID>> {
        self.persistence.load_directory_entries(parent_node_id).await
    }

    /// Commit any pending operations to persistent storage
    pub async fn commit(&self) -> Result<()> {
        self.persistence.commit().await
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
        busy.insert(id);
        Ok(())
    }

    pub(crate) async fn exit_node(&self, node: &NodePath) {
        let mut busy = self.busy.lock().await;
        busy.remove(&node.id().await);
    }

    /// Legacy methods for backward compatibility - these delegate to create_node
    pub async fn add_node(&self, node_type: NodeType) -> Result<NodeRef> {
        self.create_node(crate::node::ROOT_ID, node_type).await
    }
    
    /// Get a node by its ID
    pub async fn get_node(&self, node_id: NodeID, part_id: NodeID) -> Result<NodeRef> {
        self.get_or_create_node(node_id, part_id).await
    }
    
    /// Create a new directory node and return its NodeRef
    pub async fn create_directory(&self) -> Result<NodeRef> {
        // Generate a new node ID
        let node_id = NodeID::new_sequential();
        
        // Store the directory creation in the persistence layer
        // This will ensure that when the directory is later accessed, it loads as the correct type
        let temp_dir_handle = crate::memory::MemoryDirectory::new_handle();
        let temp_node_type = NodeType::Directory(temp_dir_handle);
        self.persistence.store_node(node_id, crate::node::ROOT_ID, &temp_node_type).await?;
        
        // Now load the directory from the persistence layer to get the proper handle type
        let loaded_node_type = self.persistence.load_node(node_id, crate::node::ROOT_ID).await?;
        
        // Create NodeRef with the same node_id (don't generate a new one!)
        let node = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node { 
            node_type: loaded_node_type, 
            id: node_id 
        })));
        Ok(node)
    }

    /// Create a new file node and return its NodeRef
    pub async fn create_file(&self, content: &[u8], parent_node_id: Option<&str>) -> Result<NodeRef> {
        let _parent_node_id = parent_node_id; // TODO: Use this for proper parent tracking
        let file_handle = crate::memory::MemoryFile::new_handle(content);
        let node_type = NodeType::File(file_handle);
        self.create_node(crate::node::ROOT_ID, node_type).await
    }

    /// Create a new symlink node and return its NodeRef
    pub async fn create_symlink(&self, target: &str, parent_node_id: Option<&str>) -> Result<NodeRef> {
        let _parent_node_id = parent_node_id; // TODO: Use this for proper parent tracking
        let symlink_handle = crate::memory::MemorySymlink::new_handle(target.into());
        let node_type = NodeType::Symlink(symlink_handle);
        self.create_node(crate::node::ROOT_ID, node_type).await
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
