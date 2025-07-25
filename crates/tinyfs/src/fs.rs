use std::collections::{HashSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::persistence::{PersistenceLayer, DirectoryOperation};
use crate::dir::*;
use crate::error::*;
use crate::node::*;
use crate::wd::WD;
use crate::EntryType;

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
        let root_node_id = crate::node::NodeID::root();
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
                // Node doesn't exist - for root node, persistence layer should handle creation
                if node_id == crate::node::NodeID::root() {
                    // For root directory, try loading again - the persistence layer will auto-create it
                    let node_type = self.persistence.load_node(node_id, part_id).await?;
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
        let node_id = NodeID::generate();
        self.persistence.store_node(node_id, part_id, &node_type).await?;
        let node = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node { 
            node_type, 
            id: node_id 
        })));
        Ok(node)
    }

    /// Load directory entries
    pub async fn load_directory_entries(&self, parent_node_id: NodeID) -> Result<HashMap<String, NodeID>> {
        self.persistence.load_directory_entries(parent_node_id).await
    }

    /// Get a u64 metadata value for a node by name
    pub async fn metadata_u64(&self, node_id: NodeID, part_id: NodeID, name: &str) -> Result<Option<u64>> {
        self.persistence.metadata_u64(node_id, part_id, name).await
    }

    /// Commit any pending operations to persistent storage
    pub async fn commit(&self) -> Result<()> {
        diagnostics::log_info!("TRANSACTION: FS::commit() called");
        self.persistence.commit().await
    }

    /// Begin an explicit transaction (clears any pending operations to start fresh)
    pub async fn begin_transaction(&self) -> Result<()> {
        diagnostics::log_info!("TRANSACTION: FS::begin_transaction() called");
        self.persistence.begin_transaction().await
    }

    /// Check if there are pending operations that need to be committed
    pub async fn has_pending_operations(&self) -> Result<bool> {
        let result = self.persistence.has_pending_operations().await?;
        diagnostics::log_info!("TRANSACTION: FS::has_pending_operations() = {result}", result: result);
        Ok(result)
    }

    /// Rollback any pending operations without committing them
    pub async fn rollback(&self) -> Result<()> {
        diagnostics::log_info!("TRANSACTION: FS::rollback() called");
        self.persistence.rollback().await
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

    /// Get a node by its ID
    pub async fn get_node(&self, node_id: NodeID, part_id: NodeID) -> Result<NodeRef> {
        self.get_or_create_node(node_id, part_id).await
    }
    
    /// Create a new directory node and return its NodeRef
    pub async fn create_directory(&self) -> Result<NodeRef> {
        // Generate a new node ID
        let node_id = NodeID::generate();
        
        // Create the directory node via persistence layer - this will create OpLogDirectory directly
        // For new directories, we don't know the parent yet, so we use the node_id as a placeholder
        let node_type = self.persistence.create_directory_node(node_id, node_id).await?;
        
        let node = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node { 
            node_type, 
            id: node_id 
        })));
        Ok(node)
    }
    
    pub async fn create_file_memory_only(&self, parent_node_id: Option<&str>, entry_type: EntryType) -> Result<NodeRef> {
        // Generate a new node ID  
        let node_id = NodeID::generate();
        
        // Use the provided parent_node_id as the part_id, or ROOT_ID as fallback
        let part_id = if let Some(parent_id_str) = parent_node_id {
            // Convert parent node ID string to NodeID
            NodeID::from_hex_string(parent_id_str)
                .map_err(|_| Error::Other(format!("Invalid parent node ID: {}", parent_id_str)))?
        } else {
            crate::node::NodeID::root()
        };
        
        // Create the file node in memory only - no immediate persistence
        let node_type = self.persistence.create_file_node_memory_only(node_id, part_id, entry_type).await?;
        
        let node = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node { 
            node_type, 
            id: node_id 
        })));
        Ok(node)
    }

    /// Create a FileSeries with temporal metadata
    pub async fn create_file_series_with_metadata(
        &self,
        node_id: NodeID,
        part_id: NodeID,
        content: &[u8],
        min_event_time: i64,
        max_event_time: i64,
        timestamp_column: &str,
    ) -> Result<NodeRef> {
        // Use the trait method to store with temporal metadata
        self.persistence.store_file_series_with_metadata(
            node_id,
            part_id,
            content,
            min_event_time,
            max_event_time,
            timestamp_column,
        ).await?;
        
        // Create and return a NodeRef for the stored file
        let node_type = self.persistence.load_node(node_id, part_id).await?;
        let node = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node { 
            node_type, 
            id: node_id 
        })));
        Ok(node)
    }

    /// Create a new symlink node and return its NodeRef
    pub async fn create_symlink(&self, target: &str, parent_node_id: Option<&str>) -> Result<NodeRef> {
        // Generate a new node ID  
        let node_id = NodeID::generate();
        
        // Use the provided parent_node_id as the part_id, or ROOT_ID as fallback
        let part_id = if let Some(parent_id_str) = parent_node_id {
            // Convert parent node ID string to NodeID
            NodeID::from_hex_string(parent_id_str)
                .map_err(|_| Error::Other(format!("Invalid parent node ID: {}", parent_id_str)))?
        } else {
            crate::node::NodeID::root()
        };
        
        // Create the symlink node via persistence layer - this will create OpLogSymlink directly
        let target_path = std::path::Path::new(target);
        let node_type = self.persistence.create_symlink_node(node_id, part_id, target_path).await?;
        
        let node = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node { 
            node_type, 
            id: node_id 
        })));
        Ok(node)
    }

    /// List all versions of a file
    pub async fn list_file_versions(&self, node_id: NodeID, part_id: NodeID) -> Result<Vec<crate::persistence::FileVersionInfo>> {
        self.persistence.list_file_versions(node_id, part_id).await
    }

    /// Read a specific version of a file
    pub async fn read_file_version(&self, node_id: NodeID, part_id: NodeID, version: Option<u64>) -> Result<Vec<u8>> {
        self.persistence.read_file_version(node_id, part_id, version).await
    }

    /// Check if a file has multiple versions
    pub async fn is_versioned_file(&self, node_id: NodeID, part_id: NodeID) -> Result<bool> {
        self.persistence.is_versioned_file(node_id, part_id).await
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
