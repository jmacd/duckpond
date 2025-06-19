use std::collections::{HashSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::backend::FilesystemBackend;
use crate::persistence::{PersistenceLayer, DirectoryOperation};
use crate::dir::*;
use crate::error::*;
use crate::node::*;
use crate::wd::WD;

/// Main filesystem structure - simplified two-layer architecture
#[derive(Clone)]
pub struct FS {
    persistence: Option<Arc<dyn PersistenceLayer>>,
    backend: Option<Arc<dyn FilesystemBackend>>, // Keep for backward compatibility during transition
    busy: Arc<Mutex<HashSet<NodeID>>>, // Only coordination state for loop detection
}

impl FS {
    /// New constructor: Creates a filesystem with a PersistenceLayer (Phase 2 approach)
    pub async fn with_persistence_layer<P: PersistenceLayer + 'static>(
        persistence: P,
    ) -> Result<Self> {
        Ok(FS {
            persistence: Some(Arc::new(persistence)),
            backend: None,
            busy: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    /// Legacy constructor: Creates a filesystem with a backend (Phase 1 compatibility)
    pub async fn with_backend<B: FilesystemBackend + 'static>(backend: B) -> Result<Self> {
        Ok(FS {
            persistence: None,
            backend: Some(Arc::new(backend)),
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

    /// Get or create a node - abstraction that works with both persistence and backend
    pub async fn get_or_create_node(&self, node_id: NodeID, part_id: NodeID) -> Result<NodeRef> {
        if let Some(persistence) = &self.persistence {
            // Phase 2: Use direct persistence calls
            match persistence.load_node(node_id, part_id).await {
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
        } else if let Some(backend) = &self.backend {
            // Phase 1: Legacy backend approach - simplified for now
            let dir_handle = backend.root_directory().await?;
            let node_type = NodeType::Directory(dir_handle);
            let node = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node { 
                node_type, 
                id: node_id 
            })));
            Ok(node)
        } else {
            Err(Error::Other("No persistence layer or backend configured".to_string()))
        }
    }

    /// Create a new node with persistence
    pub async fn create_node(&self, part_id: NodeID, node_type: NodeType) -> Result<NodeRef> {
        if let Some(persistence) = &self.persistence {
            let node_id = NodeID::new_sequential();
            persistence.store_node(node_id, part_id, &node_type).await?;
            let node = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node { 
                node_type, 
                id: node_id 
            })));
            Ok(node)
        } else {
            Err(Error::Other("No persistence layer configured for node creation".to_string()))
        }
    }

    /// Update directory entry
    pub async fn update_directory(
        &self,
        parent_node_id: NodeID,
        entry_name: &str,
        operation: DirectoryOperation,
    ) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            persistence.update_directory_entry(parent_node_id, entry_name, operation).await
        } else {
            Err(Error::Other("No persistence layer configured for directory updates".to_string()))
        }
    }

    /// Load directory entries
    pub async fn load_directory_entries(&self, parent_node_id: NodeID) -> Result<HashMap<String, NodeID>> {
        if let Some(persistence) = &self.persistence {
            persistence.load_directory_entries(parent_node_id).await
        } else {
            Ok(HashMap::new()) // Empty for legacy mode
        }
    }

    /// Commit any pending operations to persistent storage
    pub async fn commit(&self) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            persistence.commit().await
        } else if let Some(backend) = &self.backend {
            backend.commit().await
        } else {
            Ok(()) // No-op if no persistence configured
        }
    }

    /// Get the backend for this filesystem (legacy compatibility)
    pub(crate) fn backend(&self) -> Option<Arc<dyn FilesystemBackend>> {
        self.backend.clone()
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

    // Legacy methods for backward compatibility - these will delegate to create_node eventually
    pub async fn add_node(&self, node_type: NodeType) -> Result<NodeRef> {
        if let Some(_persistence) = &self.persistence {
            self.create_node(crate::node::ROOT_ID, node_type).await
        } else {
            // Legacy implementation - just create a node without persistence
            let node_id = NodeID::new_sequential();
            let node = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node { 
                node_type, 
                id: node_id 
            })));
            Ok(node)
        }
    }
    
    /// Get a node by its ID
    pub async fn get_node(&self, node_id: NodeID, part_id: NodeID) -> Result<NodeRef> {
        self.get_or_create_node(node_id, part_id).await
    }
    
    /// Create a new directory node and return its NodeRef
    pub async fn create_directory(&self) -> Result<NodeRef> {
        if let Some(_persistence) = &self.persistence {
            let dir_handle = crate::memory::MemoryDirectory::new_handle();
            let node_type = NodeType::Directory(dir_handle);
            self.create_node(crate::node::ROOT_ID, node_type).await
        } else if let Some(backend) = &self.backend {
            let node_id = NodeID::new_sequential();
            let dir_handle = backend.create_directory(node_id).await?;
            let node_type = NodeType::Directory(dir_handle);
            let node = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node { 
                node_type, 
                id: node_id 
            })));
            Ok(node)
        } else {
            Err(Error::Other("No persistence layer or backend configured".to_string()))
        }
    }

    /// Create a new file node and return its NodeRef
    pub async fn create_file(&self, content: &[u8], parent_node_id: Option<&str>) -> Result<NodeRef> {
        if let Some(_persistence) = &self.persistence {
            let file_handle = crate::memory::MemoryFile::new_handle(content);
            let node_type = NodeType::File(file_handle);
            self.create_node(crate::node::ROOT_ID, node_type).await
        } else if let Some(backend) = &self.backend {
            let node_id = NodeID::new_sequential();
            let file_handle = backend.create_file(node_id, content, parent_node_id).await?;
            let node_type = NodeType::File(file_handle);
            let node = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node { 
                node_type, 
                id: node_id 
            })));
            Ok(node)
        } else {
            Err(Error::Other("No persistence layer or backend configured".to_string()))
        }
    }

    /// Create a new symlink node and return its NodeRef
    pub async fn create_symlink(&self, target: &str, parent_node_id: Option<&str>) -> Result<NodeRef> {
        if let Some(_persistence) = &self.persistence {
            let symlink_handle = crate::memory::MemorySymlink::new_handle(target.into());
            let node_type = NodeType::Symlink(symlink_handle);
            self.create_node(crate::node::ROOT_ID, node_type).await
        } else if let Some(backend) = &self.backend {
            let node_id = NodeID::new_sequential();
            let symlink_handle = backend.create_symlink(node_id, target, parent_node_id).await?;
            let node_type = NodeType::Symlink(symlink_handle);
            let node = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node { 
                node_type, 
                id: node_id 
            })));
            Ok(node)
        } else {
            Err(Error::Other("No persistence layer or backend configured".to_string()))
        }
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
