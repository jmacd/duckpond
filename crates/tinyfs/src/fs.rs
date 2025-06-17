use std::collections::{HashSet, HashMap};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::backend::FilesystemBackend;
use crate::dir::*;
use crate::error::*;
use crate::node::*;
use crate::wd::WD;

/// Main filesystem structure that owns all nodes
#[derive(Clone)]
pub struct FS {
    state: Arc<tokio::sync::Mutex<State>>,
    backend: Arc<dyn FilesystemBackend>,
}

struct State {
    // @@@ Should these two move out?
    nodes: Vec<NodeRef>,
    restored_nodes: HashMap<NodeID, NodeRef>, // Track restored nodes by their original IDs

    busy: HashSet<NodeID>,
}

impl FS {
    /// Creates a new filesystem with the specified backend
    pub async fn with_backend<B: FilesystemBackend + 'static>(backend: B) -> Result<Self> {
        let backend = Arc::new(backend);
        
        // Get the root directory from the backend
        // Each backend handles its own initialization logic (restore vs create)
        let root_dir = backend.root_directory().await?;
        
        let node_type = NodeType::Directory(root_dir);
        let nodes = vec![NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node {
            node_type,
            id: crate::node::ROOT_ID,
        })))];
        let fs = FS {
            state: Arc::new(tokio::sync::Mutex::new(State {
                nodes,
                restored_nodes: HashMap::new(),
                busy: HashSet::new(),
            })),
            backend: backend.clone(),
        };
        
        Ok(fs)
    }

    /// Returns a working directory context for the root directory
    pub async fn root(&self) -> Result<WD> {
        let root = self.state.lock().await.nodes.first().unwrap().clone();
        let node = NodePath {
            node: root,
            path: "/".into(),
        };
        self.wd(&node).await
    }
    
    pub(crate) async fn wd(&self, np: &NodePath) -> Result<WD> {
        WD::new(np.clone(), self.clone()).await
    }

    /// Adds a new node to the filesystem
    pub async fn add_node(&self, node_type: NodeType) -> NodeRef {
        let mut state = self.state.lock().await;
        let id = NodeID::new(state.nodes.len());
        let node = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node { node_type, id })));
        state.nodes.push(node.clone());
        node
    }
    
    /// Restore a node with a specific ID from persistent storage
    /// This method is used by backends to restore nodes with their original IDs
    pub async fn restore_node(&self, node_id: NodeID, node_type: NodeType) -> NodeRef {
        let mut state = self.state.lock().await;
        let node = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node { node_type, id: node_id })));
        
        // Store in the restored_nodes map
        state.restored_nodes.insert(node_id, node.clone());
        
        node
    }
    
    /// Get a node by its ID, checking both regular nodes and restored nodes
    pub async fn get_node(&self, node_id: NodeID) -> Option<NodeRef> {
        let state = self.state.lock().await;
        
        // First check regular nodes
        if node_id.as_usize() < state.nodes.len() {
            return Some(state.nodes[node_id.as_usize()].clone());
        }
        
        // Then check restored nodes
        state.restored_nodes.get(&node_id).cloned()
    }
    
    /// Create a new directory node and return its NodeRef
    pub async fn create_directory(&self) -> Result<NodeRef> {
        // Pre-assign the NodeID that will be used
        let mut state = self.state.lock().await;
        let node_id = NodeID::new(state.nodes.len());
        
        // Create the directory handle with the assigned NodeID
        let dir_handle = self.backend.create_directory(node_id).await?;
        let node_type = NodeType::Directory(dir_handle);
        
        // Create the node with the pre-assigned ID
        let node = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node { node_type, id: node_id })));
        state.nodes.push(node.clone());
        
        Ok(node)
    }

    /// Create a new file node and return its NodeRef
    pub async fn create_file(&self, content: &[u8], parent_node_id: Option<&str>) -> Result<NodeRef> {
        // Pre-assign the NodeID that will be used
        let mut state = self.state.lock().await;
        let node_id = NodeID::new(state.nodes.len());
        
        // Create the file handle with the assigned NodeID
        let file_handle = self.backend.create_file(node_id, content, parent_node_id).await?;
        let node_type = NodeType::File(file_handle);
        
        // Create the node with the pre-assigned ID
        let node = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node { node_type, id: node_id })));
        state.nodes.push(node.clone());
        
        Ok(node)
    }

    /// Create a new symlink node and return its NodeRef
    pub async fn create_symlink(&self, target: &str, parent_node_id: Option<&str>) -> Result<NodeRef> {
        // Pre-assign the NodeID that will be used
        let mut state = self.state.lock().await;
        let node_id = NodeID::new(state.nodes.len());
        
        // Create the symlink handle with the assigned NodeID
        let symlink_handle = self.backend.create_symlink(node_id, target, parent_node_id).await?;
        let node_type = NodeType::Symlink(symlink_handle);
        
        // Create the node with the pre-assigned ID
        let node = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node { node_type, id: node_id })));
        state.nodes.push(node.clone());
        
        Ok(node)
    }

    /// Commit any pending operations to persistent storage
    /// Returns the number of operations committed
    pub async fn commit(&self) -> Result<()> {
        self.backend.commit().await
    }

    /// Get the backend for this filesystem
    pub(crate) fn backend(&self) -> Arc<dyn FilesystemBackend> {
        self.backend.clone()
    }
    
    /// Get a working directory context from a NodePath
    pub async fn working_dir_from_node(&self, node_path: &NodePath) -> Result<WD> {
        self.wd(node_path).await
    }

    pub(crate) async fn enter_node(&self, node: &NodePath) -> Result<()> {
        let mut state = self.state.lock().await;
        let id = node.id().await;
        if state.busy.get(&id).is_some() {
            return Err(Error::visit_loop(node.path()));
        }
        state.busy.insert(id);
        Ok(())
    }

    pub(crate) async fn exit_node(&self, node: &NodePath) {
        let mut state = self.state.lock().await;
        state.busy.remove(&node.id().await);
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
