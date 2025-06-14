use std::collections::HashSet;
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
    nodes: Vec<NodeRef>,
    busy: HashSet<NodeID>,
}

impl FS {
    /// Creates a new filesystem with the specified backend
    pub async fn with_backend<B: FilesystemBackend + 'static>(backend: B) -> Result<Self> {
        let backend = Arc::new(backend);
        let root_dir = backend.create_directory().await?;
        let node_type = NodeType::Directory(root_dir);
        let nodes = vec![NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node {
            node_type,
            id: crate::node::ROOT_ID,
        })))];
        Ok(FS {
            state: Arc::new(tokio::sync::Mutex::new(State {
                nodes,
                busy: HashSet::new(),
            })),
            backend,
        })
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
    
    /// Returns a working directory context for the root directory (alias for root)
    pub async fn working_dir(&self) -> Result<WD> {
        self.root().await
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
    
    /// Create a new directory node and return its NodeRef
    pub async fn create_directory(&self) -> Result<NodeRef> {
        let dir_handle = self.backend.create_directory().await?;
        let node_type = NodeType::Directory(dir_handle);
        Ok(self.add_node(node_type).await)
    }

    /// Commit any pending operations to persistent storage
    /// Returns the number of operations committed
    pub async fn commit(&self) -> Result<usize> {
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
