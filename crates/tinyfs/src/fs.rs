use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use crate::backend::FilesystemBackend;
use crate::dir::*;
use crate::error::*;
use crate::node::*;
use crate::wd::WD;

/// Main filesystem structure that owns all nodes
#[derive(Clone)]
pub struct FS {
    state: Rc<RefCell<State>>,
    backend: Rc<dyn FilesystemBackend>,
}

struct State {
    nodes: Vec<NodeRef>,
    busy: HashSet<NodeID>,
}

impl FS {
    /// Creates a new filesystem with the specified backend
    pub fn with_backend<B: FilesystemBackend + 'static>(backend: B) -> Result<Self> {
        let backend = Rc::new(backend);
        let root_dir = backend.create_directory()?;
        let node_type = NodeType::Directory(root_dir);
        let nodes = vec![NodeRef::new(Rc::new(RefCell::new(Node {
            node_type,
            id: crate::node::ROOT_ID,
        })))];
        Ok(FS {
            state: Rc::new(RefCell::new(State {
                nodes,
                busy: HashSet::new(),
            })),
            backend,
        })
    }

    /// Returns a working directory context for the root directory
    pub fn root(&self) -> WD {
        let root = self.state.borrow().nodes.first().unwrap().clone();
        let node = NodePath {
            node: root,
            path: "/".into(),
        };
        self.wd(&node).unwrap()
    }
    
    /// Returns a working directory context for the root directory (alias for root)
    pub fn working_dir(&self) -> WD {
        self.root()
    }

    pub(crate) fn wd(&self, np: &NodePath) -> Result<WD> {
        WD::new(np.clone(), self.clone())
    }

    /// Adds a new node to the filesystem
    pub fn add_node(&self, node_type: NodeType) -> NodeRef {
        let mut state = self.state.borrow_mut();
        let id = NodeID::new(state.nodes.len());
        let node = NodeRef::new(Rc::new(RefCell::new(Node { node_type, id })));
        state.nodes.push(node.clone());
        node
    }
    
    /// Create a new directory node and return its NodeRef
    pub fn create_directory(&self) -> Result<NodeRef> {
        let dir_handle = self.backend.create_directory()?;
        let node_type = NodeType::Directory(dir_handle);
        Ok(self.add_node(node_type))
    }

    /// Commit any pending operations to persistent storage
    /// Returns the number of operations committed
    pub fn commit(&self) -> Result<usize> {
        self.backend.commit()
    }

    /// Get the backend for this filesystem
    pub(crate) fn backend(&self) -> Rc<dyn FilesystemBackend> {
        self.backend.clone()
    }
    
    /// Get a working directory context from a NodePath
    pub fn working_dir_from_node(&self, node_path: &NodePath) -> Result<WD> {
        self.wd(node_path)
    }

    pub(crate) fn enter_node(&self, node: &NodePath) -> Result<()> {
        let mut state = self.state.borrow_mut();
        let id = node.id();
        if state.busy.get(&id).is_some() {
            return Err(Error::visit_loop(node.path()));
        }
        state.busy.insert(id);
        Ok(())
    }

    pub(crate) fn exit_node(&self, node: &NodePath) {
        let mut state = self.state.borrow_mut();
        state.busy.remove(&node.id());
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
