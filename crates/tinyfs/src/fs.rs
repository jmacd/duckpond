use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use crate::dir::*;
use crate::error::*;
use crate::node::*;
use crate::wd::WD;

/// Main filesystem structure that owns all nodes
#[derive(Clone)]
pub struct FS {
    state: Rc<RefCell<State>>,
}

struct State {
    nodes: Vec<NodeRef>,
    busy: HashSet<NodeID>,
}

impl FS {
    /// Creates a new filesystem with an empty memory-backed root directory
    /// This is primarily for testing
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Creates a new filesystem with a custom root directory implementation
    /// This allows injecting Delta Lake-backed or other storage implementations
    pub fn with_root_directory(root_dir: crate::dir::Handle) -> Self {
        let node_type = NodeType::Directory(root_dir);
        let nodes = vec![NodeRef::new(Rc::new(RefCell::new(Node {
            node_type,
            id: crate::node::ROOT_ID,
        })))];
        FS {
            state: Rc::new(RefCell::new(State {
                nodes,
                busy: HashSet::new(),
            })),
        }
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
    pub fn create_directory(&self) -> NodeRef {
        let dir_handle = MemoryDirectory::new_handle();
        let node_type = NodeType::Directory(dir_handle);
        self.add_node(node_type)
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

impl Default for FS {
    /// Creates a new filesystem with an empty root directory
    fn default() -> Self {
        let root = MemoryDirectory::new_handle();
        let node_type = NodeType::Directory(root);
        let nodes = vec![NodeRef::new(Rc::new(RefCell::new(Node {
            node_type,
            id: crate::fs::ROOT_ID,
        })))];
        FS {
            state: Rc::new(RefCell::new(State {
                nodes,
                busy: HashSet::new(),
            })),
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
