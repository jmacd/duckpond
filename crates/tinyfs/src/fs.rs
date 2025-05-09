use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashSet;

use crate::dir::*;
use crate::error::*;
use crate::node::*;
use crate::wd::WD;

pub const ROOT_ID: NodeID = NodeID(0);

/// Main filesystem structure that owns all nodes
#[derive(Clone)]
pub struct FS {
    // Note this is almost unused; we access the root.
    state: Rc<RefCell<State>>,
}

struct State {
    nodes: Vec<NodeRef>,
    busy: HashSet<NodeID>,
}

impl FS {
    /// Creates a new filesystem with an empty root directory
    pub fn new() -> Self {
        Self::default()
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

    pub(crate) fn wd(&self, np: &NodePath) -> Result<WD> {
        Ok(WD {
	    np: np.clone(),
            dref: np.borrow().as_dir()?,
            fs: self.clone(),
        })
    }

    /// Adds a new node to the filesystem
    pub(crate) fn add_node(&self, node_type: NodeType) -> NodeRef {
        let mut state = self.state.borrow_mut();
        let id = NodeID(state.nodes.len());
        let node = NodeRef(Rc::new(RefCell::new(Node { node_type, id })));
        state.nodes.push(node.clone());
        node
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
        let nodes = vec![NodeRef(Rc::new(RefCell::new(Node {
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

