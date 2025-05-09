use std::rc::Rc;
use std::cell::RefCell;
use std::path::PathBuf;
use std::path::Path;
use std::ops::Deref;

use crate::dir::Pathed;
use crate::error::Error;
use crate::error::Result;

/// Unique identifier for a node in the filesystem
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeID(pub usize);

/// Type of node (file, directory, or symlink)
#[derive(Clone)]
pub enum NodeType {
    File(crate::file::Handle),
    Directory(crate::dir::Handle),
    Symlink(crate::symlink::Handle),
}

/// Common interface for both files and directories
#[derive(Clone, Debug)]
pub struct Node {
    pub id: NodeID,
    pub node_type: NodeType,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NodeRef(pub(crate) Rc<RefCell<Node>>);

/// Contains a node reference and the path used to reach it
#[derive(Clone, PartialEq)]
pub struct NodePath {
    pub node: NodeRef,
    pub path: PathBuf,
}

pub struct NodePathRef<'a> {
    pub(crate) node: std::cell::Ref<'a, Node>,
    pub(crate) path: &'a PathBuf,
}

pub type DirNode = Pathed<crate::dir::Handle>;
pub type FileNode = Pathed<crate::file::Handle>;
pub type SymlinkNode = Pathed<crate::symlink::Handle>;

impl Deref for NodeRef {
    type Target = Rc<RefCell<Node>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl NodePath {
    pub fn id(&self) -> NodeID {
	return self.node.borrow().id;
    }

    pub fn basename(&self) -> String {
        crate::path_utils::basename(&self.path).unwrap_or_default()
    }

    pub fn dirname(&self) -> PathBuf {
        crate::path_utils::dirname(&self.path).unwrap_or_default()
    }
    
    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn join<P: AsRef<Path>>(&self, p: P) -> PathBuf {
        self.path.clone().join(p)
    }

    pub fn read_file(&self) -> Result<Vec<u8>> {
	self.borrow().read_file()
    }

    pub fn borrow(&self) -> NodePathRef {
	NodePathRef{
	    node: self.node.as_ref().borrow(),
	    path: &self.path,
	}
    }
}

impl NodeID {
    pub fn is_root(self) -> bool {
        self == crate::fs::ROOT_ID
    }
}

impl NodePathRef<'_> {
    pub fn as_file(&self) -> Result<FileNode> {
        if let NodeType::File(f) = &self.node.node_type {
            Ok(Pathed::new(self.path, f.clone()))
        } else {
            Err(Error::not_a_file(self.path))
        }
    }

    pub fn as_symlink(&self) -> Result<SymlinkNode> {
        if let NodeType::Symlink(s) = &self.node.node_type {
            Ok(Pathed::new(self.path, s.clone()))
        } else {
            Err(Error::not_a_symlink(self.path))
        }
    }

    pub fn as_dir(&self) -> Result<DirNode> {
        if let NodeType::Directory(d) = &self.node.node_type {
            Ok(Pathed::new(self.path, d.clone()))
        } else {
            Err(Error::not_a_directory(self.path))
        }
    }

    pub fn read_file(&self) -> Result<Vec<u8>> {
	self.as_file()?.read_file()
    }
}

impl PartialEq<Node> for Node {
    fn eq(&self, other: &Node) -> bool {
        self.id == other.id
    }
}

impl std::fmt::Debug for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeType::File(_) => write!(f, "(file)"),
            NodeType::Directory(_) => write!(f, "(directory)"),
            NodeType::Symlink(_) => write!(f, "(symlink)"),
        }
    }
}
