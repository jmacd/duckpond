use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::dir::Pathed;
use crate::error::Error;
use crate::error::Result;

pub const ROOT_ID: NodeID = NodeID(0);

/// Unique identifier for a node in the filesystem
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeID(usize);

impl std::fmt::Display for NodeID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl NodeID {
    pub fn new(id: usize) -> Self {
        Self(id)
    }
    
    /// Generate a new sequential NodeID - temporary implementation
    /// TODO: This should be managed by the persistence layer for proper uniqueness
    pub fn new_sequential() -> Self {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(1); // Start from 1, 0 is reserved for root
        Self(COUNTER.fetch_add(1, Ordering::SeqCst))
    }
    
    pub fn as_usize(&self) -> usize {
        self.0
    }
    
    /// Format as hex string for use in OpLog and storage
    /// Uses friendly format for consistency (4, 8, 12, or 16 hex digits)
    pub fn to_hex_string(&self) -> String {
        let id_value = self.0 as u64;
        
        if id_value <= 0xFFFF {
            // 0-65535: show as exactly 4 hex digits
            format!("{:04X}", id_value)
        } else if id_value <= 0xFFFFFFFF {
            // 65536-4294967295: show as exactly 8 hex digits
            format!("{:08X}", id_value)
        } else if id_value <= 0xFFFFFFFFFFFF {
            // Show as exactly 12 hex digits
            format!("{:012X}", id_value)
        } else {
            // Show as exactly 16 hex digits
            format!("{:016X}", id_value)
        }
    }
    
    /// Format as a friendly display string
    /// Shows exactly 4, 8, 12, or 16 hex digits based on the magnitude of the ID
    pub fn to_display_string(&self) -> String {
        // Use the same logic as to_hex_string for consistency
        self.to_hex_string()
    }
    
    /// Parse from hex string (reverse of to_hex_string)
    pub fn from_hex_string(hex: &str) -> std::result::Result<Self, String> {
        let id = u64::from_str_radix(hex, 16)
            .map_err(|e| format!("Failed to parse hex string '{}': {}", hex, e))?;
        Ok(NodeID(id as usize))
    }
    
    pub fn is_root(self) -> bool {
        self == ROOT_ID
    }
}

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

#[derive(Clone, Debug)]
pub struct NodeRef(Arc<tokio::sync::Mutex<Node>>);

impl PartialEq for NodeRef {
    fn eq(&self, other: &Self) -> bool {
        // Compare node IDs since Arc<Mutex<Node>> doesn't implement PartialEq
        // This is a bit tricky with async mutexes, but we can use try_lock for comparison
        match (self.0.try_lock(), other.0.try_lock()) {
            (Ok(self_guard), Ok(other_guard)) => self_guard.id == other_guard.id,
            _ => false, // If we can't lock both, assume they're different
        }
    }
}

/// Contains a node reference and the path used to reach it
#[derive(Clone, PartialEq, Debug)]
pub struct NodePath {
    pub node: NodeRef,
    pub path: PathBuf,
}

pub struct NodePathRef<'a> {
    node: Node,  // We'll need to clone the node since we can't hold async locks
    path: &'a PathBuf,
}

pub type DirNode = Pathed<crate::dir::Handle>;
pub type FileNode = Pathed<crate::file::Handle>;
pub type SymlinkNode = Pathed<crate::symlink::Handle>;

impl NodeRef {
    pub fn new(r: Arc<tokio::sync::Mutex<Node>>) -> Self {
        Self(r)
    }
    
    /// Get the NodeID for this node
    pub async fn id(&self) -> NodeID {
        self.0.lock().await.id
    }
}

impl Deref for NodeRef {
    type Target = Arc<tokio::sync::Mutex<Node>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl NodePath {
    pub async fn id(&self) -> NodeID {
        self.node.lock().await.id
    }

    pub fn basename(&self) -> String {
        crate::path::basename(&self.path).unwrap_or_default()
    }

    pub fn dirname(&self) -> PathBuf {
        crate::path::dirname(&self.path).unwrap_or_default()
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn join<P: AsRef<Path>>(&self, p: P) -> PathBuf {
        self.path.clone().join(p)
    }

    pub async fn read_file(&self) -> Result<Vec<u8>> {
        self.borrow().await.read_file().await
    }

    pub async fn borrow(&self) -> NodePathRef {
        NodePathRef {
            node: self.node.lock().await.clone(),
            path: &self.path,
        }
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

    pub async fn read_file(&self) -> Result<Vec<u8>> {
        self.as_file()?.read_file().await
    }

    pub fn is_root(&self) -> bool {
        self.id() == crate::node::ROOT_ID
    }

    pub fn node_type(&self) -> NodeType {
        self.node.node_type.clone()
    }

    pub fn id(&self) -> NodeID {
        self.node.id
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
