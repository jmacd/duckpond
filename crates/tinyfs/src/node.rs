use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use crate::EntryType;
use crate::dir::Pathed;
use crate::error::Error;
use crate::error::Result;

/// Root directory gets a special deterministic UUID7
/// Uses all zeros for timestamp, counter, and random fields
/// Only version (7) and variant (2) fields are set per UUID7 format
pub const ROOT_UUID: &str = "00000000-0000-7000-8000-000000000000";

/// Unique identifier for a node in the filesystem
/// Now uses UUID7 for global uniqueness and time ordering
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct NodeID(uuid7::Uuid);

/// Uses the 8-8-12-8 form UUID7 string for storage/filenames. This
/// supplies to_string().
impl std::fmt::Display for NodeID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl NodeID {
    /// Create NodeID from existing UUID7 string
    #[must_use]
    pub fn new(uuid_str: String) -> Self {
        let uuid = uuid_str
            .parse::<uuid7::Uuid>()
            .expect("Invalid UUID7 string");
        Self(uuid)
    }

    /// Generate a new UUID7-based NodeID
    #[must_use]
    pub fn generate() -> Self {
        Self(uuid7::uuid7())
    }

    /// Generate a deterministic NodeID from content (for stable dynamic objects)
    /// Uses SHA-256 hash of content as the random part of UUID7
    #[must_use]
    pub fn from_content(content: &[u8]) -> Self {
        use sha2::{Digest, Sha256};
        // Create SHA-256 hash of content
        let mut hasher = Sha256::new();
        hasher.update(content);
        let hash = hasher.finalize();
        // Use a fixed, valid timestamp for deterministic NodeIDs
        // @@@ WHOA
        let timestamp = 1u64;
        // Extract 74 bits for the random part (12 bits for rand_a, 62 bits for rand_b)
        // SHA-256 gives us 32 bytes = 256 bits, plenty for this
        let bits = u128::from_be_bytes([
            hash[0], hash[1], hash[2], hash[3], hash[4], hash[5], hash[6], hash[7], hash[8],
            hash[9], hash[10], hash[11], hash[12], hash[13], hash[14], hash[15],
        ]);
        // Top 12 bits for rand_a, next 62 bits for rand_b
        let rand_a = ((bits >> 66) & 0xFFF) as u16; // 12 bits
        let rand_b = ((bits >> 4) & 0x3FFF_FFFF_FFFF_FFFF) as u64; // 62 bits
        let uuid = uuid7::Uuid::from_fields_v7(timestamp, rand_a, rand_b);
        Self(uuid)
    }

    /// Get shortened display version (8 chars like git)
    /// TODO use the data-privacy feature
    #[must_use]
    pub fn to_short_string(&self) -> String {
        let full_str = self.0.to_string();
        // Remove hyphens and take last 8 hex characters (random part)
        // This avoids the timestamp prefix that makes UUIDs look similar
        let hex_only: String = full_str.chars().filter(|c| c.is_ascii_hexdigit()).collect();
        let len = hex_only.len();
        if len >= 8 {
            hex_only[len - 8..].to_string()
        } else {
            hex_only
        }
    }

    /// Root directory gets a special UUID7 (deterministic)
    #[must_use]
    pub fn root() -> Self {
        let uuid = ROOT_UUID
            .parse::<uuid7::Uuid>()
            .expect("ROOT_UUID should be a valid UUID7");
        Self(uuid)
    }

    /// Parse from UUID7 string
    pub fn from_hex_string(uuid_str: &str) -> std::result::Result<Self, String> {
        // Validate it's a proper UUID
        let uuid = uuid_str
            .parse::<uuid7::Uuid>()
            .map_err(|e| format!("Failed to parse UUID string '{}': {}", uuid_str, e))?;
        Ok(NodeID(uuid))
    }

    /// Parse from full UUID7 string
    pub fn from_string(s: &str) -> std::result::Result<Self, String> {
        Self::from_hex_string(s)
    }

    #[must_use]
    pub fn is_root(&self) -> bool {
        let root_uuid = ROOT_UUID
            .parse::<uuid7::Uuid>()
            .expect("ROOT_UUID should be a valid UUID7");
        self.0 == root_uuid
    }
}

/// Type of node (file, directory, or symlink)
#[derive(Clone)]
pub enum NodeType {
    File(crate::file::Handle),
    Directory(crate::dir::Handle),
    Symlink(crate::symlink::Handle),
}

impl NodeType {
    pub async fn entry_type(&self) -> Result<EntryType> {
        Ok(match self {
            NodeType::File(f) => f.metadata().await?.entry_type,
            NodeType::Directory(d) => d.metadata().await?.entry_type,
            NodeType::Symlink(s) => s.metadata().await?.entry_type,
        })
    }
}

/// Common interface for both files and directories
#[derive(Clone)]
pub struct Node {
    pub id: NodeID,
    pub node_type: NodeType,
}

#[derive(Clone)]
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
#[derive(Clone, PartialEq)]
pub struct NodePath {
    pub node: NodeRef,
    pub path: PathBuf,
}

pub struct NodePathRef<'a> {
    node: Node, // We'll need to clone the node since we can't hold async locks
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
    #[must_use]
    pub async fn id(&self) -> NodeID {
        self.node.lock().await.id
    }

    #[must_use]
    pub fn basename(&self) -> String {
        crate::path::basename(&self.path).unwrap_or_default()
    }

    #[must_use]
    pub fn dirname(&self) -> PathBuf {
        crate::path::dirname(&self.path).unwrap_or_default()
    }

    #[must_use]
    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    #[must_use]
    pub fn join<P: AsRef<Path>>(&self, p: P) -> PathBuf {
        self.path.clone().join(p)
    }

    #[must_use]
    pub async fn borrow(&self) -> NodePathRef<'_> {
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

    pub fn is_root(&self) -> bool {
        self.id() == NodeID::root()
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
