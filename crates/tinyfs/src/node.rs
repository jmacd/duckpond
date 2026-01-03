// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::EntryType;
use crate::dir::Pathed;
use crate::error::Error;
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::path::PathBuf;
use uuid7::Uuid;

/// Root directory gets a special deterministic UUID7
/// Only version (7) and variant (2) fields are set per UUID7 format
/// The nibble following version (7) is 1 for EntryType::DirectoryPhysical.
pub const ROOT_UUID: &str = "00000000-0000-7100-8000-000000000000";

#[must_use]
pub fn root_uuid() -> Uuid {
    ROOT_UUID
        .parse::<Uuid>()
        .expect("ROOT_UUID should be a valid UUID7")
}

/// Unique identifier for a node in the filesystem
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeID(Uuid);

/// Same as NodeID but for partitions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartID(NodeID);

impl NodeID {
    /// Create NodeID from existing UUID7 string
    #[must_use]
    pub fn new(uuid_str: String) -> Self {
        let uuid = uuid_str.parse::<Uuid>().expect("Invalid UUID7 string");
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

    /// Parse from UUID7 string
    pub fn from_hex_string(uuid_str: &str) -> std::result::Result<Self, String> {
        // Validate it's a proper UUID
        let uuid = uuid_str
            .parse::<Uuid>()
            .map_err(|e| format!("Failed to parse UUID string '{}': {}", uuid_str, e))?;
        Ok(NodeID(uuid))
    }

    /// Parse from full UUID7 string
    pub fn from_string(s: &str) -> std::result::Result<Self, String> {
        Self::from_hex_string(s)
    }

    #[must_use]
    pub fn is_root(&self) -> bool {
        self.0 == root_uuid()
    }

    /// Generate a new UUID7-based NodeID
    #[must_use]
    fn generate(et: EntryType) -> Self {
        let mut b: [u8; 16] = uuid7::uuid7().into();
        let val = 0x70 | et as u8;
        b[6] = val;
        Self(Uuid::from(b))
    }

    fn entry_type(&self) -> EntryType {
        let b: [u8; 16] = self.0.into();
        EntryType::try_from(b[6] & 0xf).expect("coded")
    }
}

impl PartID {
    /// Create PartID from UUID string (for path parsing)
    #[must_use]
    pub fn new(uuid_str: String) -> Self {
        Self(NodeID::new(uuid_str))
    }

    /// Wrap a NodeID as a PartID (for when we know it represents a partition)
    #[must_use]
    pub fn from_node_id(node_id: NodeID) -> Self {
        Self(node_id)
    }

    /// Get the root PartID (same as root NodeID)
    #[must_use]
    pub fn root() -> Self {
        Self(NodeID(root_uuid()))
    }

    /// Get the wrapped NodeID
    #[must_use]
    pub fn to_node_id(&self) -> NodeID {
        self.0
    }

    /// Parse from UUID7 hex string
    pub fn from_hex_string(uuid_str: &str) -> std::result::Result<Self, String> {
        NodeID::from_hex_string(uuid_str).map(Self)
    }
}

/// A FileID combines NodeID and PartID with embedded EntryType information.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileID {
    /// The unique identifier for this node
    node_id: NodeID,
    /// The partition identifier (containing directory's partition)
    part_id: PartID,
}

impl FileID {
    #[must_use]
    pub fn new_from_ids(part_id: PartID, node_id: NodeID) -> Self {
        Self { part_id, node_id }
    }

    #[must_use]
    pub fn is_root(&self) -> bool {
        *self == Self::root()
    }

    #[must_use]
    pub fn node_id(&self) -> NodeID {
        self.node_id
    }

    #[must_use]
    pub fn part_id(&self) -> PartID {
        self.part_id
    }

    /// Root directory has a special UUID7
    #[must_use]
    pub fn root() -> Self {
        let uuid = root_uuid();
        Self {
            node_id: NodeID(uuid),
            part_id: PartID(NodeID(uuid)),
        }
    }

    #[must_use]
    pub fn new_physical_dir_id() -> Self {
        let node_id = NodeID::generate(EntryType::DirectoryPhysical);
        let part_id = PartID(node_id);
        Self { node_id, part_id }
    }

    /// Create FileID for a physical directory using an existing NodeID
    /// Physical directories are self-partitioned (part_id == node_id)
    #[must_use]
    pub fn from_physical_dir_node_id(node_id: NodeID) -> Self {
        Self {
            part_id: PartID(node_id),
            node_id,
        }
    }

    /// Create FileID in a specific partition with a new node ID
    /// Used when creating nodes that should live in their parent's partition
    #[must_use]
    pub fn new_in_partition(parent_part_id: PartID, entry_type: EntryType) -> Self {
        let node_id = NodeID::generate(entry_type);
        Self {
            part_id: parent_part_id,
            node_id,
        }
    }

    /// Create FileID with deterministic NodeID from content
    /// Used by dynamic factories to create stable IDs for generated nodes
    /// For dynamic directories creating children:
    ///   - Use parent directory's NodeID as the PartID
    ///
    /// For root-level dynamic nodes:
    ///   - Use PartID::root()
    #[must_use]
    pub fn from_content(parent_part_id: PartID, entry_type: EntryType, content: &[u8]) -> Self {
        // Create BLAKE3 hash of content
        let hash = blake3::hash(content);
        let hash_bytes = hash.as_bytes();

        // Use a fixed timestamp for deterministic IDs
        let timestamp = 1u64;

        // Extract bits for the random part
        let bits = u128::from_be_bytes([
            hash_bytes[0],
            hash_bytes[1],
            hash_bytes[2],
            hash_bytes[3],
            hash_bytes[4],
            hash_bytes[5],
            hash_bytes[6],
            hash_bytes[7],
            hash_bytes[8],
            hash_bytes[9],
            hash_bytes[10],
            hash_bytes[11],
            hash_bytes[12],
            hash_bytes[13],
            hash_bytes[14],
            hash_bytes[15],
        ]);
        let rand_a = ((bits >> 66) & 0xFFF) as u16; // 12 bits
        let rand_b = ((bits >> 4) & 0x3FFF_FFFF_FFFF_FFFF) as u64; // 62 bits

        // Create UUID7 with our timestamp and random bits
        let uuid = Uuid::from_fields_v7(timestamp, rand_a, rand_b);

        // Set the EntryType in byte 6's lower nibble
        let mut bytes: [u8; 16] = uuid.into();
        bytes[6] = 0x70 | (entry_type as u8);
        let node_id = NodeID(Uuid::from(bytes));

        Self {
            part_id: parent_part_id,
            node_id,
        }
    }

    #[must_use]
    pub fn new_child_id(&self, et: EntryType) -> Self {
        if et == EntryType::DirectoryPhysical {
            Self::new_physical_dir_id()
        } else {
            Self {
                part_id: self.part_id,
                node_id: NodeID::generate(et),
            }
        }
    }

    #[must_use]
    pub fn entry_type(&self) -> EntryType {
        self.node_id.entry_type()
    }

    #[must_use]
    pub fn child_id(&self, child: NodeID) -> Self {
        if child.entry_type() == EntryType::DirectoryPhysical {
            Self {
                node_id: child,
                part_id: PartID(child),
            }
        } else {
            Self {
                node_id: child,
                part_id: self.part_id,
            }
        }
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
    pub id: FileID,
    pub node_type: NodeType,
}

/// Contains a node reference and the path used to reach it
#[derive(Clone)]
pub struct NodePath {
    pub node: Node,
    pub path: PathBuf,
}

pub type DirNode = Pathed<crate::dir::Handle>;
pub type FileNode = Pathed<crate::file::Handle>;
pub type SymlinkNode = Pathed<crate::symlink::Handle>;

impl Node {
    #[must_use]
    pub fn new(id: FileID, node_type: NodeType) -> Self {
        Self { id, node_type }
    }

    /// Get the FileID for this node
    #[must_use]
    pub fn id(&self) -> FileID {
        self.id
    }

    /// Get the FileID for this node
    #[must_use]
    pub fn entry_type(&self) -> EntryType {
        self.id.entry_type()
    }

    /// Get the NodeType
    #[must_use]
    pub fn node_type(&self) -> NodeType {
        self.node_type.clone()
    }
}

impl NodePath {
    #[must_use]
    pub fn new(node: Node, path: PathBuf) -> Self {
        Self { node, path }
    }

    #[must_use]
    pub fn is_root(&self) -> bool {
        self.id() == FileID::root()
    }

    #[must_use]
    pub fn id(&self) -> FileID {
        self.node.id()
    }

    #[must_use]
    pub fn entry_type(&self) -> EntryType {
        self.node.id().entry_type()
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

    pub async fn into_dir(&self) -> Option<DirNode> {
        let node_type = &self.node.node_type;
        if let NodeType::Directory(d) = node_type {
            Some(Pathed::new(self.path.clone(), d.clone()))
        } else {
            None
        }
    }

    pub async fn as_dir(&self) -> Result<DirNode> {
        self.into_dir()
            .await
            .ok_or_else(|| Error::not_a_directory(self.path.clone()))
    }

    pub async fn into_file(&self) -> Option<FileNode> {
        if let NodeType::File(d) = &self.node.node_type {
            Some(Pathed::new(self.path.clone(), d.clone()))
        } else {
            None
        }
    }

    pub async fn as_file(&self) -> Result<FileNode> {
        self.into_file()
            .await
            .ok_or_else(|| Error::not_a_file(self.path.clone()))
    }

    pub async fn into_symlink(&self) -> Option<SymlinkNode> {
        if let NodeType::Symlink(d) = &self.node.node_type {
            Some(Pathed::new(self.path.clone(), d.clone()))
        } else {
            None
        }
    }

    pub async fn as_symlink(&self) -> Result<SymlinkNode> {
        self.into_symlink()
            .await
            .ok_or_else(|| Error::not_a_symlink(self.path.clone()))
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

impl std::fmt::Display for FileID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<{}/[{}]>", self.part_id.0.0, self.node_id.0)
    }
}

impl std::fmt::Display for NodeID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Display for PartID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
