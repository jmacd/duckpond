use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncWrite;

use crate::EntryType;
use crate::error::*;
use crate::metadata::Metadata;
use crate::node::*;
use async_trait::async_trait;
use futures::stream::Stream;

/// Lightweight directory entry information without loading the full node.
/// Contains just enough information to filter entries and determine partition assignment.
#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct DirectoryEntry {
    /// Entry name within the directory
    pub name: String,
    /// NodeID of the child (combined with directory's part_id to form FileID)
    pub child_node_id: NodeID,
    /// Comprehensive entry type, redundant.
    pub entry_type: EntryType,
    /// Version number when this entry was last modified
    pub version_last_modified: i64,
}

impl DirectoryEntry {
    /// Create a new directory entry
    #[must_use]
    pub fn new(
        name: String,
        child_node_id: NodeID,
        entry_type: EntryType,
        version_last_modified: i64,
    ) -> Self {
        Self {
            name,
            child_node_id,
            entry_type,
            version_last_modified,
        }
    }
}

/// Represents a directory containing named entries.
#[async_trait]
pub trait Directory: Metadata + Send + Sync {
    async fn get(&self, name: &str) -> Result<Option<Node>>;

    async fn insert(&mut self, name: String, id: Node) -> Result<()>;

    /// Returns a stream of directory entries (lightweight metadata) without loading full nodes.
    /// Callers should use batch loading to load multiple nodes efficiently.
    /// Returns DirectoryEntry which contains the name, so no tuple needed.
    async fn entries(&self) -> Result<Pin<Box<dyn Stream<Item = Result<DirectoryEntry>> + Send>>>;
}

/// A handle for a refcounted directory.
#[derive(Clone)]
pub struct Handle(Arc<tokio::sync::Mutex<Box<dyn Directory>>>);

/// Represents a Dir/File/Symlink handle with the active path.
#[derive(Clone)]
pub struct Pathed<T> {
    pub handle: T,
    path: PathBuf,
}

impl Handle {
    pub fn new(r: Arc<tokio::sync::Mutex<Box<dyn Directory>>>) -> Self {
        Self(r)
    }

    pub async fn get(&self, name: &str) -> Result<Option<Node>> {
        let dir = self.0.lock().await;
        dir.get(name).await
    }

    pub async fn insert(&self, name: String, id: Node) -> Result<()> {
        log::debug!(
            "Handle::insert() - forwarding to Directory trait: {name}",
            name = name
        );
        let mut dir = self.0.lock().await;
        let type_name = std::any::type_name::<dyn Directory>();
        log::debug!(
            "Handle::insert() - calling Directory::insert() on: {type_name}",
            type_name = type_name
        );
        let result = dir.insert(name, id).await;
        let result_str = result
            .as_ref()
            .map(|_| "Ok")
            .map_err(|e| format!("Err({})", e));
        let result_display = format!("{:?}", result_str);
        log::debug!(
            "Handle::insert() - Directory::insert() completed with result: {result_display}",
            result_display = result_display
        );
        result
    }

    pub async fn entries(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<DirectoryEntry>> + Send>>> {
        let dir = self.0.lock().await;
        dir.entries().await
    }

    /// Get metadata through the directory handle
    pub async fn metadata(&self) -> Result<crate::NodeMetadata> {
        let dir = self.0.lock().await;
        dir.metadata().await
    }
}

impl<T> Pathed<T> {
    pub fn new<P: AsRef<Path>>(path: P, handle: T) -> Self {
        Self {
            handle,
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn path(&self) -> PathBuf {
        self.path.to_path_buf()
    }
}

impl Pathed<crate::file::Handle> {
    /// Get async reader with seek support for file content
    pub async fn async_reader(&self) -> Result<Pin<Box<dyn crate::file::AsyncReadSeek>>> {
        self.handle.async_reader().await
    }

    /// Get async writer for streaming file content
    pub async fn async_writer(&self) -> Result<Pin<Box<dyn AsyncWrite + Send>>> {
        self.handle.async_writer().await
    }

    /// Get metadata through the file handle
    pub async fn metadata(&self) -> Result<crate::NodeMetadata> {
        self.handle.metadata().await
    }
}

impl Pathed<Handle> {
    pub async fn get(&self, name: &str) -> Result<Option<NodePath>> {
        if let Some(nr) = self.handle.get(name).await? {
            Ok(Some(NodePath {
                node: nr,
                path: self.path.join(name),
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn insert(&self, name: String, id: Node) -> Result<()> {
        self.handle.insert(name, id).await
    }
}

impl Pathed<crate::symlink::Handle> {
    pub async fn readlink(&self) -> Result<PathBuf> {
        self.handle.readlink().await
    }
}
