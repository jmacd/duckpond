// Clean architecture Symlink implementation for TinyFS
use crate::persistence::State;
use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;
use tinyfs::{Metadata, FileID, NodeMetadata, Symlink, persistence::PersistenceLayer};

/// Clean architecture symlink implementation
/// Stores target path locally until persisted, then loads from persistence layer
pub struct OpLogSymlink {
    /// Unique node identifier for this symlink
    id: FileID,

    /// Symlink target path (stored locally for newly created symlinks)
    /// This is needed because store_node() calls readlink() before the symlink is persisted
    target: Option<PathBuf>,

    /// Reference to persistence layer (single source of truth for persisted symlinks)
    state: State,
}

impl OpLogSymlink {
    /// Create new symlink instance with target path
    /// Target is stored locally until persisted, then loaded from persistence layer
    #[must_use]
    pub fn new(id: FileID, target: PathBuf, state: State) -> Self {
        Self {
            id,
            target: Some(target),
            state,
        }
    }

    /// Create symlink instance for loading from persistence (no local target)
    #[must_use]
    pub fn new_from_persistence(id: FileID, state: State) -> Self {
        Self {
            id,
            target: None,
            state,
        }
    }

    /// Create a symlink handle for TinyFS integration
    #[must_use]
    pub fn create_handle(oplog_symlink: OpLogSymlink) -> tinyfs::SymlinkHandle {
        tinyfs::SymlinkHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(oplog_symlink))))
    }
}

#[async_trait]
impl Metadata for OpLogSymlink {
    async fn metadata(&self) -> tinyfs::Result<NodeMetadata> {
        // For symlinks, the partition is the parent directory (part_id)
        self.state.metadata(self.id).await
    }
}

#[async_trait]
impl Symlink for OpLogSymlink {
    async fn readlink(&self) -> tinyfs::Result<PathBuf> {
        // If we have a local target (newly created symlink), return it
        if let Some(target) = &self.target {
            return Ok(target.clone());
        }
        // Otherwise, load from persistence layer (previously persisted symlink)
        self.state.load_symlink_target(self.id).await
    }
}
