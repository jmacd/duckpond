// Clean architecture Symlink implementation for TinyFS
use crate::persistence::State;
use async_trait::async_trait;
use std::sync::Arc;
use tinyfs::{Metadata, FileID, NodeMetadata, Symlink, persistence::PersistenceLayer};

/// Clean architecture symlink implementation - COMPLETELY STATELESS
/// - NO local state or caching (persistence layer is single source of truth)
/// - Simple delegation to persistence layer for all operations
/// - Proper separation of concerns
pub struct OpLogSymlink {
    /// Unique node identifier for this symlink
    id: FileID,

    /// Reference to persistence layer (single source of truth)
    state: State,
}

impl OpLogSymlink {
    /// Create new symlink instance with persistence layer dependency injection
    #[must_use]
    pub fn new(id: FileID, state: State) -> Self {
        Self {
            id,
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
    async fn readlink(&self) -> tinyfs::Result<std::path::PathBuf> {
        // Load symlink target directly from persistence layer (avoids recursion)
        let target = self
            .state
            .load_symlink_target(self.id)
            .await?;
        Ok(target)
    }
}
