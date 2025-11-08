use crate::EntryType;
use crate::error;
use crate::metadata::{Metadata, NodeMetadata};
use crate::symlink::{Handle, Symlink};
use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Represents a symbolic link to another path
/// This implementation stores the target path in memory and is suitable for
/// testing, development, and lightweight filesystem operations.
pub struct MemorySymlink {
    target: PathBuf,
}

#[async_trait]
impl Metadata for MemorySymlink {
    async fn metadata(&self) -> error::Result<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,   // Memory symlinks don't track versions
            size: None,   // Symlinks don't have sizes
            sha256: None, // Symlinks don't have checksums
            entry_type: EntryType::Symlink,
            timestamp: 0, // TODO
        })
    }
}

#[async_trait]
impl Symlink for MemorySymlink {
    async fn readlink(&self) -> error::Result<PathBuf> {
        Ok(self.target.clone())
    }
}

impl MemorySymlink {
    /// Create a new MemorySymlink handle with the given target
    #[must_use]
    pub fn new_handle(target: PathBuf) -> Handle {
        Handle::new(Arc::new(tokio::sync::Mutex::new(Box::new(MemorySymlink {
            target,
        }))))
    }
}
