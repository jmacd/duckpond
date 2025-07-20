use crate::error;
use crate::metadata::{Metadata, NodeMetadata};
use crate::symlink::{Handle, Symlink};
use crate::EntryType;
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
            version: 1, // Memory symlinks don't track versions
            size: None, // Symlinks don't have sizes
            sha256: None, // Symlinks don't have checksums
            entry_type: EntryType::Symlink,
        })
    }

    async fn metadata_u64_impl(&self, _name: &str) -> error::Result<Option<u64>> {
        // Memory symlinks don't have persistent metadata beyond the consolidated metadata
        Ok(None)
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
    pub fn new_handle(target: PathBuf) -> Handle {
        Handle::new(Arc::new(tokio::sync::Mutex::new(Box::new(MemorySymlink { target }))))
    }
}
