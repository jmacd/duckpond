use crate::error;
use crate::metadata::Metadata;
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
    async fn metadata_u64(&self, _name: &str) -> error::Result<Option<u64>> {
        // Memory symlinks don't have persistent metadata
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
