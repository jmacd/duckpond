use async_trait::async_trait;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::error;
use super::metadata::Metadata;

pub const SYMLINK_LOOP_LIMIT: u32 = 10;

/// Represents a symlink that points to another path
#[async_trait]
pub trait Symlink: Metadata + Send + Sync {
    async fn readlink(&self) -> error::Result<PathBuf>;
}

/// A handle for a refcounted symlink.
#[derive(Clone)]
pub struct Handle(Arc<tokio::sync::Mutex<Box<dyn Symlink>>>);

impl Handle {
    pub fn new(r: Arc<tokio::sync::Mutex<Box<dyn Symlink>>>) -> Self {
        Self(r)
    }

    pub async fn readlink(&self) -> error::Result<PathBuf> {
        let symlink = self.0.lock().await;
        symlink.readlink().await
    }

    /// Get metadata through the symlink handle
    pub async fn metadata_u64(&self, name: &str) -> error::Result<Option<u64>> {
        let symlink = self.0.lock().await;
        symlink.metadata_u64(name).await
    }
}

impl Deref for Handle {
    type Target = Arc<Mutex<Box<dyn Symlink>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
