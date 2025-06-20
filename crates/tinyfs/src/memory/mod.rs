//! Memory-based implementations for TinyFS
//! 
//! This module contains in-memory implementations of the File, Directory, and Symlink traits.
//! These implementations are primarily used for testing and as basic building blocks for
//! the filesystem. In production scenarios, you may want to use OpLog-backed implementations
//! instead for persistence and advanced features.
//!
//! The memory implementations provide:
//! - Fast, non-persistent filesystem operations
//! - Simple data structures (BTreeMap for directories, Vec<u8> for files)
//! - Suitable for testing, development, and lightweight use cases

mod file;
mod directory;
mod symlink;

pub use file::MemoryFile;
pub use directory::MemoryDirectory;
pub use symlink::MemorySymlink;

use crate::error::{Error, Result};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Memory-based filesystem backend for testing and lightweight use
/// 
/// This implementation uses the memory module types and is suitable for
/// testing, development, and scenarios where persistence is not required.
pub struct MemoryBackend {
    /// Shared root directory that persists across calls
    root_dir: Arc<Mutex<Option<super::dir::Handle>>>,
}

#[async_trait]
impl super::FilesystemBackend for MemoryBackend {
    async fn create_file(&self, _node_id: crate::node::NodeID, content: &[u8], _parent_node_id: Option<&str>) -> Result<super::file::Handle> {
        Ok(crate::memory::MemoryFile::new_handle(content))
    }

    async fn commit(&self) -> Result<()> {
	Ok(())
    }
    
    async fn create_directory(&self, _node_id: crate::node::NodeID) -> Result<super::dir::Handle> {
        Ok(crate::memory::MemoryDirectory::new_handle())
    }
    
    async fn create_symlink(&self, _node_id: crate::node::NodeID, target: &str, _parent_node_id: Option<&str>) -> Result<super::symlink::Handle> {
        use std::path::PathBuf;
        Ok(crate::memory::MemorySymlink::new_handle(PathBuf::from(target)))
    }
    
    /// Get the root directory handle for this backend
    /// Memory backend maintains the same root directory across calls
    async fn root_directory(&self) -> Result<super::dir::Handle> {
        let mut root_guard = self.root_dir.lock().await;
        if let Some(ref existing_root) = *root_guard {
            // Return the existing shared root directory
            Ok(existing_root.clone())
        } else {
            // Create a new root directory and store it for future calls
            let new_root = self.create_directory(crate::node::NodeID::new(0)).await?;
            *root_guard = Some(new_root.clone());
            Ok(new_root)
        }
    }
    }

impl MemoryBackend {
    /// Create a new MemoryBackend with a shared root directory
    pub fn new() -> Self {
        Self {
            root_dir: Arc::new(Mutex::new(None)),
        }
    }
}

pub async fn new_fs() -> super::FS {
    super::FS::with_backend(MemoryBackend::new()).await.expect("infallible")
}
