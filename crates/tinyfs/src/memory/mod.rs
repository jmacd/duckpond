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

/// Memory-based filesystem backend for testing and lightweight use
/// 
/// This implementation uses the memory module types and is suitable for
/// testing, development, and scenarios where persistence is not required.
pub struct MemoryBackend;

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
    /// Memory backend always creates a new root directory (no persistence)
    async fn root_directory(&self) -> Result<super::dir::Handle> {
        // For memory backend, always create a new directory
        self.create_directory(crate::node::NodeID::new(0)).await
    }
    }

pub async fn new_fs() -> super::FS {
    super::FS::with_backend(MemoryBackend{}).await.expect("infallible")
}
