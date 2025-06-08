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

/// Memory-based filesystem backend for testing and lightweight use
/// 
/// This implementation uses the memory module types and is suitable for
/// testing, development, and scenarios where persistence is not required.
pub struct MemoryBackend;

impl super::FilesystemBackend for MemoryBackend {
    fn create_file(&self, content: &[u8]) -> Result<super::file::Handle> {
        Ok(crate::memory::MemoryFile::new_handle(content))
    }
    
    fn create_directory(&self) -> Result<super::dir::Handle> {
        Ok(crate::memory::MemoryDirectory::new_handle())
    }
    
    fn create_symlink(&self, target: &str) -> Result<super::symlink::Handle> {
        use std::path::PathBuf;
        Ok(crate::memory::MemorySymlink::new_handle(PathBuf::from(target)))
    }
}

pub fn new_fs() -> super::FS {
    super::FS::with_backend(MemoryBackend{}).expect("infallible")
}
