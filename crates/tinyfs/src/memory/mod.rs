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
pub mod persistence;

pub use file::MemoryFile;
pub use directory::MemoryDirectory;
pub use symlink::MemorySymlink;
pub use persistence::MemoryPersistence;

use crate::error::{Error, Result};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Create a new memory-based filesystem using the persistence layer architecture
pub async fn new_fs() -> super::FS {
    let memory_persistence = MemoryPersistence::new();
    super::FS::new(memory_persistence).await.expect("infallible")
}
