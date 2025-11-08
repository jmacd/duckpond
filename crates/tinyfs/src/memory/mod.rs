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

mod directory;
mod file;
pub mod persistence;
mod symlink;

pub use directory::MemoryDirectory;
pub use file::MemoryFile;
pub use persistence::MemoryPersistence;
pub use symlink::MemorySymlink;

use crate::error::{Error, Result};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Create a new memory-based filesystem using the persistence layer architecture
pub async fn new_fs() -> super::FS {
    let memory_persistence = MemoryPersistence::default();
    super::FS::new(memory_persistence)
        .await
        .expect("infallible")
}
