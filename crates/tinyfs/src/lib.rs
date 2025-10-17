#![allow(dead_code)]
#![allow(unused_imports)]

pub mod arrow;
pub mod async_helpers; // New helper module for reducing duplication
mod dir;
mod entry_type;
mod error;
mod file;
mod fs;
mod glob;
pub mod memory;
mod metadata;
mod node;
mod path;
pub mod persistence;
mod symlink;
pub mod tree_format; // Tree formatting utilities for hierarchical displays
mod wd; // Arrow integration module

// Public exports - Core filesystem API
pub use dir::{Directory, Handle as DirHandle, Pathed};
pub use file::{AsyncReadSeek, File, Handle as FileHandle};
pub use fs::FS;
pub use node::{Node, NodeID, NodePath, NodeRef, NodeType};
pub use wd::{CopyDestination, Lookup, Visitor, WD};

// Buffer utilities for tests and special cases
// WARNING: These load entire files into memory - use sparingly
pub use async_helpers::buffer_helpers;
pub use entry_type::EntryType;
pub use error::{Error, Result};
pub use memory::persistence::MemoryPersistence;
pub use metadata::{Metadata, NodeMetadata};
pub use persistence::{DirectoryOperation, FileVersionInfo, PersistenceLayer};
pub use symlink::{Handle as SymlinkHandle, Symlink};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod metadata_impl_tests;
