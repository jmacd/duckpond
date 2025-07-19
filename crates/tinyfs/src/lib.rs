#![allow(dead_code)]
#![allow(unused_imports)]

mod dir;
mod entry_type;
mod error;
mod file;
mod fs;
mod glob;
mod metadata;
mod node;
mod path;
pub mod persistence;
mod memory_persistence;
mod symlink;
mod wd;
pub mod memory;
mod async_helpers; // New helper module for reducing duplication
pub mod arrow; // Arrow integration module

// Public exports - Core filesystem API
pub use fs::FS;
pub use wd::{WD, Visitor, Lookup, CopyDestination};
pub use node::{NodePath, NodeRef, NodeID, Node, NodeType};
pub use dir::{Directory, Handle as DirHandle};
pub use file::{File, Handle as FileHandle};

// Buffer utilities for tests and special cases
// WARNING: These load entire files into memory - use sparingly
pub use async_helpers::buffer_helpers;
pub use symlink::{Symlink, Handle as SymlinkHandle};
pub use metadata::Metadata;
pub use error::{Error, Result};
pub use entry_type::EntryType;
pub use persistence::{PersistenceLayer, DirectoryOperation};
pub use memory_persistence::MemoryPersistence;

#[cfg(test)]
mod tests;
