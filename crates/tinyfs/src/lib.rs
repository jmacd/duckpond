#![allow(dead_code)]
#![allow(unused_imports)]

mod dir;
mod error;
mod file;
mod fs;
mod glob;
mod node;
mod path;
pub mod persistence;
mod memory_persistence;
mod symlink;
mod wd;
pub mod memory;

// Public exports - Core filesystem API
pub use fs::FS;
pub use wd::{WD, Visitor, Lookup};
pub use node::{NodePath, NodeRef, NodeID, Node, NodeType};
pub use dir::{Directory, Handle as DirHandle};
pub use file::{File, Handle as FileHandle};
pub use symlink::{Symlink, Handle as SymlinkHandle};
pub use error::{Error, Result};
pub use persistence::{PersistenceLayer, DirectoryOperation};
pub use memory_persistence::MemoryPersistence;

#[cfg(test)]
mod tests;
