#![allow(dead_code)]
#![allow(unused_imports)]

mod backend;
mod dir;
mod error;
mod file;
mod fs;
mod glob;
mod node;
mod path;
mod symlink;
mod wd;

// Memory implementations (for testing and basic functionality)
mod memory;

// Public exports - Core filesystem API
pub use fs::FS;
pub use wd::WD;
pub use node::{NodePath, NodeRef, NodeID, Node, NodeType};
pub use dir::{Directory, Handle as DirHandle};
pub use file::{File, Handle as FileHandle};
pub use symlink::{Symlink, Handle as SymlinkHandle};
pub use error::{Error, Result};
pub use backend::FilesystemBackend;

#[cfg(test)]
mod tests;
