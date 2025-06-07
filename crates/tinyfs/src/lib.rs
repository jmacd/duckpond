#![allow(dead_code)]
#![allow(unused_imports)]

mod dir;
mod error;
mod file;
mod fs;
mod glob;
mod node;
mod path;
mod symlink;
mod wd;

// Public exports - Core filesystem API
pub use fs::FS;
pub use wd::WD;
pub use node::{NodePath, NodeRef, NodeID};
pub use dir::{Directory, Handle as DirHandle};
pub use error::{Error, Result};

// Test utilities (should only be used in tests)
#[cfg(test)]
pub use file::MemoryFile;
#[cfg(test)]
pub use dir::MemoryDirectory;

#[cfg(test)]
mod tests;
