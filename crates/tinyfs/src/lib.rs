#![allow(dead_code)]
#![allow(unused_imports)]

// mod backend; // Phase 5: Removed old backend system
// mod derived; // Phase 3: Removed DerivedFileManager complexity
mod dir;
mod error;
mod file;
mod fs;
mod glob;
mod node;
mod path;
pub mod persistence; // Add persistence module
mod memory_persistence; // Phase 5: Memory-based persistence layer
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
// pub use backend::FilesystemBackend; // Phase 5: Removed old backend system
pub use persistence::{PersistenceLayer, DirectoryOperation}; // Export persistence types
pub use memory_persistence::MemoryPersistence; // Phase 5: Export memory persistence
// pub use derived::DerivedFileManager; // Phase 3: Removed DerivedFileManager complexity

#[cfg(test)]
mod tests;
