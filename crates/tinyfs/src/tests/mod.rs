mod memory;
mod reverse;
mod visit;

// Memory backend for testing (when feature enabled)
pub use crate::memory::MemoryBackend;

// Memory implementations (primarily for testing, also used internally)
pub use crate::memory::{MemoryFile, MemoryDirectory, MemorySymlink};
