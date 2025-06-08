use crate::error;
use crate::symlink::{Handle, Symlink};
use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

/// Represents a symbolic link to another path
/// This implementation stores the target path in memory and is suitable for
/// testing, development, and lightweight filesystem operations.
pub struct MemorySymlink {
    target: PathBuf,
}

impl Symlink for MemorySymlink {
    fn readlink(&self) -> error::Result<PathBuf> {
        Ok(self.target.clone())
    }
}

impl MemorySymlink {
    /// Create a new MemorySymlink handle with the given target
    pub fn new_handle(target: PathBuf) -> Handle {
        Handle::new(Rc::new(RefCell::new(Box::new(MemorySymlink { target }))))
    }
}
