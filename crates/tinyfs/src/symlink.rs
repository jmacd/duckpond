use std::cell::RefCell;
use std::ops::Deref;
use std::path::PathBuf;
use std::rc::Rc;

use super::error;

pub const SYMLINK_LOOP_LIMIT: u32 = 10;

/// Represents a file with binary content
pub trait Symlink {
    fn readlink(&self) -> error::Result<PathBuf>;
}

/// A handle for a refcounted file.
#[derive(Clone)]
pub struct Handle(Rc<RefCell<Box<dyn Symlink>>>);

/// Represents a symbolic link to another path
pub struct MemorySymlink {
    target: PathBuf,
}

impl Handle {
    pub fn readlink(&self) -> error::Result<PathBuf> {
        self.borrow().readlink()
    }
}

impl Deref for Handle {
    type Target = Rc<RefCell<Box<dyn Symlink>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl MemorySymlink {
    pub fn new_handle(target: PathBuf) -> Handle {
        Handle(Rc::new(RefCell::new(Box::new(MemorySymlink { target }))))
    }
}

impl Symlink for MemorySymlink {
    fn readlink(&self) -> error::Result<PathBuf> {
        Ok(self.target.clone())
    }
}
