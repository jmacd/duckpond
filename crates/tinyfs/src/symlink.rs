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

impl Handle {
    pub fn new(r: Rc<RefCell<Box<dyn Symlink>>>) -> Self {
        Self(r)
    }

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
