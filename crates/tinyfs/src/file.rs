use std::ops::Deref;
use std::rc::Rc;
use std::cell::RefCell;
use super::error;

/// A handle for a refcounted file.
#[derive(Clone)]
pub struct Handle(Rc<RefCell<Box<dyn File>>>);

/// Represents a file with binary content
pub trait File {
    fn content(&self) -> error::Result<&[u8]>;
}

/// Represents a file backed by memory
pub struct MemoryFile {
    content: Vec<u8>,
}

impl Handle {
    pub fn content(&self) -> error::Result<Vec<u8>> {
        Ok(self.borrow().content()?.to_vec())
    }
}

impl Deref for Handle {
    type Target = Rc<RefCell<Box<dyn File>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl File for MemoryFile {
    fn content(&self) -> error::Result<&[u8]> {
        Ok(&self.content)
    }
}

impl MemoryFile {
    pub fn new_handle<T: AsRef<[u8]>>(content: T) -> Handle {
        Handle(Rc::new(RefCell::new(Box::new(MemoryFile {
	    content: content.as_ref().to_vec(),
	}))))
    }
}
