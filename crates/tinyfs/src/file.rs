use std::rc::Rc;
use std::cell::RefCell;

/// A handle for a refcounted file.
#[derive(Clone)]
pub struct Handle(Rc<RefCell<Box<dyn File>>>);

/// Represents a file with binary content
pub trait File {
    fn content(&self) -> Result<&[u8]>;
}

/// Represents a file backed by memory
pub struct MemoryFile {
    content: Vec<u8>,
}

#[derive(Debug, PartialEq)]
pub enum Error {
    FileError,
}

pub type Result<T> = std::result::Result<T, Error>;

impl Handle {
    pub fn content(&self) -> Result<Vec<u8>> {
        Ok(self.0.borrow().content()?.to_vec())
    }
}

impl File for MemoryFile {
    fn content(&self) -> Result<&[u8]> {
        Ok(&self.content)
    }
}

impl MemoryFile {
    pub fn new<T: AsRef<[u8]>>(content: T) -> Handle {
        Handle(Rc::new(RefCell::new(Box::new(MemoryFile {
	    content: content.as_ref().to_vec(),
	}))))
    }
}
