use crate::error;
use crate::file::{File, Handle};
use std::cell::RefCell;
use std::rc::Rc;

/// Represents a file backed by memory
/// This implementation stores file content in a Vec<u8> and is suitable for
/// testing, development, and lightweight filesystem operations.
pub struct MemoryFile {
    content: Vec<u8>,
}

impl File for MemoryFile {
    fn content(&self) -> error::Result<&[u8]> {
        Ok(&self.content)
    }
    
    fn write_content(&mut self, content: &[u8]) -> error::Result<()> {
        self.content = content.to_vec();
        Ok(())
    }
}

impl MemoryFile {
    /// Create a new MemoryFile handle with the given content
    pub fn new_handle<T: AsRef<[u8]>>(content: T) -> Handle {
        Handle::new(Rc::new(RefCell::new(Box::new(MemoryFile {
            content: content.as_ref().to_vec(),
        }))))
    }
}
