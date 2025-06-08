use super::error;
use std::cell::RefCell;
use std::ops::Deref;
use std::rc::Rc;

/// A handle for a refcounted file.
#[derive(Clone)]
pub struct Handle(Rc<RefCell<Box<dyn File>>>);

/// Represents a file with binary content
pub trait File {
    fn content(&self) -> error::Result<&[u8]>;
    fn write_content(&mut self, content: &[u8]) -> error::Result<()>;
}

impl Handle {
    pub fn new(r: Rc<RefCell<Box<dyn File>>>) -> Self {
        Self(r)
    }

    pub fn content(&self) -> error::Result<Vec<u8>> {
        Ok(self.borrow().content()?.to_vec())
    }
    
    pub fn write_file(&self, content: &[u8]) -> error::Result<()> {
        self.borrow_mut().write_content(content)
    }
}

impl Deref for Handle {
    type Target = Rc<RefCell<Box<dyn File>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
