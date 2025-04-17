use std::ops::Deref;
use std::rc::Rc;
use std::cell::RefCell;
use super::Node;
use super::NodeID;
use std::collections::BTreeMap;

/// Represents a directory containing named entries.
pub trait Directory {
    fn get(&self, name: &str) -> Option<NodeID>;
    fn insert(&mut self, name: String, id: NodeID) -> Result<Option<NodeID>>;
    fn iter(&self) -> DIterator;
}

/// Represents an iterator over the dyn Directory.
pub struct DIterator(Box<dyn Iterator<Item = (String, NodeID)>>);

/// A handle for a refcounted directory.
#[derive(Clone)]
pub struct Handle(Rc<RefCell<Box<dyn Directory>>>);

/// Represents an iterator over Handles
pub struct HIterator<'a>(std::cell::Ref<'a, Box<dyn Directory>>);

/// Represents a directory backed by a BTree
pub struct MemoryDirectory {
    entries: BTreeMap<String, NodeID>,
}

#[derive(Debug, PartialEq)]
pub enum Error {
    DirError,
}

pub type Result<T> = std::result::Result<T, Error>;
					 
impl Handle {
    pub fn get(&self, name: &str) -> Option<NodeID> {
	self.0.deref().borrow().get(name)
    }

    pub fn insert(&self, name: String, id: NodeID) -> Result<Option<NodeID>> {
	Ok(self.0.deref().borrow_mut().insert(name, id)?)
    }

    pub fn read<'a>(&'a self) -> Result<HIterator<'a>> {
	Ok(HIterator(self.0.deref().borrow()))
    }
}

impl Iterator for DIterator {
    type Item = (String, NodeID);

    fn next(&mut self) -> Option<Self::Item> {
	self.0.next()
    }
}

impl<'a> IntoIterator for HIterator<'a> {
    type Item = (String, NodeID);
    type IntoIter = DIterator;

    fn into_iter(self) -> Self::IntoIter {
	DIterator(Box::new(self.0.iter()))
    }
}

impl MemoryDirectory {
    pub fn new() -> Node {
        Node::Directory(Handle(Rc::new(RefCell::new(Box::new(MemoryDirectory {
            entries: BTreeMap::new(),
        })))))
    }
}

impl Directory for MemoryDirectory {
    fn get(&self, name: &str) -> Option<NodeID> {
        self.entries.get(name).copied()
    }

    fn insert(&mut self, name: String, id: NodeID) -> Result<Option<NodeID>> {
        Ok(self.entries.insert(name, id))
    }

    fn iter(&self) -> DIterator {
	// Note a copy happens here! I don't know how to avoid.
	DIterator(Box::new(self.entries.clone().into_iter().map(|(x, y)| (x, y))))
    }    
}
