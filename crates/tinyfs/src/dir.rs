use std::ops::Deref;
use std::rc::Rc;
use std::cell::RefCell;
use std::path::PathBuf;
use std::path::Path;
use super::NodeRef;
use super::NodePath;
use super::error;
use std::collections::BTreeMap;
use super::file;

/// Represents a directory containing named entries.
pub trait Directory {
    fn get(&self, name: &str) -> Option<NodeRef>;
    fn insert(&mut self, name: String, id: NodeRef) -> Result<Option<NodeRef>>;
    fn iter(&self) -> DIterator;
}

/// Represents an iterator over the dyn Directory.
pub struct DIterator(Box<dyn Iterator<Item = (String, NodeRef)>>);

/// A handle for a refcounted directory.
#[derive(Clone)]
pub struct Handle(Rc<RefCell<Box<dyn Directory>>>);

/// Represents a directory backed by a BTree
pub struct MemoryDirectory {
    entries: BTreeMap<String, NodeRef>,
}

pub type Result<T> = std::result::Result<T, error::FSError>;
					 
impl Handle {
    pub fn get(&self, name: &str) -> Option<NodeRef> {
	self.0.deref().borrow().get(name)
    }

    pub fn insert(&self, name: String, id: NodeRef) -> Result<Option<NodeRef>> {
	Ok(self.0.deref().borrow_mut().insert(name, id)?)
    }
}

impl Iterator for DIterator {
    type Item = (String, NodeRef);

    fn next(&mut self) -> Option<Self::Item> {
	self.0.next()
    }
}


// /// Represents an iterator over Handles
// pub struct HIterator<'a>(std::cell::Ref<'a, Box<dyn Directory>>);

//     pub fn read<'a>(&'a self) -> Result<HIterator<'a>> {
// 	Ok(HIterator(self.0.deref().borrow()))
//     }
// impl<'a> IntoIterator for HIterator<'a> {
//     type Item = (String, NodeRef);
//     type IntoIter = DIterator;

//     fn into_iter(self) -> Self::IntoIter {
// 	DIterator(Box::new(self.0.iter()))
//     }
// }

impl MemoryDirectory {
    pub fn new() -> Handle {
        Handle(Rc::new(RefCell::new(Box::new(MemoryDirectory {
            entries: BTreeMap::new(),
        }))))
    }
}

impl Directory for MemoryDirectory {
    fn get(&self, name: &str) -> Option<NodeRef> {
        self.entries.get(name).cloned()
    }

    fn insert(&mut self, name: String, id: NodeRef) -> Result<Option<NodeRef>> {
        Ok(self.entries.insert(name, id))
    }

    fn iter(&self) -> DIterator {
	// Note a copy happens here! I don't know how to avoid.
	DIterator(Box::new(self.entries.clone().into_iter().map(|(x, y)| (x, y))))
    }    
}

pub struct Pathed<T> {
    node: T,
    path: PathBuf,
}

impl<T> Pathed<T> {
    fn new<P: AsRef<Path>> (path: P, node: T) -> Self {
	Self {
	    node,
	    path: path.as_ref().to_path_buf(),
	}
    }
}

impl Pathed<file::Handle> {
    fn content(&self) -> Result<Vec<u8>> {
	Ok(self.node.content()?)
    }
}

impl Pathed<Handle> {
    fn get(&self, name: &str) -> Option<NodePath> {
	self.node.get(name).map(|nr| NodePath{
	    node: nr,
	    path: self.path.join(name),
	})
    }

    fn insert(&mut self, name: String, id: NodeRef) -> Result<Option<NodeRef>> {
	Ok(self.node.insert(name, id)?)
    }

    fn read<'a>(&'a self) -> Result<PIterator<'a>> {
	Ok(PIterator{
	    path: self.path,
	    iter: self.node.read(),
	})
    }
}

/// Represents an iterator over Handles
pub struct PIterator<'a>(std::cell::Ref<'a, Box<dyn Directory>>);

impl<'a> IntoIterator for PIterator<'a> {
    type Item = NodePath;
    type IntoIter = DIterator;

    fn into_iter(self) -> Self::IntoIter {
	DIterator(Box::new(self.0.iter()))
    }
}
