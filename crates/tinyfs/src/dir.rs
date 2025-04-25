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
    fn insert(&mut self, name: String, id: NodeRef) -> error::Result<()>;

    fn iter(&self) -> Box<dyn Iterator<Item = (String, NodeRef)>>;
}

/// Represents an iterator over the dyn Directory.
pub struct DIterator {
    path: PathBuf,
    diter: Box<dyn Iterator<Item = (String, NodeRef)>>,
}

/// A handle for a refcounted directory.
#[derive(Clone)]
pub struct Handle(Rc<RefCell<Box<dyn Directory>>>);

/// Represents a directory backed by a BTree
pub struct MemoryDirectory {
    entries: BTreeMap<String, NodeRef>,
}

impl Handle {
    pub fn get(&self, name: &str) -> Option<NodeRef> {
	self.0.deref().borrow().get(name)
    }

    pub fn insert(&self, name: String, id: NodeRef) -> error::Result<()> {
	self.0.deref().borrow_mut().insert(name, id)
    }
}

impl Iterator for DIterator {
    type Item = NodePath;

    fn next(&mut self) -> Option<Self::Item> {
	self.diter.next().map(|(name, nref)| NodePath{
	    path: self.path.join(name),
	    node: nref,
	})
    }
}

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

    fn insert(&mut self, name: String, id: NodeRef) -> error::Result<()> {
	if self.entries.insert(name.clone(), id).is_some() {
	    // @@@ Not a full path
	    return Err(error::Error::already_exists(&name));
	}
        Ok(())
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (String, NodeRef)>>
    {
	// Note an `entries` copy happens here! I don't know how to avoid.
	Box::new(self.entries.clone().into_iter())
    }    
}

#[derive(Clone)]
pub struct Pathed<T> {
    handle: T,
    path: PathBuf,
}

impl<T> Pathed<T> {
    pub fn new<P: AsRef<Path>> (path: P, handle: T) -> Self {
	Self {
	    handle,
	    path: path.as_ref().to_path_buf(),
	}
    }

    pub fn path(&self) -> PathBuf {
	self.path.to_path_buf()
    }
}

impl Pathed<file::Handle> {
    pub fn read_file(&self) -> error::Result<Vec<u8>> {
	Ok(self.handle.content()?)
    }
}

impl Pathed<Handle> {
    pub fn get(&self, name: &str) -> Option<NodePath> {
	self.handle.get(name).map(|nr| NodePath{
	    node: nr,
	    path: self.path.join(name),
	})
    }
    
    pub fn insert(&self, name: String, id: NodeRef) -> error::Result<()> {
	self.handle.insert(name, id)
    }

    pub fn read<'a>(&'a self) -> error::Result<PIterator<'a>> {
	Ok(PIterator{
	    path: self.path.clone(),
	    borrowed: self.handle.0.borrow(),
	})
    }
}

/// Represents an iterator over Handles
pub struct PIterator<'a> {
    path: PathBuf,
    borrowed: std::cell::Ref<'a, Box<dyn Directory>>,
}

impl<'a> IntoIterator for PIterator<'a> {
    type Item = NodePath;
    type IntoIter = DIterator;

    fn into_iter(self) -> Self::IntoIter {
	DIterator{
	    path: self.path.into(),
	    diter: Box::new(self.borrowed.iter()),
	}
    }
}
