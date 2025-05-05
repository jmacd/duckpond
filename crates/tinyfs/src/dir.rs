use std::ops::Deref;
use std::rc::Rc;
use std::cell::RefCell;
use std::cell::Ref;
use std::path::PathBuf;
use std::path::Path;
use super::NodeRef;
use super::NodePath;
use super::error;
use std::collections::BTreeMap;
use super::file;

pub struct DuckData<'a> {
    diter: Box<dyn Iterator<Item = NodePath>>,
    dref: Rc<Ref<'a, Box<dyn Directory>>>,
}

pub struct DuckHandle<'a> {
    ddat: Rc<RefCell<DuckData<'a>>>,
}

// impl<'a> DuckHandle<'a> {
//     fn init(&'a mut self) -> error::Result<()> {
// 	self.ddat.borrow_mut().diter = self.dref.iter()?;
// 	Ok(())
//     }
// }

// impl<'a> IntoIterator for DuckHandle<'a> {
//     type Item = NodePath;
//     type IntoIter = Self;
//     fn into_iter(self) -> Self::IntoIter {
// 	self
//     }
// }

impl<'a> Iterator for DuckHandle<'a> {
    type Item = NodePath;

    fn next(&mut self) -> Option<Self::Item> {
	self.ddat.borrow_mut().diter.next()
    }
}

/// Represents a directory containing named entries.
pub trait Directory {
    fn get(&self, name: &str) -> Option<NodeRef>;
    fn insert(&mut self, name: String, id: NodeRef) -> error::Result<()>;

    fn iter<'a>(&'a self) -> error::Result<Box<dyn Iterator<Item = (String, NodeRef)> + 'a>>;
}

/// A handle for a refcounted directory.
#[derive(Clone)]
pub struct Handle(Rc<RefCell<Box<dyn Directory>>>);

/// Represents a directory backed by a BTree
pub struct MemoryDirectory {
    entries: BTreeMap<String, NodeRef>,
}

#[derive(Clone)]
pub struct Pathed<T> {
    handle: T,
    path: PathBuf,
}

impl Handle {
    pub fn new(r: Rc<RefCell<Box<dyn Directory>>>) -> Self {
	Self(r)
    }

    pub fn get(&self, name: &str) -> Option<NodeRef> {
	self.0.deref().borrow().get(name)
    }

    pub fn insert(&self, name: String, id: NodeRef) -> error::Result<()> {
	self.0.deref().borrow_mut().insert(name, id)
    }
}

impl MemoryDirectory {
    pub fn new_handle() -> Handle {
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

    fn iter<'a>(&'a self) -> error::Result<Box<dyn Iterator<Item = (String, NodeRef)> + 'a>> {
	Ok(Box::new(self.entries.iter().map(|(a, b)| (a.clone(), b.clone()))))
    }    
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
	self.handle.content()
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

    pub fn read_dir<'a>(&'a self) -> error::Result<DuckHandle<'a>> {
	let dvec: Vec<_> = self.handle.0.borrow().iter()?.map(|(name, nref)| NodePath{
	    node: nref,
	    path: self.path.join(name),
	}).collect();
	let dd = DuckData{
	    diter: Box::new(dvec.into_iter()),
	    dref: Rc::new(self.handle.0.borrow()),
	};
	let dh = DuckHandle{
	    ddat: Rc::new(RefCell::new(dd)),
	};
	Ok(dh)
    }
}
