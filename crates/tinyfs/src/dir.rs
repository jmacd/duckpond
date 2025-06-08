use std::cell::Ref;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;

use crate::error::*;
use crate::node::*;

/// Represents a directory containing named entries.
pub trait Directory {
    fn get(&self, name: &str) -> Result<Option<NodeRef>>;

    fn insert(&mut self, name: String, id: NodeRef) -> Result<()>;

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = (String, NodeRef)> + 'a>>;
}

/// A handle for a refcounted directory.
#[derive(Clone)]
pub struct Handle(Rc<RefCell<Box<dyn Directory>>>);

/// State computed in read_dir().
pub struct ReadDir<'a> {
    iter: Box<dyn Iterator<Item = NodePath>>,
    _dir: Rc<Ref<'a, Box<dyn Directory>>>,
}

/// This is returned by read_dir().
pub struct ReadDirHandle<'a>(Rc<RefCell<Option<ReadDir<'a>>>>);

/// This takes from the ReadDirHandle, constructs an iterator.
pub struct ReadDirIter<'a> {
    data: ReadDir<'a>,
}

/// Represents a Dir/File/Symlink handle with the active path.
#[derive(Clone)]
pub struct Pathed<T> {
    handle: T,
    path: PathBuf,
}

impl<'a> IntoIterator for ReadDirHandle<'a> {
    type Item = NodePath;
    type IntoIter = ReadDirIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        ReadDirIter {
            data: self.take().unwrap(),
        }
    }
}

impl<'a> Deref for ReadDirHandle<'a> {
    type Target = Rc<RefCell<Option<ReadDir<'a>>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> Iterator for ReadDirIter<'a> {
    type Item = NodePath;

    fn next(&mut self) -> Option<Self::Item> {
        self.data.iter.next()
    }
}

impl Handle {
    pub fn new(r: Rc<RefCell<Box<dyn Directory>>>) -> Self {
        Self(r)
    }

    pub fn get(&self, name: &str) -> Result<Option<NodeRef>> {
        self.borrow().get(name)
    }

    pub fn insert(&self, name: String, id: NodeRef) -> Result<()> {
        self.try_borrow_mut()?.insert(name, id)
    }
}

impl Deref for Handle {
    type Target = Rc<RefCell<Box<dyn Directory>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> Pathed<T> {
    pub fn new<P: AsRef<Path>>(path: P, handle: T) -> Self {
        Self {
            handle,
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn path(&self) -> PathBuf {
        self.path.to_path_buf()
    }
}

impl Pathed<crate::file::Handle> {
    pub fn read_file(&self) -> Result<Vec<u8>> {
        self.handle.content()
    }
    
    pub fn write_file(&self, content: &[u8]) -> Result<()> {
        self.handle.write_file(content)
    }
}

impl Pathed<Handle> {
    pub fn get(&self, name: &str) -> Result<Option<NodePath>> {
        Ok(self.handle.get(name)?.map(|nr| NodePath {
            node: nr,
            path: self.path.join(name),
        }))
    }

    pub fn insert(&self, name: String, id: NodeRef) -> Result<()> {
        self.handle.insert(name, id)
    }

    pub fn read_dir<'a>(&'a self) -> Result<ReadDirHandle<'a>> {
        let dvec: Vec<_> = self
            .handle
            .borrow()
            .iter()?
            .map(|(name, nref)| NodePath {
                node: nref,
                path: self.path.join(name),
            })
            .collect();
        let dd = ReadDir {
            iter: Box::new(dvec.into_iter()),
            _dir: Rc::new(self.handle.borrow()),
        };
        Ok(ReadDirHandle(Rc::new(RefCell::new(Some(dd)))))
    }
}

impl Pathed<crate::symlink::Handle> {
    pub fn readlink(&self) -> Result<PathBuf> {
        self.handle.readlink()
    }
}
