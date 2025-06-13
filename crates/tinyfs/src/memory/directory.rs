use crate::dir::{Directory, Handle};
use crate::error::{Error, Result};
use crate::node::NodeRef;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

/// Represents a directory backed by a BTreeMap
/// This implementation stores directory entries in memory and is suitable for
/// testing, development, and lightweight filesystem operations.
pub struct MemoryDirectory {
    entries: BTreeMap<String, NodeRef>,
}

impl Directory for MemoryDirectory {
    fn get(&self, name: &str) -> Result<Option<NodeRef>> {
        Ok(self.entries.get(name).cloned())
    }

    fn insert(&mut self, name: String, id: NodeRef) -> Result<()> {
        if self.entries.insert(name.clone(), id).is_some() {
            // @@@ Not a full path
            return Err(Error::already_exists(&name));
        }
        Ok(())
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = (String, NodeRef)> + 'a>> {
        let mut items = Vec::new();
        for (name, node_ref) in &self.entries {
            items.push((name.clone(), node_ref.clone()));
        }
        Ok(Box::new(items.into_iter()))
    }
}

impl MemoryDirectory {
    /// Create a new MemoryDirectory handle
    pub fn new_handle() -> Handle {
        Handle::new(Rc::new(RefCell::new(Box::new(MemoryDirectory {
            entries: BTreeMap::new(),
        }))))
    }
}
