use crate::dir::{Directory, Handle};
use crate::error::{Error, Result};
use crate::metadata::{Metadata, NodeMetadata};
use crate::node::NodeRef;
use crate::EntryType;
use async_trait::async_trait;
use futures::stream::{self, Stream};
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Represents a directory backed by a BTreeMap
/// This implementation stores directory entries in memory and is suitable for
/// testing, development, and lightweight filesystem operations.
pub struct MemoryDirectory {
    entries: BTreeMap<String, NodeRef>,
}

#[async_trait]
impl Metadata for MemoryDirectory {
    async fn metadata(&self) -> Result<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1, // Memory directories don't track versions
            size: None, // Directories don't have sizes
            sha256: None, // Directories don't have checksums
            entry_type: EntryType::Directory,
	    timestamp: 0, // TODO	    
        })
    }
}

#[async_trait]
impl Directory for MemoryDirectory {
    async fn get(&self, name: &str) -> Result<Option<NodeRef>> {
        Ok(self.entries.get(name).cloned())
    }

    async fn insert(&mut self, name: String, id: NodeRef) -> Result<()> {
        if self.entries.insert(name.clone(), id).is_some() {
            // Note this is not a full path.
            return Err(Error::already_exists(&name));
        }
        Ok(())
    }

    async fn entries(&self) -> Result<Pin<Box<dyn Stream<Item = Result<(String, NodeRef)>> + Send>>> {
        let items: Vec<_> = self.entries.iter()
            .map(|(name, node_ref)| Ok((name.clone(), node_ref.clone())))
            .collect();
        Ok(Box::pin(stream::iter(items)))
    }
}

impl MemoryDirectory {
    /// Create a new empty MemoryDirectory
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }
    
    /// Create a new MemoryDirectory handle
    pub fn new_handle() -> Handle {
        Handle::new(Arc::new(tokio::sync::Mutex::new(Box::new(MemoryDirectory::new()))))
    }
    
    /// Convert this MemoryDirectory into a Handle
    pub fn to_handle(self) -> Handle {
        Handle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }
}
