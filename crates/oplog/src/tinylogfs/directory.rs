// OpLog-backed Directory implementation for TinyFS integration
use super::{TinyLogFS, TinyLogFSError, DirectoryEntry};
use tinyfs::{Directory, NodeRef, Error as TinyFSError};
use std::rc::{Rc, Weak};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::time::SystemTime;

/// Directory implementation backed by OpLog persistence
pub struct OpLogDirectory {
    // Core identity
    node_id: String,
    
    // Performance caching
    entry_cache: RefCell<BTreeMap<String, CachedEntry>>,
    cache_dirty: RefCell<bool>,
    
    // Back-reference to TinyLogFS for transaction integration
    tinylogfs: Weak<RefCell<TinyLogFS>>,
    
    // Metadata
    metadata: RefCell<DirectoryMetadata>,
    last_loaded: RefCell<SystemTime>,
}

#[derive(Debug, Clone)]
struct CachedEntry {
    node_ref: NodeRef,
    last_accessed: SystemTime,
    is_dirty: bool,
    directory_entry: DirectoryEntry, // Cache the DirectoryEntry for this node
}

#[derive(Debug, Clone)]
struct DirectoryMetadata {
    created_at: SystemTime,
    modified_at: SystemTime,
    entry_count: usize,
    custom_attributes: serde_json::Value,
}

#[derive(Debug)]
pub struct DirectoryStatus {
    pub cached_entries: usize,
    pub is_dirty: bool,
    pub last_loaded: Option<SystemTime>,
    pub entry_count: usize,
}

impl OpLogDirectory {
    pub fn new(
        node_id: String,
        tinylogfs: Weak<RefCell<TinyLogFS>>
    ) -> Result<Self, TinyLogFSError> {
        let now = SystemTime::now();
        Ok(Self {
            node_id,
            entry_cache: RefCell::new(BTreeMap::new()),
            cache_dirty: RefCell::new(false),
            tinylogfs,
            metadata: RefCell::new(DirectoryMetadata {
                created_at: now,
                modified_at: now,
                entry_count: 0,
                custom_attributes: serde_json::Value::Object(serde_json::Map::new()),
            }),
            last_loaded: RefCell::new(now),
        })
    }
    
    /// Create a new OpLogDirectory handle for use with tinyfs
    pub fn new_handle(
        node_id: String,
        tinylogfs: Weak<RefCell<TinyLogFS>>
    ) -> Result<tinyfs::DirHandle, TinyLogFSError> {
        let oplog_dir = Self::new(node_id, tinylogfs)?;
        Ok(tinyfs::DirHandle::new(Rc::new(RefCell::new(Box::new(oplog_dir)))))
    }
    
    /// Load directory entries from OpLog storage
    fn load_directory_entries_from_oplog(&self) -> Result<Vec<DirectoryEntry>, TinyLogFSError> {
        // Get TinyLogFS reference for OpLog access
        let tinylogfs_ref = self.tinylogfs.upgrade()
            .ok_or_else(|| TinyLogFSError::Transaction {
                message: "TinyLogFS reference no longer available".to_string()
            })?;
        
        // For now, return empty vector - this would need to be implemented
        // to query the OpLog for directory entries
        // TODO: Implement actual OpLog query
        Ok(vec![])
    }
    
    /// Reconstruct NodeRef from child node ID
    fn reconstruct_node_ref(&self, child_node_id: &str) -> Result<NodeRef, TinyLogFSError> {
        // TODO: This is a placeholder implementation
        // In the real implementation, this should:
        // 1. Query OpLog for the child node's data
        // 2. Determine the node type (file/directory/symlink)
        // 3. Create appropriate lazy-loading NodeRef
        
        // For now, return an error since we can't create nodes without the internal API
        Err(TinyLogFSError::NodeNotFound { 
            path: std::path::PathBuf::from(child_node_id) 
        })
    }
    
    /// Cache an entry for future access
    fn cache_entry(&self, name: String, node_ref: NodeRef, directory_entry: DirectoryEntry) {
        let cached_entry = CachedEntry {
            node_ref,
            last_accessed: SystemTime::now(),
            is_dirty: false,
            directory_entry,
        };
        self.entry_cache.borrow_mut().insert(name, cached_entry);
    }
    
    /// Get all entries as DirectoryEntry records
    fn get_all_entries(&self) -> Result<Vec<DirectoryEntry>, TinyLogFSError> {
        let cache = self.entry_cache.borrow();
        let entries = cache.values()
            .map(|cached| cached.directory_entry.clone())
            .collect();
        Ok(entries)
    }
    
    /// Get current directory status
    pub fn get_status(&self) -> DirectoryStatus {
        let cache = self.entry_cache.borrow();
        let metadata = self.metadata.borrow();
        
        DirectoryStatus {
            cached_entries: cache.len(),
            is_dirty: *self.cache_dirty.borrow(),
            last_loaded: Some(*self.last_loaded.borrow()),
            entry_count: metadata.entry_count,
        }
    }
}

impl Directory for OpLogDirectory {
    fn get(&self, name: &str) -> Result<Option<NodeRef>, TinyFSError> {
        // 1. Check in-memory cache first (fast path)
        if let Some(cached) = self.entry_cache.borrow_mut().get_mut(name) {
            cached.last_accessed = SystemTime::now();
            return Ok(Some(cached.node_ref.clone()));
        }
        
        // 2. Query OpLog for directory entries (slow path)
        let entries = self.load_directory_entries_from_oplog()
            .map_err(|e| tinyfs::Error::Borrow(e.to_string()))?;
        
        // 3. Find requested entry and construct NodeRef
        if let Some(dir_entry) = entries.iter().find(|e| e.name == name) {
            let node_ref = self.reconstruct_node_ref(&dir_entry.child)
                .map_err(|e| tinyfs::Error::Borrow(e.to_string()))?;
            
            // 4. Cache result for future access
            self.cache_entry(name.to_string(), node_ref.clone(), dir_entry.clone());
            
            Ok(Some(node_ref))
        } else {
            Ok(None)
        }
    }
    
    fn insert(&mut self, name: String, node: NodeRef) -> Result<(), TinyFSError> {
        // 1. Create DirectoryEntry for the new node
        let node_id = format!("{:016x}", node.id()); // Convert NodeRef ID to hex string
        let directory_entry = DirectoryEntry {
            name: name.clone(),
            child: node_id.clone(),
        };
        
        // 2. Update in-memory cache immediately
        let cached_entry = CachedEntry {
            node_ref: node.clone(),
            last_accessed: SystemTime::now(),
            is_dirty: true,
            directory_entry: directory_entry.clone(),
        };
        self.entry_cache.borrow_mut().insert(name.clone(), cached_entry);
        
        // 3. Mark directory as dirty for next commit
        *self.cache_dirty.borrow_mut() = true;
        
        // 4. Update metadata
        {
            let mut metadata = self.metadata.borrow_mut();
            metadata.modified_at = SystemTime::now();
            metadata.entry_count += 1;
        }
        
        // 5. Add to TinyLogFS transaction builders (if reference available)
        if let Some(tinylogfs_ref) = self.tinylogfs.upgrade() {
            let operation = super::FilesystemOperation::UpdateDirectory {
                node_id: self.node_id.clone(),
                entries: self.get_all_entries()
                    .map_err(|e| TinyFSError::Other(e.to_string()))?,
            };
            
            // Note: This would need proper error handling in practice
            if let Ok(tinylogfs_ref) = tinylogfs_ref.try_borrow() {
                let operation = super::FilesystemOperation::UpdateDirectory {
                    node_id: self.node_id.clone(),
                    entries: self.get_all_entries()
                        .map_err(|e| TinyFSError::Other(e.to_string()))?,
                };
                
                // Add operation to TinyLogFS transaction state
                if let Err(e) = tinylogfs_ref.add_pending_operation(&operation) {
                    eprintln!("Failed to add pending operation: {}", e);
                }
            }
        }
        
        Ok(())
    }
    
    fn iter(&self) -> Result<Box<dyn Iterator<Item = (String, NodeRef)>>, TinyFSError> {
        // Load all entries from cache, then return iterator
        let cache = self.entry_cache.borrow();
        let entries: Vec<(String, NodeRef)> = cache.iter()
            .map(|(name, cached)| (name.clone(), cached.node_ref.clone()))
            .collect();
        
        Ok(Box::new(entries.into_iter()))
    }
}
