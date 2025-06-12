// Arrow-backed Directory implementation for TinyFS integration
// 
// New architecture: Directories have backend access and use DataFusion queries
// to combine committed (Delta Lake) and pending (Arrow record batches) data.
// No cached state except for pending operations.

use super::{TinyLogFSError, OplogEntry, DirectoryEntry};
use tinyfs::{DirHandle, Directory, NodeRef};
use datafusion::prelude::*;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Arrow-backed directory implementation using DataFusion for queries
#[derive(Debug)]
pub struct OpLogDirectory {
    /// Unique node identifier
    node_id: String,
    
    /// Reference to the backend for querying committed data
    backend: std::rc::Weak<std::cell::RefCell<super::backend::OpLogBackend>>,
    
    /// Pending directory operations as Arrow record batch
    /// Only holds uncommitted changes - committed data is queried from backend
    pending_operations: std::cell::RefCell<Option<RecordBatch>>,
}

impl OpLogDirectory {
    /// Create a new OpLogDirectory with backend reference
    pub fn new(
        node_id: String, 
        backend: std::rc::Weak<std::cell::RefCell<super::backend::OpLogBackend>>
    ) -> Self {
        OpLogDirectory {
            node_id,
            backend,
            pending_operations: std::cell::RefCell::new(None),
        }
    }
    
    /// Create a directory handle for TinyFS integration
    pub fn create_handle(oplog_dir: OpLogDirectory) -> DirHandle {
        use std::rc::Rc;
        use std::cell::RefCell;
        
        DirHandle::new(Rc::new(RefCell::new(Box::new(oplog_dir))))
    }
    
    /// Get all directory entries by combining committed and pending data
    async fn get_all_entries(&self) -> Result<Vec<DirectoryEntry>, TinyLogFSError> {
        let backend_ref = self.backend.upgrade()
            .ok_or_else(|| TinyLogFSError::Arrow("Backend reference dropped".to_string()))?;
        
        let committed_entries = self.query_committed_entries(&backend_ref).await?;
        let pending_entries = self.get_pending_entries()?;
        
        // Combine committed and pending entries, with pending taking precedence
        Ok(self.merge_entries(committed_entries, pending_entries))
    }
    
    /// Query committed directory entries from Delta Lake via backend
    async fn query_committed_entries(
        &self, 
        backend: &std::rc::Rc<std::cell::RefCell<super::backend::OpLogBackend>>
    ) -> Result<Vec<DirectoryEntry>, TinyLogFSError> {
        // Delegate to backend for querying committed data
        backend.borrow().query_directory_entries(&self.node_id).await
    }
    
    /// Get pending directory entries from in-memory Arrow record batch
    fn get_pending_entries(&self) -> Result<Vec<DirectoryEntry>, TinyLogFSError> {
        let pending = self.pending_operations.borrow();
        
        match pending.as_ref() {
            Some(batch) => {
                // Deserialize DirectoryEntry records from the batch
                let entries: Vec<DirectoryEntry> = serde_arrow::from_record_batch(batch)?;
                Ok(entries)
            },
            None => Ok(Vec::new())
        }
    }
    
    /// Merge committed and pending entries, with pending taking precedence
    fn merge_entries(
        &self, 
        committed: Vec<DirectoryEntry>, 
        pending: Vec<DirectoryEntry>
    ) -> Vec<DirectoryEntry> {
        use std::collections::HashMap;
        
        // Start with committed entries
        let mut entries_map: HashMap<String, DirectoryEntry> = committed
            .into_iter()
            .map(|entry| (entry.name.clone(), entry))
            .collect();
        
        // Override with pending entries
        for entry in pending {
            entries_map.insert(entry.name.clone(), entry);
        }
        
        entries_map.into_values().collect()
    }
    
    /// Add a child entry to pending operations
    pub fn add_child_pending(&self, name: String, node_ref: NodeRef) -> Result<(), TinyLogFSError> {
        let entry = DirectoryEntry {
            name,
            child: format!("{:?}", node_ref), // Convert NodeRef to string representation
        };
        
        self.add_pending_entry(entry)
    }
    
    /// Add a DirectoryEntry to pending operations
    fn add_pending_entry(&self, entry: DirectoryEntry) -> Result<(), TinyLogFSError> {
        use crate::delta::ForArrow;
        
        // Convert entry to Arrow record batch
        let batch = serde_arrow::to_record_batch(&DirectoryEntry::for_arrow(), &[entry])?;
        
        // Add to or replace pending operations
        let mut pending = self.pending_operations.borrow_mut();
        
        match pending.as_ref() {
            Some(existing_batch) => {
                // Concatenate with existing batch
                let batches = vec![existing_batch.clone(), batch];
                let combined = arrow::compute::concat_batches(
                    existing_batch.schema().as_ref(), 
                    &batches
                )?;
                *pending = Some(combined);
            },
            None => {
                *pending = Some(batch);
            }
        }
        
        Ok(())
    }
    
    /// Commit pending operations to backend
    pub async fn commit_pending(&self) -> Result<(), TinyLogFSError> {
        let pending = self.pending_operations.borrow().clone();
        
        if let Some(batch) = pending {
            let backend_ref = self.backend.upgrade()
                .ok_or_else(|| TinyLogFSError::Arrow("Backend reference dropped".to_string()))?;
            
            // Convert batch to OplogEntry
            let entries: Vec<DirectoryEntry> = serde_arrow::from_record_batch(&batch)?;
            let oplog_entry = OplogEntry {
                part_id: self.node_id.clone(),
                node_id: self.node_id.clone(),
                file_type: "directory".to_string(),
                content: self.serialize_directory_entries(&entries)?,
            };
            
            // Add to backend's pending records
            backend_ref.borrow().add_pending_record(oplog_entry)?;
            
            // Clear pending operations
            *self.pending_operations.borrow_mut() = None;
        }
        
        Ok(())
    }
    
    /// Serialize directory entries to bytes
    fn serialize_directory_entries(&self, entries: &[DirectoryEntry]) -> Result<Vec<u8>, TinyLogFSError> {
        use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
        use crate::delta::ForArrow;
        
        let batch = serde_arrow::to_record_batch(&DirectoryEntry::for_arrow(), &entries.to_vec())?;
        
        let mut buffer = Vec::new();
        let options = IpcWriteOptions::default();
        let mut writer = StreamWriter::try_new_with_options(&mut buffer, batch.schema().as_ref(), options)
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            
        writer.write(&batch)
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            
        writer.finish()
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        Ok(buffer)
    }
    
    /// Reconstruct NodeRef from string representation
    fn reconstruct_node_ref(&self, child_repr: &str) -> Result<NodeRef, TinyLogFSError> {
        // For now, create a simple node ref - this would need proper implementation
        // based on how NodeRef is actually structured in your system
        Ok(NodeRef::new()) // Placeholder - replace with actual reconstruction logic
    }
}

// Implement TinyFS Directory trait
impl Directory for OpLogDirectory {
    fn lookup(&self, name: &str) -> Option<NodeRef> {
        // Use async-to-sync bridge to query entries
        let rt = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(_) => return None,
        };
        
        let entries = match rt.block_on(self.get_all_entries()) {
            Ok(entries) => entries,
            Err(_) => return None,
        };
        
        // Find entry by name
        for entry in entries {
            if entry.name == name {
                return self.reconstruct_node_ref(&entry.child).ok();
            }
        }
        
        None
    }
    
    fn list(&self) -> Vec<String> {
        // Use async-to-sync bridge to query entries
        let rt = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(_) => return Vec::new(),
        };
        
        let entries = match rt.block_on(self.get_all_entries()) {
            Ok(entries) => entries,
            Err(_) => return Vec::new(),
        };
        
        entries.into_iter().map(|entry| entry.name).collect()
    }
    
    fn insert(&mut self, name: String, node: NodeRef) -> Result<(), Box<dyn std::error::Error>> {
        self.add_child_pending(name, node)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
    }
    
    fn remove(&mut self, name: &str) -> Result<Option<NodeRef>, Box<dyn std::error::Error>> {
        // For removal, we need to add a "tombstone" entry or handle differently
        // This is simplified - a full implementation might need deletion markers
        
        // First find the existing entry
        let existing = self.lookup(name);
        
        if existing.is_some() {
            // Add a deletion marker to pending operations
            // This would need a more sophisticated approach in a real implementation
            // For now, we'll implement this as a placeholder
            
            // TODO: Implement proper deletion with tombstones or similar mechanism
        }
        
        Ok(existing)
    }
    
    fn node_id(&self) -> String {
        self.node_id.clone()
    }
}
