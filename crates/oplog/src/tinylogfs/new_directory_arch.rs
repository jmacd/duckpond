// Test implementation of the new directory architecture
// This demonstrates how directories should work with backend access and DataFusion queries

use super::*;
use crate::delta::ForArrow;
use datafusion::prelude::*;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::rc::Weak;
use std::cell::RefCell;

/// New directory architecture that queries both committed and pending data
pub struct NewOpLogDirectory {
    node_id: String,
    backend: Weak<RefCell<super::backend::OpLogBackend>>,
    pending_ops: RefCell<Vec<DirectoryEntry>>,
}

impl NewOpLogDirectory {
    pub fn new(node_id: String, backend: Weak<RefCell<super::backend::OpLogBackend>>) -> Self {
        Self {
            node_id,
            backend,
            pending_ops: RefCell::new(Vec::new()),
        }
    }
    
    /// Get all directory entries by combining committed and pending data
    pub fn get_all_entries(&self) -> Result<Vec<DirectoryEntry>, TinyLogFSError> {
        let backend_ref = self.backend.upgrade()
            .ok_or_else(|| TinyLogFSError::Arrow("Backend reference dropped".to_string()))?;
        
        // Use synchronous runtime to query committed data
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| TinyLogFSError::Arrow(format!("Failed to create runtime: {}", e)))?;
        
        let committed_entries = rt.block_on(async {
            self.query_committed_entries(&backend_ref).await
        })?;
        
        let pending_entries = self.pending_ops.borrow().clone();
        
        // Merge committed and pending (pending takes precedence)
        Ok(self.merge_entries(committed_entries, pending_entries))
    }
    
    /// Query committed directory entries via backend
    async fn query_committed_entries(
        &self,
        backend: &Arc<RefCell<super::backend::OpLogBackend>>
    ) -> Result<Vec<DirectoryEntry>, TinyLogFSError> {
        // This is the key difference - delegate to backend for querying
        // Backend has DataFusion session context and can query Delta Lake properly
        backend.borrow().query_directory_entries(&self.node_id).await
    }
    
    /// Merge committed and pending entries
    fn merge_entries(&self, committed: Vec<DirectoryEntry>, pending: Vec<DirectoryEntry>) -> Vec<DirectoryEntry> {
        use std::collections::HashMap;
        
        let mut entries_map: HashMap<String, DirectoryEntry> = committed
            .into_iter()
            .map(|entry| (entry.name.clone(), entry))
            .collect();
        
        // Pending entries override committed entries
        for entry in pending {
            entries_map.insert(entry.name.clone(), entry);
        }
        
        entries_map.into_values().collect()
    }
    
    /// Add pending entry (for insert operations)
    pub fn add_pending(&self, name: String, node_ref: NodeRef) {
        let entry = DirectoryEntry {
            name,
            child: format!("{:?}", node_ref),
        };
        self.pending_ops.borrow_mut().push(entry);
    }
    
    /// Commit pending operations to backend
    pub fn commit_pending(&self) -> Result<(), TinyLogFSError> {
        let pending = self.pending_ops.borrow().clone();
        if pending.is_empty() {
            return Ok(());
        }
        
        let backend_ref = self.backend.upgrade()
            .ok_or_else(|| TinyLogFSError::Arrow("Backend reference dropped".to_string()))?;
        
        // Serialize pending entries to OplogEntry
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| TinyLogFSError::Arrow(format!("Failed to create runtime: {}", e)))?;
        
        rt.block_on(async {
            let serialized_entries = self.serialize_directory_entries(&pending)?;
            
            let oplog_entry = OplogEntry {
                part_id: self.node_id.clone(),
                node_id: self.node_id.clone(),
                file_type: "directory".to_string(),
                content: serialized_entries,
            };
            
            backend_ref.borrow().add_pending_record(oplog_entry)
        })?;
        
        // Clear pending after successful commit to backend
        self.pending_ops.borrow_mut().clear();
        Ok(())
    }
    
    fn serialize_directory_entries(&self, entries: &[DirectoryEntry]) -> Result<Vec<u8>, TinyLogFSError> {
        use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
        
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
}

// Test function to demonstrate the new architecture
#[cfg(test)]
mod test_new_arch {
    use super::*;
    
    #[tokio::test]
    async fn test_new_directory_architecture() {
        // This test shows how the new architecture would work
        // 1. Backend has DataFusion session for querying
        // 2. Directory gets backend reference 
        // 3. Directory queries both committed and pending data
        // 4. No cached state sync issues
        
        println!("New architecture test - this demonstrates the solution");
    }
}
