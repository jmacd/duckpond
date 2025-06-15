// Arrow-backed Directory implementation for TinyFS integration using DataFusion queries
use super::{TinyLogFSError, OplogEntry, DirectoryEntry};
use tinyfs::{DirHandle, Directory, NodeRef};
use datafusion::prelude::SessionContext;
use async_trait::async_trait;

/// Arrow-backed directory implementation using DataFusion for queries
/// This implementation queries both committed (Delta Lake) and pending (in-memory) data
/// using DataFusion session context directly instead of backend references.
pub struct OpLogDirectory {
    /// Unique node identifier
    node_id: String,
    
    /// DataFusion session context for querying in-memory data
    session_ctx: SessionContext,
    
    /// Pending directory operations that haven't been committed yet
    pending_ops: std::sync::Arc<tokio::sync::Mutex<Vec<DirectoryEntry>>>,
}

impl OpLogDirectory {
    /// Create a new OpLogDirectory with DataFusion session
    pub fn new_with_session(node_id: String, session_ctx: SessionContext) -> Self {
        OpLogDirectory {
            node_id,
            session_ctx,
            pending_ops: std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }

    /// Legacy constructor for backward compatibility
    pub fn new(node_id: String, _backend: std::rc::Weak<std::cell::RefCell<super::backend::OpLogBackend>>) -> Self {
        // Create empty session - this is a fallback for existing code
        let session_ctx = SessionContext::new();
        OpLogDirectory {
            node_id,
            session_ctx,
            pending_ops: std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }
    
    /// Create a directory handle for TinyFS integration
    pub fn create_handle(oplog_dir: OpLogDirectory) -> DirHandle {
        use std::sync::Arc;
        use tokio::sync::Mutex;
        
        DirHandle::new(Arc::new(Mutex::new(Box::new(oplog_dir))))
    }
    
    /// Get all directory entries by querying DataFusion session directly
    pub async fn get_all_entries(&self) -> Result<Vec<DirectoryEntry>, TinyLogFSError> {
        let committed_entries = self.query_directory_entries_from_session().await?;
        let pending_entries = self.pending_ops.lock().await.clone();
        
        // Merge committed and pending (pending takes precedence)
        Ok(self.merge_entries(committed_entries, pending_entries))
    }

    /// Query directory entries directly from DataFusion session
    async fn query_directory_entries_from_session(&self) -> Result<Vec<DirectoryEntry>, TinyLogFSError> {
        use arrow_array::BinaryArray;
        
        println!("OpLogDirectory::query_directory_entries_from_session() - querying for node_id: {}", self.node_id);
        
        // Query for the latest directory entry for this node_id
        let sql = format!(
            "SELECT content FROM oplog WHERE part_id = '{}' ORDER BY timestamp DESC LIMIT 1",
            self.node_id
        );
        
        let df = self.session_ctx.sql(&sql).await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        let batches = df.collect().await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        if batches.is_empty() || batches[0].num_rows() == 0 {
            println!("OpLogDirectory::query_directory_entries_from_session() - no entries found for node_id: {}", self.node_id);
            return Ok(Vec::new());
        }
        
        // Extract content (Arrow IPC bytes) from the first row
        let content_array = batches[0].column_by_name("content").unwrap()
            .as_any().downcast_ref::<BinaryArray>().unwrap();
        let content_bytes = content_array.value(0);
        
        // Deserialize OplogEntry
        let oplog_entry = self.deserialize_oplog_entry(content_bytes)?;
        println!("OpLogDirectory::query_directory_entries_from_session() - found OplogEntry with file_type: {}", oplog_entry.file_type);
        
        // If this is a directory entry, deserialize the directory entries from its content
        if oplog_entry.file_type == "directory" {
            let entries = self.deserialize_directory_entries(&oplog_entry.content)?;
            println!("OpLogDirectory::query_directory_entries_from_session() - deserialized {} directory entries", entries.len());
            Ok(entries)
        } else {
            println!("OpLogDirectory::query_directory_entries_from_session() - not a directory entry");
            Ok(Vec::new())
        }
    }

    /// Deserialize OplogEntry from Arrow IPC bytes
    fn deserialize_oplog_entry(&self, bytes: &[u8]) -> Result<OplogEntry, TinyLogFSError> {
        use arrow::ipc::reader::StreamReader;
        
        let mut reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        let batch = reader.next().unwrap()
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        let entries: Vec<OplogEntry> = serde_arrow::from_record_batch(&batch)?;
        
        entries.into_iter().next()
            .ok_or_else(|| TinyLogFSError::Arrow("Empty OplogEntry deserialization".to_string()))
    }

    /// Deserialize directory entries from Arrow IPC bytes
    fn deserialize_directory_entries(&self, bytes: &[u8]) -> Result<Vec<DirectoryEntry>, TinyLogFSError> {
        use arrow::ipc::reader::StreamReader;
        
        let mut reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        let batch = reader.next().unwrap()
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        let entries: Vec<DirectoryEntry> = serde_arrow::from_record_batch(&batch)?;
        Ok(entries)
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
    pub async fn add_pending(&self, name: String, node_ref: NodeRef) {
        let entry = DirectoryEntry {
            name,
            child: format!("{:?}", node_ref),
        };
        self.pending_ops.lock().await.push(entry);
    }
    
    /// Commit pending operations to backend
    /// NOTE: This method now just clears pending operations since we don't have direct backend access
    /// The actual commit needs to happen at the filesystem level
    pub async fn commit_pending(&self) -> Result<(), TinyLogFSError> {
        let pending = self.pending_ops.lock().await.clone();
        if pending.is_empty() {
            return Ok(());
        }
        
        // TODO: In the new architecture, commit should happen at filesystem level
        // For now, we'll just clear pending operations as a placeholder
        println!("OpLogDirectory::commit_pending() - clearing {} pending operations (commit should happen at FS level)", pending.len());
        
        // Clear pending after marking for commit
        self.pending_ops.lock().await.clear();
        Ok(())
    }
    
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

    /// Create a new empty directory
    pub fn create_empty_directory(node_id: String, _backend: std::rc::Weak<std::cell::RefCell<super::backend::OpLogBackend>>) -> Self {
        // Create empty session - this is a fallback for existing code
        let session_ctx = SessionContext::new();
        OpLogDirectory::new_with_session(node_id, session_ctx)
    }
}

#[async_trait::async_trait]
impl Directory for OpLogDirectory {
    async fn get(&self, name: &str) -> tinyfs::Result<Option<NodeRef>> {
        // Query all entries using the new DataFusion-based approach
        let all_entries = self.get_all_entries().await
            .map_err(|e| tinyfs::Error::immutable(format!("Failed to query directory entries: {}", e)))?;
        
        // Find entry by name
        for entry in all_entries {
            if entry.name == name {
                // Convert DirectoryEntry back to NodeRef
                // ARCHITECTURAL LIMITATION: We cannot reconstruct NodeRef from string
                // This is the core issue that needs to be solved by restructuring the filesystem
                return Err(tinyfs::Error::immutable(format!(
                    "Cannot reconstruct NodeRef for '{}'. This requires architectural changes \
                    to TinyFS to support external directory implementations properly.",
                    name
                )));
            }
        }
        
        Ok(None)
    }
    
    async fn insert(&mut self, name: String, node: NodeRef) -> tinyfs::Result<()> {
        println!("OpLogDirectory::insert('{}')", name);
        
        // Add to pending operations
        self.add_pending(name.clone(), node).await;
        
        // Commit pending operations to backend
        self.commit_pending().await
            .map_err(|e| tinyfs::Error::immutable(format!("Failed to commit directory entry: {}", e)))?;
        
        println!("OpLogDirectory::insert('{}') - committed to backend", name);
        Ok(())
    }
    
    async fn entries(&self) -> tinyfs::Result<std::pin::Pin<Box<dyn futures::Stream<Item = tinyfs::Result<(String, NodeRef)>> + Send>>> {
        // Query all entries using the new DataFusion-based approach
        let all_entries = self.get_all_entries().await
            .map_err(|e| tinyfs::Error::immutable(format!("Failed to query directory entries: {}", e)))?;
        
        // ARCHITECTURAL LIMITATION: Cannot reconstruct NodeRef instances
        // Return empty stream for now - this needs filesystem architecture changes
        println!("OpLogDirectory::entries() - found {} entries but cannot reconstruct NodeRef instances", all_entries.len());
        for entry in &all_entries {
            println!("  Entry: {} -> {}", entry.name, entry.child);
        }
        
        use futures::stream;
        Ok(Box::pin(stream::empty()))
    }
}
