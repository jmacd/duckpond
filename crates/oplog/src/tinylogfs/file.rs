// Arrow-backed File implementation for TinyFS
use super::{TinyLogFSError, OplogEntry};
use tinyfs::{File, FileHandle};
use std::sync::Arc;


/// Arrow-backed file implementation using DataFusion for queries
pub struct OpLogFile {
    /// Unique node identifier
    node_id: String,
    
    /// Path to the Delta Lake store
    store_path: String,
    
    /// Cached file content for synchronous access
    cached_content: Vec<u8>,
    
    /// Dirty flag to track when content needs to be persisted
    dirty: bool,
    
    /// Flag to track if content has been loaded from store
    loaded: Arc<tokio::sync::Mutex<bool>>,
}

impl OpLogFile {
    /// Create a new OpLogFile
    pub fn new(node_id: String, store_path: String) -> Self {
        OpLogFile {
            node_id,
            store_path,
            cached_content: Vec::new(),
            dirty: false,
            loaded: Arc::new(tokio::sync::Mutex::new(false)),
        }
    }
    
    /// Create a new OpLogFile with initial content
    pub fn new_with_content(node_id: String, store_path: String, content: Vec<u8>) -> Self {
        OpLogFile {
            node_id,
            store_path,
            cached_content: content,
            dirty: false,
            loaded: Arc::new(tokio::sync::Mutex::new(true)), // Mark as loaded since we have initial content
        }
    }
    
    /// Create a file handle for TinyFS integration
    pub fn create_handle(oplog_file: OpLogFile) -> FileHandle {
        FileHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(oplog_file))))
    }
    
    /// Check if file has unsaved changes
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }
    
    /// Persist cached content to Delta Lake
    pub async fn sync_to_oplog(&mut self) -> Result<(), TinyLogFSError> {
        if !self.is_dirty() {
            return Ok(());
        }
        
        let content = self.cached_content.clone();
        
        // Create OplogEntry for this file
        let entry = OplogEntry {
            part_id: self.node_id.clone(),
            node_id: self.node_id.clone(),
            file_type: "file".to_string(),
            content,
        };
        
        // Write directly to Delta Lake using the same approach as directory
        self.write_entry_to_delta_lake(entry).await?;
        
        // Mark as clean after successful write
        self.dirty = false;
        
        Ok(())
    }
    
    /// Write an OplogEntry directly to Delta Lake (same approach as OpLogDirectory)
    async fn write_entry_to_delta_lake(&self, entry: OplogEntry) -> Result<(), TinyLogFSError> {
        use crate::delta::{Record, ForArrow};
        use deltalake::{DeltaOps, protocol::SaveMode};
        
        // Serialize OplogEntry to Record
        let content = self.serialize_oplog_entry(&entry)?;
        let record = Record {
            part_id: entry.part_id.clone(),
            timestamp: chrono::Utc::now().timestamp_micros(),
            version: 1,
            content,
        };
        
        // Convert record to RecordBatch
        let batch = serde_arrow::to_record_batch(&Record::for_arrow(), &[record])?;
        
        // Write to Delta table
        let table = DeltaOps::try_from_uri(&self.store_path).await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        DeltaOps(table.into())
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        Ok(())
    }
    
    /// Serialize OplogEntry as Arrow IPC bytes (same as OpLogDirectory)
    fn serialize_oplog_entry(&self, entry: &OplogEntry) -> Result<Vec<u8>, TinyLogFSError> {
        use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
        use crate::delta::ForArrow;
        
        let batch = serde_arrow::to_record_batch(&OplogEntry::for_arrow(), &[entry.clone()])?;
        
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

#[async_trait::async_trait]
impl File for OpLogFile {
    async fn content(&self) -> tinyfs::Result<&[u8]> {
        // Check if content is already loaded
        let loaded = *self.loaded.lock().await;
        if loaded {
            return Ok(&self.cached_content);
        }
        
        // For now, return empty content - full async loading would require mutable self
        // The real loading happens at creation time via new_with_content
        Ok(&[])
    }
    
    async fn write_content(&mut self, content: &[u8]) -> tinyfs::Result<()> {
        // Update cached content and mark as dirty for later persistence
        self.cached_content = content.to_vec();
        self.dirty = true;
        *self.loaded.lock().await = true;
        Ok(())
    }
}
// 3. Local caching with async background updates
// 4. Redesign to use streaming/iterator patterns
