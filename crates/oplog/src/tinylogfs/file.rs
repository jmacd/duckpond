// Arrow-backed File implementation for TinyFS
use super::{TinyLogFSError, OplogEntry};
use tinyfs::{File, FileHandle};
use datafusion::prelude::SessionContext;
use std::sync::Arc;

/// Arrow-backed file implementation using DataFusion for queries
pub struct OpLogFile {
    /// Unique node identifier
    node_id: String,
    
    /// Path to the Delta Lake store
    store_path: String,
    
    /// DataFusion session for queries (created on demand)
    session_ctx: Option<SessionContext>,
    
    /// Cached file content for synchronous access
    cached_content: Vec<u8>,
    
    /// Dirty flag to track when content needs to be persisted
    dirty: bool,
}

impl OpLogFile {
    /// Create a new OpLogFile
    pub fn new(node_id: String, store_path: String) -> Self {
        OpLogFile {
            node_id,
            store_path,
            session_ctx: None,
            cached_content: Vec::new(),
            dirty: false,
        }
    }
    
    /// Create a new OpLogFile with initial content
    pub fn new_with_content(node_id: String, store_path: String, content: Vec<u8>) -> Self {
        OpLogFile {
            node_id,
            store_path,
            session_ctx: None,
            cached_content: content,
            dirty: false,
        }
    }
    
    /// Create a file handle for TinyFS integration
    pub fn create_handle(oplog_file: OpLogFile) -> FileHandle {
        FileHandle::new(std::rc::Rc::new(std::cell::RefCell::new(Box::new(oplog_file))))
    }
    
    /// Get or create DataFusion session context
    async fn get_session_ctx(&mut self) -> Result<&SessionContext, TinyLogFSError> {
        if self.session_ctx.is_none() {
            let session_ctx = SessionContext::new();
            let store_path = self.store_path.clone(); // Clone to avoid borrow issues
            
            // Register the Delta table for queries
            let table = deltalake::open_table(&store_path).await
                .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            session_ctx.register_table("oplog", Arc::new(table))
                .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            
            self.session_ctx = Some(session_ctx);
        }
        
        Ok(self.session_ctx.as_ref().unwrap())
    }
    
    /// Read the latest file content from Delta Lake using DataFusion
    async fn read_content_from_store(&mut self) -> Result<Vec<u8>, TinyLogFSError> {
        let node_id = self.node_id.clone(); // Clone to avoid borrow issues
        let session_ctx = self.get_session_ctx().await?;
        
        let sql = format!(
            "SELECT content FROM oplog WHERE part_id = '{}' ORDER BY timestamp DESC LIMIT 1",
            node_id
        );
        
        let df = session_ctx.sql(&sql).await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        let batch = df.collect().await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        if batch.is_empty() || batch[0].num_rows() == 0 {
            return Ok(Vec::new()); // File not found or empty
        }
        
        // Deserialize the content back to OplogEntry and extract file content
        let content_array = batch[0].column_by_name("content").unwrap()
            .as_any().downcast_ref::<arrow_array::BinaryArray>().unwrap();
        let content_bytes = content_array.value(0);
        
        let entry = self.deserialize_oplog_entry(content_bytes)?;
        Ok(entry.content)
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
    
    /// Ensure content is loaded and cached
    fn ensure_content_loaded(&self) -> tinyfs::Result<()> {
        if self.cached_content.is_empty() {
            // For now, return an error if content is not pre-loaded
            // In a real implementation, this would trigger an async load operation
            return Err(tinyfs::Error::Other("File content not loaded. OpLogFile requires content to be pre-loaded during creation.".to_string()));
        }
        Ok(())
    }
    
    /// Mark file as dirty for persistence
    fn mark_dirty(&mut self) {
        self.dirty = true;
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
            timestamp: chrono::Utc::now().timestamp(),
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

impl File for OpLogFile {
    fn content(&self) -> tinyfs::Result<&[u8]> {
        // Ensure content is loaded from OpLog storage
        self.ensure_content_loaded()?;
        
        // Return reference to cached content
        Ok(&self.cached_content)
    }
    
    fn write_content(&mut self, content: &[u8]) -> tinyfs::Result<()> {
        // Update cached content and mark as dirty for later persistence
        self.cached_content = content.to_vec();
        self.mark_dirty();
        Ok(())
    }
}

// Note: The synchronous File trait interface is challenging with async storage.
// Real implementation would need either:
// 1. Async File trait (breaking change to TinyFS)
// 2. Background async runtime with blocking calls
// 3. Local caching with async background updates
// 4. Redesign to use streaming/iterator patterns
