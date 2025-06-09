// OpLog-backed filesystem backend for TinyFS
use super::error::TinyLogFSError;
use super::schema::{OplogEntry, DirectoryEntry};
use tinyfs::{FilesystemBackend, DirHandle, FileHandle, SymlinkHandle};
use crate::delta::Record;
use crate::delta::ForArrow;
use deltalake::{DeltaOps, protocol::SaveMode};
use datafusion::prelude::SessionContext;
use arrow_array::BinaryArray;
use std::sync::Arc;
use std::path::PathBuf;
use uuid::Uuid;

/// Arrow-native filesystem backend using Delta Lake for persistence
pub struct OpLogBackend {
    /// Delta Lake store path
    store_path: String,
    
    /// DataFusion session for queries
    session_ctx: SessionContext,
    
    /// Current transaction batch accumulator
    pending_records: std::cell::RefCell<Vec<Record>>,
}

impl OpLogBackend {
    /// Create a new OpLog backend
    pub async fn new(store_path: &str) -> Result<Self, TinyLogFSError> {
        // Initialize Delta table if it doesn't exist
        if !std::path::Path::new(store_path).exists() {
            super::schema::create_oplog_table(store_path).await
                .map_err(TinyLogFSError::OpLog)?;
        }
        
        let session_ctx = SessionContext::new();
        
        // Register the Delta table for queries
        let table = deltalake::open_table(store_path).await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        session_ctx.register_table("oplog", Arc::new(table))
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        Ok(OpLogBackend {
            store_path: store_path.to_string(),
            session_ctx,
            pending_records: std::cell::RefCell::new(Vec::new()),
        })
    }
    
    /// Generate a new unique node ID
    fn generate_node_id() -> String {
        Uuid::new_v4().to_string()
    }
    
    /// Add a record to the pending transaction
    fn add_pending_record(&self, entry: OplogEntry) -> Result<(), TinyLogFSError> {
        // Serialize OplogEntry to Record
        let content = self.serialize_oplog_entry(&entry)?;
        let record = Record {
            part_id: entry.part_id.clone(),
            timestamp: chrono::Utc::now().timestamp(),
            version: 1, // For simplicity, could be improved
            content,
        };
        
        self.pending_records.borrow_mut().push(record);
        Ok(())
    }
    
    /// Serialize OplogEntry as Arrow IPC bytes
    fn serialize_oplog_entry(&self, entry: &OplogEntry) -> Result<Vec<u8>, TinyLogFSError> {
        use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
        
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
    
    /// Commit pending records to Delta Lake
    pub async fn commit(&self) -> Result<usize, TinyLogFSError> {
        let records = {
            let mut pending = self.pending_records.borrow_mut();
            let records = pending.drain(..).collect::<Vec<_>>();
            records
        };
        
        if records.is_empty() {
            return Ok(0);
        }
        
        // Convert records to RecordBatch
        let batch = serde_arrow::to_record_batch(&Record::for_arrow(), &records)?;
        
        // Write to Delta table
        let table = DeltaOps::try_from_uri(&self.store_path).await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        DeltaOps(table.into())
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        Ok(records.len())
    }
    
    /// Query for a specific node by ID
    pub async fn get_node(&self, node_id: &str) -> Result<Option<OplogEntry>, TinyLogFSError> {
        let sql = format!(
            "SELECT part_id, timestamp, version, content FROM oplog WHERE part_id = '{}' ORDER BY timestamp DESC LIMIT 1",
            node_id
        );
        
        let df = self.session_ctx.sql(&sql).await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        let batch = df.collect().await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        if batch.is_empty() || batch[0].num_rows() == 0 {
            return Ok(None);
        }
        
        // Deserialize the content back to OplogEntry
        let content_array = batch[0].column_by_name("content").unwrap()
            .as_any().downcast_ref::<BinaryArray>().unwrap();
        let content_bytes = content_array.value(0);
        
        let entry = self.deserialize_oplog_entry(content_bytes)?;
        Ok(Some(entry))
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
}

impl FilesystemBackend for OpLogBackend {
    fn create_file(&self, content: &[u8]) -> tinyfs::Result<FileHandle> {
        let node_id = Self::generate_node_id();
        
        // Create OplogEntry for file
        let entry = OplogEntry {
            part_id: node_id.clone(),
            node_id: node_id.clone(),
            file_type: "file".to_string(),
            content: content.to_vec(),
        };
        
        // Add to pending transaction
        self.add_pending_record(entry)
            .map_err(|e| tinyfs::Error::Other(format!("OpLog error: {}", e)))?;
        
        // Create Arrow-backed file handle with content
        let oplog_file = super::file::OpLogFile::new_with_content(node_id, self.store_path.clone(), content.to_vec());
        Ok(super::file::OpLogFile::create_handle(oplog_file))
    }
    
    fn create_directory(&self) -> tinyfs::Result<DirHandle> {
        let node_id = Self::generate_node_id();
        
        // Create OplogEntry for directory with empty directory entries
        let directory_entries: Vec<DirectoryEntry> = Vec::new();
        let serialized_entries = self.serialize_directory_entries(&directory_entries)
            .map_err(|e| tinyfs::Error::Other(format!("Serialization error: {}", e)))?;
        
        let entry = OplogEntry {
            part_id: node_id.clone(),
            node_id: node_id.clone(), 
            file_type: "directory".to_string(),
            content: serialized_entries,
        };
        
        // Add to pending transaction
        self.add_pending_record(entry)
            .map_err(|e| tinyfs::Error::Other(format!("OpLog error: {}", e)))?;
        
        // Create Arrow-backed directory handle
        let oplog_dir = super::directory::OpLogDirectory::new(node_id, self.store_path.clone());
        Ok(super::directory::OpLogDirectory::create_handle(oplog_dir))
    }
    
    fn create_symlink(&self, target: &str) -> tinyfs::Result<SymlinkHandle> {
        let node_id = Self::generate_node_id();
        
        // Create OplogEntry for symlink
        let entry = OplogEntry {
            part_id: node_id.clone(),
            node_id: node_id.clone(),
            file_type: "symlink".to_string(),
            content: target.as_bytes().to_vec(),
        };
        
        // Add to pending transaction
        self.add_pending_record(entry)
            .map_err(|e| tinyfs::Error::Other(format!("OpLog error: {}", e)))?;
        
        // Create Arrow-backed symlink handle
        let oplog_symlink = super::symlink::OpLogSymlink::new(node_id, PathBuf::from(target), self.store_path.clone());
        Ok(super::symlink::OpLogSymlink::create_handle(oplog_symlink))
    }
}

impl OpLogBackend {
    /// Serialize DirectoryEntry records as Arrow IPC bytes
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
