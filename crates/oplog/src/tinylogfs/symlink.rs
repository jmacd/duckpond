// Arrow-backed symlink implementation for TinyLogFS
use tinyfs::{Symlink, SymlinkHandle};
use std::path::PathBuf;
use async_trait::async_trait;

/// Symlink implementation backed by Arrow record batch storage
#[derive(Debug)]
pub struct OpLogSymlink {
    /// Unique node identifier
    node_id: String,
    
    /// Symlink target path
    target: std::sync::Arc<tokio::sync::Mutex<PathBuf>>,
    
    /// Store path for persistence  
    store_path: String,
    
    /// Dirty flag to track when target needs to be persisted
    dirty: std::sync::Arc<tokio::sync::Mutex<bool>>,
}

impl OpLogSymlink {
    /// Create a new Arrow-backed symlink
    pub fn new(
        node_id: String,
        target: PathBuf,
        store_path: String,
    ) -> Self {
        Self {
            node_id,
            target: std::sync::Arc::new(tokio::sync::Mutex::new(target)),
            store_path,
            dirty: std::sync::Arc::new(tokio::sync::Mutex::new(false)),
        }
    }
    
    /// Create a symlink handle from OpLog symlink
    pub fn create_handle(oplog_symlink: OpLogSymlink) -> SymlinkHandle {
        SymlinkHandle::new(std::sync::Arc::new(tokio::sync::Mutex::new(Box::new(oplog_symlink))))
    }
    
    /// Mark symlink as dirty for persistence
    async fn mark_dirty(&self) {
        *self.dirty.lock().await = true;
    }
    
    /// Check if symlink has unsaved changes
    pub async fn is_dirty(&self) -> bool {
        *self.dirty.lock().await
    }
    
    /// Update the symlink target
    pub async fn set_target(&self, target: PathBuf) {
        *self.target.lock().await = target;
        self.mark_dirty().await;
    }
    
    /// Persist symlink target to Delta Lake
    pub async fn sync_to_oplog(&self) -> Result<(), super::TinyLogFSError> {
        if !self.is_dirty().await {
            return Ok(());
        }
        
        let target_path = self.target.lock().await.clone();
        let target_bytes = target_path.to_string_lossy().as_bytes().to_vec();
        
        // Create OplogEntry for this symlink
        let entry = super::OplogEntry {
            part_id: self.node_id.clone(),
            node_id: self.node_id.clone(),
            file_type: "symlink".to_string(),
            content: target_bytes,
        };
        
        // Write directly to Delta Lake using the same approach as file/directory
        self.write_entry_to_delta_lake(entry).await?;
        
        // Mark as clean after successful write
        *self.dirty.lock().await = false;
        
        Ok(())
    }
    
    /// Write an OplogEntry directly to Delta Lake (same approach as OpLogFile/OpLogDirectory)
    async fn write_entry_to_delta_lake(&self, entry: super::OplogEntry) -> Result<(), super::TinyLogFSError> {
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
            .map_err(|e| super::TinyLogFSError::Arrow(e.to_string()))?;
        
        DeltaOps(table.into())
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .map_err(|e| super::TinyLogFSError::Arrow(e.to_string()))?;
        
        Ok(())
    }
    
    /// Serialize OplogEntry as Arrow IPC bytes (same as OpLogFile/OpLogDirectory)
    fn serialize_oplog_entry(&self, entry: &super::OplogEntry) -> Result<Vec<u8>, super::TinyLogFSError> {
        use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
        use crate::delta::ForArrow;
        
        let batch = serde_arrow::to_record_batch(&super::OplogEntry::for_arrow(), &[entry.clone()])?;
        
        let mut buffer = Vec::new();
        let options = IpcWriteOptions::default();
        let mut writer = StreamWriter::try_new_with_options(&mut buffer, batch.schema().as_ref(), options)
            .map_err(|e| super::TinyLogFSError::Arrow(e.to_string()))?;
        writer.write(&batch)
            .map_err(|e| super::TinyLogFSError::Arrow(e.to_string()))?;
        writer.finish()
            .map_err(|e| super::TinyLogFSError::Arrow(e.to_string()))?;
        
        Ok(buffer)
    }
}

#[async_trait::async_trait]
impl Symlink for OpLogSymlink {
    async fn readlink(&self) -> tinyfs::Result<PathBuf> {
        Ok(self.target.lock().await.clone())
    }
}
