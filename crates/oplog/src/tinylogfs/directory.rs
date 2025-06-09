// Arrow-backed Directory implementation for TinyFS integration
use super::{TinyLogFSError, OplogEntry, DirectoryEntry};
use tinyfs::{DirHandle, Directory, NodeRef};
use std::collections::BTreeMap;

/// Arrow-backed directory implementation using DataFusion for queries
#[derive(Debug)]
pub struct OpLogDirectory {
    /// Unique node identifier
    node_id: String,
    
    /// Path to the Delta Lake store  
    store_path: String,
    
    /// In-memory directory entries for fast access
    entries: std::cell::RefCell<BTreeMap<String, NodeRef>>,
    
    /// Dirty flag to track when sync is needed
    dirty: std::cell::RefCell<bool>,
    
    /// Flag to track whether entries have been loaded from store
    loaded: std::cell::RefCell<bool>,
}

impl OpLogDirectory {
    /// Create a new OpLogDirectory
    pub fn new(node_id: String, store_path: String) -> Self {
        OpLogDirectory {
            node_id,
            store_path,
            entries: std::cell::RefCell::new(BTreeMap::new()),
            dirty: std::cell::RefCell::new(false),
            loaded: std::cell::RefCell::new(false),
        }
    }
    
    /// Create a directory handle for TinyFS integration
    pub fn create_handle(oplog_dir: OpLogDirectory) -> DirHandle {
        // Create a proper TinyFS directory handle using the standard pattern
        use std::rc::Rc;
        use std::cell::RefCell;
        
        DirHandle::new(Rc::new(RefCell::new(Box::new(oplog_dir))))
    }
    
    /// Mark directory as dirty for sync
    fn mark_dirty(&self) {
        *self.dirty.borrow_mut() = true;
    }
    
    /// Check if directory needs sync
    pub fn is_dirty(&self) -> bool {
        *self.dirty.borrow()
    }
    
    /// Sync directory state to OpLog (async operation for background sync)
    pub async fn sync_to_oplog(&self) -> Result<(), TinyLogFSError> {
        if !self.is_dirty() {
            return Ok(());
        }
        
        // Convert current entries to DirectoryEntry format
        let entries = self.entries.borrow();
        let directory_entries: Vec<DirectoryEntry> = entries
            .iter()
            .map(|(name, node_ref)| DirectoryEntry {
                name: name.clone(),
                child: format!("{:?}", node_ref), // Convert NodeRef to string representation
            })
            .collect();
        
        // Serialize directory entries to OpLog format
        let serialized = self.serialize_directory_entries(&directory_entries)?;
        
        let entry = OplogEntry {
            part_id: self.node_id.clone(),
            node_id: self.node_id.clone(),
            file_type: "directory".to_string(),
            content: serialized,
        };
        
        // Write directly to Delta Lake using the same approach as the backend
        self.write_entry_to_delta_lake(entry).await?;
        
        // Mark as clean after successful write
        *self.dirty.borrow_mut() = false;
        
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
    
    /// Write an OplogEntry directly to Delta Lake (same approach as OpLogBackend)
    async fn write_entry_to_delta_lake(&self, entry: OplogEntry) -> Result<(), TinyLogFSError> {
        use crate::delta::{Record, ForArrow};
        use deltalake::{DeltaOps, protocol::SaveMode};
        
        // Serialize OplogEntry to Record (same as OpLogBackend.add_pending_record)
        let content = self.serialize_oplog_entry(&entry)?;
        let record = Record {
            part_id: entry.part_id.clone(),
            timestamp: chrono::Utc::now().timestamp(),
            version: 1,
            content,
        };
        
        // Convert record to RecordBatch
        let batch = serde_arrow::to_record_batch(&Record::for_arrow(), &[record])?;
        
        // Write to Delta table (same as OpLogBackend.commit)
        let table = DeltaOps::try_from_uri(&self.store_path).await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        DeltaOps(table.into())
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        Ok(())
    }
    
    /// Serialize OplogEntry as Arrow IPC bytes (same as OpLogBackend)
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

impl Directory for OpLogDirectory {
    fn get(&self, name: &str) -> tinyfs::Result<Option<NodeRef>> {
        let entries = self.entries.borrow();
        let result = entries.get(name).cloned();
        println!("OpLogDirectory::get('{}') -> {:?}", name, result.is_some());
        if result.is_none() {
            println!("  Available entries: {:?}", entries.keys().collect::<Vec<_>>());
        }
        Ok(result)
    }
    
    fn insert(&mut self, name: String, node: NodeRef) -> tinyfs::Result<()> {
        println!("OpLogDirectory::insert('{}', node_id={:?})", name, node.id());
        let mut entries = self.entries.borrow_mut();
        entries.insert(name.clone(), node);
        self.mark_dirty();
        println!("  Directory entries after insert: {:?}", entries.keys().collect::<Vec<_>>());
        Ok(())
    }
    
    fn iter<'a>(&'a self) -> tinyfs::Result<Box<dyn Iterator<Item = (String, NodeRef)> + 'a>> {
        let entries = self.entries.borrow();
        let iter = entries.iter().map(|(k, v)| (k.clone(), v.clone()));
        Ok(Box::new(iter.collect::<Vec<_>>().into_iter()))
    }
}
