// Arrow-backed Directory implementation for TinyFS integration
use super::{TinyLogFSError, OplogEntry, DirectoryEntry};
use tinyfs::{DirHandle, Directory, NodeRef};
use std::collections::BTreeMap;
use arrow::array::Array; // For array methods like len()

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
    
    /// Lazy load directory entries from OpLog storage
    async fn load_from_oplog(&self) -> Result<(), TinyLogFSError> {
        // Check if already loaded
        if *self.loaded.borrow() {
            return Ok(());
        }
        
        // Query Delta Lake for directory entries
        let table = deltalake::open_table(&self.store_path).await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        // Query for this directory's entries
        let query = format!(
            "SELECT * FROM oplog_entries WHERE part_id = '{}' AND file_type = 'directory' ORDER BY timestamp DESC LIMIT 1",
            self.node_id
        );
        
        // Execute query using DataFusion
        use datafusion::prelude::*;
        let ctx = SessionContext::new();
        ctx.register_table("oplog_entries", std::sync::Arc::new(table))
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        let df = ctx.sql(&query).await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        let batches = df.collect().await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        // Process results if directory exists in OpLog
        if let Some(batch) = batches.first() {
            if batch.num_rows() > 0 {
                // Extract content column (Arrow IPC bytes)
                let content_array = batch.column_by_name("content")
                    .ok_or_else(|| TinyLogFSError::Arrow("Missing content column".to_string()))?
                    .as_any()
                    .downcast_ref::<arrow::array::BinaryArray>()
                    .ok_or_else(|| TinyLogFSError::Arrow("Content column not binary".to_string()))?;
                
                if content_array.len() > 0 {
                    let content_bytes = content_array.value(0);
                    
                    // Deserialize OplogEntry from content
                    let oplog_entry = self.deserialize_oplog_entry(content_bytes)?;
                    
                    // Deserialize DirectoryEntry records from OplogEntry content
                    let directory_entries = self.deserialize_directory_entries(&oplog_entry.content)?;
                    
                    // Reconstruct NodeRef instances and populate entries
                    let mut entries = self.entries.borrow_mut();
                    for dir_entry in directory_entries {
                        // Create a placeholder NodeRef - in a full implementation this would
                        // reconstruct the proper NodeRef from the child_node_id
                        let node_ref = self.reconstruct_node_ref(&dir_entry.child)?;
                        entries.insert(dir_entry.name, node_ref);
                    }
                }
            }
        }
        
        // Mark as loaded
        *self.loaded.borrow_mut() = true;
        
        Ok(())
    }
    
    /// Synchronously ensure entries are loaded from OpLog
    fn ensure_loaded(&self) -> Result<(), TinyLogFSError> {
        if *self.loaded.borrow() {
            return Ok(());
        }
        
        // For now, mark as loaded without actually loading to avoid async issues
        // This demonstrates the sync/async mismatch challenge
        *self.loaded.borrow_mut() = true;
        
        // TODO: Implement proper sync/async bridge for lazy loading
        // The real implementation would need to handle this constraint
        println!("OpLogDirectory::ensure_loaded() - marked as loaded but didn't load from Delta Lake (async/sync mismatch)");
        
        Ok(())
    }
    
    /// Reconstruct NodeRef from child node ID (placeholder implementation)
    fn reconstruct_node_ref(&self, _child_node_id: &str) -> Result<NodeRef, TinyLogFSError> {
        // This is a placeholder implementation - a full implementation would:
        // 1. Query OpLog for the child node's entry
        // 2. Determine the node type (file, directory, symlink)
        // 3. Create the appropriate NodeRef with proper handles
        
        // For now, return an error as this needs proper implementation
        // In the actual system, this would create proper NodeRef instances
        // based on the OpLog data for the child node
        Err(TinyLogFSError::Arrow(
            "NodeRef reconstruction not yet implemented - needs full OpLog query system".to_string()
        ))
    }
    
    /// Deserialize OplogEntry from Arrow IPC bytes
    fn deserialize_oplog_entry(&self, bytes: &[u8]) -> Result<OplogEntry, TinyLogFSError> {
        use arrow::ipc::reader::StreamReader;
        
        let mut reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        if let Some(batch) = reader.next() {
            let batch = batch.map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            let entries: Vec<OplogEntry> = serde_arrow::from_record_batch(&batch)?;
            entries.into_iter().next()
                .ok_or_else(|| TinyLogFSError::Arrow("No OplogEntry found".to_string()))
        } else {
            Err(TinyLogFSError::Arrow("No record batch found".to_string()))
        }
    }
    
    /// Deserialize DirectoryEntry records from bytes  
    fn deserialize_directory_entries(&self, bytes: &[u8]) -> Result<Vec<DirectoryEntry>, TinyLogFSError> {
        use arrow::ipc::reader::StreamReader;
        
        let mut reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        if let Some(batch) = reader.next() {
            let batch = batch.map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            let entries: Vec<DirectoryEntry> = serde_arrow::from_record_batch(&batch)?;
            Ok(entries)
        } else {
            Ok(vec![]) // Empty directory
        }
    }

    /// Create a new empty directory
    pub fn create_empty_directory(node_id: String, store_path: String) -> Self {
        let directory = OpLogDirectory::new(node_id, store_path);
        
        // Insert a special entry for the empty directory marker
        let empty_marker = DirectoryEntry {
            name: "__empty__".to_string(),
            child: "".to_string(), // No child node
        };
        
        // Serialize the empty directory marker
        let _ = directory.serialize_directory_entries(&vec![empty_marker]);
        
        directory
    }
}

impl Directory for OpLogDirectory {
    fn get(&self, name: &str) -> tinyfs::Result<Option<NodeRef>> {
        // Ensure entries are loaded from OpLog storage
        self.ensure_loaded().map_err(|e| tinyfs::Error::immutable(format!("Failed to load directory: {}", e)))?;
        
        let entries = self.entries.borrow();
        let result = entries.get(name).cloned();
        println!("OpLogDirectory::get('{}') -> {:?}", name, result.is_some());
        if result.is_none() {
            println!("  Available entries: {:?}", entries.keys().collect::<Vec<_>>());
        }
        Ok(result)
    }
    
    fn insert(&mut self, name: String, node: NodeRef) -> tinyfs::Result<()> {
        // Ensure entries are loaded first (in case this is a fresh instance)
        self.ensure_loaded().map_err(|e| tinyfs::Error::immutable(format!("Failed to load directory: {}", e)))?;
        
        println!("OpLogDirectory::insert('{}', node_id={:?})", name, node.id());
        let mut entries = self.entries.borrow_mut();
        entries.insert(name.clone(), node);
        self.mark_dirty();
        println!("  Directory entries after insert: {:?}", entries.keys().collect::<Vec<_>>());
        Ok(())
    }
    
    fn iter<'a>(&'a self) -> tinyfs::Result<Box<dyn Iterator<Item = (String, NodeRef)> + 'a>> {
        // Ensure entries are loaded from OpLog storage
        self.ensure_loaded().map_err(|e| tinyfs::Error::immutable(format!("Failed to load directory: {}", e)))?;
        
        let entries = self.entries.borrow();
        let iter = entries.iter().map(|(k, v)| (k.clone(), v.clone()));
        Ok(Box::new(iter.collect::<Vec<_>>().into_iter()))
    }
}
