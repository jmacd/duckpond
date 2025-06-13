// OpLog-backed filesystem backend for TinyFS
//
// ## Partition Design Implementation
// This backend implements the TinyLogFS partition design where:
// - Directories: part_id = node_id (they are their own partition)
// - Files: part_id = parent_directory_node_id (stored in parent's partition)
// - Symlinks: part_id = parent_directory_node_id (stored in parent's partition)
//
// This ensures that each directory stores itself and its immediate children
// (except child directories) together in the same partition for efficient querying.
//
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
        
        // Create empty in-memory table with OplogEntry schema first
        use crate::delta::{Record, ForArrow};
        let empty_records: Vec<Record> = Vec::new();
        let empty_batch = serde_arrow::to_record_batch(&Record::for_arrow(), &empty_records)
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        let provider = datafusion::datasource::memory::MemTable::try_new(
            empty_batch.schema(), 
            vec![vec![empty_batch]]
        ).map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        session_ctx.register_table("oplog", Arc::new(provider))
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        // Now load existing data if any
        let backend = OpLogBackend {
            store_path: store_path.to_string(),
            session_ctx,
            pending_records: std::cell::RefCell::new(Vec::new()),
        };
        
        // Load existing data from Delta Lake
        backend.refresh_memory_table().await?;
        
        Ok(backend)
    }
    
    /// Generate a random 64-bit node ID encoded as 16 hex digits
    fn generate_node_id() -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        use std::time::{SystemTime, UNIX_EPOCH};
        
        let mut hasher = DefaultHasher::new();
        
        // Use current time and a random component for uniqueness
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        
        timestamp.hash(&mut hasher);
        
        // Add some randomness from thread id and process id if available
        std::thread::current().id().hash(&mut hasher);
        
        let hash = hasher.finish();
        format!("{:016x}", hash)
    }
    
    /// Add a record to the pending transaction (public for directory use)
    pub fn add_pending_record(&self, entry: OplogEntry) -> Result<(), TinyLogFSError> {
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
        
        // Refresh in-memory table with new data
        self.refresh_memory_table().await?;
        
        Ok(records.len())
    }
    
    /// Refresh the in-memory DataFusion table with latest Delta Lake data
    async fn refresh_memory_table(&self) -> Result<(), TinyLogFSError> {
        // Re-read all data from Delta table using DataFusion
        let table = deltalake::open_table(&self.store_path).await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        // Use Delta table directly as table provider for DataFusion
        self.session_ctx.register_table("oplog", Arc::new(table))
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        Ok(())
    }
    
    /// Synchronous version of query_directory_entries using thread spawn
    pub fn query_directory_entries_sync(&self, node_id: &str) -> Result<Vec<super::schema::DirectoryEntry>, TinyLogFSError> {
        let session_ctx = self.session_ctx.clone();
        let node_id = node_id.to_string();
        
        let handle = std::thread::spawn(move || {
            // Create a new tokio runtime for this thread
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| TinyLogFSError::Arrow(format!("Failed to create tokio runtime: {}", e)))?;
            
            rt.block_on(async {
                println!("OpLogBackend::query_directory_entries_sync() - querying for node_id: {}", node_id);
                
                // Query for the latest directory entry for this node_id
                let sql = format!(
                    "SELECT content FROM oplog WHERE part_id = '{}' ORDER BY timestamp DESC LIMIT 1",
                    node_id
                );
                
                let df = session_ctx.sql(&sql).await
                    .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
                let batches = df.collect().await
                    .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
                
                if batches.is_empty() || batches[0].num_rows() == 0 {
                    println!("OpLogBackend::query_directory_entries_sync() - no entries found for node_id: {}", node_id);
                    return Ok(Vec::new());
                }
                
                // Extract content (Arrow IPC bytes) from the first row
                let content_array = batches[0].column_by_name("content").unwrap()
                    .as_any().downcast_ref::<BinaryArray>().unwrap();
                let content_bytes = content_array.value(0);
                
                // Deserialize OplogEntry
                let oplog_entry = deserialize_oplog_entry(content_bytes)?;
                println!("OpLogBackend::query_directory_entries_sync() - found OplogEntry with file_type: {}", oplog_entry.file_type);
                
                // If this is a directory entry, deserialize the directory entries from its content
                if oplog_entry.file_type == "directory" {
                    let entries = deserialize_directory_entries(&oplog_entry.content)?;
                    println!("OpLogBackend::query_directory_entries_sync() - deserialized {} directory entries", entries.len());
                    Ok(entries)
                } else {
                    println!("OpLogBackend::query_directory_entries_sync() - not a directory entry");
                    Ok(Vec::new())
                }
            })
        });

        handle.join()
            .map_err(|_| TinyLogFSError::Arrow("Thread join failed during directory query".to_string()))?
    }
}

/// Deserialize OplogEntry from Arrow IPC bytes
fn deserialize_oplog_entry(bytes: &[u8]) -> Result<OplogEntry, TinyLogFSError> {
    use arrow::ipc::reader::StreamReader;
    
    let mut reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
    
    let batch = reader.next().unwrap()
        .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
    
    let entries: Vec<OplogEntry> = serde_arrow::from_record_batch(&batch)
        .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
    
    entries.into_iter().next()
        .ok_or_else(|| TinyLogFSError::Arrow("Empty OplogEntry deserialization".to_string()))
}

/// Deserialize directory entries from Arrow IPC bytes
fn deserialize_directory_entries(bytes: &[u8]) -> Result<Vec<super::schema::DirectoryEntry>, TinyLogFSError> {
    use arrow::ipc::reader::StreamReader;
    
    let mut reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
    
    let batch = reader.next().unwrap()
        .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
    
    let entries: Vec<super::schema::DirectoryEntry> = serde_arrow::from_record_batch(&batch)
        .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
    Ok(entries)
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
    fn create_file(&self, content: &[u8], parent_node_id: Option<&str>) -> tinyfs::Result<FileHandle> {
        let node_id = Self::generate_node_id();
        
        // Use parent directory's node_id as part_id for proper partitioning
        let part_id = parent_node_id.unwrap_or(&node_id).to_string();
        
        // Create OplogEntry for file
        let entry = OplogEntry {
            part_id,
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
        
        // Directories are their own partition (part_id == node_id)
        let entry = OplogEntry {
            part_id: node_id.clone(),
            node_id: node_id.clone(), 
            file_type: "directory".to_string(),
            content: serialized_entries,
        };
        
        // Add to pending transaction
        self.add_pending_record(entry)
            .map_err(|e| tinyfs::Error::Other(format!("OpLog error: {}", e)))?;
        
        // Create Arrow-backed directory handle with DataFusion session access
        // Instead of backend references, pass the session context for queries
        let oplog_dir = crate::tinylogfs::directory::OpLogDirectory::new_with_session(
            node_id, 
            self.session_ctx.clone()
        );
        Ok(crate::tinylogfs::directory::OpLogDirectory::create_handle(oplog_dir))
    }
    
    fn create_symlink(&self, target: &str, parent_node_id: Option<&str>) -> tinyfs::Result<SymlinkHandle> {
        let node_id = Self::generate_node_id();
        
        // Use parent directory's node_id as part_id for proper partitioning
        let part_id = parent_node_id.unwrap_or(&node_id).to_string();
        
        // Create OplogEntry for symlink
        let entry = OplogEntry {
            part_id,
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
    
    /// Commit any pending operations to persistent storage
    /// Returns the number of operations committed
    fn commit(&self) -> tinyfs::Result<usize> {
        // Use thread-based approach to run async code in sync context
        // This avoids tokio runtime conflicts in test environments
        let store_path = self.store_path.clone();
        let pending_records = {
            let mut pending = self.pending_records.borrow_mut();
            let records = pending.drain(..).collect::<Vec<_>>();
            records
        };
        
        if pending_records.is_empty() {
            return Ok(0);
        }
        
        // Use std::thread::spawn to run async code in a separate thread
        let handle = std::thread::spawn(move || {
            // Create a new tokio runtime for this thread
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| tinyfs::Error::Other(format!("Failed to create tokio runtime: {}", e)))?;
            
            rt.block_on(async {
                use crate::delta::{Record, ForArrow};
                use deltalake::{DeltaOps, protocol::SaveMode};
                
                // Convert records to RecordBatch
                let batch = serde_arrow::to_record_batch(&Record::for_arrow(), &pending_records)
                    .map_err(|e| tinyfs::Error::Other(format!("Arrow serialization error: {}", e)))?;
                
                // Write to Delta table
                let table = DeltaOps::try_from_uri(&store_path).await
                    .map_err(|e| tinyfs::Error::Other(format!("Delta Lake error: {}", e)))?;
                
                DeltaOps(table.into())
                    .write(vec![batch])
                    .with_save_mode(SaveMode::Append)
                    .await
                    .map_err(|e| tinyfs::Error::Other(format!("Delta Lake write error: {}", e)))?;
                
                Ok(pending_records.len())
            })
        });

        handle.join()
            .map_err(|_| tinyfs::Error::Other("Thread join failed during commit".to_string()))?
    }
}

impl OpLogBackend {
    /// Serialize DirectoryEntry records as Arrow IPC bytes
    pub fn serialize_directory_entries(&self, entries: &[DirectoryEntry]) -> Result<Vec<u8>, TinyLogFSError> {
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
    
    /// Query directory entries for a specific directory node
    /// This is the key method that enables the new directory architecture
    pub async fn query_directory_entries(&self, node_id: &str) -> Result<Vec<super::schema::DirectoryEntry>, TinyLogFSError> {
        println!("OpLogBackend::query_directory_entries() - querying for node_id: {}", node_id);
        
        // Query for the latest directory entry for this node_id
        let sql = format!(
            "SELECT content FROM oplog WHERE part_id = '{}' ORDER BY timestamp DESC LIMIT 1",
            node_id
        );
        
        let df = self.session_ctx.sql(&sql).await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        let batches = df.collect().await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        if batches.is_empty() || batches[0].num_rows() == 0 {
            println!("OpLogBackend::query_directory_entries() - no entries found for node_id: {}", node_id);
            return Ok(Vec::new());
        }
        
        // Extract content (Arrow IPC bytes) from the first row
        let content_array = batches[0].column_by_name("content").unwrap()
            .as_any().downcast_ref::<BinaryArray>().unwrap();
        let content_bytes = content_array.value(0);
        
        // Deserialize OplogEntry
        let oplog_entry = self.deserialize_oplog_entry(content_bytes)?;
        println!("OpLogBackend::query_directory_entries() - found OplogEntry with file_type: {}", oplog_entry.file_type);
        
        // If this is a directory entry, deserialize the directory entries from its content
        if oplog_entry.file_type == "directory" {
            let entries = self.deserialize_directory_entries(&oplog_entry.content)?;
            println!("OpLogBackend::query_directory_entries() - deserialized {} directory entries", entries.len());
            Ok(entries)
        } else {
            println!("OpLogBackend::query_directory_entries() - not a directory entry");
            Ok(Vec::new())
        }
    }
    
    /// Deserialize directory entries from Arrow IPC bytes
    fn deserialize_directory_entries(&self, bytes: &[u8]) -> Result<Vec<super::schema::DirectoryEntry>, TinyLogFSError> {
        use arrow::ipc::reader::StreamReader;
        
        let mut reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        let batch = reader.next().unwrap()
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        let entries: Vec<super::schema::DirectoryEntry> = serde_arrow::from_record_batch(&batch)?;
        Ok(entries)
    }
}
