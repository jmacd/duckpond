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
use async_trait::async_trait;

/// Arrow-native filesystem backend using Delta Lake for persistence
pub struct OpLogBackend {
    /// Delta Lake store path
    store_path: String,
    
    /// DataFusion session for queries
    session_ctx: SessionContext,
    
    /// Current transaction batch accumulator
    pending_records: std::sync::Arc<tokio::sync::Mutex<Vec<Record>>>,
    
    /// Unique table name for this backend instance
    table_name: String,
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
        
        // Use a consistent table name based on the store path so reopened backends can access the same data
        let table_name = format!("oplog_store");
        
        // Create empty in-memory table with OplogEntry schema first
        use crate::delta::{Record, ForArrow};
        let empty_records: Vec<Record> = Vec::new();
        let empty_batch = serde_arrow::to_record_batch(&Record::for_arrow(), &empty_records)
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        let provider = datafusion::datasource::memory::MemTable::try_new(
            empty_batch.schema(), 
            vec![vec![empty_batch]]
        ).map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        session_ctx.register_table(&table_name, Arc::new(provider))
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        // Now load existing data if any
        let backend = OpLogBackend {
            store_path: store_path.to_string(),
            session_ctx,
            pending_records: std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new())),
            table_name: table_name.clone(),
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
    pub async fn add_pending_record(&self, entry: OplogEntry) -> Result<(), TinyLogFSError> {
        println!("OpLogBackend::add_pending_record() - adding entry: part_id={}, node_id={}, file_type={}", 
                 entry.part_id, entry.node_id, entry.file_type);
        
        // Serialize OplogEntry to Record
        let content = self.serialize_oplog_entry(&entry)?;
        let record = Record {
            part_id: entry.part_id.clone(),
            timestamp: chrono::Utc::now().timestamp_micros(),
            version: 1, // For simplicity, could be improved
            content,
        };
        
        self.pending_records.lock().await.push(record);
        println!("OpLogBackend::add_pending_record() - successfully added to pending records");
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
    pub async fn commit_internal(&self) -> Result<usize, TinyLogFSError> {
        let records = {
            let mut pending = self.pending_records.lock().await;
            let records = pending.drain(..).collect::<Vec<_>>();
            println!("OpLogBackend::commit() - found {} pending records", records.len());
            for (i, record) in records.iter().enumerate() {
                println!("  Record {}: part_id={}, timestamp={}", i, record.part_id, record.timestamp);
            }
            records
        };
        
        if records.is_empty() {
            println!("OpLogBackend::commit() - no pending records to commit");
            return Ok(0);
        }

        // Convert records to RecordBatch
        let batch = serde_arrow::to_record_batch(&Record::for_arrow(), &records)?;
        println!("OpLogBackend::commit() - created RecordBatch with {} rows, {} columns", 
                 batch.num_rows(), batch.num_columns());

        // Write to Delta table
        let table = DeltaOps::try_from_uri(&self.store_path).await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;

        println!("OpLogBackend::commit() - writing to Delta Lake at {}", self.store_path);
        DeltaOps(table.into())
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        println!("OpLogBackend::commit() - successfully wrote to Delta Lake, refreshing table");

        // Refresh in-memory table with new data
        self.refresh_memory_table().await?;
        
        println!("OpLogBackend::commit() - successfully refreshed in-memory table");
        Ok(records.len())
    }
    
    /// Refresh the in-memory DataFusion table with latest Delta Lake data
    async fn refresh_memory_table(&self) -> Result<(), TinyLogFSError> {
        println!("OpLogBackend::refresh_memory_table() - attempting to load from {}", self.store_path);
        
        // Check if Delta table exists
        if !std::path::Path::new(&self.store_path).exists() {
            println!("OpLogBackend::refresh_memory_table() - store path does not exist, nothing to load");
            return Ok(());
        }
        
        // Re-read all data from Delta table using DataFusion
        match deltalake::open_table(&self.store_path).await {
            Ok(table) => {
                println!("OpLogBackend::refresh_memory_table() - successfully opened Delta table");
                
                // Try to get some info about the table
                if let Ok(files) = table.get_file_uris() {
                    let file_count: usize = files.count();
                    println!("OpLogBackend::refresh_memory_table() - Delta table has {} files", file_count);
                } else {
                    println!("OpLogBackend::refresh_memory_table() - could not get file count");
                }
                
                // Print table schema for debugging
                let table_schema = table.schema();
                println!("OpLogBackend::refresh_memory_table() - table schema: {:?}", table_schema);
                
                // Register the table - if one already exists, it will be replaced
                let table_arc = Arc::new(table);
                let _ = self.session_ctx.register_table(&self.table_name, table_arc);
                println!("OpLogBackend::refresh_memory_table() - registered table as {}", self.table_name);
                
                // Test query to see if data is accessible - let's try a more specific query
                let test_sql = format!("SELECT * FROM {} LIMIT 5", self.table_name);
                println!("OpLogBackend::refresh_memory_table() - running test query: {}", test_sql);
                match self.session_ctx.sql(&test_sql).await {
                    Ok(df) => {
                        match df.collect().await {
                            Ok(batches) => {
                                println!("OpLogBackend::refresh_memory_table() - test query returned {} batches", batches.len());
                                if let Some(batch) = batches.first() {
                                    println!("OpLogBackend::refresh_memory_table() - first batch has {} rows, {} columns", batch.num_rows(), batch.num_columns());
                                    if batch.num_rows() > 0 {
                                        println!("OpLogBackend::refresh_memory_table() - batch schema: {:?}", batch.schema());
                                        // Print first row data for debugging
                                        for col_idx in 0..batch.num_columns() {
                                            let column = batch.column(col_idx);
                                            let schema = batch.schema();
                                            let field = schema.field(col_idx);
                                            println!("OpLogBackend::refresh_memory_table() - column '{}': {} values", field.name(), column.len());
                                        }
                                    }
                                } else {
                                    println!("OpLogBackend::refresh_memory_table() - no batches returned");
                                }
                            }
                            Err(e) => println!("OpLogBackend::refresh_memory_table() - failed to collect: {}", e)
                        }
                    }
                    Err(e) => println!("OpLogBackend::refresh_memory_table() - test query failed: {}", e)
                }
            }
            Err(e) => {
                println!("OpLogBackend::refresh_memory_table() - failed to open Delta table: {}", e);
                return Err(TinyLogFSError::Arrow(e.to_string()));
            }
        }
        
        Ok(())
    }
    
    /// Query for a specific node by ID
    pub async fn get_node(&self, node_id: &str) -> Result<Option<OplogEntry>, TinyLogFSError> {
        let sql = format!(
            "SELECT part_id, timestamp, version, content FROM {} WHERE part_id = '{}' ORDER BY timestamp DESC LIMIT 1",
            self.table_name, node_id
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

#[async_trait]
impl FilesystemBackend for OpLogBackend {
    async fn create_file(&self, content: &[u8], parent_node_id: Option<&str>) -> tinyfs::Result<FileHandle> {
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
        self.add_pending_record(entry).await
            .map_err(|e| tinyfs::Error::Other(format!("OpLog error: {}", e)))?;
        
        // Create Arrow-backed file handle with content
        let oplog_file = super::file::OpLogFile::new_with_content(node_id, self.store_path.clone(), content.to_vec());
        Ok(super::file::OpLogFile::create_handle(oplog_file))
    }
    
    async fn create_directory(&self) -> tinyfs::Result<DirHandle> {
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
        self.add_pending_record(entry).await
            .map_err(|e| tinyfs::Error::Other(format!("OpLog error: {}", e)))?;
        
        // Create Arrow-backed directory handle with DataFusion session access
        // Instead of backend references, pass the session context for queries
        let oplog_dir = crate::tinylogfs::directory::OpLogDirectory::new_with_session(
            node_id, 
            self.session_ctx.clone(),
            self.table_name.clone(),
            self.store_path.clone()
        );
        Ok(crate::tinylogfs::directory::OpLogDirectory::create_handle(oplog_dir))
    }
    
    async fn create_root_directory(&self) -> tinyfs::Result<DirHandle> {
        // Root directory always uses node_id "0000000000000000" (hex for 0)
        let node_id = "0000000000000000".to_string();
        
        println!("OpLogBackend::create_root_directory() - creating root with node_id: {}", node_id);
        
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
        self.add_pending_record(entry).await
            .map_err(|e| tinyfs::Error::Other(format!("OpLog error: {}", e)))?;
        
        // Create Arrow-backed directory handle with DataFusion session access
        let oplog_dir = crate::tinylogfs::directory::OpLogDirectory::new_with_session(
            node_id, 
            self.session_ctx.clone(),
            self.table_name.clone(),
            self.store_path.clone()
        );
        Ok(crate::tinylogfs::directory::OpLogDirectory::create_handle(oplog_dir))
    }
    
    async fn create_symlink(&self, target: &str, parent_node_id: Option<&str>) -> tinyfs::Result<SymlinkHandle> {
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
        self.add_pending_record(entry).await
            .map_err(|e| tinyfs::Error::Other(format!("OpLog error: {}", e)))?;
        
        // Create Arrow-backed symlink handle
        let oplog_symlink = super::symlink::OpLogSymlink::new(node_id, PathBuf::from(target), self.store_path.clone());
        Ok(super::symlink::OpLogSymlink::create_handle(oplog_symlink))
    }
    
    /// Commit any pending operations to persistent storage
    /// Returns the number of operations committed
    async fn commit(&self) -> tinyfs::Result<usize> {
        // Delegate to the internal commit method
        self.commit_internal().await
            .map_err(|e| tinyfs::Error::Other(format!("OpLog commit error: {}", e)))
    }
    
    /// Check if this backend has an existing root directory that should be restored
    /// For OpLogBackend, we query the Delta Lake store for any existing directory entries
    async fn restore_root_directory(&self) -> tinyfs::Result<Option<DirHandle>> {
        println!("OpLogBackend::restore_root_directory() - checking for existing root directory");
        
        // Check if Delta table exists and has any directory entries
        if !std::path::Path::new(&self.store_path).exists() {
            println!("OpLogBackend::restore_root_directory() - store path does not exist, no root to restore");
            return Ok(None);
        }

        // Instead of using DataFusion, read directly from Delta Lake using DeltaOps
        println!("OpLogBackend::restore_root_directory() - reading directly from Delta Lake");
        
        use deltalake::DeltaOps;
        use futures::stream::StreamExt;
        
        match DeltaOps::try_from_uri(&self.store_path).await {
            Ok(delta_ops) => {
                println!("OpLogBackend::restore_root_directory() - opened Delta Lake table");
                
                match delta_ops.load().await {
                    Ok((_table, stream)) => {
                        println!("OpLogBackend::restore_root_directory() - created data stream");
                        
                        // Collect all batches
                        let batches: Vec<_> = stream.collect().await;
                        println!("OpLogBackend::restore_root_directory() - collected {} batches", batches.len());
                        
                        for (batch_idx, batch_result) in batches.iter().enumerate() {
                            match batch_result {
                                Ok(batch) => {
                                    println!("OpLogBackend::restore_root_directory() - batch {} has {} rows", batch_idx, batch.num_rows());
                                    
                                    if batch.num_rows() > 0 {
                                        // Look for content column
                                        if let Some(content_array) = batch.column_by_name("content") {
                                            if let Some(content_array) = content_array.as_any().downcast_ref::<arrow_array::BinaryArray>() {
                                                println!("OpLogBackend::restore_root_directory() - examining {} records for directories", batch.num_rows());
                                                
                                                // Check each record's content to find a directory
                                                for i in 0..batch.num_rows() {
                                                    let content_bytes = content_array.value(i);
                                                    println!("OpLogBackend::restore_root_directory() - record {} has {} bytes of content", i, content_bytes.len());
                                                    
                                                    // Try to deserialize the OplogEntry from the content
                                                    match self.deserialize_oplog_entry(content_bytes) {
                                                        Ok(oplog_entry) => {
                                                            println!("OpLogBackend::restore_root_directory() - record {} is {} with node_id: {}", i, oplog_entry.file_type, oplog_entry.node_id);
                                                            if oplog_entry.file_type == "directory" {
                                                                // Check if this is the root directory (node_id = "0000000000000000")
                                                                if oplog_entry.node_id == "0000000000000000" {
                                                                    println!("OpLogBackend::restore_root_directory() - ✅ found ROOT directory with node_id: {}", oplog_entry.node_id);
                                                                    
                                                                    // Create directory handle for the existing root
                                                                    let oplog_dir = crate::tinylogfs::directory::OpLogDirectory::new_with_session(
                                                                        oplog_entry.node_id.clone(),
                                                                        self.session_ctx.clone(),
                                                                        self.table_name.clone(),
                                                                        self.store_path.clone()
                                                                    );
                                                                    return Ok(Some(crate::tinylogfs::directory::OpLogDirectory::create_handle(oplog_dir)));
                                                                } else {
                                                                    println!("OpLogBackend::restore_root_directory() - found non-root directory with node_id: {}, continuing search", oplog_entry.node_id);
                                                                }
                                                            }
                                                        }
                                                        Err(e) => {
                                                            println!("OpLogBackend::restore_root_directory() - failed to deserialize record {}: {}", i, e);
                                                            continue;
                                                        }
                                                    }
                                                }
                                            } else {
                                                println!("OpLogBackend::restore_root_directory() - could not cast content column to BinaryArray");
                                            }
                                        } else {
                                            println!("OpLogBackend::restore_root_directory() - could not find content column in batch {}", batch_idx);
                                        }
                                    }
                                }
                                Err(e) => {
                                    println!("OpLogBackend::restore_root_directory() - batch {} error: {}", batch_idx, e);
                                }
                            }
                        }
                        
                        println!("OpLogBackend::restore_root_directory() - processed all batches, no directories found");
                        Ok(None)
                    }
                    Err(e) => {
                        println!("OpLogBackend::restore_root_directory() - failed to load Delta Lake data: {}", e);
                        Ok(None)
                    }
                }
            }
            Err(e) => {
                println!("OpLogBackend::restore_root_directory() - failed to open Delta Lake table: {}", e);
                Ok(None)
            }
        }
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
            "SELECT content FROM {} WHERE part_id = '{}' ORDER BY timestamp DESC LIMIT 1",
            self.table_name, node_id
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
