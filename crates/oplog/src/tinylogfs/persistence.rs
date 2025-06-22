use super::error::TinyLogFSError;
use super::schema::{OplogEntry, DirectoryEntry, VersionedDirectoryEntry, OperationType, create_oplog_table};
use tinyfs::persistence::{PersistenceLayer, DirectoryOperation};
use tinyfs::{NodeID, NodeType, Result as TinyFSResult};
use crate::delta::{Record, ForArrow};
use datafusion::prelude::SessionContext;
use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use uuid::Uuid;
use chrono::Utc;

pub struct OpLogPersistence {
    store_path: String,
    session_ctx: SessionContext,
    pending_records: Arc<tokio::sync::Mutex<Vec<Record>>>,
    table_name: String,
    version_counter: Arc<tokio::sync::Mutex<i64>>,
}

impl OpLogPersistence {
    pub async fn new(store_path: &str) -> Result<Self, TinyLogFSError> {
        // Initialize Delta table if it doesn't exist
        if !std::path::Path::new(store_path).exists() {
            create_oplog_table(store_path).await
                .map_err(TinyLogFSError::OpLog)?;
        }
        
        let session_ctx = SessionContext::new();
        let table_name = format!("oplog_store_{}", Uuid::new_v4().simple());
        
        Ok(Self {
            store_path: store_path.to_string(),
            session_ctx,
            pending_records: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            table_name,
            version_counter: Arc::new(tokio::sync::Mutex::new(0)),
        })
    }
    
    async fn next_version(&self) -> Result<i64, TinyLogFSError> {
        let mut counter = self.version_counter.lock().await;
        *counter += 1;
        Ok(*counter)
    }
    
    /// Serialize VersionedDirectoryEntry records as Arrow IPC bytes
    fn serialize_directory_entries(&self, entries: &[VersionedDirectoryEntry]) -> Result<Vec<u8>, TinyLogFSError> {
        use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
        
        let batch = serde_arrow::to_record_batch(&VersionedDirectoryEntry::for_arrow(), &entries.to_vec())?;
        
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
    async fn commit_internal(&self) -> Result<(), TinyLogFSError> {
        use deltalake::{DeltaOps, protocol::SaveMode};
        
        let records = {
            let mut pending = self.pending_records.lock().await;
            let records = pending.drain(..).collect::<Vec<_>>();
            records
        };
        
        if records.is_empty() {
            return Ok(());
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
        
        Ok(())
    }
    
    /// Deserialize OplogEntry from Arrow IPC bytes
    fn deserialize_oplog_entry(&self, content: &[u8]) -> Result<OplogEntry, TinyLogFSError> {
        use arrow::ipc::reader::StreamReader;
        
        let mut reader = StreamReader::try_new(std::io::Cursor::new(content), None)
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        if let Some(batch) = reader.next() {
            let batch = batch.map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            let entries: Vec<OplogEntry> = serde_arrow::from_record_batch(&batch)?;
            entries.into_iter().next()
                .ok_or_else(|| TinyLogFSError::Arrow("Empty batch".to_string()))
        } else {
            Err(TinyLogFSError::Arrow("No data in stream".to_string()))
        }
    }
    
    /// Deserialize VersionedDirectoryEntry records from Arrow IPC bytes  
    fn deserialize_directory_entries(&self, content: &[u8]) -> Result<Vec<VersionedDirectoryEntry>, TinyLogFSError> {
        use arrow::ipc::reader::StreamReader;
        
        let mut reader = StreamReader::try_new(std::io::Cursor::new(content), None)
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        if let Some(batch) = reader.next() {
            let batch = batch.map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            let entries: Vec<VersionedDirectoryEntry> = serde_arrow::from_record_batch(&batch)?;
            Ok(entries)
        } else {
            Ok(Vec::new()) // Empty directory
        }
    }
    
    /// Query records from both committed (Delta Lake) and pending (in-memory) data
    /// This ensures TinyFS operations can see pending data before commit
    async fn query_records(&self, part_id: &str, _node_id: Option<&str>) -> Result<Vec<Record>, TinyLogFSError> {
        // Step 1: Get committed records from Delta Lake
        let mut committed_records = Vec::new();
        
        if std::path::Path::new(&self.store_path).exists() {
            // Open Delta table
            let table = deltalake::open_table(&self.store_path).await
                .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            
            // Query the table with DataFusion
            let ctx = datafusion::prelude::SessionContext::new();
            let table_name = format!("query_table_{}", Uuid::new_v4().simple());
            
            // Register the Delta table 
            ctx.register_table(&table_name, Arc::new(table))
                .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            
            // Execute query - only filter by part_id for now
            let sql = format!("SELECT * FROM {} WHERE part_id = '{}' ORDER BY version DESC", table_name, part_id);
            let df = ctx.sql(&sql).await
                .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            
            // Collect results
            let batches = df.collect().await
                .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            
            for batch in batches {
                let batch_records: Vec<Record> = serde_arrow::from_record_batch(&batch)?;
                committed_records.extend(batch_records);
            }
        }
        
        // Step 2: Get pending records from memory
        let pending_records = {
            let pending = self.pending_records.lock().await;
            pending.iter()
                .filter(|record| record.part_id == part_id)
                .cloned()
                .collect::<Vec<_>>()
        };
        
        // Step 3: Combine committed and pending records
        let mut all_records = committed_records;
        all_records.extend(pending_records);
        
        // Step 4: Sort by version (descending) to get latest first
        all_records.sort_by(|a, b| b.version.cmp(&a.version));
        
        // Step 5: If node_id was specified, filter the records by deserializing content
        if let Some(target_node_id) = _node_id {
            all_records = all_records.into_iter().filter(|record| {
                if let Ok(oplog_entry) = self.deserialize_oplog_entry(&record.content) {
                    oplog_entry.node_id == target_node_id
                } else {
                    false
                }
            }).collect();
        }
        
        Ok(all_records)
    }
    
    /// Query directory entries for a parent node
    async fn query_directory_entries(&self, parent_node_id: NodeID) -> Result<Vec<VersionedDirectoryEntry>, TinyLogFSError> {
        let part_id_str = parent_node_id.to_hex_string();
        
        // Query all records for this directory
        let records = self.query_records(&part_id_str, None).await?;
        
        let mut all_entries = Vec::new();
        
        for record in records {
            // Try to deserialize as OplogEntry first
            if let Ok(oplog_entry) = self.deserialize_oplog_entry(&record.content) {
                if oplog_entry.file_type == "directory" {
                    // This is directory content - deserialize the inner directory entries
                    if let Ok(dir_entries) = self.deserialize_directory_entries(&oplog_entry.content) {
                        all_entries.extend(dir_entries);
                    }
                }
            }
        }
        
        // Sort by version to get proper ordering
        all_entries.sort_by_key(|e| e.version);
        
        Ok(all_entries)
    }
}

#[async_trait]
impl PersistenceLayer for OpLogPersistence {
    async fn load_node(&self, node_id: NodeID, part_id: NodeID) -> TinyFSResult<NodeType> {
        let node_id_str = node_id.to_hex_string();
        let part_id_str = part_id.to_hex_string();
        
        // Query Delta Lake for the most recent record for this node
        let records = self.query_records(&part_id_str, Some(&node_id_str)).await
            .map_err(|e| tinyfs::Error::Other(format!("Query error: {}", e)))?;
        
        if let Some(record) = records.first() {
            // Deserialize the OplogEntry from the record content
            let oplog_entry = self.deserialize_oplog_entry(&record.content)
                .map_err(|e| tinyfs::Error::Other(format!("Deserialization error: {}", e)))?;
            
            // Convert OplogEntry to NodeType based on file_type
            match oplog_entry.file_type.as_str() {
                "file" => {
                    // For files, we need to create a proper file handle
                    // For Phase 4, we'll defer this to keep the implementation simple
                    Err(tinyfs::Error::Other("File loading via PersistenceLayer not yet implemented - use OpLogBackend for now".to_string()))
                }
                "directory" => {
                    // For directories, create an OpLogDirectory handle using the session context
                    let oplog_dir = super::directory::OpLogDirectory::new_with_session(
                        oplog_entry.node_id.clone(),
                        self.session_ctx.clone(),
                        self.table_name.clone(),
                        self.store_path.clone()
                    );
                    let dir_handle = super::directory::OpLogDirectory::create_handle(oplog_dir);
                    Ok(tinyfs::NodeType::Directory(dir_handle))
                }
                "symlink" => {
                    // For symlinks, we need to create a proper symlink handle
                    // For Phase 4, we'll defer this to keep the implementation simple
                    Err(tinyfs::Error::Other("Symlink loading via PersistenceLayer not yet implemented - use OpLogBackend for now".to_string()))
                }
                _ => Err(tinyfs::Error::Other(format!("Unknown node type: {}", oplog_entry.file_type)))
            }
        } else {
            // Node doesn't exist in database yet
            // For the root directory (NodeID::new(0)), create a new empty directory
            if node_id == NodeID::new(0) {
                let oplog_dir = super::directory::OpLogDirectory::new_with_session(
                    node_id_str,
                    self.session_ctx.clone(),
                    self.table_name.clone(),
                    self.store_path.clone()
                );
                let dir_handle = super::directory::OpLogDirectory::create_handle(oplog_dir);
                Ok(tinyfs::NodeType::Directory(dir_handle))
            } else {
                Err(tinyfs::Error::NotFound(std::path::PathBuf::from(format!("Node {} not found", node_id_str))))
            }
        }
    }
    
    async fn store_node(&self, node_id: NodeID, part_id: NodeID, node_type: &NodeType) -> TinyFSResult<()> {
        // Create a simple OplogEntry and serialize it into a Record
        let node_id_str = node_id.to_hex_string();
        let part_id_str = part_id.to_hex_string();
        
        // Create OplogEntry based on node type
        let (file_type, content) = match node_type {
            tinyfs::NodeType::File(file_handle) => {
                // For files, extract the content from the file handle
                let file_content = file_handle.content().await
                    .map_err(|e| tinyfs::Error::Other(format!("File content error: {}", e)))?;
                ("file".to_string(), file_content)
            }
            tinyfs::NodeType::Directory(_) => {
                // For directories, create empty directory entries
                let empty_entries: Vec<VersionedDirectoryEntry> = Vec::new();
                let content = self.serialize_directory_entries(&empty_entries)
                    .map_err(|e| tinyfs::Error::Other(format!("Serialization error: {}", e)))?;
                ("directory".to_string(), content)
            }
            tinyfs::NodeType::Symlink(symlink_handle) => {
                // For symlinks, serialize the target path
                let target = symlink_handle.readlink().await
                    .map_err(|e| tinyfs::Error::Other(format!("Symlink readlink error: {}", e)))?;
                let target_bytes = target.to_string_lossy().as_bytes().to_vec();
                ("symlink".to_string(), target_bytes)
            }
        };
        
        let oplog_entry = OplogEntry {
            part_id: part_id_str.clone(),
            node_id: node_id_str,
            file_type,
            content,
        };
        
        // Serialize the OplogEntry into a Record
        let content_bytes = self.serialize_oplog_entry(&oplog_entry)
            .map_err(|e| tinyfs::Error::Other(format!("OplogEntry serialization error: {}", e)))?;
        
        let version = self.next_version().await
            .map_err(|e| tinyfs::Error::Other(format!("Version error: {}", e)))?;
        
        let record = Record {
            part_id: part_id_str,
            timestamp: Utc::now().timestamp_micros(),
            version,
            content: content_bytes,
        };
        
        // Add to pending records
        self.pending_records.lock().await.push(record);
        Ok(())
    }
    
    async fn exists_node(&self, _node_id: NodeID, _part_id: NodeID) -> TinyFSResult<bool> {
        // For Phase 4 simplicity, assume nodes don't exist
        Ok(false)
    }
    
    async fn load_directory_entries(&self, parent_node_id: NodeID) -> TinyFSResult<HashMap<String, NodeID>> {
        let all_entries = self.query_directory_entries(parent_node_id).await
            .map_err(|e| tinyfs::Error::Other(format!("Query error: {}", e)))?;
        
        // Apply operations in version order to get current state
        let mut current_state = HashMap::new();
        
        for entry in all_entries.into_iter() {
            match entry.operation_type {
                crate::tinylogfs::schema::OperationType::Insert | crate::tinylogfs::schema::OperationType::Update => {
                    if let Ok(child_id) = NodeID::from_hex_string(&entry.child_node_id) {
                        current_state.insert(entry.name, child_id);
                    }
                },
                crate::tinylogfs::schema::OperationType::Delete => {
                    current_state.remove(&entry.name);
                }
            }
        }
        
        Ok(current_state)
    }
    
    async fn update_directory_entry(
        &self, 
        _parent_node_id: NodeID, 
        _entry_name: &str, 
        _operation: DirectoryOperation
    ) -> TinyFSResult<()> {
        // For Phase 4 simplicity, just succeed without doing anything
        // This maintains API compatibility while focusing on architecture
        Ok(())
    }
    
    async fn commit(&self) -> TinyFSResult<()> {
        // Commit all pending records to Delta Lake
        self.commit_internal().await
            .map_err(|e| tinyfs::Error::Other(format!("Commit error: {}", e)))
    }
    
    async fn rollback(&self) -> TinyFSResult<()> {
        // Clear pending records
        self.pending_records.lock().await.clear();
        Ok(())
    }
}
