use super::error::TinyLogFSError;
use super::schema::{OplogEntry, VersionedDirectoryEntry, OperationType, create_oplog_table};
use super::delta_manager::DeltaTableManager;
use tinyfs::persistence::{PersistenceLayer, DirectoryOperation};
use tinyfs::{NodeID, NodeType, Result as TinyFSResult};
use crate::delta::{Record, ForArrow};
use datafusion::prelude::SessionContext;
use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use uuid::Uuid;
use chrono::Utc;

#[derive(Clone)]
pub struct OpLogPersistence {
    store_path: String,
    session_ctx: SessionContext,
    pending_records: Arc<tokio::sync::Mutex<Vec<Record>>>,
    table_name: String,
    version_counter: Arc<tokio::sync::Mutex<i64>>,
    delta_manager: DeltaTableManager,
    // Comprehensive I/O metrics for performance analysis
    io_metrics: Arc<tokio::sync::Mutex<IOMetrics>>,
}

/// Comprehensive I/O operation counters for performance analysis
#[derive(Debug, Clone, Default)]
pub struct IOMetrics {
    // High-level operation counts
    pub directory_queries: u64,
    pub file_reads: u64,
    pub file_writes: u64,
    
    // Delta Lake operation counts
    pub delta_table_opens: u64,
    pub delta_queries_executed: u64,
    pub delta_batches_processed: u64,
    pub delta_records_read: u64,
    pub delta_commits: u64,
    
    // Deserialization counts
    pub oplog_entries_deserialized: u64,
    pub directory_entries_deserialized: u64,
    pub arrow_batches_deserialized: u64,
    
    // Object store operations (future expansion)
    pub object_store_gets: u64,
    pub object_store_puts: u64,
    pub object_store_lists: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

impl IOMetrics {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn reset(&mut self) {
        *self = Self::default();
    }
    
    pub fn print_summary(&self) {
        println!("=== I/O Metrics Summary ===");
        println!("High-level operations:");
        println!("  Directory queries:      {}", self.directory_queries);
        println!("  File reads:             {}", self.file_reads);
        println!("  File writes:            {}", self.file_writes);
        println!();
        println!("Delta Lake operations:");
        println!("  Table opens:            {}", self.delta_table_opens);
        println!("  Queries executed:       {}", self.delta_queries_executed);
        println!("  Batches processed:      {}", self.delta_batches_processed);
        println!("  Records read:           {}", self.delta_records_read);
        println!("  Commits:                {}", self.delta_commits);
        println!();
        println!("Deserialization operations:");
        println!("  OpLog entries:          {}", self.oplog_entries_deserialized);
        println!("  Directory entries:      {}", self.directory_entries_deserialized);
        println!("  Arrow batches:          {}", self.arrow_batches_deserialized);
        println!();
        println!("Data transfer:");
        println!("  Bytes read:             {}", self.bytes_read);
        println!("  Bytes written:          {}", self.bytes_written);
        println!("===========================");
    }
    
    pub fn print_compact(&self) {
        println!("I/O: dir_q={}, delta_q={}, records={}, bytes_r={}", 
                 self.directory_queries, self.delta_queries_executed, 
                 self.delta_records_read, self.bytes_read);
    }
}

impl OpLogPersistence {
    pub async fn new(store_path: &str) -> Result<Self, TinyLogFSError> {
        let delta_manager = DeltaTableManager::new();
        
        // Try to open the table; if it doesn't exist, create it
        // The create_oplog_table function is idempotent and handles "already exists"
        match delta_manager.get_table(store_path).await {
            Ok(_) => {
                // Table exists, continue
            }
            Err(_) => {
                // Table doesn't exist, create it
                create_oplog_table(store_path).await
                    .map_err(TinyLogFSError::OpLog)?;
            }
        }
        
        let session_ctx = SessionContext::new();
        let table_name = format!("oplog_store_{}", Uuid::new_v4().simple());
        
        Ok(Self {
            store_path: store_path.to_string(),
            session_ctx,
            pending_records: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            table_name,
            version_counter: Arc::new(tokio::sync::Mutex::new(0)),
            delta_manager,
            io_metrics: Arc::new(tokio::sync::Mutex::new(IOMetrics::default())),
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
        use deltalake::protocol::SaveMode;
        
        let records = {
            let mut pending = self.pending_records.lock().await;
            let records = pending.drain(..).collect::<Vec<_>>();
            records
        };
        
        println!("OpLogPersistence::commit_internal() - committing {} records", records.len());
        
        if records.is_empty() {
            println!("  No records to commit");
            return Ok(());
        }

        // Convert records to RecordBatch
        let batch = serde_arrow::to_record_batch(&Record::for_arrow(), &records)?;
        
        println!("  Created batch with {} rows, {} columns", batch.num_rows(), batch.num_columns());

        // Use cached Delta operations for write
        let delta_ops = self.delta_manager.get_ops(&self.store_path).await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;

        let result = delta_ops
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        println!("  Successfully written to Delta table, version: {}", result.version());
        
        // Invalidate the cache so subsequent reads see the new data
        self.delta_manager.invalidate_table(&self.store_path).await;
        println!("  Invalidated cache for: {}", self.store_path);
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
        println!("OpLogPersistence::query_records() - querying for part_id: {}, node_id: {:?}", part_id, _node_id);
        
        // Step 1: Get committed records from Delta Lake
        let mut committed_records = Vec::new();
        
        // Try to get table and query it
        match self.delta_manager.get_table_for_read(&self.store_path).await {
            Ok(table) => {
                println!("  Successfully opened cached Delta table, version: {}", table.version());
                
                // Query the table with DataFusion
                let ctx = datafusion::prelude::SessionContext::new();
                let table_name = format!("query_table_{}", Uuid::new_v4().simple());
            
            // Register the Delta table 
            ctx.register_table(&table_name, Arc::new(table))
                .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            
            // Execute query - only filter by part_id for now
            let sql = format!("SELECT * FROM {} WHERE part_id = '{}' ORDER BY version DESC", table_name, part_id);
            println!("  Executing SQL: {}", sql);
            
            let df = ctx.sql(&sql).await
                .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            
            // Collect results
            let batches = df.collect().await
                .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            
            println!("  Query returned {} batches", batches.len());                for (i, batch) in batches.iter().enumerate() {
                    println!("    Batch {}: {} rows, {} columns", i, batch.num_rows(), batch.num_columns());
                    let batch_records: Vec<Record> = serde_arrow::from_record_batch(&batch)?;
                    println!("    Deserialized {} records from batch", batch_records.len());
                    committed_records.extend(batch_records);
                }
            }
            Err(_) => {
                println!("  Delta table does not exist at: {}", self.store_path);
            }
        }
        
        // Step 2: Get pending records from memory
        let pending_records = {
            let pending = self.pending_records.lock().await;
            let filtered = pending.iter()
                .filter(|record| record.part_id == part_id)
                .cloned()
                .collect::<Vec<_>>();
            println!("  Found {} pending records matching part_id", filtered.len());
            filtered
        };
        
        // Step 3: Combine committed and pending records
        let mut all_records = committed_records;
        all_records.extend(pending_records);
        
        println!("  Total records (committed + pending): {}", all_records.len());
        
        // Step 4: Sort by version (descending) to get latest first
        all_records.sort_by(|a, b| b.version.cmp(&a.version));
        
        // Step 5: If node_id was specified, filter the records by deserializing content
        if let Some(target_node_id) = _node_id {
            println!("  Filtering records by node_id: {}", target_node_id);
            all_records = all_records.into_iter().filter(|record| {
                if let Ok(oplog_entry) = self.deserialize_oplog_entry(&record.content) {
                    oplog_entry.node_id == target_node_id
                } else {
                    false
                }
            }).collect();
            println!("  After node_id filtering: {} records", all_records.len());
        }
        
        println!("OpLogPersistence::query_records() - returning {} records", all_records.len());
        Ok(all_records)
    }
    
    /// Query directory entries for a parent node
    async fn query_directory_entries(&self, parent_node_id: NodeID) -> Result<Vec<VersionedDirectoryEntry>, TinyLogFSError> {
        let part_id_str = parent_node_id.to_hex_string();
        println!("OpLogPersistence::query_directory_entries() - querying for part_id: {}", part_id_str);
        
        // Query all records for this directory
        let records = self.query_records(&part_id_str, None).await?;
        println!("OpLogPersistence::query_directory_entries() - found {} records", records.len());
        
        let mut all_entries = Vec::new();
        
        for record in records {
            println!("  Processing record with {} content bytes", record.content.len());
            
            // Try to deserialize as OplogEntry first
            if let Ok(oplog_entry) = self.deserialize_oplog_entry(&record.content) {
                println!("    Deserialized OplogEntry: file_type={}", oplog_entry.file_type);
                
                match oplog_entry.file_type.as_str() {
                    "directory" => {
                        // This is directory content - deserialize the inner directory entries
                        if let Ok(dir_entries) = self.deserialize_directory_entries(&oplog_entry.content) {
                            println!("    Found {} directory entries in content", dir_entries.len());
                            all_entries.extend(dir_entries);
                        } else {
                            println!("    Failed to deserialize directory entries");
                        }
                    },
                    "file" | "symlink" => {
                        // Note: Files and symlinks stored with parent part_id should be 
                        // included in directory entries, but the correct filename mapping
                        // should come from explicit directory entry records, not from
                        // individual file records. 
                        // TODO: This indicates a bug - files should have corresponding
                        // directory entries created via insert() calls.
                        println!("    Found {} without corresponding directory entry (this may indicate a bug)", oplog_entry.file_type);
                    },
                    _ => {
                        println!("    Skipping unknown entry type: {}", oplog_entry.file_type);
                    }
                }
            } else {
                println!("    Failed to deserialize OplogEntry");
            }
        }
        
        // Sort by version to get proper ordering
        all_entries.sort_by_key(|e| e.version);
        
        println!("OpLogPersistence::query_directory_entries() - returning {} total entries", all_entries.len());
        Ok(all_entries)
    }
    
    /// Efficiently query for a single directory entry by name, scanning in reverse order
    /// Returns immediately when the first (most recent) entry is found
    async fn query_single_directory_entry(&self, parent_node_id: NodeID, entry_name: &str) -> Result<Option<VersionedDirectoryEntry>, TinyLogFSError> {
        let part_id_str = parent_node_id.to_hex_string();
        println!("OpLogPersistence::query_single_directory_entry() - querying for entry '{}' in part_id: {}", entry_name, part_id_str);
        
        // Step 1: Get committed records from Delta table
        let mut committed_records = Vec::new();
        match self.delta_manager.get_table_for_read(&self.store_path).await {
            Ok(table) => {
                self.increment_delta_table_opens().await;
                println!("  Successfully opened cached Delta table, version: {}", table.version());
                
                // Query the table with DataFusion
                let ctx = datafusion::prelude::SessionContext::new();
                let table_name = format!("query_table_{}", Uuid::new_v4().simple());
            
                // Register the Delta table 
                ctx.register_table(&table_name, Arc::new(table))
                    .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            
                // Execute query - only filter by part_id, order by version DESC for reverse scan
                let sql = format!("SELECT * FROM {} WHERE part_id = '{}' ORDER BY version DESC", table_name, part_id_str);
                println!("  Executing SQL: {}", sql);
                
                self.increment_delta_queries().await;
            
                let df = ctx.sql(&sql).await
                    .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            
                // Collect results
                let batches = df.collect().await
                    .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            
                println!("  Query returned {} batches", batches.len());
                for batch in batches {
                    self.increment_batches_processed().await;
                    let batch_records: Vec<Record> = serde_arrow::from_record_batch(&batch)?;
                    self.increment_records_read(batch_records.len() as u64).await;
                    committed_records.extend(batch_records);
                }
            }
            Err(_) => {
                println!("  Delta table does not exist at: {}", self.store_path);
            }
        }
        
        // Step 2: Get pending records from memory
        let pending_records = {
            let pending = self.pending_records.lock().await;
            pending.iter()
                .filter(|record| record.part_id == part_id_str)
                .cloned()
                .collect::<Vec<_>>()
        };
        
        // Step 3: Combine and sort by version descending (newest first)
        let mut all_records = committed_records;
        all_records.extend(pending_records);
        all_records.sort_by(|a, b| b.version.cmp(&a.version));
        
        println!("  Scanning {} records in reverse order for entry '{}'", all_records.len(), entry_name);
        
        // Step 4: Scan records in reverse order, return immediately when found
        for record in all_records {
            if let Ok(oplog_entry) = self.deserialize_oplog_entry(&record.content) {
                self.increment_oplog_entries_deserialized().await;
                // Only process directory-type entries
                if oplog_entry.file_type == "directory" {
                    // Deserialize the directory content to check for our target entry
                    if let Ok(directory_entries) = self.deserialize_directory_entries(&oplog_entry.content) {
                        self.increment_directory_entries_deserialized(directory_entries.len() as u64).await;
                        // Look for the specific entry name - scan from end to beginning (latest to earliest within the record)
                        for entry in directory_entries.iter().rev() {
                            if entry.name == entry_name {
                                println!("  Found entry '{}' at version {}, operation: {:?}", entry_name, entry.version, entry.operation_type);
                                // Check operation type - only return if it's an Insert or Update
                                match entry.operation_type {
                                    crate::tinylogfs::schema::OperationType::Insert | 
                                    crate::tinylogfs::schema::OperationType::Update => {
                                        println!("  Found entry '{}' -> {}", entry_name, entry.child_node_id);
                                        return Ok(Some(entry.clone()));
                                    }
                                    crate::tinylogfs::schema::OperationType::Delete => {
                                        println!("  Found deleted entry '{}', returning None", entry_name);
                                        return Ok(None);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        println!("  Entry '{}' not found in any record", entry_name);
        Ok(None)
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
                    // For files, create an OpLogFile handle with persistence layer dependency injection
                    let oplog_file = crate::tinylogfs::file::OpLogFile::new(
                        node_id,
                        part_id,
                        Arc::new(self.clone()) // Clone self to provide persistence layer reference
                    );
                    let file_handle = crate::tinylogfs::file::OpLogFile::create_handle(oplog_file);
                    Ok(tinyfs::NodeType::File(file_handle))
                }
                "directory" => {
                    // For directories, create an OpLogDirectory handle using the clean architecture
                    let oplog_dir = super::directory::OpLogDirectory::new(
                        oplog_entry.node_id.clone(),
                        Arc::new(OpLogPersistence {
                            store_path: self.store_path.clone(),
                            session_ctx: self.session_ctx.clone(),
                            pending_records: self.pending_records.clone(),
                            table_name: self.table_name.clone(),
                            version_counter: self.version_counter.clone(),
                            delta_manager: self.delta_manager.clone(),
                            io_metrics: self.io_metrics.clone(),
                        }) // cloned persistence layer reference
                    );
                    let dir_handle = super::directory::OpLogDirectory::create_handle(oplog_dir);
                    Ok(tinyfs::NodeType::Directory(dir_handle))
                }
                "symlink" => {
                    // For symlinks, create an OpLogSymlink handle with persistence layer dependency injection
                    let oplog_symlink = super::symlink::OpLogSymlink::new(
                        node_id,
                        part_id,
                        Arc::new(self.clone())
                    );
                    let symlink_handle = super::symlink::OpLogSymlink::create_handle(oplog_symlink);
                    Ok(tinyfs::NodeType::Symlink(symlink_handle))
                }
                _ => Err(tinyfs::Error::Other(format!("Unknown node type: {}", oplog_entry.file_type)))
            }
        } else {
            // Node doesn't exist in database yet
            // For the root directory (NodeID::new(0)), create a new empty directory
            if node_id == NodeID::new(0) {
                let oplog_dir = super::directory::OpLogDirectory::new(
                    node_id_str,
                    Arc::new(self.clone()) // cloned persistence layer reference
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
    
    async fn exists_node(&self, node_id: NodeID, part_id: NodeID) -> TinyFSResult<bool> {
        let node_id_str = node_id.to_hex_string();
        let part_id_str = part_id.to_hex_string();
        
        // Query Delta Lake for records matching this node
        let records = self.query_records(&part_id_str, Some(&node_id_str)).await
            .map_err(|e| tinyfs::Error::Other(format!("Query error: {}", e)))?;
        
        // Node exists if we have any records for it
        Ok(!records.is_empty())
    }
    
    async fn load_directory_entries(&self, parent_node_id: NodeID) -> TinyFSResult<HashMap<String, NodeID>> {
        println!("OpLogPersistence::load_directory_entries() - querying for parent: {}", parent_node_id.to_hex_string());
        
        let all_entries = self.query_directory_entries(parent_node_id).await
            .map_err(|e| tinyfs::Error::Other(format!("Query error: {}", e)))?;
        
        println!("OpLogPersistence::load_directory_entries() - found {} raw entries", all_entries.len());
        
        // Apply operations in version order to get current state
        let mut current_state = HashMap::new();
        
        for entry in all_entries.into_iter() {
            println!("  Processing entry: {} -> {} (op: {:?})", entry.name, entry.child_node_id, entry.operation_type);
            
            match entry.operation_type {
                crate::tinylogfs::schema::OperationType::Insert | crate::tinylogfs::schema::OperationType::Update => {
                    if let Ok(child_id) = NodeID::from_hex_string(&entry.child_node_id) {
                        println!("    Added: {} -> {}", entry.name, child_id.to_hex_string());
                        current_state.insert(entry.name, child_id);
                    }
                },
                crate::tinylogfs::schema::OperationType::Delete => {
                    println!("    Removed: {}", entry.name);
                    current_state.remove(&entry.name);
                }
            }
        }
        
        println!("OpLogPersistence::load_directory_entries() - final state has {} entries", current_state.len());
        Ok(current_state)
    }
    
    async fn update_directory_entry(
        &self, 
        parent_node_id: NodeID, 
        entry_name: &str, 
        operation: DirectoryOperation
    ) -> TinyFSResult<()> {
        println!("OpLogPersistence::update_directory_entry() - parent: {}, entry: {}, op: {:?}", 
                 parent_node_id.to_hex_string(), entry_name, operation);
        
        // Convert parent_node_id to hex string for part_id
        let part_id_str = parent_node_id.to_hex_string();
        let version = self.next_version().await
            .map_err(|e| tinyfs::Error::Other(format!("Version error: {}", e)))?;
        
        println!("  Using version: {}", version);
        
        // Load current directory entries for this parent
        let current_entries = self.query_directory_entries(parent_node_id).await
            .map_err(|e| tinyfs::Error::Other(format!("Query error: {}", e)))?;
        
        println!("  Found {} existing entries", current_entries.len());
        
        // Apply the directory operation to create new state
        let mut versioned_entries = Vec::new();
        
        // Start with existing entries, updating their version
        for mut entry in current_entries {
            entry.version = version;
            entry.timestamp = Utc::now().timestamp_micros();
            versioned_entries.push(entry);
        }
        
        // Apply the new operation
        match operation {
            DirectoryOperation::Insert(child_node_id) => {
                println!("  Adding new entry: {} -> {}", entry_name, child_node_id.to_hex_string());
                versioned_entries.push(VersionedDirectoryEntry {
                    name: entry_name.to_string(),
                    child_node_id: child_node_id.to_hex_string(),
                    operation_type: OperationType::Insert,
                    timestamp: Utc::now().timestamp_micros(),
                    version,
                });
            }
            DirectoryOperation::Delete => {
                versioned_entries.push(VersionedDirectoryEntry {
                    name: entry_name.to_string(),
                    child_node_id: "".to_string(), // Empty for deletions
                    operation_type: OperationType::Delete,
                    timestamp: Utc::now().timestamp_micros(),
                    version,
                });
            }
            DirectoryOperation::Rename(new_name, child_node_id) => {
                // Delete the old entry
                versioned_entries.push(VersionedDirectoryEntry {
                    name: entry_name.to_string(),
                    child_node_id: "".to_string(),
                    operation_type: OperationType::Delete,
                    timestamp: Utc::now().timestamp_micros(),
                    version,
                });
                // Insert with new name
                versioned_entries.push(VersionedDirectoryEntry {
                    name: new_name,
                    child_node_id: child_node_id.to_hex_string(),
                    operation_type: OperationType::Insert,
                    timestamp: Utc::now().timestamp_micros(),
                    version,
                });
            }
        }
        
        println!("  Final versioned_entries count: {}", versioned_entries.len());
        for entry in &versioned_entries {
            println!("    Entry: {} -> {} (op: {:?}, v: {})", entry.name, entry.child_node_id, entry.operation_type, entry.version);
        }
        
        // Serialize the directory entries as Arrow IPC
        let content_bytes = self.serialize_directory_entries(&versioned_entries)
            .map_err(|e| tinyfs::Error::Other(format!("Serialization error: {}", e)))?;
        
        println!("  Serialized {} entries to {} bytes", versioned_entries.len(), content_bytes.len());
        
        // Create OplogEntry for this directory update
        let oplog_entry = OplogEntry {
            part_id: part_id_str.clone(),
            node_id: part_id_str.clone(), // For directories, node_id == part_id
            file_type: "directory".to_string(),
            content: content_bytes,
        };
        
        // Serialize the OplogEntry as Arrow IPC
        let oplog_content = self.serialize_oplog_entry(&oplog_entry)
            .map_err(|e| tinyfs::Error::Other(format!("OplogEntry serialization error: {}", e)))?;
        
        println!("  Serialized OplogEntry to {} bytes", oplog_content.len());
        
        // Create Record for the pending transaction
        let record = Record {
            part_id: part_id_str.clone(),
            timestamp: Utc::now().timestamp_micros(),
            version,
            content: oplog_content,
        };
        
        // Add to pending records
        let pending_count_before = self.pending_records.lock().await.len();
        self.pending_records.lock().await.push(record);
        let pending_count_after = self.pending_records.lock().await.len();
        
        println!("  Added record to pending (before: {}, after: {})", pending_count_before, pending_count_after);
        Ok(())
    }
    
    async fn load_file_content(&self, node_id: NodeID, part_id: NodeID) -> TinyFSResult<Vec<u8>> {
        let node_id_str = node_id.to_hex_string();
        let part_id_str = part_id.to_hex_string();
        
        // Query Delta Lake for the most recent record for this node
        let records = self.query_records(&part_id_str, Some(&node_id_str)).await
            .map_err(|e| tinyfs::Error::Other(format!("Query error: {}", e)))?;
        
        if let Some(record) = records.first() {
            // Deserialize the OplogEntry from the record content
            let oplog_entry = self.deserialize_oplog_entry(&record.content)
                .map_err(|e| tinyfs::Error::Other(format!("Deserialization error: {}", e)))?;
            
            // Return the raw content directly (no recursion)
            if oplog_entry.file_type == "file" {
                Ok(oplog_entry.content)
            } else {
                Err(tinyfs::Error::Other("Expected file node type".to_string()))
            }
        } else {
            Err(tinyfs::Error::NotFound(std::path::PathBuf::from(format!("File {} not found", node_id_str))))
        }
    }
    
    async fn store_file_content(&self, node_id: NodeID, part_id: NodeID, content: &[u8]) -> TinyFSResult<()> {
        // Create a memory file with the content and store it
        let memory_file = tinyfs::memory::MemoryFile::new_handle(content);
        let node_type = tinyfs::NodeType::File(memory_file);
        self.store_node(node_id, part_id, &node_type).await
    }
    
    async fn load_symlink_target(&self, node_id: NodeID, part_id: NodeID) -> TinyFSResult<std::path::PathBuf> {
        let node_id_str = node_id.to_hex_string();
        let part_id_str = part_id.to_hex_string();
        
        // Query Delta Lake for the most recent record for this node
        let records = self.query_records(&part_id_str, Some(&node_id_str)).await
            .map_err(|e| tinyfs::Error::Other(format!("Query error: {}", e)))?;
        
        if let Some(record) = records.first() {
            // Deserialize the OplogEntry from the record content
            let oplog_entry = self.deserialize_oplog_entry(&record.content)
                .map_err(|e| tinyfs::Error::Other(format!("Deserialization error: {}", e)))?;
            
            // Return the target path directly (no recursion)
            if oplog_entry.file_type == "symlink" {
                let target_str = String::from_utf8(oplog_entry.content)
                    .map_err(|e| tinyfs::Error::Other(format!("Invalid UTF-8 in symlink target: {}", e)))?;
                Ok(std::path::PathBuf::from(target_str))
            } else {
                Err(tinyfs::Error::Other("Expected symlink node type".to_string()))
            }
        } else {
            Err(tinyfs::Error::NotFound(std::path::PathBuf::from(format!("Symlink {} not found", node_id_str))))
        }
    }
    
    async fn store_symlink_target(&self, node_id: NodeID, part_id: NodeID, target: &std::path::Path) -> TinyFSResult<()> {
        // Create a memory symlink with the target and store it
        let symlink_handle = tinyfs::memory::MemorySymlink::new_handle(target.to_path_buf());
        let node_type = tinyfs::NodeType::Symlink(symlink_handle);
        self.store_node(node_id, part_id, &node_type).await
    }
    
    async fn create_file_node(&self, node_id: NodeID, part_id: NodeID, content: &[u8]) -> TinyFSResult<NodeType> {
        // Create an OpLogFile with the content
        let oplog_file = crate::tinylogfs::file::OpLogFile::new(
            node_id,
            part_id,
            Arc::new(self.clone())
        );
        
        // Store the content immediately
        let memory_file = tinyfs::memory::MemoryFile::new_handle(content);
        let temp_node_type = tinyfs::NodeType::File(memory_file);
        self.store_node(node_id, part_id, &temp_node_type).await?;
        
        // Return the OpLogFile handle
        let file_handle = crate::tinylogfs::file::OpLogFile::create_handle(oplog_file);
        Ok(tinyfs::NodeType::File(file_handle))
    }
    
    async fn create_directory_node(&self, node_id: NodeID) -> TinyFSResult<NodeType> {
        // Create an OpLogDirectory
        let node_id_str = node_id.to_hex_string();
        let oplog_dir = super::directory::OpLogDirectory::new(
            node_id_str,
            Arc::new(self.clone())
        );
        
        // Create the handle  
        let dir_handle = super::directory::OpLogDirectory::create_handle(oplog_dir);
        Ok(tinyfs::NodeType::Directory(dir_handle))
    }
    
    async fn create_symlink_node(&self, node_id: NodeID, part_id: NodeID, target: &std::path::Path) -> TinyFSResult<NodeType> {
        // Create an OpLogSymlink
        let oplog_symlink = super::symlink::OpLogSymlink::new(
            node_id,
            part_id,
            Arc::new(self.clone())
        );
        
        // Store the target immediately
        let memory_symlink = tinyfs::memory::MemorySymlink::new_handle(target.to_path_buf());
        let temp_node_type = tinyfs::NodeType::Symlink(memory_symlink);
        self.store_node(node_id, part_id, &temp_node_type).await?;
        
        // Return the OpLogSymlink handle
        let symlink_handle = super::symlink::OpLogSymlink::create_handle(oplog_symlink);
        Ok(tinyfs::NodeType::Symlink(symlink_handle))
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
    
    async fn query_directory_entry_by_name(&self, parent_node_id: NodeID, entry_name: &str) -> TinyFSResult<Option<NodeID>> {
        println!("OpLogPersistence::query_directory_entry_by_name() - querying for entry '{}' in parent: {}", entry_name, parent_node_id.to_hex_string());
        
        // Use our efficient single entry query method
        match self.query_single_directory_entry(parent_node_id, entry_name).await {
            Ok(Some(entry)) => {
                if let Ok(child_node_id) = NodeID::from_hex_string(&entry.child_node_id) {
                    // Check if this is a delete operation (should not return the entry)
                    match entry.operation_type {
                        OperationType::Delete => {
                            println!("  Entry '{}' was deleted, returning None", entry_name);
                            Ok(None)
                        }
                        _ => {
                            println!("  Found entry '{}' -> {}", entry_name, child_node_id.to_hex_string());
                            Ok(Some(child_node_id))
                        }
                    }
                } else {
                    println!("  Invalid child_node_id format: {}", entry.child_node_id);
                    Ok(None)
                }
            }
            Ok(None) => {
                println!("  Entry '{}' not found", entry_name);
                Ok(None)
            }
            Err(e) => {
                println!("  Error querying entry '{}': {}", entry_name, e);
                Err(tinyfs::Error::Other(format!("Query error: {}", e)))
            }
        }
    }
    
    // async fn query_directory_entry_by_name(&self, parent_node_id: NodeID, entry_name: &str) -> TinyFSResult<Option<NodeID>> {
    //     println!("OpLogPersistence::query_directory_entry_by_name() - querying for entry '{}' in parent: {}", entry_name, parent_node_id.to_hex_string());
        
    //     // Use our efficient single entry query method
    //     match self.query_single_directory_entry(parent_node_id, entry_name).await {
    //         Ok(Some(entry)) => {
    //             if let Ok(child_node_id) = NodeID::from_hex_string(&entry.child_node_id) {
    //                 // Check if this is a delete operation (should not return the entry)
    //                 match entry.operation_type {
    //                     OperationType::Delete => {
    //                         println!("  Entry '{}' was deleted, returning None", entry_name);
    //                         Ok(None)
    //                     }
    //                     _ => {
    //                         println!("  Found entry '{}' -> {}", entry_name, child_node_id.to_hex_string());
    //                         Ok(Some(child_node_id))
    //                     }
    //                 }
    //             } else {
    //                 println!("  Invalid child_node_id format: {}", entry.child_node_id);
    //                 Ok(None)
    //             }
    //         }
    //         Ok(None) => {
    //             println!("  Entry '{}' not found", entry_name);
    //             Ok(None)
    //         }
    //         Err(e) => {
    //             println!("  Error querying entry '{}': {}", entry_name, e);
    //             Err(tinyfs::Error::Other(format!("Query error: {}", e)))
    //         }
    //     }
    // }
}

impl OpLogPersistence {
    // I/O Metrics instrumentation methods
    pub async fn get_io_metrics(&self) -> IOMetrics {
        self.io_metrics.lock().await.clone()
    }
    
    pub async fn reset_io_metrics(&self) {
        self.io_metrics.lock().await.reset();
    }
    
    pub async fn print_io_metrics(&self) {
        self.io_metrics.lock().await.print_summary();
    }
    
    pub async fn print_io_metrics_compact(&self) {
        self.io_metrics.lock().await.print_compact();
    }
    
    // Internal counter increment methods
    async fn increment_delta_table_opens(&self) {
        self.io_metrics.lock().await.delta_table_opens += 1;
    }
    
    async fn increment_delta_queries(&self) {
        self.io_metrics.lock().await.delta_queries_executed += 1;
    }
    
    async fn increment_batches_processed(&self) {
        self.io_metrics.lock().await.delta_batches_processed += 1;
    }
    
    async fn increment_records_read(&self, count: u64) {
        self.io_metrics.lock().await.delta_records_read += count;
    }
    
    async fn increment_oplog_entries_deserialized(&self) {
        self.io_metrics.lock().await.oplog_entries_deserialized += 1;
    }
    
    async fn increment_directory_entries_deserialized(&self, count: u64) {
        self.io_metrics.lock().await.directory_entries_deserialized += count;
    }
}

/// Factory function to create an FS with OpLogPersistence (Phase 4 architecture)
/// This is the new two-layer architecture approach that uses OpLogPersistence
pub async fn create_oplog_fs(store_path: &str) -> Result<tinyfs::FS, TinyLogFSError> {
    let persistence = OpLogPersistence::new(store_path).await?;
    tinyfs::FS::with_persistence_layer(persistence).await
        .map_err(|e| TinyLogFSError::TinyFS(e))
}
