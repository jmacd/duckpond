use super::error::TinyLogFSError;
use super::schema::{OplogEntry, VersionedDirectoryEntry, OperationType, create_oplog_table};
use super::delta_manager::DeltaTableManager;
use tinyfs::persistence::{PersistenceLayer, DirectoryOperation};
use tinyfs::{NodeID, NodeType, Result as TinyFSResult};
use oplog::delta::{Record, ForArrow};
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
    current_transaction_version: Arc<tokio::sync::Mutex<Option<i64>>>,
    // Directory update coalescing - accumulate directory changes during transaction
    pending_directory_operations: Arc<tokio::sync::Mutex<HashMap<NodeID, HashMap<String, DirectoryOperation>>>>,
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
        diagnostics::log_info!("=== I/O Metrics Summary ===");
        diagnostics::log_info!("High-level operations:");
        let directory_queries = self.directory_queries;
        let file_reads = self.file_reads;
        let file_writes = self.file_writes;
        diagnostics::log_info!("  Directory queries:      {directory_queries}");
        diagnostics::log_info!("  File reads:             {file_reads}");
        diagnostics::log_info!("  File writes:            {file_writes}");
        diagnostics::log_info!("");
        diagnostics::log_info!("Delta Lake operations:");
        let delta_table_opens = self.delta_table_opens;
        let delta_queries_executed = self.delta_queries_executed;
        let delta_batches_processed = self.delta_batches_processed;
        let delta_records_read = self.delta_records_read;
        let delta_commits = self.delta_commits;
        diagnostics::log_info!("  Table opens:            {delta_table_opens}");
        diagnostics::log_info!("  Queries executed:       {delta_queries_executed}");
        diagnostics::log_info!("  Batches processed:      {delta_batches_processed}");
        diagnostics::log_info!("  Records read:           {delta_records_read}");
        diagnostics::log_info!("  Commits:                {delta_commits}");
        diagnostics::log_info!("");
        diagnostics::log_info!("Deserialization operations:");
        let oplog_entries_deserialized = self.oplog_entries_deserialized;
        let directory_entries_deserialized = self.directory_entries_deserialized;
        let arrow_batches_deserialized = self.arrow_batches_deserialized;
        diagnostics::log_info!("  OpLog entries:          {oplog_entries_deserialized}");
        diagnostics::log_info!("  Directory entries:      {directory_entries_deserialized}");
        diagnostics::log_info!("  Arrow batches:          {arrow_batches_deserialized}");
        diagnostics::log_info!("");
        diagnostics::log_info!("Data transfer:");
        let bytes_read = self.bytes_read;
        let bytes_written = self.bytes_written;
        diagnostics::log_info!("  Bytes read:             {bytes_read}");
        diagnostics::log_info!("  Bytes written:          {bytes_written}");
        diagnostics::log_info!("===========================");
    }
    
    pub fn print_compact(&self) {
        let dir_q = self.directory_queries;
        let delta_q = self.delta_queries_executed;
        let records = self.delta_records_read;
        let bytes_r = self.bytes_read;
        diagnostics::log_info!("I/O summary: dir_queries={dir_q}, delta_queries={delta_q}, records={records}, bytes_read={bytes_r}", 
                               dir_q: dir_q, delta_q: delta_q, records: records, bytes_r: bytes_r);
    }
}

impl OpLogPersistence {
    pub async fn new(store_path: &str) -> Result<Self, TinyLogFSError> {
        // Initialize diagnostics on first use
        diagnostics::init_diagnostics();
        
        let delta_manager = DeltaTableManager::new();
        
        // Try to open the table; if it doesn't exist, create it
        // The create_oplog_table function is idempotent and handles "already exists"
        match delta_manager.get_table(store_path).await {
            Ok(_) => {
                diagnostics::log_debug!("Delta table exists at: {store_path}");
            }
            Err(_) => {
                diagnostics::log_info!("Creating new Delta table at: {store_path}");
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
            current_transaction_version: Arc::new(tokio::sync::Mutex::new(None)),
            pending_directory_operations: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            delta_manager,
            io_metrics: Arc::new(tokio::sync::Mutex::new(IOMetrics::default())),
        })
    }
    
    async fn next_version(&self) -> Result<i64, TinyLogFSError> {
        // Check if we're in a transaction - if so, reuse the transaction version
        let mut current_transaction = self.current_transaction_version.lock().await;
        if let Some(transaction_version) = *current_transaction {
            diagnostics::log_debug!("Reusing transaction sequence: {transaction_version}");
            Ok(transaction_version)
        } else {
            // Not in a transaction or first operation in transaction - create new version
            let mut counter = self.version_counter.lock().await;
            *counter += 1;
            let new_version = *counter;
            *current_transaction = Some(new_version);
            diagnostics::log_info!("Started new transaction sequence: {new_version}");
            Ok(new_version)
        }
    }
    
    // /// Begin a new transaction - clear pending operations and reset transaction state
    // async fn begin_transaction_internal(&self) -> Result<(), TinyLogFSError> {
    //     println!("TRANSACTION: Beginning new transaction");
        
    //     // Clear any pending operations
    //     {
    //         let mut pending = self.pending_records.lock().await;
    //         pending.clear();
    //     }
        
    //     // Clear any pending directory operations
    //     {
    //         let mut pending_dirs = self.pending_directory_operations.lock().await;
    //         pending_dirs.clear();
    //     }
        
    //     // Reset transaction version
    //     {
    //         let mut current_transaction = self.current_transaction_version.lock().await;
    //         *current_transaction = None;
    //     }
        
    //     println!("TRANSACTION: Transaction begun successfully");
    //     Ok(())
    // }
    
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
        
        let count = records.len();
        diagnostics::log_info!("TRANSACTION: OpLogPersistence::commit_internal() - committing {count} records");
        
        if records.is_empty() {
            diagnostics::log_debug!("TRANSACTION: No records to commit");
            return Ok(());
        }

        // Convert records to RecordBatch
        let batch = serde_arrow::to_record_batch(&Record::for_arrow(), &records)?;
        
        let rows = batch.num_rows();
        let columns = batch.num_columns();
        diagnostics::log_debug!("TRANSACTION: Created batch with {rows} rows, {columns} columns");

        // Use cached Delta operations for write
        let delta_ops = self.delta_manager.get_ops(&self.store_path).await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;

        let result = delta_ops
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        let version = result.version();
        diagnostics::log_info!("TRANSACTION: Successfully written to Delta table, version: {version}", version: version);
        
        // Invalidate the cache so subsequent reads see the new data
        self.delta_manager.invalidate_table(&self.store_path).await;
        let store_path = &self.store_path;
        diagnostics::log_debug!("TRANSACTION: Invalidated cache for: {store_path}", store_path: store_path);
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
        let part_id_bound = part_id;
        let node_id_str = format!("{:?}", _node_id);
        diagnostics::log_debug!("OpLogPersistence::query_records() - querying for part_id: {part_id}, node_id: {node_id}", 
                                part_id: part_id_bound, node_id: node_id_str);
        
        // Step 1: Get committed records from Delta Lake
        let mut committed_records = Vec::new();
        
        // Try to get table and query it
        match self.delta_manager.get_table_for_read(&self.store_path).await {
            Ok(table) => {
                let version = table.version();
                diagnostics::log_debug!("  Successfully opened cached Delta table, version: {version}", version: version);
                
                // Query the table with DataFusion
                let ctx = datafusion::prelude::SessionContext::new();
                let table_name = format!("query_table_{}", Uuid::new_v4().simple());
            
            // Register the Delta table 
            ctx.register_table(&table_name, Arc::new(table))
                .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            
            // Execute query - only filter by part_id for now
            let sql = format!("SELECT * FROM {} WHERE part_id = '{}' ORDER BY version DESC", table_name, part_id);
            let sql_bound = &sql;
            diagnostics::log_debug!("  Executing SQL: {sql}", sql: sql_bound);
            
            let df = ctx.sql(&sql).await
                .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            
            // Collect results
            let batches = df.collect().await
                .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            
            let batch_count = batches.len();
            diagnostics::log_debug!("  Query returned {batch_count} batches", batch_count: batch_count);
            for (i, batch) in batches.iter().enumerate() {
                let rows = batch.num_rows();
                let cols = batch.num_columns();
                diagnostics::log_debug!("    Batch {i}: {rows} rows, {cols} columns", i: i, rows: rows, cols: cols);
                let batch_records: Vec<Record> = serde_arrow::from_record_batch(&batch)?;
                let record_count = batch_records.len();
                diagnostics::log_debug!("    Deserialized {record_count} records from batch", record_count: record_count);
                committed_records.extend(batch_records);
            }
            }
            Err(_) => {
                let store_path = &self.store_path;
                diagnostics::log_debug!("  Delta table does not exist at: {store_path}", store_path: store_path);
            }
        }
        
        // Step 2: Get pending records from memory
        let pending_records = {
            let pending = self.pending_records.lock().await;
            let filtered = pending.iter()
                .filter(|record| record.part_id == part_id)
                .cloned()
                .collect::<Vec<_>>();
            let filtered_count = filtered.len();
            diagnostics::log_debug!("  Found {filtered_count} pending records matching part_id", filtered_count: filtered_count);
            filtered
        };
        
        // Step 3: Combine committed and pending records
        let mut all_records = committed_records;
        all_records.extend(pending_records);
        
        let total_count = all_records.len();
        diagnostics::log_debug!("  Total records (committed + pending): {total_count}", total_count: total_count);
        
        // Step 4: Sort by version (descending) to get latest first
        all_records.sort_by(|a, b| b.version.cmp(&a.version));
        
        // Step 5: If node_id was specified, filter the records by deserializing content
        if let Some(target_node_id) = _node_id {
            let target_node_id_bound = target_node_id;
            diagnostics::log_debug!("  Filtering records by node_id: {target_node_id}", target_node_id: target_node_id_bound);
            all_records = all_records.into_iter().filter(|record| {
                if let Ok(oplog_entry) = self.deserialize_oplog_entry(&record.content) {
                    oplog_entry.node_id == target_node_id
                } else {
                    false
                }
            }).collect();
            let filtered_count = all_records.len();
            diagnostics::log_debug!("  After node_id filtering: {filtered_count} records", filtered_count: filtered_count);
        }
        
        let result_count = all_records.len();
        diagnostics::log_debug!("OpLogPersistence::query_records() - returning {result_count} records", result_count: result_count);
        Ok(all_records)
    }
    
    /// Query directory entries for a parent node - returns latest version of each entry
    async fn query_directory_entries(&self, parent_node_id: NodeID) -> Result<Vec<VersionedDirectoryEntry>, TinyLogFSError> {
        let part_id_str = parent_node_id.to_hex_string();
        let part_id_bound = &part_id_str;
        diagnostics::log_debug!("OpLogPersistence::query_directory_entries() - querying for part_id: {part_id}", part_id: part_id_bound);
        
        // Query all records for this directory
        let records = self.query_records(&part_id_str, None).await?;
        let record_count = records.len();
        diagnostics::log_debug!("OpLogPersistence::query_directory_entries() - found {record_count} records", record_count: record_count);
        
        let mut all_entries = Vec::new();
        
        for record in records {
            let content_len = record.content.len();
            diagnostics::log_debug!("  Processing record with {content_len} content bytes", content_len: content_len);
            
            // Try to deserialize as OplogEntry first
            if let Ok(oplog_entry) = self.deserialize_oplog_entry(&record.content) {
                let file_type = &oplog_entry.file_type;
                diagnostics::log_debug!("    Deserialized OplogEntry: file_type={file_type}", file_type: file_type);
                
                match oplog_entry.file_type.as_str() {
                    "directory" => {
                        // This is directory content - deserialize the inner directory entries
                        if let Ok(dir_entries) = self.deserialize_directory_entries(&oplog_entry.content) {
                            let entry_count = dir_entries.len();
                            diagnostics::log_debug!("    Found {entry_count} directory entries in content", entry_count: entry_count);
                            all_entries.extend(dir_entries);
                        } else {
                            diagnostics::log_debug!("    Failed to deserialize directory entries");
                        }
                    },
                    "file" | "symlink" => {
                        // Note: Files and symlinks stored with parent part_id should be 
                        // included in directory entries, but the correct filename mapping
                        // should come from explicit directory entry records, not from
                        // individual file records. 
                        // TODO: This indicates a bug - files should have corresponding
                        // directory entries created via insert() calls.
                        let file_type = &oplog_entry.file_type;
                        diagnostics::log_debug!("    Found {file_type} without corresponding directory entry (this may indicate a bug)", file_type: file_type);
                    },
                    _ => {
                        let file_type = &oplog_entry.file_type;
                        diagnostics::log_debug!("    Skipping unknown entry type: {file_type}", file_type: file_type);
                    }
                }
            } else {
                diagnostics::log_debug!("    Failed to deserialize OplogEntry");
            }
        }
        
        // CRITICAL FIX: Deduplicate entries by name, keeping only the latest version
        // Sort by version in descending order (newest first)
        all_entries.sort_by(|a, b| b.version.cmp(&a.version));
        
        // Deduplicate by name, keeping only the first (newest) occurrence of each name
        use std::collections::HashSet;
        let mut seen_names = HashSet::new();
        let mut deduplicated_entries = Vec::new();
        
        for entry in all_entries {
            if !seen_names.contains(&entry.name) {
                seen_names.insert(entry.name.clone());
                // Only include Insert operations in the current directory state
                if matches!(entry.operation_type, OperationType::Insert) {
                    deduplicated_entries.push(entry);
                }
            }
        }
        
        // Sort final result by name for consistent ordering
        deduplicated_entries.sort_by(|a, b| a.name.cmp(&b.name));
        
        let entry_count = deduplicated_entries.len();
        diagnostics::log_debug!("OpLogPersistence::query_directory_entries() - returning {entry_count} deduplicated entries", entry_count: entry_count);
        Ok(deduplicated_entries)
    }
    
    /// Efficiently query for a single directory entry by name, scanning in reverse order
    /// Returns immediately when the first (most recent) entry is found
    async fn query_single_directory_entry(&self, parent_node_id: NodeID, entry_name: &str) -> Result<Option<VersionedDirectoryEntry>, TinyLogFSError> {
        let part_id_str = parent_node_id.to_hex_string();
        diagnostics::log_debug!("Querying single directory entry '{entry_name}' in part_id: {part_id_str}");
        
        // Step 1: Check pending directory operations first (most recent)
        {
            let pending_dirs = self.pending_directory_operations.lock().await;
            if let Some(operations) = pending_dirs.get(&parent_node_id) {
                let count = operations.len();
                diagnostics::log_debug!("Checking {count} pending directory operations for entry '{entry_name}'", 
                          count: count);
                // Check if the specific entry name has a pending operation
                if let Some(operation) = operations.get(entry_name) {
                    let current_version = {
                        let version_guard = self.current_transaction_version.lock().await;
                        version_guard.unwrap_or(0)
                    };
                    
                    match operation {
                        tinyfs::DirectoryOperation::Insert(node_id) => {
                            let node_id_str = node_id.to_hex_string();
                            diagnostics::log_debug!("Found pending INSERT operation for '{entry_name}' -> {node_id}", node_id: node_id_str);
                            return Ok(Some(VersionedDirectoryEntry {
                                name: entry_name.to_string(),
                                child_node_id: node_id.to_hex_string(),
                                operation_type: crate::schema::OperationType::Insert,
                                version: current_version,
                                timestamp: Utc::now().timestamp_micros(),
                            }));
                        }
                        tinyfs::DirectoryOperation::Delete => {
                            diagnostics::log_debug!("Found pending DELETE operation for '{entry_name}', returning None");
                            return Ok(None);
                        }
                        tinyfs::DirectoryOperation::Rename(_new_name, node_id) => {
                            // Handle renames if needed
                            let node_id_str = node_id.to_hex_string();
                            diagnostics::log_debug!("Found pending RENAME operation for '{entry_name}' -> {node_id}", node_id: node_id_str);
                            return Ok(Some(VersionedDirectoryEntry {
                                name: entry_name.to_string(),
                                child_node_id: node_id.to_hex_string(),
                                operation_type: crate::schema::OperationType::Update,
                                version: current_version,
                                timestamp: Utc::now().timestamp_micros(),
                            }));
                        }
                    }
                }
                diagnostics::log_debug!("Entry '{entry_name}' not found in pending operations");
            }
        }
        
        // Step 2: Get committed records from Delta table
        let mut committed_records = Vec::new();
        match self.delta_manager.get_table_for_read(&self.store_path).await {
            Ok(table) => {
                self.increment_delta_table_opens().await;
                let version = table.version();
                diagnostics::log_debug!("Successfully opened cached Delta table, version: {version}", version: version);
                
                // Query the table with DataFusion
                let ctx = datafusion::prelude::SessionContext::new();
                let table_name = format!("query_table_{}", Uuid::new_v4().simple());
            
                // Register the Delta table 
                ctx.register_table(&table_name, Arc::new(table))
                    .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            
                // Execute query - only filter by part_id, order by version DESC for reverse scan
                let sql = format!("SELECT * FROM {} WHERE part_id = '{}' ORDER BY version DESC", table_name, part_id_str);
                diagnostics::log_debug!("Executing SQL: {sql}");
                
                self.increment_delta_queries().await;
            
                let df = ctx.sql(&sql).await
                    .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            
                // Collect results
                let batches = df.collect().await
                    .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            
                let batch_count = batches.len();
                diagnostics::log_debug!("Query returned {batch_count} batches", batch_count: batch_count);
                for batch in batches {
                    self.increment_batches_processed().await;
                    let batch_records: Vec<Record> = serde_arrow::from_record_batch(&batch)?;
                    self.increment_records_read(batch_records.len() as u64).await;
                    committed_records.extend(batch_records);
                }
            }
            Err(_) => {
                diagnostics::log_debug!("Delta table does not exist at: {path}", path: self.store_path);
            }
        }
        
        // Step 3: Get pending records from memory (non-directory operations)
        let pending_records = {
            let pending = self.pending_records.lock().await;
            pending.iter()
                .filter(|record| record.part_id == part_id_str)
                .cloned()
                .collect::<Vec<_>>()
        };
        
        // Step 4: Combine and sort by version descending (newest first)
        let mut all_records = committed_records;
        all_records.extend(pending_records);
        all_records.sort_by(|a, b| b.version.cmp(&a.version));
        
        let count = all_records.len();
        diagnostics::log_debug!("Scanning {count} committed records in reverse order for entry '{entry_name}'", 
                   count: count);
        
        // Step 5: Scan records in reverse order, return immediately when found
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
                                let entry_name_bound = entry_name;
                                let version = entry.version;
                                let operation_debug = format!("{:?}", entry.operation_type);
                                diagnostics::log_debug!("  Found entry '{entry_name}' at version {version}, operation: {operation}", 
                                                        entry_name: entry_name_bound, version: version, operation: operation_debug);
                                // Check operation type - only return if it's an Insert or Update
                                match entry.operation_type {
                                    crate::schema::OperationType::Insert | 
                                    crate::schema::OperationType::Update => {
                                        let entry_name_bound2 = entry_name;
                                        let child_node_id_str = entry.child_node_id.to_string();
                                        diagnostics::log_debug!("  Found entry '{entry_name}' -> {child_node_id}", 
                                                                entry_name: entry_name_bound2, child_node_id: child_node_id_str);
                                        return Ok(Some(entry.clone()));
                                    }
                                    crate::schema::OperationType::Delete => {
                                        let entry_name_bound3 = entry_name;
                                        diagnostics::log_debug!("  Found deleted entry '{entry_name}', returning None", entry_name: entry_name_bound3);
                                        return Ok(None);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        let entry_name_bound = entry_name;
        diagnostics::log_debug!("Entry '{entry_name}' not found in any record", entry_name: entry_name_bound);
        Ok(None)
    }
    
    /// Process all accumulated directory operations in a batch
    async fn flush_directory_operations(&self) -> Result<(), TinyLogFSError> {
        diagnostics::log_info!("TRANSACTION: Flushing accumulated directory operations");
        
        let pending_dirs = {
            let mut pending = self.pending_directory_operations.lock().await;
            let operations = std::mem::take(&mut *pending);
            operations
        };
        
        if pending_dirs.is_empty() {
            diagnostics::log_info!("TRANSACTION: No directory operations to flush");
            return Ok(());
        }
        
        let dir_count = pending_dirs.len();
        diagnostics::log_info!("TRANSACTION: Processing {dir_count} directories with pending operations", dir_count: dir_count);
        
        for (parent_node_id, operations) in pending_dirs {
            let parent_hex = parent_node_id.to_hex_string();
            let op_count = operations.len();
            diagnostics::log_debug!("TRANSACTION: Processing directory {parent_hex} with {op_count} operations", 
                                    parent_hex: parent_hex, op_count: op_count);
            
            // Convert parent_node_id to hex string for part_id
            let part_id_str = parent_node_id.to_hex_string();
            let version = self.next_version().await?;
            
            // COALESCING: Process all operations for this directory in a single record
            let mut versioned_entries = Vec::new();
            
            for (entry_name, operation) in operations {
                match operation {
                    DirectoryOperation::Insert(child_node_id) => {
                        let entry_name_bound = &entry_name;
                        let child_hex = child_node_id.to_hex_string();
                        diagnostics::log_debug!("  Coalesced insert: {entry_name} -> {child_hex}", 
                                                entry_name: entry_name_bound, child_hex: child_hex);
                        versioned_entries.push(VersionedDirectoryEntry {
                            name: entry_name,
                            child_node_id: child_node_id.to_hex_string(),
                            operation_type: OperationType::Insert,
                            timestamp: Utc::now().timestamp_micros(),
                            version,
                        });
                    }
                    DirectoryOperation::Delete => {
                        versioned_entries.push(VersionedDirectoryEntry {
                            name: entry_name,
                            child_node_id: "".to_string(),
                            operation_type: OperationType::Delete,
                            timestamp: Utc::now().timestamp_micros(),
                            version,
                        });
                    }
                    DirectoryOperation::Rename(new_name, child_node_id) => {
                        // Delete the old entry
                        versioned_entries.push(VersionedDirectoryEntry {
                            name: entry_name,
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
            }
            
            let entry_count = versioned_entries.len();
            diagnostics::log_debug!("  Final versioned_entries count: {entry_count}", entry_count: entry_count);
            
            // Serialize the directory entries as Arrow IPC
            let content_bytes = self.serialize_directory_entries(&versioned_entries)?;
            
            // Create OplogEntry for this directory update
            let oplog_entry = OplogEntry {
                part_id: part_id_str.clone(),
                node_id: part_id_str.clone(), // For directories, node_id == part_id
                file_type: "directory".to_string(),
                content: content_bytes,
            };
            
            // Serialize the OplogEntry as Arrow IPC
            let oplog_content = self.serialize_oplog_entry(&oplog_entry)?;
            
            // Create Record for the pending transaction
            let record = Record {
                part_id: part_id_str.clone(),
                timestamp: Utc::now().timestamp_micros(),
                version,
                content: oplog_content,
            };
            
            // Add to pending records
            self.pending_records.lock().await.push(record);
            let parent_hex = parent_node_id.to_hex_string();
            diagnostics::log_debug!("TRANSACTION: Added coalesced directory record for {parent_hex}", parent_hex: parent_hex);
        }
        
        Ok(())
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
                    let oplog_file = crate::file::OpLogFile::new(
                        node_id,
                        part_id,
                        Arc::new(self.clone()) // Clone self to provide persistence layer reference
                    );
                    let file_handle = crate::file::OpLogFile::create_handle(oplog_file);
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
                            current_transaction_version: self.current_transaction_version.clone(),
                            pending_directory_operations: self.pending_directory_operations.clone(),
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
        let node_hex = node_id.to_hex_string();
        let part_hex = part_id.to_hex_string();
        diagnostics::log_debug!("TRANSACTION: OpLogPersistence::store_node() - node: {node_hex}, part: {part_hex}", 
                                node_hex: node_hex, part_hex: part_hex);
        
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
        
        diagnostics::log_debug!("TRANSACTION: store_node() - calling next_version()");
        let version = self.next_version().await
            .map_err(|e| tinyfs::Error::Other(format!("Version error: {}", e)))?;
        diagnostics::log_debug!("TRANSACTION: store_node() - assigned version: {version}", version: version);
        
        let record = Record {
            part_id: part_id_str,
            timestamp: Utc::now().timestamp_micros(),
            version,
            content: content_bytes,
        };
        
        // Add to pending records
        let pending_count_before = self.pending_records.lock().await.len();
        self.pending_records.lock().await.push(record);
        let pending_count_after = self.pending_records.lock().await.len();
        
        diagnostics::log_debug!("TRANSACTION: store_node() - added record to pending (before: {before}, after: {after})", 
                                before: pending_count_before, after: pending_count_after);
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
        let parent_hex = parent_node_id.to_hex_string();
        diagnostics::log_debug!("OpLogPersistence::load_directory_entries() - querying for parent: {parent_hex}", parent_hex: parent_hex);
        
        let all_entries = self.query_directory_entries(parent_node_id).await
            .map_err(|e| tinyfs::Error::Other(format!("Query error: {}", e)))?;
        
        let entry_count = all_entries.len();
        diagnostics::log_debug!("OpLogPersistence::load_directory_entries() - found {entry_count} raw entries", entry_count: entry_count);
        
        // Apply operations in version order to get current state
        let mut current_state = HashMap::new();
        
        for entry in all_entries.into_iter() {
            let entry_name = &entry.name;
            let child_node_id = &entry.child_node_id;
            let operation_debug = format!("{:?}", entry.operation_type);
            diagnostics::log_debug!("  Processing entry: {entry_name} -> {child_node_id} (op: {operation})", 
                                    entry_name: entry_name, child_node_id: child_node_id, operation: operation_debug);
            
            match entry.operation_type {
                crate::schema::OperationType::Insert | crate::schema::OperationType::Update => {
                    if let Ok(child_id) = NodeID::from_hex_string(&entry.child_node_id) {
                        let entry_name = &entry.name;
                        let child_hex = child_id.to_hex_string();
                        diagnostics::log_debug!("    Added: {entry_name} -> {child_hex}", entry_name: entry_name, child_hex: child_hex);
                        current_state.insert(entry.name, child_id);
                    }
                },
                crate::schema::OperationType::Delete => {
                    let entry_name = &entry.name;
                    diagnostics::log_debug!("    Removed: {entry_name}", entry_name: entry_name);
                    current_state.remove(&entry.name);
                }
            }
        }
        
        let final_count = current_state.len();
        diagnostics::log_debug!("OpLogPersistence::load_directory_entries() - final state has {final_count} entries", final_count: final_count);
        Ok(current_state)
    }
    
    async fn update_directory_entry(
        &self, 
        parent_node_id: NodeID, 
        entry_name: &str, 
        operation: DirectoryOperation
    ) -> TinyFSResult<()> {
        let parent_hex = parent_node_id.to_hex_string();
        let entry_name_bound = entry_name;
        let operation_debug = format!("{:?}", operation);
        diagnostics::log_debug!("TRANSACTION: OpLogPersistence::update_directory_entry() - parent: {parent_hex}, entry: {entry_name}, op: {operation}", 
                                parent_hex: parent_hex, entry_name: entry_name_bound, operation: operation_debug);
        
        // DIRECTORY COALESCING: Instead of immediately persisting, accumulate directory operations
        // They will be batched and committed during commit()
        {
            let mut pending_dirs = self.pending_directory_operations.lock().await;
            let dir_ops = pending_dirs.entry(parent_node_id).or_insert_with(HashMap::new);
            dir_ops.insert(entry_name.to_string(), operation);
            let op_count = dir_ops.len();
            diagnostics::log_debug!("TRANSACTION: Coalesced directory operation - parent has {op_count} pending operations", op_count: op_count);
        }
        
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
        let oplog_file = crate::file::OpLogFile::new(
            node_id,
            part_id,
            Arc::new(self.clone())
        );
        
        // Store the content immediately
        let memory_file = tinyfs::memory::MemoryFile::new_handle(content);
        let temp_node_type = tinyfs::NodeType::File(memory_file);
        self.store_node(node_id, part_id, &temp_node_type).await?;
        
        // Return the OpLogFile handle
        let file_handle = crate::file::OpLogFile::create_handle(oplog_file);
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
        let pending_count = self.pending_records.lock().await.len();
        let pending_dir_count = self.pending_directory_operations.lock().await.len();
        diagnostics::log_info!("TRANSACTION: OpLogPersistence::commit() called with {pending_count} pending records and {pending_dir_count} pending directory operations", 
                               pending_count: pending_count, pending_dir_count: pending_dir_count);
        
        // First, flush any accumulated directory operations to pending records
        self.flush_directory_operations().await
            .map_err(|e| tinyfs::Error::Other(format!("Directory flush error: {}", e)))?;
        
        // Commit all pending records to Delta Lake
        self.commit_internal().await
            .map_err(|e| tinyfs::Error::Other(format!("Commit error: {}", e)))?;
        
        // Reset transaction state after successful commit
        {
            let mut current_transaction = self.current_transaction_version.lock().await;
            *current_transaction = None;
        }
        
        diagnostics::log_info!("TRANSACTION: Transaction committed and reset successfully");
        Ok(())
    }
    
    async fn rollback(&self) -> TinyFSResult<()> {
        let pending_count = self.pending_records.lock().await.len();
        let pending_dir_count = self.pending_directory_operations.lock().await.len();
        diagnostics::log_info!("TRANSACTION: OpLogPersistence::rollback() called with {pending_count} pending records and {pending_dir_count} pending directory operations", 
                               pending_count: pending_count, pending_dir_count: pending_dir_count);
        
        // Clear pending records
        self.pending_records.lock().await.clear();
        
        // Clear pending directory operations  
        self.pending_directory_operations.lock().await.clear();
        
        // Reset transaction state
        {
            let mut current_transaction = self.current_transaction_version.lock().await;
            *current_transaction = None;
        }
        
        diagnostics::log_info!("TRANSACTION: Transaction rolled back successfully");
        Ok(())
    }
    
    async fn has_pending_operations(&self) -> TinyFSResult<bool> {
        // Check if there are any pending records OR pending directory operations
        let pending_records = self.pending_records.lock().await;
        let pending_dirs = self.pending_directory_operations.lock().await;
        let has_pending = !pending_records.is_empty() || !pending_dirs.is_empty();
        Ok(has_pending)
    }
    
    async fn query_directory_entry_by_name(&self, parent_node_id: NodeID, entry_name: &str) -> TinyFSResult<Option<NodeID>> {
        let entry_name_bound = entry_name;
        let parent_hex = parent_node_id.to_hex_string();
        diagnostics::log_debug!("OpLogPersistence::query_directory_entry_by_name() - querying for entry '{entry_name}' in parent: {parent_hex}", 
                                entry_name: entry_name_bound, parent_hex: parent_hex);
        
        // Use our efficient single entry query method
        match self.query_single_directory_entry(parent_node_id, entry_name).await {
            Ok(Some(entry)) => {
                if let Ok(child_node_id) = NodeID::from_hex_string(&entry.child_node_id) {
                    // Check if this is a delete operation (should not return the entry)
                    match entry.operation_type {
                        OperationType::Delete => {
                            let entry_name_bound2 = entry_name;
                            diagnostics::log_debug!("  Entry '{entry_name}' was deleted, returning None", entry_name: entry_name_bound2);
                            Ok(None)
                        }
                        _ => {
                            let entry_name_bound3 = entry_name;
                            let child_hex = child_node_id.to_hex_string();
                            diagnostics::log_debug!("  Found entry '{entry_name}' -> {child_hex}", 
                                                    entry_name: entry_name_bound3, child_hex: child_hex);
                            Ok(Some(child_node_id))
                        }
                    }
                } else {
                    let child_node_id_str = &entry.child_node_id;
                    diagnostics::log_debug!("  Invalid child_node_id format: {child_node_id}", child_node_id: child_node_id_str);
                    Ok(None)
                }
            }
            Ok(None) => {
                let entry_name_bound4 = entry_name;
                diagnostics::log_debug!("  Entry '{entry_name}' not found", entry_name: entry_name_bound4);
                Ok(None)
            }
            Err(e) => {
                let entry_name_bound5 = entry_name;
                let error_str = format!("{}", e);
                diagnostics::log_debug!("  Error querying entry '{entry_name}': {error}", entry_name: entry_name_bound5, error: error_str);
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
