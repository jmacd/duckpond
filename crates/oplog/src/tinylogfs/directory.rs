// Arrow-backed Directory implementation for TinyFS integration using DataFusion queries
use super::{TinyLogFSError, OplogEntry, DirectoryEntry};
use super::schema::{VersionedDirectoryEntry, OperationType};
use tinyfs::{DirHandle, Directory, NodeRef};
use datafusion::prelude::SessionContext;

/// Arrow-backed directory implementation using DataFusion for queries
/// This implementation queries both committed (Delta Lake) and pending (in-memory) data
/// using DataFusion session context directly instead of backend references.
pub struct OpLogDirectory {
    /// Unique node identifier
    node_id: String,
    
    /// DataFusion session context for querying in-memory data
    session_ctx: SessionContext,
    
    /// Table name in the DataFusion session
    table_name: String,
    
    /// Delta Lake store path for direct reading
    store_path: String,
    
    /// Pending directory operations that haven't been committed yet
    /// Using a different structure that can store actual NodeRef instances
    pending_ops: std::sync::Arc<tokio::sync::Mutex<Vec<DirectoryEntry>>>,
    
    /// Pending NodeRef mappings for quick lookup (name -> NodeRef)
    pending_nodes: std::sync::Arc<tokio::sync::Mutex<std::collections::HashMap<String, NodeRef>>>,
}

impl OpLogDirectory {
    /// Create a new OpLogDirectory with DataFusion session
    pub fn new_with_session(node_id: String, session_ctx: SessionContext, table_name: String, store_path: String) -> Self {
        OpLogDirectory {
            node_id,
            session_ctx,
            table_name,
            store_path,
            pending_ops: std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new())),
            pending_nodes: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Legacy constructor for backward compatibility (Phase 5: Removed old backend system)
    /* REMOVED: Old backend-based constructor
    pub fn new(node_id: String, _backend: std::rc::Weak<std::cell::RefCell<super::backend::OpLogBackend>>) -> Self {
        // Create empty session - this is a fallback for existing code
        let session_ctx = SessionContext::new();
        OpLogDirectory {
            node_id,
            session_ctx,
            table_name: "oplog".to_string(), // Default table name for backward compatibility
            store_path: "".to_string(), // Empty store path for legacy compatibility
            pending_ops: std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new())),
            pending_nodes: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }
    */
    
    /// Create a directory handle for TinyFS integration
    pub fn create_handle(oplog_dir: OpLogDirectory) -> DirHandle {
        use std::sync::Arc;
        use tokio::sync::Mutex;
        
        DirHandle::new(Arc::new(Mutex::new(Box::new(oplog_dir))))
    }
    
    /// Get all directory entries by querying DataFusion session directly
    pub async fn get_all_entries(&self) -> Result<Vec<DirectoryEntry>, TinyLogFSError> {
        let committed_entries = self.query_directory_entries_from_session().await?;
        let pending_entries = self.pending_ops.lock().await.clone();
        
        println!("OpLogDirectory::get_all_entries() - found {} committed entries, {} pending entries", 
                 committed_entries.len(), pending_entries.len());
        for entry in &pending_entries {
            println!("  Pending entry: {} -> {}", entry.name, entry.child);
        }
        
        // Merge committed and pending (pending takes precedence)
        let merged = self.merge_entries(committed_entries, pending_entries);
        println!("OpLogDirectory::get_all_entries() - returning {} merged entries", merged.len());
        Ok(merged)
    }

    /// Query directory entries directly using store path
    async fn query_directory_entries_from_session(&self) -> Result<Vec<DirectoryEntry>, TinyLogFSError> {
        println!("OpLogDirectory::query_directory_entries_from_session() - querying for node_id: {}", self.node_id);
        
        // Use direct Delta Lake reading with the store path
        use deltalake::DeltaOps;
        use futures::stream::StreamExt;
        
        match DeltaOps::try_from_uri(&self.store_path).await {
            Ok(delta_ops) => {
                println!("OpLogDirectory::query_directory_entries_from_session() - opened Delta Lake table");
                
                match delta_ops.load().await {
                    Ok((_table, stream)) => {
                        println!("OpLogDirectory::query_directory_entries_from_session() - created data stream");
                        
                        // Collect all batches
                        let batches: Vec<_> = stream.collect().await;
                        println!("OpLogDirectory::query_directory_entries_from_session() - collected {} batches", batches.len());
                        
                        // Track the latest directory entry by timestamp
                        let mut latest_timestamp = 0i64;
                        let mut latest_entries: Option<Vec<DirectoryEntry>> = None;
                        
                        for (batch_idx, batch_result) in batches.iter().enumerate() {
                            match batch_result {
                                Ok(batch) => {
                                    println!("OpLogDirectory::query_directory_entries_from_session() - batch {} has {} rows", batch_idx, batch.num_rows());
                                    
                                    if batch.num_rows() > 0 {
                                        println!("OpLogDirectory::query_directory_entries_from_session() - processing batch {} with {} rows", batch_idx, batch.num_rows());
                                        
                                        // Look for our specific node_id in the part_id column
                                        if let Some(part_id_array) = batch.column_by_name("part_id") {
                                            println!("OpLogDirectory::query_directory_entries_from_session() - found part_id column with data type: {:?}", part_id_array.data_type());
                                            
                                            // Handle Dictionary(UInt16, Utf8) encoded strings
                                            if let Some(dict_array) = part_id_array.as_any().downcast_ref::<arrow_array::DictionaryArray<arrow_array::types::UInt16Type>>() {
                                                println!("OpLogDirectory::query_directory_entries_from_session() - successfully cast part_id to DictionaryArray");
                                                
                                                // Look for timestamp and content columns
                                                if let Some(timestamp_array) = batch.column_by_name("timestamp") {
                                                    println!("OpLogDirectory::query_directory_entries_from_session() - found timestamp column with data type: {:?}", timestamp_array.data_type());
                                                    if let Some(timestamp_array) = timestamp_array.as_any().downcast_ref::<arrow_array::TimestampMicrosecondArray>() {
                                                        println!("OpLogDirectory::query_directory_entries_from_session() - successfully cast timestamp to TimestampMicrosecondArray");
                                                        
                                                        if let Some(content_array) = batch.column_by_name("content") {
                                                            println!("OpLogDirectory::query_directory_entries_from_session() - found content column with data type: {:?}", content_array.data_type());
                                                            if let Some(content_array) = content_array.as_any().downcast_ref::<arrow_array::BinaryArray>() {
                                                                println!("OpLogDirectory::query_directory_entries_from_session() - successfully cast content to BinaryArray");
                                                                
                                                                // Check each record for our node_id
                                                                for i in 0..batch.num_rows() {
                                                                    // Get the part_id from dictionary array
                                                                    let key_index = dict_array.key(i).unwrap_or(0);
                                                                    let values_array = dict_array.values().as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
                                                                    let part_id = values_array.value(key_index as usize);
                                                                    
                                                                    println!("OpLogDirectory::query_directory_entries_from_session() - checking part_id: '{}' against node_id: '{}'", part_id, self.node_id);
                                                                    // Skip part_id filtering to allow cross-partition queries - check OplogEntry node_id instead
                                                                    // if part_id == self.node_id {
                                                                        let timestamp = timestamp_array.value(i);
                                                                        println!("OpLogDirectory::query_directory_entries_from_session() - found record for node_id: {} with timestamp: {}", self.node_id, timestamp);
                                                                        
                                                                        let content_bytes = content_array.value(i);
                                                                        println!("OpLogDirectory::query_directory_entries_from_session() - record has {} bytes of content", content_bytes.len());
                                                                        // Try to deserialize the OplogEntry from the content
                                                                        match self.deserialize_oplog_entry(content_bytes) {
                                                                            Ok(oplog_entry) => {
                                                                                println!("OpLogDirectory::query_directory_entries_from_session() - found OplogEntry with file_type: {}, node_id: {}", oplog_entry.file_type, oplog_entry.node_id);
                                                                                
                                                                                // CRITICAL: Check that the OplogEntry's node_id matches our target node_id
                                                                                // This ensures we only process directory entries for the correct directory
                                                                                if oplog_entry.node_id != self.node_id {
                                                                                    println!("OpLogDirectory::query_directory_entries_from_session() - skipping record: OplogEntry node_id '{}' != target node_id '{}'", oplog_entry.node_id, self.node_id);
                                                                                    continue;
                                                                                }
                                                                                
                                                                                // Only process if this is newer than what we've seen for our target node_id
                                                                                if timestamp > latest_timestamp {
                                                                                    // If this is a directory entry, deserialize the directory entries from its content
                                                                                    if oplog_entry.file_type == "directory" {
                                                                                        match self.deserialize_directory_entries(&oplog_entry.content) {
                                                                                            Ok(entries) => {
                                                                                                println!("OpLogDirectory::query_directory_entries_from_session() - deserialized {} directory entries from timestamp {} for node_id {}", entries.len(), timestamp, self.node_id);
                                                                                                latest_timestamp = timestamp;
                                                                                                latest_entries = Some(entries);
                                                                                            }
                                                                                            Err(e) => {
                                                                                                println!("OpLogDirectory::query_directory_entries_from_session() - failed to deserialize directory entries: {}", e);
                                                                                            }
                                                                                        }
                                                                                    } else {
                                                                                        println!("OpLogDirectory::query_directory_entries_from_session() - skipping record: file_type '{}' is not directory", oplog_entry.file_type);
                                                                                    }
                                                                                } else {
                                                                                    println!("OpLogDirectory::query_directory_entries_from_session() - skipping older record with timestamp: {} (latest: {})", timestamp, latest_timestamp);
                                                                                }
                                                                            }
                                                                            Err(e) => {
                                                                                println!("OpLogDirectory::query_directory_entries_from_session() - failed to deserialize record: {}", e);
                                                                            }
                                                                        }
                                                                    // }  // Commented out the part_id filter closing brace
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    println!("OpLogDirectory::query_directory_entries_from_session() - batch {} error: {}", batch_idx, e);
                                }
                            }
                        }
                        
                        // Return the latest entries found, or empty if none
                        match latest_entries {
                            Some(entries) => {
                                println!("OpLogDirectory::query_directory_entries_from_session() - returning {} entries from latest timestamp {}", entries.len(), latest_timestamp);
                                Ok(entries)
                            }
                            None => {
                                println!("OpLogDirectory::query_directory_entries_from_session() - no entries found for node_id: {}", self.node_id);
                                Ok(Vec::new())
                            }
                        }
                    }
                    Err(e) => {
                        println!("OpLogDirectory::query_directory_entries_from_session() - failed to load Delta Lake data: {}", e);
                        Ok(Vec::new())
                    }
                }
            }
            Err(e) => {
                println!("OpLogDirectory::query_directory_entries_from_session() - failed to open Delta Lake table: {}", e);
                Ok(Vec::new())
            }
        }
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

    /// Deserialize directory entries from Arrow IPC bytes
    /// Handles both old DirectoryEntry format (2 columns) and new VersionedDirectoryEntry format (5 columns)
    fn deserialize_directory_entries(&self, bytes: &[u8]) -> Result<Vec<DirectoryEntry>, TinyLogFSError> {
        println!("OpLogDirectory::deserialize_directory_entries() - attempting to deserialize {} bytes", bytes.len());
        
        use arrow::ipc::reader::StreamReader;
        
        let mut reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        println!("OpLogDirectory::deserialize_directory_entries() - created StreamReader successfully");
        
        let batch = reader.next().unwrap()
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        println!("OpLogDirectory::deserialize_directory_entries() - read batch with {} rows, {} columns", 
                 batch.num_rows(), batch.num_columns());
        
        // Debug: Print the actual Arrow schema
        println!("OpLogDirectory::deserialize_directory_entries() - batch schema: {:?}", batch.schema());
        for (i, field) in batch.schema().fields().iter().enumerate() {
            println!("  Column {}: name='{}', data_type={:?}", i, field.name(), field.data_type());
        }
        
        // Determine format based on number of columns
        if batch.num_columns() == 5 {
            println!("OpLogDirectory::deserialize_directory_entries() - detected new VersionedDirectoryEntry format (5 columns)");
            
            // Try to deserialize as VersionedDirectoryEntry first
            match serde_arrow::from_record_batch::<Vec<VersionedDirectoryEntry>>(&batch) {
                Ok(versioned_entries) => {
                    println!("OpLogDirectory::deserialize_directory_entries() - successfully deserialized {} versioned entries", versioned_entries.len());
                    
                    // Convert VersionedDirectoryEntry to DirectoryEntry
                    let mut converted_entries = Vec::new();
                    for (i, versioned) in versioned_entries.iter().enumerate() {
                        let entry = DirectoryEntry {
                            name: versioned.name.clone(),
                            child: versioned.child_node_id.clone(),
                        };
                        println!("  Converted entry {}: name='{}', child='{}'", i, entry.name, entry.child);
                        converted_entries.push(entry);
                    }
                    
                    println!("OpLogDirectory::deserialize_directory_entries() - successfully converted {} versioned entries to DirectoryEntry", converted_entries.len());
                    Ok(converted_entries)
                }
                Err(e) => {
                    println!("OpLogDirectory::deserialize_directory_entries() - failed to deserialize as VersionedDirectoryEntry: {}", e);
                    
                    // Manual extraction as fallback for schema mismatches
                    self.extract_directory_entries_manually(&batch)
                }
            }
        } else if batch.num_columns() == 2 {
            println!("OpLogDirectory::deserialize_directory_entries() - detected old DirectoryEntry format (2 columns)");
            
            // Old format: DirectoryEntry
            let entries: Vec<DirectoryEntry> = serde_arrow::from_record_batch(&batch)?;
            println!("OpLogDirectory::deserialize_directory_entries() - successfully deserialized {} old format entries", entries.len());
            Ok(entries)
        } else {
            return Err(TinyLogFSError::Arrow(format!(
                "Unexpected directory entry format: {} columns (expected 2 or 5)", 
                batch.num_columns()
            )));
        }
    }
    
    /// Manual extraction of directory entries from RecordBatch when serde_arrow fails
    /// This handles schema evolution and field name mismatches
    fn extract_directory_entries_manually(&self, batch: &arrow::record_batch::RecordBatch) -> Result<Vec<DirectoryEntry>, TinyLogFSError> {
        use arrow::array::StringArray;
        
        println!("OpLogDirectory::extract_directory_entries_manually() - attempting manual extraction from {} rows", batch.num_rows());
        
        // Find name and child_node_id columns by name
        let mut name_column = None;
        let mut child_column = None;
        
        for (i, field) in batch.schema().fields().iter().enumerate() {
            match field.name().as_str() {
                "name" => name_column = Some(i),
                "child_node_id" => child_column = Some(i),
                "child" => child_column = Some(i), // Handle both old and new field names
                _ => {}
            }
        }
        
        let name_idx = name_column.ok_or_else(|| TinyLogFSError::Arrow("Missing 'name' column".to_string()))?;
        let child_idx = child_column.ok_or_else(|| TinyLogFSError::Arrow("Missing 'child' or 'child_node_id' column".to_string()))?;
        
        // Extract string arrays
        let name_array = batch.column(name_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| TinyLogFSError::Arrow("Name column is not StringArray".to_string()))?;
            
        let child_array = batch.column(child_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| TinyLogFSError::Arrow("Child column is not StringArray".to_string()))?;
        
        // Build DirectoryEntry list
        let mut entries = Vec::new();
        for i in 0..batch.num_rows() {
            let name = name_array.value(i);
            let child = child_array.value(i);
            let entry = DirectoryEntry {
                name: name.to_string(),
                child: child.to_string(),
            };
            println!("  Manually extracted entry {}: name='{}', child='{}'", i, entry.name, entry.child);
            entries.push(entry);
        }
        
        println!("OpLogDirectory::extract_directory_entries_manually() - successfully extracted {} entries", entries.len());
        Ok(entries)
    }
    
    /// Merge committed and pending entries
    fn merge_entries(&self, committed: Vec<DirectoryEntry>, pending: Vec<DirectoryEntry>) -> Vec<DirectoryEntry> {
        use std::collections::HashMap;
        
        let mut entries_map: HashMap<String, DirectoryEntry> = committed
            .into_iter()
            .map(|entry| (entry.name.clone(), entry))
            .collect();
        
        // Pending entries override committed entries
        for entry in pending {
            entries_map.insert(entry.name.clone(), entry);
        }
        
        entries_map.into_values().collect()
    }
    
    /// Add pending entry (for insert operations)
    pub async fn add_pending(&self, name: String, node_ref: NodeRef) {
        // Now that the OpLog backend uses TinyFS node IDs, we can directly use the node's ID
        let tinyfs_node_id = {
            let node_guard = node_ref.lock().await;
            node_guard.id.to_hex_string()
        };
        
        let entry = DirectoryEntry {
            name: name.clone(),
            child: tinyfs_node_id,
        };
        self.pending_ops.lock().await.push(entry);
        
        // Also store the actual NodeRef for quick lookup
        self.pending_nodes.lock().await.insert(name, node_ref);
    }
    
    /// REMOVED: Legacy commit method that used old backend system
    /*
    pub async fn commit_pending(&self, backend: &super::backend::OpLogBackend) -> Result<(), TinyLogFSError> {
        let pending = self.pending_ops.lock().await.clone();
        if pending.is_empty() {
            return Ok(());
        }
        
        println!("OpLogDirectory::commit_pending() - committing {} pending operations for directory {}", pending.len(), self.node_id);
        
        // Get current committed entries and merge with pending
        let committed_entries = self.query_directory_entries_from_session().await.unwrap_or_else(|_| Vec::new());
        let all_entries = self.merge_entries(committed_entries, pending.clone());
        
        // Serialize the updated directory entries
        let serialized_entries = self.serialize_directory_entries(&all_entries)?;
        
        // Create new OplogEntry for this directory with updated content
        let entry = super::schema::OplogEntry {
            part_id: self.node_id.clone(), // Directories are their own partition
            node_id: self.node_id.clone(),
            file_type: "directory".to_string(),
            content: serialized_entries,
        };
        
        // Add to backend's pending records
        backend.add_pending_record(entry).await?;
        
        // Clear pending after adding to backend
        self.pending_ops.lock().await.clear();
        
        println!("OpLogDirectory::commit_pending() - successfully added directory update to backend pending records");
        Ok(())
    }
    */
    
    /// Persist directory content by writing directly to Delta Lake
    /// This bypasses the backend and writes directory updates immediately
    async fn persist_directory_content(&self, entries: Vec<DirectoryEntry>) -> Result<(), TinyLogFSError> {
        use deltalake::{DeltaOps, protocol::SaveMode};
        use crate::delta::{Record, ForArrow};
        
        // Serialize the directory entries
        let serialized_entries = self.serialize_directory_entries(&entries)?;
        
        // Create OplogEntry for this directory with updated content
        let oplog_entry = super::schema::OplogEntry {
            part_id: self.node_id.clone(), // Directories are their own partition
            node_id: self.node_id.clone(),
            file_type: "directory".to_string(),
            content: serialized_entries,
        };
        
        // Create Record for Delta Lake storage
        let record = Record {
            part_id: oplog_entry.part_id.clone(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as i64,
            version: 1,
            content: {
                // Serialize OplogEntry as Arrow IPC bytes
                use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
                let batch = serde_arrow::to_record_batch(&super::schema::OplogEntry::for_arrow(), &vec![oplog_entry])
                    .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
                
                let mut buffer = Vec::new();
                let options = IpcWriteOptions::default();
                let mut writer = StreamWriter::try_new_with_options(&mut buffer, batch.schema().as_ref(), options)
                    .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
                writer.write(&batch)
                    .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
                writer.finish()
                    .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
                buffer
            }
        };
        
        // Write directly to Delta Lake
        let batch = serde_arrow::to_record_batch(&Record::for_arrow(), &vec![record])
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        let table = DeltaOps::try_from_uri(&self.store_path).await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;

        DeltaOps(table.into())
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        println!("OpLogDirectory::persist_directory_content() - successfully wrote {} entries to Delta Lake", entries.len());
        Ok(())
    }

    fn serialize_directory_entries(&self, entries: &[DirectoryEntry]) -> Result<Vec<u8>, TinyLogFSError> {
        use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
        use crate::delta::ForArrow;
        
        println!("OpLogDirectory::serialize_directory_entries() - serializing {} entries", entries.len());
        for (i, entry) in entries.iter().enumerate() {
            println!("  Entry {}: name='{}', child='{}'", i, entry.name, entry.child);
        }
        
        // Convert DirectoryEntry to VersionedDirectoryEntry for consistent schema
        let versioned_entries: Vec<VersionedDirectoryEntry> = entries.iter().map(|entry| {
            VersionedDirectoryEntry {
                name: entry.name.clone(),
                child_node_id: entry.child.clone(),
                operation_type: OperationType::Insert,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as i64,
                version: 1,
            }
        }).collect();
        
        let batch = serde_arrow::to_record_batch(&VersionedDirectoryEntry::for_arrow(), &versioned_entries)?;
        println!("OpLogDirectory::serialize_directory_entries() - created record batch with {} rows, {} columns", 
                 batch.num_rows(), batch.num_columns());
        
        let mut buffer = Vec::new();
        let options = IpcWriteOptions::default();
        let mut writer = StreamWriter::try_new_with_options(&mut buffer, batch.schema().as_ref(), options)
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            
        writer.write(&batch)
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
            
        writer.finish()
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?;
        
        println!("OpLogDirectory::serialize_directory_entries() - serialized to {} bytes", buffer.len());
        Ok(buffer)
    }
}

#[async_trait::async_trait]
impl Directory for OpLogDirectory {
    async fn get(&self, name: &str) -> tinyfs::Result<Option<NodeRef>> {
        // First check pending nodes (newly created nodes)
        if let Some(node_ref) = self.pending_nodes.lock().await.get(name) {
            println!("OpLogDirectory::get('{}') - found in pending nodes", name);
            return Ok(Some(node_ref.clone()));
        }
        
        // Query all entries from storage to find the requested name
        let all_entries = self.get_all_entries().await
            .map_err(|e| tinyfs::Error::immutable(format!("Failed to query directory entries: {}", e)))?;
        
        println!("OpLogDirectory::get('{}') - searching in {} committed entries", name, all_entries.len());
        let entry_count = all_entries.len(); // Store the count before moving
        
        // Find entry by name and create NodeRef on-demand
        for entry in all_entries {
            println!("OpLogDirectory::get('{}') - checking entry '{}' (looking for '{}')", name, entry.name, name);
            if entry.name == name {
                println!("OpLogDirectory::get('{}') - ✅ FOUND entry '{}' with child node_id: {}", name, entry.name, entry.child);
                
                // Parse the hex node_id back to NodeID
                if let Ok(node_id_value) = u64::from_str_radix(&entry.child, 16) {
                    let node_id = tinyfs::NodeID::new(node_id_value as usize);
                    
                    // Query the actual entry from Delta Lake to determine its type and create the appropriate handle
                    match self.query_entry_by_node_id(&entry.child).await {
                        Ok(Some(oplog_entry)) => {
                            println!("OpLogDirectory::get('{}') - found OplogEntry with file_type: {}", name, oplog_entry.file_type);
                            
                            match oplog_entry.file_type.as_str() {
                                "directory" => {
                                    // Create directory handle for this restored directory
                                    println!("OpLogDirectory::get('{}') - creating child directory with oplog_entry.node_id: {}", name, oplog_entry.node_id);
                                    let child_oplog_dir = OpLogDirectory::new_with_session(
                                        oplog_entry.node_id.clone(),
                                        self.session_ctx.clone(),
                                        self.table_name.clone(),
                                        self.store_path.clone()
                                    );
                                    let dir_handle = OpLogDirectory::create_handle(child_oplog_dir);
                                    let node_type = tinyfs::NodeType::Directory(dir_handle);
                                    
                                    // Create the node directly here (on-demand creation)
                                    let node = tinyfs::Node {
                                        id: node_id,
                                        node_type,
                                    };
                                    let node_ref = tinyfs::NodeRef::new(std::sync::Arc::new(tokio::sync::Mutex::new(node)));
                                    
                                    println!("OpLogDirectory::get('{}') - ✅ created on-demand directory NodeRef with id: {}", name, node_id_value);
                                    return Ok(Some(node_ref));
                                }
                                "file" => {
                                    // Create file handle for this restored file
                                    let oplog_file = super::file::OpLogFile::new_with_content(
                                        oplog_entry.node_id.clone(), 
                                        self.store_path.clone(), 
                                        oplog_entry.content.clone()
                                    );
                                    let file_handle = super::file::OpLogFile::create_handle(oplog_file);
                                    let node_type = tinyfs::NodeType::File(file_handle);
                                    
                                    // Create the node directly here (on-demand creation)
                                    let node = tinyfs::Node {
                                        id: node_id,
                                        node_type,
                                    };
                                    let node_ref = tinyfs::NodeRef::new(std::sync::Arc::new(tokio::sync::Mutex::new(node)));
                                    
                                    println!("OpLogDirectory::get('{}') - ✅ created on-demand file NodeRef with id: {}", name, node_id_value);
                                    return Ok(Some(node_ref));
                                }
                                "symlink" => {
                                    // Create symlink handle for this restored symlink
                                    let target = std::str::from_utf8(&oplog_entry.content)
                                        .unwrap_or("").to_string();
                                    let oplog_symlink = super::symlink::OpLogSymlink::new(
                                        oplog_entry.node_id.clone(),
                                        std::path::PathBuf::from(target),
                                        self.store_path.clone()
                                    );
                                    let symlink_handle = super::symlink::OpLogSymlink::create_handle(oplog_symlink);
                                    let node_type = tinyfs::NodeType::Symlink(symlink_handle);
                                    
                                    // Create the node directly here (on-demand creation)
                                    let node = tinyfs::Node {
                                        id: node_id,
                                        node_type,
                                    };
                                    let node_ref = tinyfs::NodeRef::new(std::sync::Arc::new(tokio::sync::Mutex::new(node)));
                                    
                                    println!("OpLogDirectory::get('{}') - ✅ created on-demand symlink NodeRef with id: {}", name, node_id_value);
                                    return Ok(Some(node_ref));
                                }
                                _ => {
                                    println!("OpLogDirectory::get('{}') - unknown entry type: {}", name, oplog_entry.file_type);
                                    return Ok(None);
                                }
                            }
                        }
                        Ok(None) => {
                            println!("OpLogDirectory::get('{}') - entry not found in Delta Lake for node_id: {}", name, entry.child);
                            return Ok(None);
                        }
                        Err(e) => {
                            println!("OpLogDirectory::get('{}') - error querying Delta Lake: {}", name, e);
                            return Ok(None);
                        }
                    }
                } else {
                    println!("OpLogDirectory::get('{}') - failed to parse hex node_id: {}", name, entry.child);
                    return Ok(None);
                }
            }
        }
        
        println!("OpLogDirectory::get('{}') - not found in {} entries", name, entry_count);
        Ok(None)
    }
    
    async fn insert(&mut self, name: String, node: NodeRef) -> tinyfs::Result<()> {
        println!("OpLogDirectory::insert('{}')", name);
        
        // Add to pending operations - this is enough for the immediate exists() check to work
        // because get_all_entries() includes both committed and pending entries
        self.add_pending(name.clone(), node).await;
        
        // IMMEDIATE PERSISTENCE: Update the directory content immediately
        // This ensures that when the filesystem is reopened, the directory entries are persisted
        let pending = self.pending_ops.lock().await.clone();
        
        // Get current committed entries and merge with pending
        let committed_entries = self.query_directory_entries_from_session().await.unwrap_or_else(|_| Vec::new());
        let all_entries = self.merge_entries(committed_entries, pending.clone());
        
        // Create a temporary backend access through static methods to persist the directory update
        // We need to create the OplogEntry and add it to a temporary backend for committing
        match self.persist_directory_content(all_entries).await {
            Ok(_) => {
                println!("OpLogDirectory::insert('{}') - successfully persisted directory content", name);
                // Clear pending after successful persistence
                self.pending_ops.lock().await.clear();
            }
            Err(e) => {
                println!("OpLogDirectory::insert('{}') - failed to persist directory content: {}", name, e);
                // Keep in pending if persistence failed - will be tried again later
            }
        }
        
        println!("OpLogDirectory::insert('{}') - completed", name);
        Ok(())
    }
    
    async fn entries(&self) -> tinyfs::Result<std::pin::Pin<Box<dyn futures::Stream<Item = tinyfs::Result<(String, NodeRef)>> + Send>>> {
        // Query all entries using the new DataFusion-based approach
        let all_entries = self.get_all_entries().await
            .map_err(|e| tinyfs::Error::immutable(format!("Failed to query directory entries: {}", e)))?;
        
        println!("OpLogDirectory::entries() - found {} entries, creating NodeRef instances", all_entries.len());
        
        // Create NodeRef instances for each entry using the same logic as get()
        let mut entry_results = Vec::new();
        
        for entry in all_entries {
            println!("  Processing entry: {} -> {}", entry.name, entry.child);
            
            // Parse the hex node_id back to NodeID
            if let Ok(node_id_value) = u64::from_str_radix(&entry.child, 16) {
                let node_id = tinyfs::NodeID::new(node_id_value as usize);
                
                // Query the actual entry from Delta Lake to determine its type and create the appropriate handle
                match self.query_entry_by_node_id(&entry.child).await {
                    Ok(Some(oplog_entry)) => {
                        match oplog_entry.file_type.as_str() {
                            "directory" => {
                                // Create directory handle for this restored directory
                                let child_oplog_dir = OpLogDirectory::new_with_session(
                                    oplog_entry.node_id.clone(),
                                    self.session_ctx.clone(),
                                    self.table_name.clone(),
                                    self.store_path.clone()
                                );
                                let dir_handle = OpLogDirectory::create_handle(child_oplog_dir);
                                let node_type = tinyfs::NodeType::Directory(dir_handle);
                                
                                let node = tinyfs::Node {
                                    id: node_id,
                                    node_type,
                                };
                                let node_ref = tinyfs::NodeRef::new(std::sync::Arc::new(tokio::sync::Mutex::new(node)));
                                entry_results.push(Ok((entry.name, node_ref)));
                            }
                            "file" => {
                                // Create file handle for this restored file
                                let oplog_file = super::file::OpLogFile::new_with_content(
                                    oplog_entry.node_id.clone(), 
                                    self.store_path.clone(), 
                                    oplog_entry.content.clone()
                                );
                                let file_handle = super::file::OpLogFile::create_handle(oplog_file);
                                let node_type = tinyfs::NodeType::File(file_handle);
                                
                                let node = tinyfs::Node {
                                    id: node_id,
                                    node_type,
                                };
                                let node_ref = tinyfs::NodeRef::new(std::sync::Arc::new(tokio::sync::Mutex::new(node)));
                                entry_results.push(Ok((entry.name, node_ref)));
                            }
                            "symlink" => {
                                // Create symlink handle for this restored symlink
                                let target = std::str::from_utf8(&oplog_entry.content)
                                    .unwrap_or("").to_string();
                                let oplog_symlink = super::symlink::OpLogSymlink::new(
                                    oplog_entry.node_id.clone(),
                                    std::path::PathBuf::from(target),
                                    self.store_path.clone()
                                );
                                let symlink_handle = super::symlink::OpLogSymlink::create_handle(oplog_symlink);
                                let node_type = tinyfs::NodeType::Symlink(symlink_handle);
                                
                                let node = tinyfs::Node {
                                    id: node_id,
                                    node_type,
                                };
                                let node_ref = tinyfs::NodeRef::new(std::sync::Arc::new(tokio::sync::Mutex::new(node)));
                                entry_results.push(Ok((entry.name, node_ref)));
                            }
                            _ => {
                                println!("  Unknown entry type: {}", oplog_entry.file_type);
                                entry_results.push(Err(tinyfs::Error::immutable(format!("Unknown entry type: {}", oplog_entry.file_type))));
                            }
                        }
                    }
                    Ok(None) => {
                        println!("  Entry not found in Delta Lake for node_id: {}", entry.child);
                        entry_results.push(Err(tinyfs::Error::immutable(format!("Entry not found for node_id: {}", entry.child))));
                    }
                    Err(e) => {
                        println!("  Error querying Delta Lake: {}", e);
                        entry_results.push(Err(tinyfs::Error::immutable(format!("Query error: {}", e))));
                    }
                }
            } else {
                println!("  Failed to parse hex node_id: {}", entry.child);
                entry_results.push(Err(tinyfs::Error::immutable(format!("Invalid node_id: {}", entry.child))));
            }
        }
        
        println!("OpLogDirectory::entries() - created {} entry results", entry_results.len());
        
        use futures::stream;
        Ok(Box::pin(stream::iter(entry_results)))
    }
}

impl OpLogDirectory {
    /// Query a specific OplogEntry by node_id from Delta Lake
    /// This is used for on-demand node creation when directory entries are accessed
    async fn query_entry_by_node_id(&self, node_id: &str) -> Result<Option<super::schema::OplogEntry>, TinyLogFSError> {
        println!("OpLogDirectory::query_entry_by_node_id() - querying for node_id: {}", node_id);
        
        // Read directly from Delta Lake to find the specific node
        use deltalake::DeltaOps;
        use futures::stream::StreamExt;
        
        match DeltaOps::try_from_uri(&self.store_path).await {
            Ok(delta_ops) => {
                match delta_ops.load().await {
                    Ok((_table, stream)) => {
                        // Collect all batches
                        let batches: Vec<_> = stream.collect().await;
                        
                        for batch_result in batches.iter() {
                            match batch_result {
                                Ok(batch) => {
                                    if batch.num_rows() > 0 && batch.column_by_name("content").is_some() {
                                        if let Some(content_array) = batch.column_by_name("content")
                                            .and_then(|col| col.as_any().downcast_ref::<arrow_array::BinaryArray>()) 
                                        {
                                            // Process each record in this batch
                                            for i in 0..batch.num_rows() {
                                                let content_bytes = content_array.value(i);
                                                
                                                // Try to deserialize the OplogEntry
                                                match self.deserialize_oplog_entry(content_bytes) {
                                                    Ok(oplog_entry) => {
                                                        if oplog_entry.node_id == node_id {
                                                            println!("OpLogDirectory::query_entry_by_node_id() - found matching entry");
                                                            return Ok(Some(oplog_entry));
                                                        }
                                                    }
                                                    Err(_) => {
                                                        // Skip entries that can't be deserialized
                                                        continue;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(_) => {
                                    // Skip batches with errors
                                    continue;
                                }
                            }
                        }
                        
                        println!("OpLogDirectory::query_entry_by_node_id() - entry not found for node_id: {}", node_id);
                        Ok(None)
                    }
                    Err(e) => {
                        println!("OpLogDirectory::query_entry_by_node_id() - failed to load Delta Lake data: {}", e);
                        Ok(None)
                    }
                }
            }
            Err(e) => {
                println!("OpLogDirectory::query_entry_by_node_id() - failed to open Delta Lake table: {}", e);
                Ok(None)
            }
        }
    }
}
