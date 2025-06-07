// Transaction state management using Arrow Array builders
use crate::tinylogfs::{OplogEntry, DirectoryEntry};
use super::error::TinyLogFSError;
use arrow_array::builder::{StringBuilder, Int64Builder, BinaryBuilder};
use arrow_array::{RecordBatch, ArrayRef};
use arrow_schema::{Schema, Field, DataType};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::Arc;
use uuid::Uuid;

/// Transaction state using Arrow Array builders for columnar accumulation
#[derive(Debug)]
pub struct TransactionState {
    // Arrow Array builders for pending transaction data
    part_id_builder: StringBuilder,      // Partition IDs (parent directory)
    timestamp_builder: Int64Builder,     // Operation timestamps  
    version_builder: Int64Builder,       // Version numbers
    content_builder: BinaryBuilder,      // Serialized Arrow IPC content
    
    // Transaction metadata
    transaction_id: String,
    started_at: SystemTime,
    operation_count: usize,
}

/// Filesystem operations that can be accumulated in transaction state
#[derive(Debug, Clone)]
pub enum FilesystemOperation {
    CreateFile { 
        path: PathBuf, 
        content: Vec<u8>,
        node_id: String,
        parent_id: String,
    },
    CreateDirectory { 
        path: PathBuf,
        node_id: String,
        parent_id: Option<String>,
    },
    CreateSymlink { 
        path: PathBuf, 
        target: PathBuf,
        node_id: String,
        parent_id: String,
    },
    UpdateFile { 
        node_id: String, 
        content: Vec<u8> 
    },
    DeleteNode { 
        node_id: String 
    },
    UpdateDirectory {
        node_id: String,
        entries: Vec<DirectoryEntry>,
    },
}

impl TransactionState {
    pub fn new() -> Self {
        Self {
            part_id_builder: StringBuilder::new(),
            timestamp_builder: Int64Builder::new(),
            version_builder: Int64Builder::new(),
            content_builder: BinaryBuilder::new(),
            transaction_id: Uuid::new_v4().to_string(),
            started_at: SystemTime::now(),
            operation_count: 0,
        }
    }
    
    /// Add a filesystem operation to the transaction builders
    pub fn add_operation(&mut self, operation: &FilesystemOperation) -> Result<(), TinyLogFSError> {
        match operation {
            FilesystemOperation::CreateFile { content, node_id, parent_id, .. } => {
                self.add_file_operation(parent_id, node_id, "file", content)?;
            },
            FilesystemOperation::CreateDirectory { node_id, parent_id, .. } => {
                // Directory is its own partition, but use parent_id if available
                let part_id = parent_id.as_ref().unwrap_or(node_id);
                let directory_entries: Vec<DirectoryEntry> = vec![];
                let directory_content = serialize_directory_entries(&directory_entries)?;
                
                let oplog_entry = OplogEntry {
                    part_id: part_id.clone(),
                    node_id: node_id.clone(),
                    file_type: "directory".to_string(),
                    content: directory_content,
                };
                let serialized = serialize_oplog_entry(&oplog_entry)?;
                
                self.part_id_builder.append_value(part_id);
                self.add_common_fields(&serialized)?;
            },
            FilesystemOperation::CreateSymlink { target, node_id, parent_id, .. } => {
                let target_bytes = target.to_string_lossy().as_bytes().to_vec();
                self.add_file_operation(parent_id, node_id, "symlink", &target_bytes)?;
            },
            FilesystemOperation::UpdateFile { node_id, content } => {
                // For updates, we need to determine the parent_id (would need metadata lookup)
                // For now, use node_id as partition (could be optimized later)
                self.add_file_operation(node_id, node_id, "file", content)?;
            },
            FilesystemOperation::UpdateDirectory { node_id, entries } => {
                let directory_content = serialize_directory_entries(entries)?;
                let oplog_entry = OplogEntry {
                    part_id: node_id.clone(), // Directory is its own partition
                    node_id: node_id.clone(),
                    file_type: "directory".to_string(),
                    content: directory_content,
                };
                let serialized = serialize_oplog_entry(&oplog_entry)?;
                
                self.part_id_builder.append_value(node_id);
                self.add_common_fields(&serialized)?;
            },
            FilesystemOperation::DeleteNode { node_id } => {
                // Deletion could be represented as an empty content with special metadata
                let oplog_entry = OplogEntry {
                    part_id: node_id.clone(),
                    node_id: node_id.clone(),
                    file_type: "deleted".to_string(),
                    content: vec![],
                };
                let serialized = serialize_oplog_entry(&oplog_entry)?;
                
                self.part_id_builder.append_value(node_id);
                self.add_common_fields(&serialized)?;
            },
        }
        Ok(())
    }
    
    /// Helper method to add file/symlink operations
    fn add_file_operation(
        &mut self, 
        parent_id: &str, 
        node_id: &str, 
        file_type: &str, 
        content: &[u8]
    ) -> Result<(), TinyLogFSError> {
        let oplog_entry = OplogEntry {
            part_id: parent_id.to_string(),
            node_id: node_id.to_string(),
            file_type: file_type.to_string(),
            content: content.to_vec(),
        };
        let serialized = serialize_oplog_entry(&oplog_entry)?;
        
        self.part_id_builder.append_value(parent_id);
        self.add_common_fields(&serialized)?;
        Ok(())
    }
    
    /// Add common timestamp, version, content fields
    fn add_common_fields(&mut self, serialized_content: &[u8]) -> Result<(), TinyLogFSError> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))?
            .as_millis() as i64;
            
        self.timestamp_builder.append_value(timestamp);
        self.version_builder.append_value(1); // For now, always version 1
        self.content_builder.append_value(serialized_content);
        
        self.operation_count += 1;
        Ok(())
    }
    
    /// Snapshot the current builders into a RecordBatch and reset
    pub fn snapshot_to_record_batch(&mut self) -> Result<RecordBatch, TinyLogFSError> {
        let part_id_array = self.part_id_builder.finish();
        let timestamp_array = self.timestamp_builder.finish();
        let version_array = self.version_builder.finish();
        let content_array = self.content_builder.finish();
        
        // Create new builders for next batch
        self.part_id_builder = StringBuilder::new();
        self.timestamp_builder = Int64Builder::new();
        self.version_builder = Int64Builder::new();
        self.content_builder = BinaryBuilder::new();
        
        // Reset operation count
        let committed_count = self.operation_count;
        self.operation_count = 0;
        
        // Create RecordBatch with delta Record schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("part_id", DataType::Utf8, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("version", DataType::Int64, false),
            Field::new("content", DataType::Binary, false),
        ]));
        
        let columns: Vec<ArrayRef> = vec![
            Arc::new(part_id_array),
            Arc::new(timestamp_array),
            Arc::new(version_array),
            Arc::new(content_array),
        ];
        
        RecordBatch::try_new(schema, columns)
            .map_err(|e| TinyLogFSError::Arrow(e.to_string()))
    }
    
    /// Get current transaction status
    pub fn get_status(&self) -> TransactionStatus {
        TransactionStatus {
            transaction_id: self.transaction_id.clone(),
            started_at: self.started_at,
            operation_count: self.operation_count,
            has_pending_operations: self.operation_count > 0,
        }
    }
    
    /// Check if there are pending operations
    pub fn has_pending_operations(&self) -> bool {
        self.operation_count > 0
    }
}

#[derive(Debug, Clone)]
pub struct TransactionStatus {
    pub transaction_id: String,
    pub started_at: SystemTime,
    pub operation_count: usize,
    pub has_pending_operations: bool,
}

/// Serialize OplogEntry as Arrow IPC bytes
fn serialize_oplog_entry(entry: &OplogEntry) -> Result<Vec<u8>, TinyLogFSError> {
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

/// Serialize DirectoryEntry records as Arrow IPC bytes
fn serialize_directory_entries(entries: &[DirectoryEntry]) -> Result<Vec<u8>, TinyLogFSError> {
    use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
    use crate::delta::ForArrow;

    let batch = serde_arrow::to_record_batch(&DirectoryEntry::for_arrow(), entries)?;

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
