// Phase 1 TLogFS Schema Implementation - Working and Tested
use crate::delta::ForArrow;
use arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use chrono::Utc;
use deltalake::DeltaOps;
use deltalake::protocol::SaveMode;
use datafusion::common::Result;
use tinyfs::NodeID;

/// Filesystem entry stored in the operation log
/// This represents a single filesystem operation (create, update, delete)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OplogEntry {
    /// Hex-encoded partition ID (parent directory for files/symlinks, self for directories)
    pub part_id: String,
    /// Hex-encoded NodeID from TinyFS
    pub node_id: String,
    /// Type of filesystem entry (file, directory, or symlink)
    pub file_type: tinyfs::EntryType,
    /// Timestamp when this node was modified (microseconds since Unix epoch)
    pub timestamp: i64,
    /// Per-node modification version counter (starts at 1, increments on each change)
    pub version: i64,
    /// Type-specific content:
    /// - For files: raw file data
    /// - For symlinks: target path
    /// - For directories: Arrow IPC encoded VersionedDirectoryEntry records
    pub content: Vec<u8>,
}

impl ForArrow for OplogEntry {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("part_id", DataType::Utf8, false)),
            Arc::new(Field::new("node_id", DataType::Utf8, false)),
            Arc::new(Field::new("file_type", DataType::Utf8, false)),
            Arc::new(Field::new(
                "timestamp",
                DataType::Timestamp(
                    TimeUnit::Microsecond,
                    Some("UTC".into()),
                ),
                false,
            )),
            Arc::new(Field::new("version", DataType::Int64, false)),
            Arc::new(Field::new("content", DataType::Binary, false)),
        ]
    }
}

impl OplogEntry {
    /// Create Arrow schema for OplogEntry
    pub fn create_schema() -> Arc<arrow::datatypes::Schema> {
        Arc::new(arrow::datatypes::Schema::new(Self::for_arrow()))
    }
}

/// Extended directory entry with versioning support
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionedDirectoryEntry {
    /// Entry name within the directory
    pub name: String,
    /// Hex-encoded NodeID of the child
    pub child_node_id: String,
    /// Type of operation
    pub operation_type: OperationType,
    /// Type of node (file, directory, or symlink)
    pub node_type: tinyfs::EntryType,
}

impl VersionedDirectoryEntry {
    /// Create a new directory entry with EntryType (convenience constructor)
    pub fn new(name: String, child_node_id: String, operation_type: OperationType, entry_type: tinyfs::EntryType) -> Self {
        Self {
            name,
            child_node_id,
            operation_type,
            node_type: entry_type,
        }
    }
    
    /// Get the node type as an EntryType (Copy trait makes this simple)
    pub fn entry_type(&self) -> tinyfs::EntryType {
        self.node_type
    }
}

/// Operation type for directory mutations
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum OperationType {
    Insert,
    Delete,
    Update,
}

impl ForArrow for VersionedDirectoryEntry {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("child_node_id", DataType::Utf8, false)),
            Arc::new(Field::new("operation_type", DataType::Utf8, false)),
            Arc::new(Field::new("node_type", DataType::Utf8, false)),
        ]
    }
}

/// Creates a new Delta table with Record schema and initializes it with a root directory OplogEntry
pub async fn create_oplog_table(table_path: &str) -> Result<(), crate::error::TLogFSError> {
    // Try to open existing table first
    match deltalake::open_table(table_path).await {
        Ok(_) => {
            // Table already exists, nothing to do
            return Ok(());
        }
        Err(_) => {
            // Table doesn't exist, create it
        }
    }
    
    // Create the table with Record schema (same as the original delta.rs approach)
    let table = DeltaOps::try_from_uri(table_path).await?;
    let table = table
        .create()
        .with_columns(crate::delta::Record::for_delta())
        .with_partition_columns(["part_id"])
        .await?;

    // Create a root directory entry as the initial OplogEntry
    let root_node_id = NodeID::root().to_string();
    let now = Utc::now().timestamp_micros();
    let root_entry = OplogEntry {
        part_id: root_node_id.clone(), // Root directory is its own partition
        node_id: root_node_id.clone(),
        file_type: tinyfs::EntryType::Directory,
        content: encode_versioned_directory_entries(&vec![])?, // Empty directory with versioned schema
        timestamp: now, // Node modification time
        version: 1, // First version of root directory node
    };

    // Serialize the OplogEntry as a Record for storage
    let record = crate::delta::Record {
        part_id: root_node_id.clone(), // Use the same part_id
        node_id: root_node_id, // For root directory, node_id equals part_id
        timestamp: Utc::now().timestamp_micros(),
        content: encode_oplog_entry_to_buffer(root_entry)?,
    };

    // Create a record batch and write it
    let batch = serde_arrow::to_record_batch(&crate::delta::Record::for_arrow(), &[record])?;
    let _table = DeltaOps(table)
        .write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .await?;

    Ok(())
}

/// Encode OplogEntry as Arrow IPC bytes for storage in Record.content
fn encode_oplog_entry_to_buffer(entry: OplogEntry) -> Result<Vec<u8>, crate::error::TLogFSError> {
    use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};

    let batch = serde_arrow::to_record_batch(&OplogEntry::for_arrow(), &[entry])?;

    let mut buffer = Vec::new();
    let options = IpcWriteOptions::default();
    let mut writer =
        StreamWriter::try_new_with_options(&mut buffer, batch.schema().as_ref(), options)?;
    writer.write(&batch)?;
    writer.finish()?;
    Ok(buffer)
}

/// Encode VersionedDirectoryEntry records as Arrow IPC bytes for storage in OplogEntry.content
fn encode_versioned_directory_entries(entries: &Vec<VersionedDirectoryEntry>) -> Result<Vec<u8>, crate::error::TLogFSError> {
    use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
    use arrow::array::{StringArray, RecordBatch};

    let entry_count = entries.len();
    diagnostics::log_debug!("encode_versioned_directory_entries() - encoding {entry_count} entries", entry_count: entry_count);
    for (i, entry) in entries.iter().enumerate() {
        let name = &entry.name;
        let child_node_id = &entry.child_node_id;
        diagnostics::log_debug!("  Entry {i}: name='{name}', child_node_id='{child_node_id}'", 
                                i: i, name: name, child_node_id: child_node_id);
    }

    // Handle empty directory case by creating an empty record batch with proper schema
    let batch = if entries.is_empty() {
        let schema = Arc::new(arrow::datatypes::Schema::new(VersionedDirectoryEntry::for_arrow()));
        let empty_name_array = StringArray::from(Vec::<String>::new());
        let empty_child_id_array = StringArray::from(Vec::<String>::new());
        let empty_op_type_array = StringArray::from(Vec::<String>::new());
        let empty_node_type_array = StringArray::from(Vec::<String>::new());
        
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(empty_name_array),
                Arc::new(empty_child_id_array),
                Arc::new(empty_op_type_array),
                Arc::new(empty_node_type_array),
            ],
        )?
    } else {
        serde_arrow::to_record_batch(&VersionedDirectoryEntry::for_arrow(), entries)?
    };
    
    let row_count = batch.num_rows();
    let col_count = batch.num_columns();
    diagnostics::log_debug!("encode_versioned_directory_entries() - created batch with {row_count} rows, {col_count} columns", 
                            row_count: row_count, col_count: col_count);

    let mut buffer = Vec::new();
    let options = IpcWriteOptions::default();
    let mut writer =
        StreamWriter::try_new_with_options(&mut buffer, batch.schema().as_ref(), options)?;
    writer.write(&batch)?;
    writer.finish()?;
    
    let buffer_len = buffer.len();
    diagnostics::log_debug!("encode_versioned_directory_entries() - encoded to {buffer_len} bytes", buffer_len: buffer_len);
    Ok(buffer)
}
