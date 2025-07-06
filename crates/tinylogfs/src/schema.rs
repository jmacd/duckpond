// Phase 1 TinyLogFS Schema Implementation - Working and Tested
use oplog::delta::ForArrow;
use arrow::datatypes::{DataType, Field, FieldRef};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use chrono::Utc;
use deltalake::DeltaOps;
use deltalake::protocol::SaveMode;
use datafusion::common::Result;

fn nodestr(id: u64) -> String {
    // Use friendly format for consistency everywhere
    if id <= 0xFFFF {
        // 0-65535: show as exactly 4 hex digits
        format!("{:04X}", id)
    } else if id <= 0xFFFFFFFF {
        // 65536-4294967295: show as exactly 8 hex digits
        format!("{:08X}", id)
    } else if id <= 0xFFFFFFFFFFFF {
        // Show as exactly 12 hex digits
        format!("{:012X}", id)
    } else {
        // Show as exactly 16 hex digits
        format!("{:016X}", id)
    }
}

/// Filesystem entry stored in the operation log
/// This represents a single filesystem operation (create, update, delete)
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct OplogEntry {
    /// Hex-encoded partition ID (parent directory for files/symlinks, self for directories)
    pub part_id: String,
    /// Hex-encoded NodeID from TinyFS
    pub node_id: String,
    /// Type of filesystem entry: "file", "directory", or "symlink"
    pub file_type: String,
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
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct VersionedDirectoryEntry {
    /// Entry name within the directory
    pub name: String,
    /// Hex-encoded NodeID of the child
    pub child_node_id: String,
    /// Type of operation
    pub operation_type: OperationType,
    /// Timestamp when operation occurred
    pub timestamp: i64,
    /// Version number for ordering
    pub version: i64,
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
            Arc::new(Field::new("timestamp", DataType::Int64, false)),
            Arc::new(Field::new("version", DataType::Int64, false)),
        ]
    }
}

/// Creates a new Delta table with Record schema and initializes it with a root directory OplogEntry
pub async fn create_oplog_table(table_path: &str) -> Result<(), oplog::error::Error> {
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
        .with_columns(oplog::delta::Record::for_delta())
        .with_partition_columns(["part_id"])
        .await?;

    // Create a root directory entry as the initial OplogEntry
    let root_node_id = nodestr(0);
    let root_entry = OplogEntry {
        part_id: root_node_id.clone(), // Root directory is its own partition
        node_id: root_node_id.clone(),
        file_type: "directory".to_string(),
        content: encode_versioned_directory_entries(&vec![])?, // Empty directory with versioned schema
    };

    // Serialize the OplogEntry as a Record for storage
    let record = oplog::delta::Record {
        part_id: root_node_id.clone(), // Use the same part_id
        timestamp: Utc::now().timestamp_micros(),
        content: encode_oplog_entry_to_buffer(root_entry)?,
        version: 0, // Root directory is created as version 0 (initial version)
    };

    // Create a record batch and write it
    let batch = serde_arrow::to_record_batch(&oplog::delta::Record::for_arrow(), &[record])?;
    let _table = DeltaOps(table)
        .write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .await?;

    Ok(())
}

/// Encode OplogEntry as Arrow IPC bytes for storage in Record.content
fn encode_oplog_entry_to_buffer(entry: OplogEntry) -> Result<Vec<u8>, oplog::error::Error> {
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
fn encode_versioned_directory_entries(entries: &Vec<VersionedDirectoryEntry>) -> Result<Vec<u8>, oplog::error::Error> {
    use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};

    let entry_count = entries.len();
    diagnostics::log_debug!("encode_versioned_directory_entries() - encoding {entry_count} entries", entry_count: entry_count);
    for (i, entry) in entries.iter().enumerate() {
        let name = &entry.name;
        let child_node_id = &entry.child_node_id;
        diagnostics::log_debug!("  Entry {i}: name='{name}', child_node_id='{child_node_id}'", 
                                i: i, name: name, child_node_id: child_node_id);
    }

    let batch = serde_arrow::to_record_batch(&VersionedDirectoryEntry::for_arrow(), entries)?;
    
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
