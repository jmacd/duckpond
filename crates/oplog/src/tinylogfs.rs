use crate::delta::ForArrow;
use arrow::datatypes::{DataType, Field, FieldRef};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use chrono::Utc;
use deltalake::DeltaOps;
use deltalake::protocol::SaveMode;
use datafusion::common::Result;

fn nodestr(id: u64) -> String {
    format!("{:016x}", id)
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
    /// - For directories: Arrow IPC encoded DirectoryEntry records
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

/// Directory entry for nested storage within OplogEntry content
/// Used when OplogEntry.file_type == "directory"
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct DirectoryEntry {
    /// Entry name within the directory
    pub name: String,
    /// Hex-encoded NodeID of the child
    pub child: String,
}

impl ForArrow for DirectoryEntry {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("child", DataType::Utf8, false)),
        ]
    }
}

/// Creates a new Delta table with Record schema and initializes it with a root directory OplogEntry
pub async fn create_oplog_table(table_path: &str) -> Result<(), crate::error::Error> {
    // Create the table with Record schema (same as the original delta.rs approach)
    let table = DeltaOps::try_from_uri(table_path).await?;
    let table = table
        .create()
        .with_columns(crate::delta::Record::for_delta())
        .with_partition_columns(["part_id"])
        .await?;

    // Create a root directory entry as the initial OplogEntry
    let root_node_id = nodestr(0);
    let root_entry = OplogEntry {
        part_id: root_node_id.clone(), // Root directory is its own partition
        node_id: root_node_id.clone(),
        file_type: "directory".to_string(),
        content: encode_directory_entries(&vec![])?, // Empty directory
    };

    // Serialize the OplogEntry as a Record for storage
    let record = crate::delta::Record {
        part_id: root_node_id.clone(), // Use the same part_id
        timestamp: Utc::now().timestamp_micros(),
        version: 0,
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
fn encode_oplog_entry_to_buffer(entry: OplogEntry) -> Result<Vec<u8>, crate::error::Error> {
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

/// Encode DirectoryEntry records as Arrow IPC bytes for storage in OplogEntry.content
fn encode_directory_entries(entries: &Vec<DirectoryEntry>) -> Result<Vec<u8>, crate::error::Error> {
    use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};

    let batch = serde_arrow::to_record_batch(&DirectoryEntry::for_arrow(), entries)?;

    let mut buffer = Vec::new();
    let options = IpcWriteOptions::default();
    let mut writer =
        StreamWriter::try_new_with_options(&mut buffer, batch.schema().as_ref(), options)?;
    writer.write(&batch)?;
    writer.finish()?;
    Ok(buffer)
}
