// Phase 2 TLogFS Schema Implementation - Abstraction Consolidation
use arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use chrono::Utc;
use deltalake::DeltaOps;
use deltalake::protocol::SaveMode;
use datafusion::common::Result;
use tinyfs::NodeID;
use std::collections::HashMap;
use deltalake::kernel::{
    DataType as DeltaDataType, PrimitiveType, StructField as DeltaStructField,
};

/// Trait for converting data structures to Arrow and Delta Lake schemas
pub trait ForArrow {
    fn for_arrow() -> Vec<FieldRef>;

    /// Default implementation that converts Arrow schema to Delta Lake schema
    fn for_delta() -> Vec<DeltaStructField> {
        let afs = Self::for_arrow();

        afs.into_iter()
            .map(|af| {
                let prim_type = match af.data_type() {
                    DataType::Timestamp(TimeUnit::Microsecond, _) => PrimitiveType::Timestamp,
                    DataType::Utf8 => PrimitiveType::String,
                    DataType::Binary => PrimitiveType::Binary,
                    DataType::Int64 => PrimitiveType::Long,
                    _ => panic!("configure this type"),
                };

                DeltaStructField {
                    name: af.name().to_string(),
                    data_type: DeltaDataType::Primitive(prim_type),
                    nullable: af.is_nullable(),
                    metadata: HashMap::new(),
                }
            })
            .collect()
    }
}

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

/// Creates a new Delta table with OplogEntry schema and initializes it with a root directory entry
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
    
    // Create the table with OplogEntry schema directly - no more Record nesting
    let table = DeltaOps::try_from_uri(table_path).await?;
    let table = table
        .create()
        .with_columns(OplogEntry::for_delta())  // Use OplogEntry directly
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

    // Write OplogEntry directly to Delta Lake - no more Record wrapper
    let batch = serde_arrow::to_record_batch(&OplogEntry::for_arrow(), &[root_entry])?;
    let _table = DeltaOps(table)
        .write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .await?;

    Ok(())
}

/// Encode VersionedDirectoryEntry records as Arrow IPC bytes for storage in OplogEntry.content
pub fn encode_versioned_directory_entries(entries: &Vec<VersionedDirectoryEntry>) -> Result<Vec<u8>, crate::error::TLogFSError> {
    use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};

    let entry_count = entries.len();
    diagnostics::log_debug!("encode_versioned_directory_entries() - encoding {entry_count} entries", entry_count: entry_count);
    for (i, entry) in entries.iter().enumerate() {
        let name = &entry.name;
        let child_node_id = &entry.child_node_id;
        diagnostics::log_debug!("  Entry {i}: name='{name}', child_node_id='{child_node_id}'", 
                                i: i, name: name, child_node_id: child_node_id);
    }

    // Use serde_arrow consistently for both empty and non-empty cases
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
