// Phase 2 TLogFS Schema Implementation - Abstraction Consolidation
use arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use chrono::Utc;
use deltalake::protocol::SaveMode;
use datafusion::common::Result;
use tinyfs::NodeID;
use std::collections::HashMap;
use deltalake::kernel::{
    DataType as DeltaDataType, PrimitiveType, StructField as DeltaStructField,
};
use sha2::{Sha256, Digest};

/// Extended attributes - immutable metadata set at file creation
#[derive(Debug, Clone, PartialEq)]
pub struct ExtendedAttributes {
    /// Simple key → String value mapping
    pub attributes: HashMap<String, String>,
}

/// DuckPond system metadata key constants
pub mod duckpond {
    pub const TIMESTAMP_COLUMN: &str = "duckpond.timestamp_column";
}

impl ExtendedAttributes {
    pub fn new() -> Self {
        Self { attributes: HashMap::new() }
    }
    
    /// Create from JSON string (for reading from OplogEntry)
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        let attributes: HashMap<String, String> = serde_json::from_str(json)?;
        Ok(Self { attributes })
    }
    
    /// Serialize to JSON string (for storing in OplogEntry)
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self.attributes)
    }
    
    /// Set timestamp column name (defaults to "Timestamp" if not set)
    pub fn set_timestamp_column(&mut self, column_name: &str) -> &mut Self {
        self.attributes.insert(duckpond::TIMESTAMP_COLUMN.to_string(), column_name.to_string());
        self
    }
    
    /// Get timestamp column name with default fallback
    pub fn timestamp_column(&self) -> &str {
        self.attributes.get(duckpond::TIMESTAMP_COLUMN)
            .map(|s| s.as_str())
            .unwrap_or("Timestamp")  // Default column name
    }
    
    /// Set/get raw attributes (for future extensibility)
    pub fn set_raw(&mut self, key: &str, value: &str) -> &mut Self {
        self.attributes.insert(key.to_string(), value.to_string());
        self
    }
    
    pub fn get_raw(&self, key: &str) -> Option<&str> {
        self.attributes.get(key).map(|s| s.as_str())
    }
}

/// Extract temporal range from Arrow RecordBatch
/// This function extracts min/max timestamps from a specified column in the batch
pub fn extract_temporal_range_from_batch(
    batch: &arrow::record_batch::RecordBatch,
    time_column: &str,
) -> Result<(i64, i64), crate::error::TLogFSError> {
    use arrow::datatypes::{DataType, TimeUnit};
    use arrow::array::Array;
    
    let time_array = batch
        .column_by_name(time_column)
        .ok_or_else(|| crate::error::TLogFSError::ArrowMessage(
            format!("Time column '{}' not found in batch", time_column)
        ))?;
    
    // Handle different timestamp types
    match time_array.data_type() {
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let array = time_array.as_any().downcast_ref::<arrow::array::TimestampMillisecondArray>()
                .ok_or_else(|| crate::error::TLogFSError::ArrowMessage("Failed to downcast timestamp array".to_string()))?;
            let min = array.iter().flatten().min().unwrap_or(0);
            let max = array.iter().flatten().max().unwrap_or(0);
            Ok((min, max))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let array = time_array.as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .ok_or_else(|| crate::error::TLogFSError::ArrowMessage("Failed to downcast timestamp array".to_string()))?;
            let min = array.iter().flatten().min().unwrap_or(0);
            let max = array.iter().flatten().max().unwrap_or(0);
            Ok((min, max))
        }
        DataType::Int64 => {
            // Handle raw int64 timestamps
            let array = time_array.as_any().downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| crate::error::TLogFSError::ArrowMessage("Failed to downcast int64 array".to_string()))?;
            let min = array.iter().flatten().min().unwrap_or(0);
            let max = array.iter().flatten().max().unwrap_or(0);
            Ok((min, max))
        }
        _ => Err(crate::error::TLogFSError::ArrowMessage(
            format!("Unsupported timestamp type: {:?}", time_array.data_type())
        ))
    }
}

/// Auto-detect timestamp column with priority order
pub fn detect_timestamp_column(schema: &arrow::datatypes::Schema) -> Result<String, crate::error::TLogFSError> {
    use arrow::datatypes::DataType;
    
    // Priority order for auto-detection
    let candidates = ["timestamp", "Timestamp", "event_time", "time", "ts", "datetime"];
    
    for candidate in candidates {
        if let Some(field) = schema.column_with_name(candidate) {
            match field.1.data_type() {
                DataType::Timestamp(_, _) | DataType::Int64 => {
                    return Ok(candidate.to_string());
                }
                _ => continue,
            }
        }
    }
    
    Err(crate::error::TLogFSError::ArrowMessage(
        "No timestamp column found in schema".to_string()
    ))
}

/// Compute SHA256 for any content (small or large files)
pub fn compute_sha256(content: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content);
    format!("{:x}", hasher.finalize())
}

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
                    DataType::UInt64 => PrimitiveType::Long, // UInt64 -> Long for size field
                    _ => panic!("configure this type: {:?}", af.data_type()),
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
    /// - For files: raw file data (if <= threshold) or None (if > threshold, stored externally)
    /// - For symlinks: target path
    /// - For directories: Arrow IPC encoded VersionedDirectoryEntry records
    pub content: Option<Vec<u8>>,
    /// SHA256 checksum for large files (> threshold)
    /// Some() for large files stored externally, None for small files stored inline
    pub sha256: Option<String>,
    /// File size in bytes (Some() for all files, None for directories/symlinks)
    pub size: Option<u64>,
    
    // NEW: Event time metadata for efficient temporal queries (FileSeries support)
    /// Minimum timestamp from data column for fast SQL range queries
    /// Some() for FileSeries entries, None for other entry types
    pub min_event_time: Option<i64>,
    /// Maximum timestamp from data column for fast SQL range queries
    /// Some() for FileSeries entries, None for other entry types
    pub max_event_time: Option<i64>,
    /// Extended attributes - immutable metadata set at file creation
    /// JSON-encoded key-value pairs for application-specific metadata
    /// For FileSeries: includes timestamp column name and other series metadata
    pub extended_attributes: Option<String>,
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
            Arc::new(Field::new("content", DataType::Binary, true)), // Now nullable for large files
            Arc::new(Field::new("sha256", DataType::Utf8, true)), // New field for large file checksums
            Arc::new(Field::new("size", DataType::UInt64, true)), // File size in bytes
            
            // NEW: Temporal metadata fields for FileSeries support
            Arc::new(Field::new("min_event_time", DataType::Int64, true)), // Min timestamp from data for fast queries
            Arc::new(Field::new("max_event_time", DataType::Int64, true)), // Max timestamp from data for fast queries
            Arc::new(Field::new("extended_attributes", DataType::Utf8, true)), // JSON-encoded application metadata
        ]
    }
}

impl OplogEntry {
    /// Create Arrow schema for OplogEntry
    pub fn create_schema() -> Arc<arrow::datatypes::Schema> {
        Arc::new(arrow::datatypes::Schema::new(Self::for_arrow()))
    }
    
    /// Create entry for small file (<= threshold)
    pub fn new_small_file(
        part_id: String, 
        node_id: String, 
        file_type: tinyfs::EntryType,
        timestamp: i64,
        version: i64,
        content: Vec<u8>
    ) -> Self {
        let size = content.len() as u64;
        Self {
            part_id,
            node_id,
            file_type,
            timestamp,
            version,
            content: Some(content.clone()),
            sha256: Some(compute_sha256(&content)), // NEW: Always compute SHA256
            size: Some(size), // NEW: Store size explicitly
            // Temporal metadata - None for non-series files
            min_event_time: None,
            max_event_time: None,
            extended_attributes: None,
        }
    }
    
    /// Create entry for large file (> threshold)
    pub fn new_large_file(
        part_id: String, 
        node_id: String, 
        file_type: tinyfs::EntryType,
        timestamp: i64,
        version: i64,
        sha256: String,
        size: u64  // NEW PARAMETER
    ) -> Self {
        Self {
            part_id,
            node_id,
            file_type,
            timestamp,
            version,
            content: None,
            sha256: Some(sha256),
            size: Some(size), // NEW: Store size explicitly
            // Temporal metadata - None for non-series files
            min_event_time: None,
            max_event_time: None,
            extended_attributes: None,
        }
    }
    
    /// Create entry for non-file types (directories, symlinks) - always inline
    pub fn new_inline(
        part_id: String,
        node_id: String,
        file_type: tinyfs::EntryType,
        timestamp: i64,
        version: i64,
        content: Vec<u8>
    ) -> Self {
        Self {
            part_id,
            node_id,
            file_type,
            timestamp,
            version,
            content: Some(content),
            sha256: None,
            size: None, // None for directories and symlinks
            // Temporal metadata - None for directories and symlinks
            min_event_time: None,
            max_event_time: None,
            extended_attributes: None,
        }
    }
    
    /// Check if this entry represents a large file (based on content absence)
    pub fn is_large_file(&self) -> bool {
        self.content.is_none() && self.file_type.is_file()
    }
    
    /// Get file size (guaranteed for files, None for directories/symlinks)
    pub fn file_size(&self) -> Option<u64> {
        self.size
    }
    
    /// Create entry for FileSeries with temporal metadata extraction
    /// This is the specialized constructor for time series data
    pub fn new_file_series(
        part_id: String,
        node_id: String,
        timestamp: i64,
        version: i64,
        content: Vec<u8>,
        min_event_time: i64,
        max_event_time: i64,
        extended_attributes: ExtendedAttributes,
    ) -> Self {
        let size = content.len() as u64;
        Self {
            part_id,
            node_id,
            file_type: tinyfs::EntryType::FileSeries,
            timestamp,
            version,
            content: Some(content.clone()),
            sha256: Some(compute_sha256(&content)),
            size: Some(size),
            // Temporal metadata for efficient DataFusion queries
            min_event_time: Some(min_event_time),
            max_event_time: Some(max_event_time),
            extended_attributes: Some(extended_attributes.to_json().unwrap_or_default()),
        }
    }
    
    /// Create entry for large FileSeries (> threshold) with temporal metadata
    pub fn new_large_file_series(
        part_id: String,
        node_id: String,
        timestamp: i64,
        version: i64,
        sha256: String,
        size: u64,
        min_event_time: i64,
        max_event_time: i64,
        extended_attributes: ExtendedAttributes,
    ) -> Self {
        Self {
            part_id,
            node_id,
            file_type: tinyfs::EntryType::FileSeries,
            timestamp,
            version,
            content: None,
            sha256: Some(sha256),
            size: Some(size),
            // Temporal metadata for efficient DataFusion queries
            min_event_time: Some(min_event_time),
            max_event_time: Some(max_event_time),
            extended_attributes: Some(extended_attributes.to_json().unwrap_or_default()),
        }
    }
    
    /// Get extended attributes if present
    pub fn get_extended_attributes(&self) -> Option<ExtendedAttributes> {
        self.extended_attributes.as_ref()
            .and_then(|json| ExtendedAttributes::from_json(json).ok())
    }
    
    /// Check if this entry is a FileSeries with temporal metadata
    pub fn is_series_file(&self) -> bool {
        self.file_type == tinyfs::EntryType::FileSeries
    }
    
    /// Get temporal range for series files
    pub fn temporal_range(&self) -> Option<(i64, i64)> {
        match (self.min_event_time, self.max_event_time) {
            (Some(min), Some(max)) => Some((min, max)),
            _ => None,
        }
    }
    
    /// Extract consolidated metadata
    pub fn metadata(&self) -> tinyfs::NodeMetadata {
        tinyfs::NodeMetadata {
            version: self.version as u64,
            size: self.size,
            sha256: self.sha256.clone(),
            entry_type: self.file_type,
	    timestamp: self.timestamp,
        }
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
pub async fn create_oplog_table(
    table_path: &str,
    delta_manager: &crate::delta::manager::DeltaTableManager,
) -> Result<(), crate::error::TLogFSError> {
    // Try to open existing table first
    match delta_manager.get_table(table_path).await {
        Ok(_) => {
            // Table already exists, nothing to do
            return Ok(());
        }
        Err(_) => {
            // Table doesn't exist, create it
        }
    }
    
    // Create the table with OplogEntry schema using the manager
    let _table = delta_manager
        .create_table(
            table_path,
            OplogEntry::for_delta(),
            Some(vec!["part_id".to_string()]),
        )
        .await?;

    // Create a root directory entry as the initial OplogEntry
    let root_node_id = NodeID::root().to_string();
    let now = Utc::now().timestamp_micros();
    let root_entry = OplogEntry::new_inline(
        root_node_id.clone(), // Root directory is its own partition
        root_node_id.clone(),
        tinyfs::EntryType::Directory,
        now, // Node modification time
        1, // First version of root directory node
        encode_versioned_directory_entries(&vec![])?, // Empty directory with versioned schema
    );

    // Write OplogEntry using the manager
    let batch = serde_arrow::to_record_batch(&OplogEntry::for_arrow(), &[root_entry])?;
    delta_manager
        .write_to_table(
            table_path,
            vec![batch],
            SaveMode::Append,
        )
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
