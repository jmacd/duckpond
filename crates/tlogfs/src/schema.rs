// Phase 2 TLogFS Schema Implementation - Abstraction Consolidation
use arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::Result;
use deltalake::kernel::{
    DataType as DeltaDataType, PrimitiveType, StructField as DeltaStructField,
};
use log::debug;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use tinyfs::{FileID, PartID, NodeID, EntryType};

/// Extended attributes - immutable metadata set at file creation
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ExtendedAttributes {
    /// Simple key → String value mapping
    pub attributes: HashMap<String, String>,
}

/// DuckPond system metadata key constants
pub mod duckpond {
    pub const TIMESTAMP_COLUMN: &str = "duckpond.timestamp_column";
    pub const MIN_TEMPORAL_OVERRIDE: &str = "duckpond.min_temporal_override";
    pub const MAX_TEMPORAL_OVERRIDE: &str = "duckpond.max_temporal_override";
}

impl ExtendedAttributes {
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
        _ = self.attributes.insert(
            duckpond::TIMESTAMP_COLUMN.to_string(),
            column_name.to_string(),
        );
        self
    }

    /// Get timestamp column name with default fallback
    #[must_use]
    pub fn timestamp_column(&self) -> &str {
        self.attributes
            .get(duckpond::TIMESTAMP_COLUMN)
            .map(|s| s.as_str())
            .unwrap_or("Timestamp") // Default column name
    }

    /// Set temporal overrides (convenience method for setting both min/max)
    pub fn set_temporal_overrides(
        &mut self,
        min_override: Option<i64>,
        max_override: Option<i64>,
    ) -> &mut Self {
        if let Some(min) = min_override {
            _ = self
                .attributes
                .insert(duckpond::MIN_TEMPORAL_OVERRIDE.to_string(), min.to_string());
        }
        if let Some(max) = max_override {
            _ = self
                .attributes
                .insert(duckpond::MAX_TEMPORAL_OVERRIDE.to_string(), max.to_string());
        }
        self
    }

    /// Get temporal overrides from extended attributes
    #[must_use]
    pub fn temporal_overrides(&self) -> Option<(i64, i64)> {
        let min = self
            .attributes
            .get(duckpond::MIN_TEMPORAL_OVERRIDE)
            .and_then(|s| s.parse::<i64>().ok());
        let max = self
            .attributes
            .get(duckpond::MAX_TEMPORAL_OVERRIDE)
            .and_then(|s| s.parse::<i64>().ok());

        match (min, max) {
            (Some(min), Some(max)) => Some((min, max)),
            _ => None,
        }
    }

    /// Set/get raw attributes (for future extensibility)
    pub fn set_raw(&mut self, key: &str, value: &str) -> &mut Self {
        _ = self.attributes.insert(key.to_string(), value.to_string());
        self
    }

    #[must_use]
    pub fn get_raw(&self, key: &str) -> Option<&str> {
        self.attributes.get(key).map(|s| s.as_str())
    }

    /// Create from a HashMap of arbitrary attributes
    #[must_use]
    pub fn from_map(attributes: HashMap<String, String>) -> Self {
        Self { attributes }
    }
}

/// Extract temporal range from Arrow RecordBatch
/// This function extracts min/max timestamps from a specified column in the batch
pub fn extract_temporal_range_from_batch(
    batch: &arrow::record_batch::RecordBatch,
    time_column: &str,
) -> Result<(i64, i64), crate::error::TLogFSError> {
    use arrow::array::{Array, TimestampSecondArray};
    use arrow::datatypes::{DataType, TimeUnit};

    let time_array = batch.column_by_name(time_column).ok_or_else(|| {
        crate::error::TLogFSError::ArrowMessage(format!(
            "Time column '{}' not found in batch",
            time_column
        ))
    })?;

    // Handle different timestamp types
    match time_array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => {
            let array = time_array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| {
                    crate::error::TLogFSError::ArrowMessage(
                        "Failed to downcast timestamp array".to_string(),
                    )
                })?;
            let min = array.iter().flatten().min().unwrap_or(0);
            let max = array.iter().flatten().max().unwrap_or(0);
            // Keep original units
            Ok((min, max))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let array = time_array
                .as_any()
                .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                .ok_or_else(|| {
                    crate::error::TLogFSError::ArrowMessage(
                        "Failed to downcast timestamp array".to_string(),
                    )
                })?;
            let min = array.iter().flatten().min().unwrap_or(0);
            let max = array.iter().flatten().max().unwrap_or(0);
            Ok((min, max))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let array = time_array
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    crate::error::TLogFSError::ArrowMessage(
                        "Failed to downcast timestamp array".to_string(),
                    )
                })?;
            let min = array.iter().flatten().min().unwrap_or(0);
            let max = array.iter().flatten().max().unwrap_or(0);
            // Keep microseconds - maintain native precision
            Ok((min, max))
        }
        DataType::Int64 => {
            // Handle raw int64 timestamps
            let array = time_array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| {
                    crate::error::TLogFSError::ArrowMessage(
                        "Failed to downcast int64 array".to_string(),
                    )
                })?;
            let min = array.iter().flatten().min().unwrap_or(0);
            let max = array.iter().flatten().max().unwrap_or(0);
            Ok((min, max))
        }
        _ => Err(crate::error::TLogFSError::ArrowMessage(format!(
            "Unsupported timestamp type: {:?}",
            time_array.data_type()
        ))),
    }
}

/// Auto-detect timestamp column with priority order
pub fn detect_timestamp_column(
    schema: &arrow::datatypes::Schema,
) -> Result<String, crate::error::TLogFSError> {
    use arrow::datatypes::DataType;

    // Priority order for auto-detection
    let candidates = [
        "timestamp",
        "Timestamp",
        "event_time",
        "time",
        "ts",
        "datetime",
    ];

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
        "No timestamp column found in schema".to_string(),
    ))
}

/// Compute SHA256 for any content (small or large files)
#[must_use]
pub fn compute_sha256(content: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content);
    format!("{:x}", hasher.finalize())
}

/// Storage format for OplogEntry content
/// This field enables forward compatibility for different storage strategies
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum StorageFormat {
    /// Small files, symlinks, and dynamic nodes - content stored inline in OplogEntry
    #[serde(rename = "inline")]
    Inline,
    
    /// Physical directories - full directory snapshot stored in content field
    #[serde(rename = "fulldir")]
    FullDir,
}

/// Trait for converting data structures to Arrow and Delta Lake schemas
pub trait ForArrow {
    fn for_arrow() -> Vec<FieldRef>;

    /// Default implementation that converts Arrow schema to Delta Lake schema
    #[must_use]
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
    /// Partition ID (parent directory for files/symlinks, self for directories)
    pub part_id: PartID,
    /// NodeID from TinyFS
    pub node_id: NodeID,
    /// Type of filesystem entry (file, directory, or symlink)
    pub file_type: EntryType, // @@@ rename one or other
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
    /// NOTE: Uses i64 instead of u64 to match Delta Lake protocol (Java ecosystem legacy)
    pub size: Option<i64>,

    // NEW: Event time metadata for efficient temporal queries (FileSeries support)
    /// Minimum timestamp from data column for fast SQL range queries
    /// Some() for FileSeries entries, None for other entry types
    pub min_event_time: Option<i64>,
    /// Maximum timestamp from data column for fast SQL range queries
    /// Some() for FileSeries entries, None for other entry types
    pub max_event_time: Option<i64>,
    /// Manual override for minimum timestamp (temporal overlap resolution)
    /// Some() when user manually restricts the temporal range, None for auto-detected bounds
    pub min_override: Option<i64>,
    /// Manual override for maximum timestamp (temporal overlap resolution)
    /// Some() when user manually restricts the temporal range, None for auto-detected bounds
    pub max_override: Option<i64>,
    /// Extended attributes - immutable metadata set at file creation
    /// JSON-encoded key-value pairs for application-specific metadata
    /// For FileSeries: includes timestamp column name and other series metadata
    pub extended_attributes: Option<String>,

    /// Factory type for dynamic files/directories
    pub factory: Option<String>,

    /// Storage format for this entry's content
    /// Determines how to interpret the content field and optimize access patterns
    pub format: StorageFormat,

    /// Transaction sequence number from Steward
    /// This links OpLog records to transaction sequences for chronological ordering
    pub txn_seq: i64,
}

impl ForArrow for OplogEntry {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("part_id", DataType::Utf8, false)),
            Arc::new(Field::new("node_id", DataType::Utf8, false)),
            Arc::new(Field::new("file_type", DataType::Utf8, false)),
            Arc::new(Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            )),
            Arc::new(Field::new("version", DataType::Int64, false)),
            Arc::new(Field::new("content", DataType::Binary, true)), // Now nullable for large files
            Arc::new(Field::new("sha256", DataType::Utf8, true)), // New field for large file checksums
            Arc::new(Field::new("size", DataType::Int64, true)), // File size in bytes (Int64 to match Delta Lake protocol)
            // NEW: Temporal metadata fields for FileSeries support
            Arc::new(Field::new("min_event_time", DataType::Int64, true)), // Min timestamp from data for fast queries
            Arc::new(Field::new("max_event_time", DataType::Int64, true)), // Max timestamp from data for fast queries
            Arc::new(Field::new("min_override", DataType::Int64, true)), // Manual override for temporal bounds
            Arc::new(Field::new("max_override", DataType::Int64, true)), // Manual override for temporal bounds
            Arc::new(Field::new("extended_attributes", DataType::Utf8, true)), // JSON-encoded application metadata
            Arc::new(Field::new("factory", DataType::Utf8, true)), // Factory type for dynamic files/directories
            Arc::new(Field::new("format", DataType::Utf8, false)), // Storage format - required field
            Arc::new(Field::new("txn_seq", DataType::Int64, false)), // Transaction sequence number from Steward (required)
        ]
    }
}

impl OplogEntry {
    /// Create Arrow schema for OplogEntry
    #[must_use]
    pub fn create_schema() -> Arc<arrow::datatypes::Schema> {
        Arc::new(arrow::datatypes::Schema::new(Self::for_arrow()))
    }

    /// Create entry for small file (<= threshold)
    #[must_use]
    pub fn new_small_file(
        id: FileID,
        timestamp: i64,
        version: i64,
        content: Vec<u8>,
        txn_seq: i64,
    ) -> Self {
        let size = content.len() as u64;
        Self {
            part_id: id.part_id(),
            node_id: id.node_id(),
            file_type: id.entry_type(),
            timestamp,
            version,
            content: Some(content.clone()),
            sha256: Some(compute_sha256(&content)), // NEW: Always compute SHA256
            size: Some(size as i64),                // Cast to i64 to match Delta Lake protocol
            // Temporal metadata - None for non-series files
            min_event_time: None,
            max_event_time: None,
            min_override: None,
            max_override: None,
            extended_attributes: None,
            factory: None,
            format: StorageFormat::Inline, // Small files use inline storage
            txn_seq,
        }
    }

    /// Create entry for large file (> threshold)
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new_large_file(
        id: FileID,
        timestamp: i64,
        version: i64,
        sha256: String,
        size: i64,
        txn_seq: i64,
    ) -> Self {
        Self {
            part_id: id.part_id(),
            node_id: id.node_id(),
            file_type: id.entry_type(),
            timestamp,
            version,
            content: None,
            sha256: Some(sha256),
            size: Some(size), // NEW: Store size explicitly
            // Temporal metadata - None for non-series files
            min_event_time: None,
            max_event_time: None,
            min_override: None,
            max_override: None,
            extended_attributes: None,
            factory: None,
            format: StorageFormat::Inline, // Large files use inline format (content is external)
            txn_seq,
        }
    }

    /// Create entry for non-file types (directories, symlinks) - always inline
    #[must_use]
    pub fn new_inline(
        id: FileID,
        timestamp: i64,
        version: i64,
        content: Vec<u8>,
        txn_seq: i64,
    ) -> Self {
        Self {
            part_id: id.part_id(),
            node_id: id.node_id(),
            file_type: id.entry_type(),
            timestamp,
            version,
            content: Some(content),
            sha256: None,
            size: None, // None for directories and symlinks
            // Temporal metadata - None for directories and symlinks
            min_event_time: None,
            max_event_time: None,
            min_override: None,
            max_override: None,
            extended_attributes: None,
            factory: None,
            format: StorageFormat::Inline, // Symlinks and small metadata use inline storage
            txn_seq,
        }
    }

    /// Check if this entry represents a large file (based on content absence)
    #[must_use]
    pub fn is_large_file(&self) -> bool {
        self.content.is_none() && self.file_type.is_file()
    }

    /// Get file size (guaranteed for files, None for directories/symlinks)
    #[must_use]
    pub fn file_size(&self) -> Option<i64> {
        self.size
    }

    /// Create entry for FileSeries with temporal metadata extraction
    /// This is the specialized constructor for time series data
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new_file_series(
        id: FileID,
        timestamp: i64,
        version: i64,
        content: Vec<u8>,
        min_event_time: i64,
        max_event_time: i64,
        extended_attributes: ExtendedAttributes,
        txn_seq: i64,
    ) -> Self {
        let size = content.len() as u64;
        Self {
            part_id: id.part_id(),
            node_id: id.node_id(),
            file_type: id.entry_type(),
            timestamp,
            version,
            content: Some(content.clone()),
            sha256: Some(compute_sha256(&content)),
            size: Some(size as i64), // Cast to i64 to match Delta Lake protocol
            // Temporal metadata for efficient DataFusion queries
            min_event_time: Some(min_event_time),
            max_event_time: Some(max_event_time),
            min_override: None, // No overrides by default
            max_override: None, // No overrides by default
            extended_attributes: Some(extended_attributes.to_json().unwrap_or_default()),
            factory: None, // Physical file, no factory
            format: StorageFormat::Inline, // Small FileSeries use inline storage
            txn_seq,
        }
    }

    /// Create entry for large FileSeries (> threshold) with temporal metadata
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new_large_file_series(
        id: FileID,
        timestamp: i64,
        version: i64,
        sha256: String,
        size: i64, // Changed from u64 to i64 to match Delta Lake protocol
        min_event_time: i64,
        max_event_time: i64,
        extended_attributes: ExtendedAttributes,
        txn_seq: i64,
    ) -> Self {
	assert_eq!(EntryType::FileSeriesPhysical, id.entry_type());
        Self {
            part_id: id.part_id(),
            node_id: id.node_id(),
            file_type: EntryType::FileSeriesPhysical,
            timestamp,
            version,
            content: None,
            sha256: Some(sha256),
            size: Some(size),
            // Temporal metadata for efficient DataFusion queries
            min_event_time: Some(min_event_time),
            max_event_time: Some(max_event_time),
            min_override: None, // No overrides by default
            max_override: None, // No overrides by default
            extended_attributes: Some(extended_attributes.to_json().unwrap_or_default()),
            factory: None, // Physical file, no factory
            format: StorageFormat::Inline, // Large FileSeries use inline format (content is external)
            txn_seq,
        }
    }

    /// Get extended attributes if present
    #[must_use]
    pub fn get_extended_attributes(&self) -> Option<ExtendedAttributes> {
        self.extended_attributes
            .as_ref()
            .and_then(|json| ExtendedAttributes::from_json(json).ok())
    }

    /// Check if this entry is a FileSeries with temporal metadata
    #[must_use]
    pub fn is_series_file(&self) -> bool {
        self.file_type.is_series_file()
    }

    /// Get temporal range for series files
    #[must_use]
    pub fn temporal_range(&self) -> Option<(i64, i64)> {
        match (self.min_event_time, self.max_event_time) {
            (Some(min), Some(max)) => Some((min, max)),
            _ => None,
        }
    }

    /// Get temporal overrides if set
    #[must_use]
    pub fn temporal_overrides(&self) -> Option<(i64, i64)> {
        match (self.min_override, self.max_override) {
            (Some(min), Some(max)) => Some((min, max)),
            _ => None,
        }
    }

    /// Get effective temporal range (overrides take precedence over auto-detected range)
    /// This is the key method for temporal overlap detection and resolution
    #[must_use]
    pub fn effective_temporal_range(&self) -> Option<(i64, i64)> {
        // Use overrides if available, otherwise fall back to auto-detected range
        if let Some(overrides) = self.temporal_overrides() {
            Some(overrides)
        } else {
            self.temporal_range()
        }
    }

    /// Set temporal overrides (for command-line tools)
    pub fn set_temporal_overrides(&mut self, min_override: Option<i64>, max_override: Option<i64>) {
        self.min_override = min_override;
        self.max_override = max_override;
    }

    /// Clear temporal overrides (reset to auto-detected bounds)
    pub fn clear_temporal_overrides(&mut self) {
        self.min_override = None;
        self.max_override = None;
    }

    /// Extract consolidated metadata
    #[must_use]
    pub fn metadata(&self) -> tinyfs::NodeMetadata {
        tinyfs::NodeMetadata {
            version: self.version as u64,
            size: self.size.map(|s| s as u64), // Cast i64 back to u64 for tinyfs interface
            sha256: self.sha256.clone(),
            entry_type: self.file_type,
            timestamp: self.timestamp,
        }
    }

    /// Create entry for dynamic node with factory type and configuration
    #[must_use]
    pub fn new_dynamic_node(
        id: FileID,
        timestamp: i64,
        version: i64,
        factory_type: &str,
        config_content: Vec<u8>,
        txn_seq: i64,
    ) -> Self {
        Self {
            part_id: id.part_id(),
            node_id: id.node_id(),
            file_type: id.entry_type(),
            timestamp,
            version,
            content: Some(config_content),
            sha256: None,
            size: None,
            min_event_time: None,
            max_event_time: None,
            min_override: None,
            max_override: None,
            extended_attributes: None,
            factory: Some(factory_type.to_string()), // Factory type identifier
            format: StorageFormat::Inline, // Dynamic directories use inline storage
            txn_seq,
        }
    }

    /// Check if this entry is a dynamic node (has factory type)
    #[must_use]
    pub fn is_dynamic(&self) -> bool {
        self.factory.is_some()
    }

    /// Get factory type if this is a dynamic node
    #[must_use]
    pub fn factory_type(&self) -> Option<&str> {
        self.factory.as_deref()
    }

    /// Get factory configuration content if this is a dynamic node
    #[must_use]
    pub fn factory_config(&self) -> Option<&[u8]> {
        if self.is_dynamic() {
            self.content.as_deref()
        } else {
            None
        }
    }

    /// Create entry for directory full snapshot (new storage format)
    /// This constructor creates a complete directory state snapshot instead of incremental changes
    #[must_use]
    pub fn new_directory_full_snapshot(
        id: FileID,
        timestamp: i64,
        version: i64,
        content: Vec<u8>, // Serialized DirectoryEntry[]
        txn_seq: i64,
    ) -> Self {
        Self {
            part_id: id.part_id(),
            node_id: id.node_id(),
            file_type: id.entry_type(),
            timestamp,
            version,
            content: Some(content),
            sha256: None,
            size: None,
            min_event_time: None,
            max_event_time: None,
            min_override: None,
            max_override: None,
            extended_attributes: None,
            factory: None,
            format: StorageFormat::FullDir, // Full directory snapshot
            txn_seq,
        }
    }

    // /// Get comprehensive EntryType that includes physical/dynamic distinction
    // /// This is the authoritative method for determining the complete entry type
    // /// including whether the node is factory-based or not.
    // #[must_use]
    // pub fn comprehensive_entry_type(&self) -> EntryType {
    //     let is_dynamic = self.is_dynamic();

    //     match self.file_type {
    //         EntryType::DirectoryPhysical | EntryType::DirectoryDynamic => {
    //             // Directory: check factory field to determine physical vs dynamic
    //             if is_dynamic {
    //                 EntryType::DirectoryDynamic
    //             } else {
    //                 EntryType::DirectoryPhysical
    //             }
    //         }
    //         EntryType::Symlink => EntryType::Symlink,

    //         // Files: check factory field and preserve base format
    //         EntryType::FileDataPhysical | EntryType::FileDataDynamic => {
    //             if is_dynamic {
    //                 EntryType::FileDataDynamic
    //             } else {
    //                 EntryType::FileDataPhysical
    //             }
    //         }
    //         EntryType::FileTablePhysical | EntryType::FileTableDynamic => {
    //             if is_dynamic {
    //                 EntryType::FileTableDynamic
    //             } else {
    //                 EntryType::FileTablePhysical
    //             }
    //         }
    //         EntryType::FileSeriesPhysical | EntryType::FileSeriesDynamic => {
    //             if is_dynamic {
    //                 EntryType::FileSeriesDynamic
    //             } else {
    //                 EntryType::FileSeriesPhysical
    //             }
    //         }
    //     }
    // }
}

/// Type alias - use tinyfs::DirectoryEntry as the canonical type
/// This avoids duplicate struct definitions and maintains a single source of truth.
pub type DirectoryEntry = tinyfs::DirectoryEntry;

/// Type alias for backward compatibility during transition
#[deprecated(note = "Use DirectoryEntry instead")]
pub type VersionedDirectoryEntry = tinyfs::DirectoryEntry;

/// ForArrow implementation for tinyfs::DirectoryEntry to enable Arrow/Parquet serialization
impl ForArrow for tinyfs::DirectoryEntry {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("child_node_id", DataType::Utf8, false)),
            Arc::new(Field::new("entry_type", DataType::Utf8, false)),
            Arc::new(Field::new("version_last_modified", DataType::Int64, false)),
        ]
    }
}

/// Encode DirectoryEntry records as Arrow IPC bytes for storage in OplogEntry.content
pub fn encode_directory_entries(
    entries: &Vec<DirectoryEntry>,
) -> Result<Vec<u8>, crate::error::TLogFSError> {
    use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};

    let entry_count = entries.len();
    debug!("encode_directory_entries() - encoding {entry_count} entries");
    for (i, entry) in entries.iter().enumerate() {
        let name = &entry.name;
        let child_node_id = &entry.child_node_id;
        debug!("  Entry {i}: name='{name}', child_node_id='{child_node_id}'");
    }

    // Use serde_arrow consistently for both empty and non-empty cases
    let batch = serde_arrow::to_record_batch(&DirectoryEntry::for_arrow(), entries)?;

    let row_count = batch.num_rows();
    let col_count = batch.num_columns();
    debug!(
        "encode_directory_entries() - created batch with {row_count} rows, {col_count} columns"
    );

    let mut buffer = Vec::new();
    let options = IpcWriteOptions::default();
    let mut writer =
        StreamWriter::try_new_with_options(&mut buffer, batch.schema().as_ref(), options)?;
    writer.write(&batch)?;
    writer.finish()?;

    let buffer_len = buffer.len();
    debug!("encode_directory_entries() - encoded to {buffer_len} bytes");
    Ok(buffer)
}

/// Legacy function name for backward compatibility
#[deprecated(note = "Use encode_directory_entries instead")]
pub fn encode_versioned_directory_entries(
    entries: &Vec<DirectoryEntry>,
) -> Result<Vec<u8>, crate::error::TLogFSError> {
    encode_directory_entries(&entries.clone())
}

/// Decode DirectoryEntry records from Arrow IPC bytes
pub fn decode_directory_entries(
    content: &[u8],
) -> Result<Vec<DirectoryEntry>, crate::error::TLogFSError> {
    use arrow::ipc::reader::StreamReader;

    // Handle empty directories (0 bytes of content)
    if content.is_empty() {
        return Ok(Vec::new());
    }

    debug!(
        "decode_directory_entries() - processing {} bytes",
        content.len()
    );

    let mut reader = StreamReader::try_new(std::io::Cursor::new(content), None).map_err(|e| {
        crate::error::TLogFSError::ArrowMessage(format!("Failed to create IPC StreamReader: {}", e))
    })?;

    if let Some(batch_result) = reader.next() {
        let batch = batch_result.map_err(|e| {
            crate::error::TLogFSError::ArrowMessage(format!("Failed to read IPC batch: {}", e))
        })?;

        let entries: Vec<DirectoryEntry> = serde_arrow::from_record_batch(&batch)?;
        debug!(
            "decode_directory_entries() - decoded {} entries",
            entries.len()
        );
        Ok(entries)
    } else {
        Ok(Vec::new())
    }
}
