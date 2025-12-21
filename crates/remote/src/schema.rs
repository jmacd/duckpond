// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Schema definitions for chunked file storage in Delta Lake

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use deltalake::kernel::{
    DataType as DeltaDataType, PrimitiveType, StructField as DeltaStructField,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Default chunk size: 50MB
/// Balances between:
/// - Granular checksums (smaller = better)
/// - Fewer rows/overhead (larger = better)
/// - In-memory buffer size (smaller = better)
pub const CHUNK_SIZE_DEFAULT: usize = 50 * 1024 * 1024;

/// Minimum chunk size: 10MB
pub const CHUNK_SIZE_MIN: usize = 10 * 1024 * 1024;

/// Maximum chunk size: 100MB
pub const CHUNK_SIZE_MAX: usize = 100 * 1024 * 1024;

/// Type of file being backed up
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileType {
    /// Parquet file from pond data filesystem
    PondParquet,
    /// Large file stored in tlogfs
    LargeFile,
    /// Transaction metadata summary
    Metadata,
}

impl FileType {
    /// Convert to string for Delta Lake storage
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PondParquet => "pond_parquet",
            Self::LargeFile => "large_file",
            Self::Metadata => "metadata",
        }
    }

    /// Parse from string stored in Delta Lake
    pub fn from_str(s: &str) -> Result<Self, crate::RemoteError> {
        match s {
            "pond_parquet" => Ok(Self::PondParquet),
            "large_file" => Ok(Self::LargeFile),
            "metadata" => Ok(Self::Metadata),
            _ => Err(crate::RemoteError::Configuration(format!(
                "Invalid file_type: {}",
                s
            ))),
        }
    }
}

/// A single row in the chunked files table
/// Each row represents one chunk of a file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkedFileRecord {
    // === PARTITION COLUMN ===
    /// File identifier (SHA256 hash) or "metadata_{pond_txn_id}" for metadata records
    /// This is the PARTITION column in Delta Lake
    pub file_id: String,

    // === File identity and transaction context ===
    /// Transaction sequence number from pond
    pub pond_txn_id: i64,
    /// Original path in pond (e.g., "part_id=abc.../part-00001.parquet")
    pub original_path: String,
    /// Type of file
    pub file_type: String, // Stored as string in Delta Lake

    // === Chunk information ===
    /// Chunk sequence number (0, 1, 2, ...)
    pub chunk_id: i64,
    /// CRC32 checksum of chunk_data (stored as i32 for Delta Lake, cast from u32)
    pub chunk_crc32: i32,
    /// The actual chunk data (10-100MB)
    pub chunk_data: Vec<u8>,

    // === File-level metadata (same for all chunks) ===
    /// Total file size in bytes
    pub total_size: i64,
    /// SHA256 hash of entire file
    pub total_sha256: String,
    /// Total number of chunks in this file
    pub chunk_count: i64,

    // === Transaction metadata ===
    /// CLI arguments that created this backup (JSON array as string)
    pub cli_args: String,
    /// Unix timestamp in milliseconds
    pub created_at: i64,
}

impl ChunkedFileRecord {
    /// Create Arrow schema for the chunked files table
    #[must_use]
    pub fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            // PARTITION COLUMN - must be first for Delta Lake
            Field::new("bundle_id", DataType::Utf8, false),
            // File identity and context
            Field::new("pond_id", DataType::Utf8, false),
            Field::new("pond_txn_id", DataType::Int64, false),
            Field::new("path", DataType::Utf8, false),
            Field::new("version", DataType::Int64, false),
            Field::new("file_type", DataType::Utf8, false),
            // Chunk information
            Field::new("chunk_id", DataType::Int64, false),
            Field::new("chunk_crc32", DataType::Int32, false),
            Field::new("chunk_data", DataType::Binary, false),
            // File-level metadata
            Field::new("total_size", DataType::Int64, false),
            Field::new("total_sha256", DataType::Utf8, false),
            Field::new("chunk_count", DataType::Int64, false),
            // Transaction metadata
            Field::new("cli_args", DataType::Utf8, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
        ]))
    }

    /// Convert Arrow schema to Delta Lake schema
    #[must_use]
    pub fn delta_schema() -> Vec<DeltaStructField> {
        let arrow_schema = Self::arrow_schema();
        arrow_schema
            .fields()
            .iter()
            .map(|field| {
                let delta_type = match field.data_type() {
                    DataType::Int32 => DeltaDataType::Primitive(PrimitiveType::Integer),
                    DataType::Int64 => DeltaDataType::Primitive(PrimitiveType::Long),
                    DataType::Utf8 => DeltaDataType::Primitive(PrimitiveType::String),
                    DataType::Binary => DeltaDataType::Primitive(PrimitiveType::Binary),
                    DataType::Timestamp(TimeUnit::Microsecond, _) => {
                        DeltaDataType::Primitive(PrimitiveType::Timestamp)
                    }
                    _ => panic!("Unsupported Arrow type: {:?}", field.data_type()),
                };

                DeltaStructField::new(field.name().clone(), delta_type, field.is_nullable())
            })
            .collect()
    }

    /// Generate a metadata bundle_id for a transaction
    /// Format: "metadata_{pond_txn_id}"
    #[must_use]
    pub fn metadata_bundle_id(pond_txn_id: i64) -> String {
        format!("metadata_{}", pond_txn_id)
    }

    /// Check if a bundle_id is a metadata record
    #[must_use]
    pub fn is_metadata_bundle_id(bundle_id: &str) -> bool {
        bundle_id.starts_with("metadata_")
    }
}

/// Transaction metadata stored in a special metadata file
/// This summarizes an entire pond commit backup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionMetadata {
    /// Pond UUID
    pub pond_id: String,
    /// Number of files in this transaction
    pub file_count: usize,
    /// List of files backed up
    pub files: Vec<FileInfo>,
    /// CLI arguments that created this transaction
    pub cli_args: Vec<String>,
    /// Unix timestamp in milliseconds
    pub created_at: i64,
    /// Total size of all files (sum of uncompressed data)
    pub total_size: u64,
}

/// Information about a file in the transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    /// Original path in pond
    pub path: String,
    /// SHA256 hash (= file_id for this file)
    pub sha256: String,
    /// Total file size in bytes
    pub size: u64,
    /// Type of file
    pub file_type: FileType,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_bundle_id() {
        let bundle_id = ChunkedFileRecord::metadata_bundle_id(123);
        assert_eq!(bundle_id, "metadata_123");
        assert!(ChunkedFileRecord::is_metadata_bundle_id(&bundle_id));
        assert!(!ChunkedFileRecord::is_metadata_bundle_id("abc123def"));
    }

    #[test]
    fn test_file_type_roundtrip() {
        for ft in [
            FileType::PondParquet,
            FileType::LargeFile,
            FileType::Metadata,
        ] {
            let s = ft.as_str();
            let parsed = FileType::from_str(s).unwrap();
            assert_eq!(ft, parsed);
        }
    }

    #[test]
    fn test_schema_has_partition_column() {
        let schema = ChunkedFileRecord::arrow_schema();
        assert_eq!(schema.fields()[0].name(), "bundle_id");
    }
}
