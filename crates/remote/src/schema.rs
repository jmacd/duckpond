// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Schema definitions for chunked file storage in Delta Lake

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use deltalake::kernel::{
    DataType as DeltaDataType, PrimitiveType, StructField as DeltaStructField,
};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;

/// Type of file being backed up
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileType {
    /// Large file stored in tlogfs or pond data
    LargeFile,
    /// Transaction metadata summary
    Metadata,
}

impl FileType {
    /// Convert to string for Delta Lake storage
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::LargeFile => "large_file",
            Self::Metadata => "metadata",
        }
    }
}

impl FromStr for FileType {
    type Err = crate::RemoteError;

    /// Parse from string stored in Delta Lake
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
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
    /// Bundle identifier (BLAKE3 root hash) or "metadata_{pond_txn_id}" for metadata records
    /// This is the PARTITION column in Delta Lake
    pub bundle_id: String,

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
    /// BLAKE3 subtree hash of this chunk (hex-encoded, 64 chars)
    /// Computed with is_root=false and start_chunk = chunk_id * blocks_per_chunk
    pub chunk_hash: String,
    /// BLAKE3 outboard data for this chunk (Merkle tree parent nodes)
    /// Size: (blocks - 1) * 64 bytes, where blocks = chunk_size / 16KB
    pub chunk_outboard: Vec<u8>,
    /// The actual chunk data (4-64MB)
    pub chunk_data: Vec<u8>,

    // === File-level metadata (same for all chunks) ===
    /// Total file size in bytes
    pub total_size: i64,
    /// BLAKE3 root hash of entire file (hex-encoded, 64 chars)
    /// Computed by combining all chunk subtree hashes with is_root=true
    pub root_hash: String,
}

impl ChunkedFileRecord {
    /// Create Arrow schema for the chunked files table
    #[must_use]
    pub fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            // PARTITION COLUMN - must be first for Delta Lake
            Field::new("bundle_id", DataType::Utf8, false),
            // File identity
            Field::new("pond_txn_id", DataType::Int64, false),
            Field::new("path", DataType::Utf8, false),
            // Chunk information
            Field::new("chunk_id", DataType::Int64, false),
            Field::new("chunk_hash", DataType::Utf8, false),
            Field::new("chunk_outboard", DataType::Binary, false),
            Field::new("chunk_data", DataType::Binary, false),
            // File-level metadata
            Field::new("total_size", DataType::Int64, false),
            Field::new("root_hash", DataType::Utf8, false),
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

    /// Generate a metadata bundle_id for a pond
    /// Format: "POND-META-{pond_id}"
    ///
    /// All metadata for a pond shares the same bundle_id (partition),
    /// with individual transactions distinguished by pond_txn_id column.
    #[must_use]
    pub fn metadata_bundle_id(pond_id: &str) -> String {
        format!("POND-META-{}", pond_id)
    }

    /// Generate bundle_id for a transaction file (parquet or Delta log)
    /// Format: "FILE-META-{YYYY-MM-DD}-{txn_seq}"
    ///
    /// All files in a transaction share this bundle_id for partitioning.
    /// Individual files are distinguished by the path column.
    /// Date and txn_seq enable chronological grouping and transaction identification.
    #[must_use]
    pub fn transaction_bundle_id(txn_seq: i64) -> String {
        let now = chrono::Utc::now();
        format!("FILE-META-{}-{}", now.format("%Y-%m-%d"), txn_seq)
    }

    /// Generate a large file bundle_id
    /// Format: "POND-FILE-{blake3_root_hash}"
    #[must_use]
    pub fn large_file_bundle_id(root_hash: &str) -> String {
        format!("POND-FILE-{}", root_hash)
    }

    /// Check if a bundle_id is a transaction record
    #[must_use]
    pub fn is_transaction_bundle_id(bundle_id: &str) -> bool {
        bundle_id.starts_with("FILE-META-")
    }

    /// Check if a bundle_id is a large file record
    #[must_use]
    pub fn is_large_file_bundle_id(bundle_id: &str) -> bool {
        bundle_id.starts_with("POND-FILE-")
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
    /// BLAKE3 root hash (= bundle_id for this file)
    pub root_hash: String,
    /// Total file size in bytes
    pub size: u64,
    /// Type of file
    pub file_type: FileType,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_bundle_id() {
        let txn_seq = 42;
        let bundle_id = ChunkedFileRecord::transaction_bundle_id(txn_seq);
        // Should be in format FILE-META-YYYY-MM-DD-42
        assert!(bundle_id.starts_with("FILE-META-"));
        assert!(bundle_id.ends_with("-42"));
        assert!(ChunkedFileRecord::is_transaction_bundle_id(&bundle_id));
        assert!(!ChunkedFileRecord::is_transaction_bundle_id(
            "POND-FILE-abc123def"
        ));
    }

    #[test]
    fn test_large_file_bundle_id() {
        let root_hash = "abc123def456";
        let bundle_id = ChunkedFileRecord::large_file_bundle_id(root_hash);
        assert_eq!(bundle_id, "POND-FILE-abc123def456");
        assert!(ChunkedFileRecord::is_large_file_bundle_id(&bundle_id));
        assert!(!ChunkedFileRecord::is_large_file_bundle_id(
            "POND-META-something"
        ));
    }

    #[test]
    fn test_file_type_roundtrip() {
        for ft in [FileType::LargeFile, FileType::Metadata] {
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
