// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Error types for remote backup operations

use thiserror::Error;

#[derive(Error, Debug)]
pub enum RemoteError {
    #[error("Delta Lake error: {0}")]
    Delta(#[from] deltalake::DeltaTableError),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("File not found: {0}")]
    FileNotFound(String),

    #[error(
        "Chunk integrity check failed for bundle_id={bundle_id} chunk_id={chunk_id}: expected CRC32={expected:08x}, got {actual:08x}"
    )]
    ChunkIntegrityFailed {
        bundle_id: String,
        chunk_id: i64,
        expected: i64,
        actual: i64,
    },

    #[error(
        "File integrity check failed for bundle_id={bundle_id}: expected SHA256={expected}, got {actual}"
    )]
    FileIntegrityFailed {
        bundle_id: String,
        expected: String,
        actual: String,
    },

    #[error("Invalid chunk sequence: expected chunk_id={expected}, got {actual}")]
    InvalidChunkSequence { expected: i64, actual: i64 },

    #[error("Invalid bundle_id format: {0}")]
    Configuration(String),

    #[error("Invalid file_id format: {0}")]
    InvalidFileId(String),

    #[error("Table operation error: {0}")]
    TableOperation(String),

    #[error("Command parsing error: {0}")]
    CommandParsing(String),

    #[error("Delta table version {0} not found: {1}")]
    VersionNotFound(i64, String),

    #[error("Delta table error: {0}")]
    DeltaTableError(String),
}

impl From<clap::Error> for RemoteError {
    fn from(e: clap::Error) -> Self {
        RemoteError::CommandParsing(e.to_string())
    }
}

impl From<String> for RemoteError {
    fn from(s: String) -> Self {
        RemoteError::Configuration(s)
    }
}
