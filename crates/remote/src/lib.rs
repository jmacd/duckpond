// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Remote backup system using chunked parquet files in Delta Lake
//!
//! This crate provides streaming backup and restore functionality for pond commits.
//! Files are chunked into manageable pieces (default 16MB) and stored in parquet
//! format within a Delta Lake table. Each chunk has BLAKE3 hash validation with
//! outboard Merkle tree data for verified streaming.
//!
//! # Architecture
//!
//! - **RemoteTable**: Delta Lake table managing all backup data
//! - **ChunkedWriter**: Streams data to chunked parquet files with BLAKE3 hashes
//! - **ChunkedReader**: Streams data from chunked parquet files with verification
//! - **Partitioning**: Uses `bundle_id` (BLAKE3 root hash) as partition column
//!
//! # Schema
//!
//! Each row represents one chunk of a file:
//! - `bundle_id` (partition): BLAKE3 root hash of file, or `metadata_{txn_id}` for metadata
//! - `pond_txn_id`: Transaction sequence number from pond
//! - `chunk_id`: Sequential chunk number within file
//! - `chunk_data`: The actual data (4-64MB per chunk)
//! - `chunk_hash`: BLAKE3 subtree hash of chunk_data
//! - `chunk_outboard`: BLAKE3 Merkle tree outboard data (~0.4% of chunk)
//! - Plus metadata fields for file type, size, paths, root_hash
//!
//! # Usage
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use remote::RemoteTable;
//!
//! // Create or open remote backup table
//! let mut table = RemoteTable::create("/path/to/remote").await?;
//!
//! // Write a file in chunks
//! let file_data = std::fs::File::open("/data/large_file.parquet")?;
//! table.write_file(
//!     123,  // pond_txn_id
//!     "part_id=abc/file.parquet",  // original_path
//!     file_data,
//! ).await?;
//!
//! // Read file back with verification
//! let mut output = std::fs::File::create("/restore/file.parquet")?;
//! table.read_file("abc123def...blake3...", &mut output).await?;
//! # Ok(())
//! # }
//! ```

mod changes;
mod error;
pub mod factory;
mod reader;
mod s3_registration;
mod schema;
mod table;
mod writer;

pub use changes::{Changeset, FileChange, detect_changes_from_delta_log};
pub use error::RemoteError;
pub use factory::{
    RemoteConfig, ReplicationConfig, apply_parquet_files, apply_parquet_files_from_remote,
    build_object_store, download_bundle, extract_bundle, extract_txn_seq_from_bundle,
    scan_remote_versions,
};
pub use reader::ChunkedReader;
pub use s3_registration::register_s3_handlers;
pub use schema::{ChunkedFileRecord, FileInfo, FileType, TransactionMetadata};
pub use table::RemoteTable;
pub use writer::ChunkedWriter;

// Re-export chunking constants from utilities for backward compatibility
pub use utilities::chunked_files::{
    BLAKE3_BLOCK_SIZE, CHUNK_SIZE_DEFAULT, CHUNK_SIZE_MAX, CHUNK_SIZE_MIN,
};

/// Result type for remote operations
pub type Result<T> = std::result::Result<T, RemoteError>;
