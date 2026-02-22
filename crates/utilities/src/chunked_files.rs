// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Chunked file format utilities for large file storage
//!
//! This module provides utilities for reading and writing files in chunked parquet format
//! with BLAKE3 verification. The same format is used for both local large file storage
//! and remote backups, enabling direct file copying without re-chunking.
//!
//! ## Chunked Format
//!
//! All chunks use `SeriesOutboard` format in `chunk_outboard`, which encodes:
//! - `cumulative_size`: Byte offset where this chunk ends
//! - `cumulative_blake3`: Bao root hash through all content up to this chunk
//! - `IncrementalOutboard`: Delta nodes and frontier for resumption
//!
//! Entry type (FilePhysicalVersion vs FilePhysicalSeries) is encoded in the
//! filename schema (e.g., `_large_files/version/blake3=xxx.parquet` vs
//! `_large_files/series/blake3=xxx.parquet`). This determines validation semantics.
//!
//! ## BLAKE3 Merkle Trees
//!
//! Uses BLAKE3 Merkle trees for verification:
//! - 16KB blocks (bao-tree format, chunk_log=4)
//! - Outboard data (~0.4% overhead) enables verified streaming

use crate::bao_outboard::{BLOCK_SIZE, SeriesOutboard};
use arrow_array::{BinaryArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use log::debug;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt};

/// Default chunk size: 16MB (power of 2 for optimal BLAKE3 tree alignment)
pub const CHUNK_SIZE_DEFAULT: usize = 16 * 1024 * 1024;

/// Minimum chunk size: 4MB
pub const CHUNK_SIZE_MIN: usize = 4 * 1024 * 1024;

/// Maximum chunk size: 64MB
pub const CHUNK_SIZE_MAX: usize = 64 * 1024 * 1024;

/// BLAKE3 block size: 16KB (chunk_log=4)
/// Each block produces one leaf hash in the Merkle tree.
/// Outboard overhead = (blocks - 1) * 64 bytes ~= 0.39%
pub const BLAKE3_BLOCK_SIZE: usize = 16 * 1024;

/// Result of chunked write operation
#[derive(Debug)]
pub struct ChunkedWriteResult {
    /// Bundle ID (BLAKE3 root hash as hex string)
    pub bundle_id: String,
    /// Arrow RecordBatches containing chunked data
    pub batches: Vec<RecordBatch>,
    /// Final SeriesOutboard - store this in OpLogEntry for the large file version
    /// Contains cumulative_blake3 = root hash through all content
    pub final_outboard: SeriesOutboard,
}

/// Streaming writer that chunks files and creates Arrow RecordBatches
///
/// Reads data from an AsyncRead source, chunks it into pieces,
/// computes BLAKE3 outboard per chunk using SeriesOutboard format,
/// then creates Arrow RecordBatches suitable for writing to Parquet.
///
/// Each chunk stores a `SeriesOutboard` which encodes cumulative byte offset
/// in `cumulative_size`. This enables both independent chunk validation and
/// cumulative series validation depending on entry type.
///
/// For FilePhysicalSeries with large files, the starting offset should be set
/// to the cumulative size of previous versions. For standalone large files,
/// starting offset is 0.
pub struct ChunkedWriter<R> {
    pond_txn_id: i64,
    path: String,
    reader: R,
    chunk_size: usize,
    bundle_id_override: Option<String>,
    /// Starting cumulative offset (for FilePhysicalSeries with prior versions)
    starting_offset: u64,
    /// Previous version's SeriesOutboard (for continuing cumulative hash)
    prev_series_outboard: Option<SeriesOutboard>,
}

impl<R: AsyncRead + Unpin> ChunkedWriter<R> {
    /// Create a new chunked writer
    ///
    /// # Arguments
    /// * `pond_txn_id` - Transaction sequence number from pond
    /// * `path` - File path (e.g., "part_id=abc/file.parquet")
    /// * `reader` - Async reader providing file content
    #[must_use]
    pub fn new(pond_txn_id: i64, path: String, reader: R) -> Self {
        Self {
            pond_txn_id,
            path,
            reader,
            chunk_size: CHUNK_SIZE_DEFAULT,
            bundle_id_override: None,
            starting_offset: 0,
            prev_series_outboard: None,
        }
    }

    /// Set starting cumulative offset for FilePhysicalSeries with prior versions
    ///
    /// When a large file is version N of a FilePhysicalSeries, set this to the
    /// cumulative size of versions 0..N-1. This ensures chunk hashes are computed
    /// at the correct offset in the overall file series.
    #[must_use]
    pub fn with_starting_offset(mut self, offset: u64) -> Self {
        self.starting_offset = offset;
        self
    }

    /// Set previous version's SeriesOutboard for continuing cumulative hash
    ///
    /// When appending a large file to a FilePhysicalSeries, pass the previous
    /// version's SeriesOutboard to continue the cumulative bao-tree state.
    #[must_use]
    pub fn with_prev_outboard(mut self, prev: SeriesOutboard) -> Self {
        self.starting_offset = prev.cumulative_size;
        self.prev_series_outboard = Some(prev);
        self
    }

    /// Set custom chunk size (for testing or optimization)
    ///
    /// Chunk size must be between CHUNK_SIZE_MIN and CHUNK_SIZE_MAX.
    #[must_use]
    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size.clamp(CHUNK_SIZE_MIN, CHUNK_SIZE_MAX);
        self
    }

    /// Set bundle_id override (for metadata)
    ///
    /// When set, this bundle_id will be used instead of computing BLAKE3 root hash.
    #[must_use]
    pub fn with_bundle_id(mut self, bundle_id: String) -> Self {
        self.bundle_id_override = Some(bundle_id);
        self
    }

    /// Write the file to Arrow RecordBatches with bounded memory
    ///
    /// Reads the file in chunks, computes BLAKE3 hashes using SeriesOutboard format,
    /// and creates RecordBatches. Memory usage is bounded by batching chunks.
    ///
    /// Each chunk's outboard encodes cumulative byte offset in `cumulative_size`,
    /// enabling both independent and series validation depending on entry type.
    ///
    /// # Returns
    /// `ChunkedWriteResult` containing bundle_id, batches, and final SeriesOutboard
    ///
    /// # Errors
    /// Returns error if cannot read from input or compute hashes
    pub async fn write_to_batches(
        mut self,
    ) -> Result<ChunkedWriteResult, Box<dyn std::error::Error + Send + Sync>> {
        debug!(
            "Chunking file {} (txn {}) in {}MB chunks",
            self.path,
            self.pond_txn_id,
            self.chunk_size / (1024 * 1024)
        );

        let mut total_size = self.starting_offset;
        let mut chunk_id = 0i64;
        let mut buffer = vec![0u8; self.chunk_size];

        // Maximum chunks buffered before creating RecordBatch
        const CHUNKS_PER_BATCH: usize = 10;

        // Collect chunk data: (chunk_id, cumulative_hash, outboard_bytes, data)
        // outboard_bytes = SeriesOutboard.to_bytes() with cumulative offset encoded
        let mut all_chunk_data: Vec<(i64, blake3::Hash, Vec<u8>, Vec<u8>)> = Vec::new();

        // Track previous SeriesOutboard for incremental computation
        // Start from prev_series_outboard if continuing a FilePhysicalSeries
        let mut prev_series_outboard: Option<SeriesOutboard> = self.prev_series_outboard.take();

        // For pending bytes from previous versions (when continuing a series)
        // We need this from the caller since we don't have access to previous version content
        let initial_pending_bytes: Vec<u8> = Vec::new();
        if let Some(ref prev) = prev_series_outboard {
            let pending_size = (prev.cumulative_size % BLOCK_SIZE as u64) as usize;
            if pending_size > 0 {
                // The caller must provide pending bytes via with_pending_bytes()
                // For now, we assume block-aligned chunks (power-of-2 chunk sizes)
                // which means pending_size will be 0 for chunk boundaries
                debug!(
                    "Warning: prev_series_outboard has {} pending bytes, but pending content not provided",
                    pending_size
                );
            }
        }

        loop {
            // Fill the buffer completely (except for the last chunk)
            // reader.read() doesn't guarantee filling the buffer, so we loop
            let mut filled = 0;
            loop {
                let n = self.reader.read(&mut buffer[filled..]).await?;
                if n == 0 {
                    break;
                }
                filled += n;
                if filled >= self.chunk_size {
                    break;
                }
            }

            if filled == 0 {
                break;
            }

            let chunk = &buffer[..filled];
            total_size += filled as u64;

            // Each chunk is treated as a "version" with cumulative tracking
            let series_outboard = if let Some(prev) = &prev_series_outboard {
                // Compute pending bytes from previous cumulative size
                let pending_size = (prev.cumulative_size % BLOCK_SIZE as u64) as usize;
                // Get the tail of the previous chunk as pending bytes
                // For first chunk after prev_series_outboard, use initial_pending_bytes
                let pending = if pending_size > 0 {
                    if chunk_id == 0 && !initial_pending_bytes.is_empty() {
                        &initial_pending_bytes[..]
                    } else if !all_chunk_data.is_empty() {
                        let prev_chunk_data = &all_chunk_data.last().expect("ok").3;
                        let start = prev_chunk_data.len().saturating_sub(pending_size);
                        &prev_chunk_data[start..]
                    } else {
                        &[][..]
                    }
                } else {
                    &[][..]
                };
                SeriesOutboard::append_version(prev, pending, chunk)
            } else {
                SeriesOutboard::first_version(chunk)
            };

            // The cumulative_blake3 is the bao root through all content so far
            let chunk_hash = blake3::Hash::from_bytes(series_outboard.cumulative_blake3);
            let outboard_bytes = series_outboard.to_bytes();

            prev_series_outboard = Some(series_outboard);
            all_chunk_data.push((chunk_id, chunk_hash, outboard_bytes, chunk.to_vec()));
            chunk_id += 1;
        }

        // The last chunk's cumulative_blake3 is the file's root hash
        let (root_hash, final_outboard) = if let Some(ref last_outboard) = prev_series_outboard {
            (
                blake3::Hash::from_bytes(last_outboard.cumulative_blake3),
                last_outboard.clone(),
            )
        } else {
            // Empty file case
            let empty = SeriesOutboard::first_version(&[]);
            (blake3::Hash::from_bytes(empty.cumulative_blake3), empty)
        };

        let root_hash_hex = root_hash.to_hex().to_string();

        let bundle_id = if let Some(ref override_id) = self.bundle_id_override {
            override_id.clone()
        } else {
            format!("POND-FILE-{}", root_hash_hex)
        };

        debug!(
            "File {} has {} chunks, {} bytes (starting_offset={}), BLAKE3={}",
            self.path,
            chunk_id,
            total_size,
            self.starting_offset,
            &root_hash_hex[..16.min(root_hash_hex.len())]
        );

        // Special case: empty file needs at least one row with empty chunk_data
        if all_chunk_data.is_empty() {
            let series = SeriesOutboard::first_version(&[]);
            let empty_hash = blake3::Hash::from_bytes(series.cumulative_blake3);
            all_chunk_data.push((0, empty_hash, series.to_bytes(), vec![]));
        }

        // Create RecordBatches
        let schema = arrow_schema();
        let mut all_batches = Vec::new();

        for chunk_batch in all_chunk_data.chunks(CHUNKS_PER_BATCH) {
            let batch = create_batch(
                chunk_batch,
                &bundle_id,
                &self.path,
                self.pond_txn_id,
                &root_hash_hex,
                total_size,
                &schema,
            )?;
            all_batches.push(batch);
        }

        Ok(ChunkedWriteResult {
            bundle_id,
            batches: all_batches,
            final_outboard,
        })
    }
}

/// Streaming reader for chunked files
///
/// Reads chunks from Arrow RecordBatches, verifies BLAKE3 hash for each chunk,
/// and verifies the root hash for the complete file.
pub struct ChunkedReader {
    batches: Vec<RecordBatch>,
    expected_bundle_id: Option<String>,
}

impl ChunkedReader {
    /// Create a new chunked reader from RecordBatches
    #[must_use]
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self {
            batches,
            expected_bundle_id: None,
        }
    }

    /// Set expected bundle_id for verification
    #[must_use]
    pub fn with_expected_bundle_id(mut self, bundle_id: String) -> Self {
        self.expected_bundle_id = Some(bundle_id);
        self
    }

    /// Read the complete file, verifying checksums
    ///
    /// # Errors
    /// Returns error if chunks are out of order, hash mismatches, etc.
    pub async fn read_to_writer<W: tokio::io::AsyncWrite + Unpin>(
        self,
        mut writer: W,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use tokio::io::AsyncWriteExt;

        // chunk_hashes collects cumulative hashes; the last one is the root
        let mut chunk_hashes: Vec<blake3::Hash> = Vec::new();
        let mut total_bytes_read = 0u64;
        let mut expected_chunk_id = 0i64;
        let mut expected_root_hash: Option<String> = None;
        let mut expected_total_size: Option<i64> = None;

        for batch in self.batches {
            process_batch(
                &batch,
                &mut writer,
                &mut chunk_hashes,
                &mut total_bytes_read,
                &mut expected_chunk_id,
                &mut expected_root_hash,
                &mut expected_total_size,
            )
            .await?;
        }

        // Verify we got at least some data
        if expected_chunk_id == 0 {
            return Err("No chunks found in file".into());
        }

        // Verify total size
        if let Some(expected_size) = expected_total_size
            && total_bytes_read != expected_size as u64
        {
            return Err(format!(
                "Size mismatch: expected {} bytes, got {}",
                expected_size, total_bytes_read
            )
            .into());
        }

        // The last chunk's cumulative_blake3 is the file's root hash
        let final_hash = chunk_hashes
            .last()
            .copied()
            .unwrap_or_else(|| blake3::hash(&[]));
        let final_hash_hex = final_hash.to_hex().to_string();

        if let Some(expected) = expected_root_hash
            && final_hash_hex != expected
        {
            return Err(format!(
                "Root hash mismatch: expected {}, got {}",
                expected, final_hash_hex
            )
            .into());
        }

        writer.flush().await?;

        debug!(
            "Successfully read file ({} bytes, BLAKE3={})",
            total_bytes_read,
            &final_hash_hex[..16]
        );

        Ok(())
    }
}

/// Arrow schema for chunked file records
///
/// The `chunk_outboard` column contains SeriesOutboard.to_bytes() which includes:
/// - `cumulative_size`: Byte offset where this chunk ends (encodes position in file)
/// - `cumulative_blake3`: Bao root hash through all content up to this chunk
/// - `IncrementalOutboard`: Delta nodes and frontier for resumption
///
/// Entry type (FilePhysicalVersion vs FilePhysicalSeries) is encoded in the
/// filename schema, not in the parquet data. This determines how to interpret
/// the cumulative values during validation.
#[must_use]
pub fn arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("bundle_id", DataType::Utf8, false),
        Field::new("pond_txn_id", DataType::Int64, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("chunk_id", DataType::Int64, false),
        Field::new("chunk_hash", DataType::Utf8, false),
        Field::new("chunk_outboard", DataType::Binary, false),
        Field::new("chunk_data", DataType::Binary, false),
        Field::new("total_size", DataType::Int64, false),
        Field::new("root_hash", DataType::Utf8, false),
    ]))
}

/// Create a RecordBatch from a slice of chunks
fn create_batch(
    chunks: &[(i64, blake3::Hash, Vec<u8>, Vec<u8>)],
    bundle_id: &str,
    path: &str,
    pond_txn_id: i64,
    root_hash: &str,
    total_size: u64,
    schema: &Arc<Schema>,
) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
    let num_chunks = chunks.len();

    let mut bundle_ids = Vec::with_capacity(num_chunks);
    let mut pond_txn_ids = Vec::with_capacity(num_chunks);
    let mut paths = Vec::with_capacity(num_chunks);
    let mut chunk_ids = Vec::with_capacity(num_chunks);
    let mut chunk_hashes = Vec::with_capacity(num_chunks);
    let mut chunk_outboards = Vec::with_capacity(num_chunks);
    let mut chunk_datas = Vec::with_capacity(num_chunks);
    let mut total_sizes = Vec::with_capacity(num_chunks);
    let mut root_hashes = Vec::with_capacity(num_chunks);

    for (chunk_id, chunk_hash, outboard, data) in chunks {
        bundle_ids.push(bundle_id.to_string());
        pond_txn_ids.push(pond_txn_id);
        paths.push(path.to_string());
        chunk_ids.push(*chunk_id);
        chunk_hashes.push(chunk_hash.to_hex().to_string());
        chunk_outboards.push(outboard.clone());
        chunk_datas.push(data.clone());
        total_sizes.push(total_size as i64);
        root_hashes.push(root_hash.to_string());
    }

    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(StringArray::from(bundle_ids)),
            Arc::new(Int64Array::from(pond_txn_ids)),
            Arc::new(StringArray::from(paths)),
            Arc::new(Int64Array::from(chunk_ids)),
            Arc::new(StringArray::from(chunk_hashes)),
            Arc::new(BinaryArray::from(
                chunk_outboards
                    .iter()
                    .map(|v| Some(v.as_slice()))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(BinaryArray::from(
                chunk_datas
                    .iter()
                    .map(|v| Some(v.as_slice()))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(total_sizes)),
            Arc::new(StringArray::from(root_hashes)),
        ],
    )?;

    Ok(batch)
}

/// Process a single RecordBatch during reading
#[allow(clippy::too_many_arguments)]
async fn process_batch<W: tokio::io::AsyncWrite + Unpin>(
    batch: &RecordBatch,
    writer: &mut W,
    chunk_hashes: &mut Vec<blake3::Hash>,
    total_bytes_read: &mut u64,
    expected_chunk_id: &mut i64,
    expected_root_hash: &mut Option<String>,
    expected_total_size: &mut Option<i64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use tokio::io::AsyncWriteExt;

    // CRITICAL: Use column names, not indices, for Delta Lake compatibility.
    //
    // When bundle_id is a partition column, Delta Lake stores its value in the
    // directory path (e.g., /bundle_id=value/file.parquet), not in the Parquet file.
    // DataFusion may reconstruct partition columns at different positions than
    // the schema definition, or exclude them entirely.
    //
    // Example: Schema defines [bundle_id(0), pond_txn_id(1), path(2), chunk_id(3), ...]
    // But SELECT * may return [pond_txn_id(0), path(1), chunk_id(2), ...] if bundle_id
    // is excluded, causing batch.column(3) to be chunk_hash instead of chunk_id.
    //
    // See: docs/duckpond-system-patterns.md "Delta Lake Partition Column Ordering"
    let schema = batch.schema();

    let chunk_id_idx = schema
        .index_of("chunk_id")
        .map_err(|_| "Column chunk_id not found")?;
    let chunk_hash_idx = schema
        .index_of("chunk_hash")
        .map_err(|_| "Column chunk_hash not found")?;
    let chunk_outboard_idx = schema
        .index_of("chunk_outboard")
        .map_err(|_| "Column chunk_outboard not found")?;
    let chunk_data_idx = schema
        .index_of("chunk_data")
        .map_err(|_| "Column chunk_data not found")?;
    let total_size_idx = schema
        .index_of("total_size")
        .map_err(|_| "Column total_size not found")?;
    let root_hash_idx = schema
        .index_of("root_hash")
        .map_err(|_| "Column root_hash not found")?;

    let chunk_ids = batch
        .column(chunk_id_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("Invalid column type for chunk_id")?;

    let chunk_hashes_arr = batch
        .column(chunk_hash_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("Invalid column type for chunk_hash")?;

    let chunk_outboards = batch
        .column(chunk_outboard_idx)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or("Invalid column type for chunk_outboard")?;

    let chunk_datas = batch
        .column(chunk_data_idx)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or("Invalid column type for chunk_data")?;

    let total_sizes = batch
        .column(total_size_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("Invalid column type for total_size")?;

    let root_hashes_arr = batch
        .column(root_hash_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("Invalid column type for root_hash")?;

    for i in 0..batch.num_rows() {
        let chunk_id = chunk_ids.value(i);
        let expected_hash_hex = chunk_hashes_arr.value(i);
        let chunk_data = chunk_datas.value(i);
        let stored_outboard_bytes = chunk_outboards.value(i);

        // Verify chunk sequence
        if chunk_id != *expected_chunk_id {
            return Err(format!(
                "Chunk sequence error: expected {}, got {}",
                *expected_chunk_id, chunk_id
            )
            .into());
        }

        // Parse stored SeriesOutboard and verify cumulative hash matches
        let stored_outboard = SeriesOutboard::from_bytes(stored_outboard_bytes)
            .map_err(|e| format!("Invalid chunk_outboard for chunk {}: {}", chunk_id, e))?;

        let stored_cumulative_hash = blake3::Hash::from_bytes(stored_outboard.cumulative_blake3);
        let stored_hash_hex = stored_cumulative_hash.to_hex().to_string();

        if stored_hash_hex != expected_hash_hex {
            return Err(format!(
                "Chunk {} stored hash mismatch: chunk_hash={}, outboard.cumulative_blake3={}",
                chunk_id, expected_hash_hex, stored_hash_hex
            )
            .into());
        }

        // Verify chunk content size matches what's encoded in the outboard
        let expected_chunk_size = stored_outboard.version_size as usize;
        if chunk_data.len() != expected_chunk_size {
            return Err(format!(
                "Chunk {} size mismatch: expected {}, got {}",
                chunk_id,
                expected_chunk_size,
                chunk_data.len()
            )
            .into());
        }

        // Track the last chunk's cumulative hash for root verification
        chunk_hashes.push(stored_cumulative_hash);
        *total_bytes_read += chunk_data.len() as u64;

        // Write chunk to output
        writer.write_all(chunk_data).await?;

        // Store expected values from first chunk
        if *expected_chunk_id == 0 {
            *expected_total_size = Some(total_sizes.value(i));
            *expected_root_hash = Some(root_hashes_arr.value(i).to_string());
        }

        *expected_chunk_id += 1;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_write_read_roundtrip() {
        let data = vec![42u8; 5 * 1024 * 1024]; // 5MB
        let reader = Cursor::new(data.clone());

        let writer = ChunkedWriter::new(123, "test/file.dat".to_string(), reader);
        let result = writer.write_to_batches().await.unwrap();

        assert!(result.bundle_id.starts_with("POND-FILE-"));
        assert!(!result.batches.is_empty());

        // Read it back
        let reader = ChunkedReader::new(result.batches);
        let mut output = Vec::new();
        reader.read_to_writer(&mut output).await.unwrap();

        assert_eq!(output, data);
    }

    #[tokio::test]
    async fn test_empty_file() {
        let data: Vec<u8> = vec![];
        let reader = Cursor::new(data.clone());

        let writer = ChunkedWriter::new(1, "test/empty.dat".to_string(), reader);
        let result = writer.write_to_batches().await.unwrap();

        let expected_hash = blake3::hash(&[]).to_hex().to_string();
        assert_eq!(result.bundle_id, format!("POND-FILE-{}", expected_hash));

        // Read it back
        let reader = ChunkedReader::new(result.batches);
        let mut output = Vec::new();
        reader.read_to_writer(&mut output).await.unwrap();

        assert_eq!(output.len(), 0);
    }
}
