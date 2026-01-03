// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Chunked file format utilities for large file storage
//!
//! This module provides utilities for reading and writing files in chunked parquet format
//! with BLAKE3 verification. The same format is used for both local large file storage
//! and remote backups, enabling direct file copying without re-chunking.
//!
//! The chunking format uses BLAKE3 Merkle trees for verification:
//! - Each chunk gets its own Merkle tree with 16KB blocks (bao-tree format)
//! - Chunk hashes are combined into a file root hash using BLAKE3 tree structure
//! - Outboard data (~0.4% overhead) enables verified streaming

use arrow_array::{BinaryArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use bao_tree::BlockSize;
use bao_tree::io::outboard::PostOrderMemOutboard;
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
/// Outboard overhead = (blocks - 1) * 64 bytes ≈ 0.39%
pub const BLAKE3_BLOCK_SIZE: usize = 16 * 1024;

/// Streaming writer that chunks files and creates Arrow RecordBatches
///
/// Reads data from an AsyncRead source, chunks it into pieces,
/// computes BLAKE3 subtree hash and outboard per chunk, then combines
/// into a file root hash. Creates Arrow RecordBatches suitable for writing
/// to Parquet or Delta Lake.
pub struct ChunkedWriter<R> {
    pond_txn_id: i64,
    path: String,
    reader: R,
    chunk_size: usize,
    bundle_id_override: Option<String>,
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
        }
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
    /// Reads the file in chunks, computes BLAKE3 hashes, and creates RecordBatches.
    /// Memory usage is bounded by batching chunks rather than accumulating all
    /// chunks before returning.
    ///
    /// # Returns
    /// Tuple of (bundle_id, Vec<RecordBatch>) where bundle_id is the BLAKE3 root hash
    ///
    /// # Errors
    /// Returns error if cannot read from input or compute hashes
    pub async fn write_to_batches(
        mut self,
    ) -> Result<(String, Vec<RecordBatch>), Box<dyn std::error::Error + Send + Sync>> {
        debug!(
            "Chunking file {} (txn {}) in {}MB chunks",
            self.path,
            self.pond_txn_id,
            self.chunk_size / (1024 * 1024)
        );

        // BLAKE3 block size: 16KB (chunk_log=4)
        let block_size = BlockSize::from_chunk_log((BLAKE3_BLOCK_SIZE.trailing_zeros() - 10) as u8);

        let mut total_size = 0u64;
        let mut chunk_id = 0i64;
        let mut buffer = vec![0u8; self.chunk_size];

        // Maximum chunks buffered before creating RecordBatch
        const CHUNKS_PER_BATCH: usize = 10;

        // Collect chunk data: (chunk_id, subtree_hash, outboard, data)
        let mut all_chunk_data: Vec<(i64, blake3::Hash, Vec<u8>, Vec<u8>)> = Vec::new();

        loop {
            let n = self.reader.read(&mut buffer).await?;
            if n == 0 {
                break;
            }

            let chunk = &buffer[..n];
            total_size += n as u64;

            // Create BLAKE3 outboard for this chunk
            let outboard = PostOrderMemOutboard::create(chunk, block_size);
            let chunk_hash = outboard.root;
            let outboard_data = outboard.data;

            all_chunk_data.push((chunk_id, chunk_hash, outboard_data, chunk.to_vec()));
            chunk_id += 1;
        }

        // Compute file root hash by combining all chunk hashes
        let root_hash = if all_chunk_data.is_empty() {
            blake3::hash(&[])
        } else if all_chunk_data.len() == 1 {
            all_chunk_data[0].1
        } else {
            combine_chunk_hashes(
                &all_chunk_data
                    .iter()
                    .map(|(_, h, _, _)| *h)
                    .collect::<Vec<_>>(),
            )
        };

        let root_hash_hex = root_hash.to_hex().to_string();

        let bundle_id = if let Some(ref override_id) = self.bundle_id_override {
            override_id.clone()
        } else {
            format!("POND-FILE-{}", root_hash_hex)
        };

        debug!(
            "File {} has {} chunks, {} bytes, BLAKE3={}",
            self.path,
            chunk_id,
            total_size,
            &root_hash_hex[..16.min(root_hash_hex.len())]
        );

        // Special case: empty file needs at least one row with empty chunk_data
        if all_chunk_data.is_empty() {
            let outboard = PostOrderMemOutboard::create([], block_size);
            all_chunk_data.push((0, outboard.root, outboard.data, vec![]));
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

        Ok((bundle_id, all_batches))
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

        let block_size = BlockSize::from_chunk_log((BLAKE3_BLOCK_SIZE.trailing_zeros() - 10) as u8);

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
                block_size,
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

        // Compute and verify root hash
        let computed_root = combine_chunk_hashes(&chunk_hashes);
        let computed_root_hex = computed_root.to_hex().to_string();

        if let Some(expected) = expected_root_hash
            && computed_root_hex != expected
        {
            return Err(format!(
                "Root hash mismatch: expected {}, got {}",
                expected, computed_root_hex
            )
            .into());
        }

        writer.flush().await?;

        debug!(
            "Successfully read file ({} bytes, BLAKE3={})",
            total_bytes_read,
            &computed_root_hex[..16]
        );

        Ok(())
    }
}

/// Arrow schema for chunked file records
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
    block_size: BlockSize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use tokio::io::AsyncWriteExt;

    let chunk_ids = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("Invalid column type for chunk_id")?;

    let chunk_hashes_arr = batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("Invalid column type for chunk_hash")?;

    let _chunk_outboards = batch
        .column(5)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or("Invalid column type for chunk_outboard")?;

    let chunk_datas = batch
        .column(6)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or("Invalid column type for chunk_data")?;

    let total_sizes = batch
        .column(7)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("Invalid column type for total_size")?;

    let root_hashes_arr = batch
        .column(8)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("Invalid column type for root_hash")?;

    for i in 0..batch.num_rows() {
        let chunk_id = chunk_ids.value(i);
        let expected_hash_hex = chunk_hashes_arr.value(i);
        let chunk_data = chunk_datas.value(i);

        // Verify chunk sequence
        if chunk_id != *expected_chunk_id {
            return Err(format!(
                "Chunk sequence error: expected {}, got {}",
                *expected_chunk_id, chunk_id
            )
            .into());
        }

        // Verify BLAKE3 hash
        let computed_outboard = PostOrderMemOutboard::create(chunk_data, block_size);
        let computed_hash_hex = computed_outboard.root.to_hex().to_string();

        if computed_hash_hex != expected_hash_hex {
            return Err(format!(
                "Chunk {} hash mismatch: expected {}, got {}",
                chunk_id, expected_hash_hex, computed_hash_hex
            )
            .into());
        }

        // Collect chunk hash for root hash verification
        chunk_hashes.push(computed_outboard.root);
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

/// Combine multiple chunk hashes into a single root hash using BLAKE3 tree structure
fn combine_chunk_hashes(hashes: &[blake3::Hash]) -> blake3::Hash {
    use blake3::hazmat::{ChainingValue, Mode, merge_subtrees_non_root, merge_subtrees_root};

    if hashes.is_empty() {
        return blake3::hash(&[]);
    }
    if hashes.len() == 1 {
        return hashes[0];
    }

    // Convert hashes to chaining values
    let mut current_level: Vec<ChainingValue> = hashes.iter().map(|h| *h.as_bytes()).collect();

    while current_level.len() > 1 {
        let mut next_level = Vec::new();
        let is_final_level = current_level.len() <= 2;

        for pair in current_level.chunks(2) {
            match pair {
                [left, right] => {
                    if is_final_level && next_level.is_empty() {
                        return merge_subtrees_root(left, right, Mode::Hash);
                    } else {
                        let parent = merge_subtrees_non_root(left, right, Mode::Hash);
                        next_level.push(parent);
                    }
                }
                [single] => {
                    next_level.push(*single);
                }
                _ => unreachable!(),
            }
        }

        current_level = next_level;
    }

    blake3::Hash::from(current_level[0])
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
        let (bundle_id, batches) = writer.write_to_batches().await.unwrap();

        assert!(bundle_id.starts_with("POND-FILE-"));
        assert!(!batches.is_empty());

        // Read it back
        let reader = ChunkedReader::new(batches);
        let mut output = Vec::new();
        reader.read_to_writer(&mut output).await.unwrap();

        assert_eq!(output, data);
    }

    #[tokio::test]
    async fn test_empty_file() {
        let data: Vec<u8> = vec![];
        let reader = Cursor::new(data.clone());

        let writer = ChunkedWriter::new(1, "test/empty.dat".to_string(), reader);
        let (bundle_id, batches) = writer.write_to_batches().await.unwrap();

        let expected_hash = blake3::hash(&[]).to_hex().to_string();
        assert_eq!(bundle_id, format!("POND-FILE-{}", expected_hash));

        // Read it back
        let reader = ChunkedReader::new(batches);
        let mut output = Vec::new();
        reader.read_to_writer(&mut output).await.unwrap();

        assert_eq!(output.len(), 0);
    }
}
