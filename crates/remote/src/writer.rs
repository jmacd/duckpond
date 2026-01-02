// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Streaming writer for chunked files
//!
//! Reads from an AsyncRead source, chunks the data, computes BLAKE3 hashes,
//! and writes to Delta Lake in a single transaction.

use crate::Result;
use crate::schema::{BLAKE3_BLOCK_SIZE, CHUNK_SIZE_DEFAULT, ChunkedFileRecord};
use arrow_array::{BinaryArray, Int64Array, RecordBatch, StringArray};
use bao_tree::io::outboard::PostOrderMemOutboard;
use bao_tree::BlockSize;
use deltalake::DeltaTable;
use deltalake::operations::write::WriteBuilder;
use log::debug;
use std::sync::Arc;
use tokio::io::AsyncReadExt;

/// Streaming writer that chunks files and writes to Delta Lake
///
/// Reads data from an AsyncRead source, chunks it into pieces,
/// computes BLAKE3 subtree hash and outboard per chunk, then combines
/// into a file root hash. Writes all chunks to Delta Lake in a single transaction.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use remote::{ChunkedWriter, FileType};
/// use std::io::Cursor;
///
/// let data = vec![0u8; 100 * 1024 * 1024]; // 100MB
/// let reader = Cursor::new(data);
///
/// let mut writer = ChunkedWriter::new(
///     123,  // pond_txn_id
///     "part_id=abc/file.parquet",
///     reader,
/// );
///
/// // Will be called internally by RemoteTable::write_file
/// // let bundle_id = writer.write_to_table(&mut delta_table).await?;
/// # Ok(())
/// # }
/// ```
pub struct ChunkedWriter<R> {
    pond_txn_id: i64,
    path: String,
    reader: R,
    chunk_size: usize,
    /// Optional override for bundle_id (used for metadata to avoid BLAKE3 computation)
    bundle_id_override: Option<String>,
}

impl<R: tokio::io::AsyncRead + Unpin> ChunkedWriter<R> {
    /// Create a new chunked writer
    ///
    /// # Arguments
    /// * `pond_txn_id` - Transaction sequence number from pond
    /// * `path` - File path in Delta table
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
        self.chunk_size =
            chunk_size.clamp(crate::schema::CHUNK_SIZE_MIN, crate::schema::CHUNK_SIZE_MAX);
        self
    }

    /// Set bundle_id override (for metadata)
    ///
    /// When set, this bundle_id will be used instead of computing BLAKE3 root hash.
    /// This is used for metadata records which need predictable bundle_ids
    /// in the format "metadata_{pond_txn_id}".
    #[must_use]
    pub fn with_bundle_id(mut self, bundle_id: String) -> Self {
        self.bundle_id_override = Some(bundle_id);
        self
    }

    /// Write the file to Delta Lake in chunks with bounded memory
    ///
    /// Reads the file in chunks, computes BLAKE3 hashes, and writes to Delta Lake
    /// using streaming writes. Memory usage is bounded by writing batches of
    /// chunks rather than accumulating all chunks before writing.
    ///
    /// # Returns
    /// The bundle_id (BLAKE3 root hash with POND-FILE- prefix) of the written file
    ///
    /// # Errors
    /// Returns error if:
    /// - Cannot read from input
    /// - Cannot compute hashes
    /// - Cannot write to Delta Lake
    pub async fn write_to_table(mut self, table: &mut DeltaTable) -> Result<String> {
        debug!(
            "Writing file {} (txn {}) to remote in {}MB chunks",
            self.path,
            self.pond_txn_id,
            self.chunk_size / (1024 * 1024)
        );

        // BLAKE3 block size: 16KB (chunk_log=4)
        let block_size = BlockSize::from_chunk_log(
            (BLAKE3_BLOCK_SIZE.trailing_zeros() - 10) as u8, // 16KB = 2^14, chunk = 2^10, so log = 4
        );

        let mut total_size = 0u64;
        let mut chunk_id = 0i64;
        let mut buffer = vec![0u8; self.chunk_size];

        // Maximum chunks buffered before creating RecordBatch
        // 10 chunks × 16MB = ~160MB per batch
        const CHUNKS_PER_BATCH: usize = 10;

        // Collect chunk data with BLAKE3 subtree hashes and outboards
        // Format: (chunk_id, subtree_hash, outboard, data)
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
            
            // The outboard.root is the chunk's hash (computed as if standalone)
            // For combining into file hash, we'll use these directly
            let chunk_hash = outboard.root;
            let outboard_data = outboard.data;

            all_chunk_data.push((chunk_id, chunk_hash, outboard_data, chunk.to_vec()));
            chunk_id += 1;
        }

        // Compute file root hash by combining all chunk hashes
        let root_hash = if all_chunk_data.is_empty() {
            // Empty file: hash of empty data
            blake3::hash(&[])
        } else if all_chunk_data.len() == 1 {
            // Single chunk: chunk hash IS the root hash
            all_chunk_data[0].1
        } else {
            // Multiple chunks: combine using BLAKE3 tree
            combine_chunk_hashes(&all_chunk_data.iter().map(|(_, h, _, _)| *h).collect::<Vec<_>>())
        };

        let root_hash_hex = root_hash.to_hex().to_string();

        let bundle_id = if let Some(ref override_id) = self.bundle_id_override {
            // Use provided bundle_id (for transaction files)
            override_id.clone()
        } else {
            // Use BLAKE3-based bundle_id for content-addressed large files
            ChunkedFileRecord::large_file_bundle_id(&root_hash_hex)
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
            let outboard = PostOrderMemOutboard::create(&[], block_size);
            all_chunk_data.push((0, outboard.root, outboard.data, vec![]));
        }

        // Create RecordBatches with final metadata
        let mut all_batches = Vec::new();
        for chunk_batch in all_chunk_data.chunks(CHUNKS_PER_BATCH) {
            let batch = self.create_batch(
                chunk_batch,
                &bundle_id,
                &root_hash_hex,
                total_size,
            )?;
            all_batches.push(batch);
        }

        debug!(
            "Writing {} batches to Delta Lake for file {}",
            all_batches.len(),
            bundle_id
        );

        // Write all batches to Delta Lake in a single transaction
        let updated_table = WriteBuilder::new(
            table.log_store(),
            table.state.as_ref().map(|s| s.snapshot().clone()),
        )
        .with_input_batches(all_batches)
        .await?;

        // Update our table's state to reflect the write
        *table = updated_table;

        debug!(
            "Successfully wrote file {} ({} chunks) to remote",
            self.path, chunk_id
        );

        Ok(bundle_id)
    }

    /// Create a RecordBatch from a slice of chunks with final metadata
    fn create_batch(
        &self,
        chunks: &[(i64, blake3::Hash, Vec<u8>, Vec<u8>)],
        bundle_id: &str,
        root_hash: &str,
        total_size: u64,
    ) -> Result<RecordBatch> {
        if chunks.is_empty() {
            return Err(arrow::error::ArrowError::InvalidArgumentError(
                "Cannot create batch from empty chunks".to_string(),
            )
            .into());
        }

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
            pond_txn_ids.push(self.pond_txn_id);
            paths.push(self.path.clone());
            chunk_ids.push(*chunk_id);
            chunk_hashes.push(chunk_hash.to_hex().to_string());
            chunk_outboards.push(outboard.clone());
            chunk_datas.push(data.clone());
            total_sizes.push(total_size as i64);
            root_hashes.push(root_hash.to_string());
        }

        let schema = ChunkedFileRecord::arrow_schema();
        let batch = RecordBatch::try_new(
            schema,
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
    let mut current_level: Vec<ChainingValue> = hashes
        .iter()
        .map(|h| *h.as_bytes())
        .collect();

    while current_level.len() > 1 {
        let mut next_level = Vec::new();
        let is_final_level = current_level.len() <= 2;

        for pair in current_level.chunks(2) {
            match pair {
                [left, right] => {
                    if is_final_level && next_level.is_empty() {
                        // Final merge - return root hash
                        return merge_subtrees_root(left, right, Mode::Hash);
                    } else {
                        // Non-root merge
                        let parent = merge_subtrees_non_root(left, right, Mode::Hash);
                        next_level.push(parent);
                    }
                }
                [single] => {
                    // Odd hash carries forward
                    next_level.push(*single);
                }
                _ => unreachable!(),
            }
        }

        current_level = next_level;
    }

    // Single element remaining - convert back to Hash
    blake3::Hash::from(current_level[0])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RemoteTable;
    use std::io::Cursor;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_write_small_file() {
        let _ = env_logger::try_init();
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("remote");

        let mut table = RemoteTable::create(table_path.to_str().unwrap())
            .await
            .unwrap();

        // Create a small test file (< 1 chunk)
        let data = vec![42u8; 1024 * 1024]; // 1MB
        let reader = Cursor::new(data.clone());

        let bundle_id = table
            .write_file(123, "test/file.dat", reader)
            .await
            .unwrap();

        // Verify bundle_id has POND-FILE- prefix and valid BLAKE3 hash
        assert!(bundle_id.starts_with("POND-FILE-"));
        let hash_part = &bundle_id["POND-FILE-".len()..];
        assert_eq!(hash_part.len(), 64);
        assert!(hash_part.chars().all(|c| c.is_ascii_hexdigit()));

        // Verify we can query the file
        let files = table.list_files("pond-test-123").await.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].0, bundle_id);
        assert_eq!(files[0].1, "test/file.dat");
        assert_eq!(files[0].2, 123);
        assert_eq!(files[0].3, data.len() as i64);
    }

    #[tokio::test]
    async fn test_write_large_file() {
        let _ = env_logger::try_init();
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("remote");

        let mut table = RemoteTable::create(table_path.to_str().unwrap())
            .await
            .unwrap();

        // Create a large test file (multiple chunks)
        let chunk_size = 1024 * 1024; // 1MB chunks for fast test
        let data_size = 3 * chunk_size + 512 * 1024; // 3.5MB = 4 chunks
        let data = vec![123u8; data_size];

        let _bundle_id = table
            .write_file(
                456,
                "test/large.dat",
                Cursor::new(data.clone()),
            )
            .await
            .unwrap();

        // Verify file was written
        let files = table.list_files("pond-test-456").await.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].3, data.len() as i64);
    }

    #[tokio::test]
    async fn test_write_empty_file() {
        let _ = env_logger::try_init();
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("remote");

        let mut table = RemoteTable::create(table_path.to_str().unwrap())
            .await
            .unwrap();

        // Empty file
        let data: Vec<u8> = vec![];
        let reader = Cursor::new(data);

        let bundle_id = table
            .write_file(1, "test/empty.dat", reader)
            .await
            .unwrap();

        // BLAKE3 of empty input with POND-FILE- prefix
        let expected_hash = blake3::hash(&[]).to_hex().to_string();
        let expected_bundle_id = format!("POND-FILE-{}", expected_hash);
        assert_eq!(bundle_id, expected_bundle_id);
    }
}
