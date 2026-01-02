// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Streaming reader for chunked files
//!
//! Reads chunked files from Delta Lake, verifies BLAKE3 hashes
//! for each chunk and the complete file root hash.

use crate::Result;
use crate::error::RemoteError;
use crate::schema::BLAKE3_BLOCK_SIZE;
use arrow_array::RecordBatch;
use bao_tree::io::outboard::PostOrderMemOutboard;
use bao_tree::BlockSize;
use datafusion::prelude::*;
use deltalake::DeltaTable;
use futures::TryStreamExt;
use log::debug;
use tokio::io::AsyncWriteExt;

/// Streaming reader for chunked files
///
/// Reads chunks from Delta Lake in order, verifies BLAKE3 hash for each chunk,
/// and verifies the root hash for the complete file.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use remote::ChunkedReader;
/// use std::io::Cursor;
///
/// # let table: deltalake::DeltaTable = unimplemented!();
/// let reader = ChunkedReader::new(&table, "abc123def...blake3...");
///
/// let mut output = Vec::new();
/// reader.read_to_writer(&mut output).await?;
/// # Ok(())
/// # }
/// ```
pub struct ChunkedReader<'a> {
    table: &'a DeltaTable,
    bundle_id: String,
    path: String,
    pond_txn_id: i64,
}

impl<'a> ChunkedReader<'a> {
    /// Create a new chunked reader
    ///
    /// # Arguments
    /// * `table` - Delta Lake table containing the chunks
    /// * `bundle_id` - Partition identifier (e.g., FILE-META-{date}-{txn} or POND-FILE-{blake3})
    /// * `path` - Original file path to uniquely identify file within partition
    /// * `pond_txn_id` - Transaction sequence number
    #[must_use]
    pub fn new(
        table: &'a DeltaTable,
        bundle_id: impl Into<String>,
        path: impl Into<String>,
        pond_txn_id: i64,
    ) -> Self {
        Self {
            table,
            bundle_id: bundle_id.into(),
            path: path.into(),
            pond_txn_id,
        }
    }

    /// Read the complete file, verifying checksums
    ///
    /// Reads all chunks in order, verifies BLAKE3 hash for each chunk,
    /// and verifies root hash for the complete file after reading.
    ///
    /// # Arguments
    /// * `writer` - Async writer to receive the file content
    ///
    /// # Errors
    /// Returns error if:
    /// - File not found
    /// - Chunks out of order
    /// - BLAKE3 hash mismatch
    /// - Root hash mismatch
    /// - Cannot read from Delta Lake
    /// - Cannot write to output
    pub async fn read_to_writer<W: tokio::io::AsyncWrite + Unpin>(
        self,
        mut writer: W,
    ) -> Result<()> {
        debug!("Reading file {} from remote", self.bundle_id);

        // Create a SessionContext to query Delta Lake
        let ctx = SessionContext::new();
        ctx.register_table("remote_files", std::sync::Arc::new(self.table.clone()))?;

        // Query for all chunks of this file, ordered by chunk_id
        // Note: chunk_outboard is index 1, chunk_hash is index 5
        let df = ctx
            .sql(&format!(
                "SELECT chunk_id, chunk_hash, chunk_outboard, chunk_data, total_size, root_hash 
                 FROM remote_files 
                 WHERE bundle_id = '{}' AND path = '{}' AND pond_txn_id = {} 
                 ORDER BY chunk_id",
                self.bundle_id, self.path, self.pond_txn_id
            ))
            .await?;

        log::info!(
            "Reading chunks for bundle_id: {}, path: {}, txn: {}",
            self.bundle_id,
            self.path,
            self.pond_txn_id
        );

        let mut stream = df.execute_stream().await?;

        let mut chunk_hashes: Vec<blake3::Hash> = Vec::new();
        let mut total_bytes_read = 0u64;
        let mut expected_chunk_id = 0i64;
        let mut expected_root_hash: Option<String> = None;
        let mut expected_total_size: Option<i64> = None;

        while let Some(batch) = stream.try_next().await? {
            self.process_batch(
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
            return Err(RemoteError::FileNotFound(self.bundle_id.clone()));
        }

        // Verify total size
        if let Some(expected_size) = expected_total_size
            && total_bytes_read != expected_size as u64
        {
            return Err(RemoteError::FileIntegrityFailed {
                bundle_id: self.bundle_id.clone(),
                expected: format!("{} bytes", expected_size),
                actual: format!("{} bytes", total_bytes_read),
            });
        }

        // Compute and verify root hash from chunk hashes
        let computed_root = combine_chunk_hashes(&chunk_hashes);
        let computed_root_hex = computed_root.to_hex().to_string();
        
        if let Some(expected) = expected_root_hash
            && computed_root_hex != expected
        {
            return Err(RemoteError::FileIntegrityFailed {
                bundle_id: self.bundle_id.clone(),
                expected,
                actual: computed_root_hex,
            });
        }

        writer.flush().await?;

        debug!(
            "Successfully read file {} ({} bytes, BLAKE3={})",
            self.bundle_id,
            total_bytes_read,
            &computed_root_hex[..16]
        );

        Ok(())
    }

    /// Process a single record batch
    #[allow(clippy::too_many_arguments)]
    async fn process_batch<W: tokio::io::AsyncWrite + Unpin>(
        &self,
        batch: &RecordBatch,
        writer: &mut W,
        chunk_hashes: &mut Vec<blake3::Hash>,
        total_bytes_read: &mut u64,
        expected_chunk_id: &mut i64,
        expected_root_hash: &mut Option<String>,
        expected_total_size: &mut Option<i64>,
    ) -> Result<()> {
        // BLAKE3 block size for verification
        let block_size = BlockSize::from_chunk_log(
            (BLAKE3_BLOCK_SIZE.trailing_zeros() - 10) as u8,
        );

        let chunk_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .ok_or_else(|| {
                RemoteError::TableOperation("Invalid column type for chunk_id".to_string())
            })?;

        let chunk_hashes_arr = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| {
                RemoteError::TableOperation("Invalid column type for chunk_hash".to_string())
            })?;

        let chunk_outboards = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .ok_or_else(|| {
                RemoteError::TableOperation("Invalid column type for chunk_outboard".to_string())
            })?;

        let chunk_datas = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .ok_or_else(|| {
                RemoteError::TableOperation("Invalid column type for chunk_data".to_string())
            })?;

        let total_sizes = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .ok_or_else(|| {
                RemoteError::TableOperation("Invalid column type for total_size".to_string())
            })?;

        let root_hashes = batch
            .column(5)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| {
                RemoteError::TableOperation("Invalid column type for root_hash".to_string())
            })?;

        for i in 0..batch.num_rows() {
            let chunk_id = chunk_ids.value(i);
            let expected_hash_hex = chunk_hashes_arr.value(i);
            let stored_outboard = chunk_outboards.value(i);
            let chunk_data = chunk_datas.value(i);

            log::info!(
                "Processing chunk_id={}, expected={}, batch row {}/{}",
                chunk_id,
                *expected_chunk_id,
                i + 1,
                batch.num_rows()
            );

            // Verify chunk sequence
            if chunk_id != *expected_chunk_id {
                return Err(RemoteError::InvalidChunkSequence {
                    expected: *expected_chunk_id,
                    actual: chunk_id,
                });
            }

            // Verify BLAKE3 hash by recomputing outboard
            let computed_outboard = PostOrderMemOutboard::create(chunk_data, block_size);
            let computed_hash_hex = computed_outboard.root.to_hex().to_string();
            
            if computed_hash_hex != expected_hash_hex {
                return Err(RemoteError::ChunkIntegrityFailed {
                    bundle_id: self.bundle_id.clone(),
                    chunk_id,
                    expected: i64::from_str_radix(&expected_hash_hex[..16], 16).unwrap_or(0),
                    actual: i64::from_str_radix(&computed_hash_hex[..16], 16).unwrap_or(0),
                });
            }

            // Optionally verify stored outboard matches computed (belt and suspenders)
            if stored_outboard != computed_outboard.data.as_slice() {
                log::warn!(
                    "Chunk {} outboard data mismatch (hash still valid)",
                    chunk_id
                );
            }

            // Collect chunk hash for root hash verification
            chunk_hashes.push(computed_outboard.root);
            *total_bytes_read += chunk_data.len() as u64;

            // Write chunk to output
            writer.write_all(chunk_data).await?;

            // Store expected values from first chunk
            if expected_chunk_id == &0 {
                *expected_total_size = Some(total_sizes.value(i));
                *expected_root_hash = Some(root_hashes.value(i).to_string());
            }

            *expected_chunk_id += 1;
        }

        Ok(())
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
    async fn test_read_write_roundtrip() {
        let _ = env_logger::try_init();
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("remote");

        let mut table = RemoteTable::create(table_path.to_str().unwrap())
            .await
            .unwrap();

        // Write a test file
        let original_data = vec![42u8; 5 * 1024 * 1024]; // 5MB
        let reader = Cursor::new(original_data.clone());

        let bundle_id = table
            .write_file(123, "test/roundtrip.dat", reader)
            .await
            .unwrap();

        // Read it back
        let mut output = Vec::new();
        table
            .read_file(&bundle_id, "test/roundtrip.dat", 123, &mut output)
            .await
            .unwrap();

        // Verify data matches
        assert_eq!(output, original_data);
    }

    #[tokio::test]
    async fn test_read_nonexistent_file() {
        let _ = env_logger::try_init();
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("remote");

        let table = RemoteTable::create(table_path.to_str().unwrap())
            .await
            .unwrap();

        // Try to read a file that doesn't exist
        let mut output = Vec::new();
        let result = table
            .read_file("nonexistent_bundle_id", "nonexistent.dat", 0, &mut output)
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            RemoteError::FileNotFound(_) => {}
            e => panic!("Expected FileNotFound, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_read_empty_file() {
        let _ = env_logger::try_init();
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("remote");

        let mut table = RemoteTable::create(table_path.to_str().unwrap())
            .await
            .unwrap();

        // Write empty file
        let reader = Cursor::new(vec![]);
        let bundle_id = table
            .write_file(789, "test/empty.dat", reader)
            .await
            .unwrap();

        // Read it back
        let mut output = Vec::new();
        table
            .read_file(&bundle_id, "test/empty.dat", 789, &mut output)
            .await
            .unwrap();

        assert_eq!(output.len(), 0);
    }

    #[tokio::test]
    async fn test_read_large_file() {
        let _ = env_logger::try_init();
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("remote");

        let mut table = RemoteTable::create(table_path.to_str().unwrap())
            .await
            .unwrap();

        // Write a large file with multiple chunks
        let chunk_size = 1024 * 1024; // 1MB chunks
        let data_size = 3 * chunk_size + 512 * 1024; // 3.5MB
        let original_data: Vec<u8> = (0..data_size).map(|i| (i % 256) as u8).collect();

        let reader = Cursor::new(original_data.clone());
        let bundle_id = table
            .write_file(456, "test/chunks.dat", reader)
            .await
            .unwrap();

        // Read it back
        let mut output = Vec::new();
        table
            .read_file(&bundle_id, "test/chunks.dat", 456, &mut output)
            .await
            .unwrap();

        // Verify data matches exactly
        assert_eq!(output, original_data);
    }
}
