// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Streaming reader for chunked files
//!
//! Reads chunked files from Delta Lake, verifies CRC32 checksums
//! for each chunk and SHA256 for the complete file.

use crate::Result;
use crate::error::RemoteError;
use arrow_array::RecordBatch;
use datafusion::prelude::*;
use deltalake::DeltaTable;
use futures::TryStreamExt;
use log::debug;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

/// Streaming reader for chunked files
///
/// Reads chunks from Delta Lake in order, verifies CRC32 for each chunk,
/// and verifies SHA256 for the complete file.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use remote::ChunkedReader;
/// use std::io::Cursor;
///
/// # let table: deltalake::DeltaTable = unimplemented!();
/// let reader = ChunkedReader::new(&table, "abc123def...sha256...");
///
/// let mut output = Vec::new();
/// reader.read_to_writer(&mut output).await?;
/// # Ok(())
/// # }
/// ```
pub struct ChunkedReader<'a> {
    table: &'a DeltaTable,
    bundle_id: String,
}

impl<'a> ChunkedReader<'a> {
    /// Create a new chunked reader
    ///
    /// # Arguments
    /// * `table` - Delta Lake table containing the chunks
    /// * `bundle_id` - SHA256 hash identifying the file
    #[must_use]
    pub fn new(table: &'a DeltaTable, bundle_id: impl Into<String>) -> Self {
        Self {
            table,
            bundle_id: bundle_id.into(),
        }
    }

    /// Read the complete file, verifying checksums
    ///
    /// Reads all chunks in order, verifies CRC32 for each chunk,
    /// and verifies SHA256 for the complete file after reading.
    ///
    /// # Arguments
    /// * `writer` - Async writer to receive the file content
    ///
    /// # Errors
    /// Returns error if:
    /// - File not found
    /// - Chunks out of order
    /// - CRC32 checksum mismatch
    /// - SHA256 hash mismatch
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
        let df = ctx
            .sql(&format!(
                "SELECT chunk_id, chunk_crc32, chunk_data, total_size, total_sha256, chunk_count 
                 FROM remote_files 
                 WHERE bundle_id = '{}' 
                 ORDER BY chunk_id",
                self.bundle_id
            ))
            .await?;

        let mut stream = df.execute_stream().await?;

        let mut file_hasher = Sha256::new();
        let mut total_bytes_read = 0u64;
        let mut expected_chunk_id = 0i64;
        let mut expected_sha256: Option<String> = None;
        let mut expected_total_size: Option<i64> = None;

        while let Some(batch) = stream.try_next().await? {
            self.process_batch(
                &batch,
                &mut writer,
                &mut file_hasher,
                &mut total_bytes_read,
                &mut expected_chunk_id,
                &mut expected_sha256,
                &mut expected_total_size,
            )
            .await?;
        }

        // Verify we got at least some data
        if expected_chunk_id == 0 {
            return Err(RemoteError::FileNotFound(self.bundle_id.clone()));
        }

        // Verify total size
        if let Some(expected_size) = expected_total_size {
            if total_bytes_read != expected_size as u64 {
                return Err(RemoteError::FileIntegrityFailed {
                    bundle_id: self.bundle_id.clone(),
                    expected: format!("{} bytes", expected_size),
                    actual: format!("{} bytes", total_bytes_read),
                });
            }
        }

        // Verify SHA256
        let actual_sha256 = format!("{:x}", file_hasher.finalize());
        if let Some(expected) = expected_sha256 {
            if actual_sha256 != expected {
                return Err(RemoteError::FileIntegrityFailed {
                    bundle_id: self.bundle_id.clone(),
                    expected,
                    actual: actual_sha256,
                });
            }
        }

        writer.flush().await?;

        debug!(
            "Successfully read file {} ({} bytes, SHA256={})",
            self.bundle_id,
            total_bytes_read,
            &actual_sha256[..16]
        );

        Ok(())
    }

    /// Process a single record batch
    #[allow(clippy::too_many_arguments)]
    async fn process_batch<W: tokio::io::AsyncWrite + Unpin>(
        &self,
        batch: &RecordBatch,
        writer: &mut W,
        file_hasher: &mut Sha256,
        total_bytes_read: &mut u64,
        expected_chunk_id: &mut i64,
        expected_sha256: &mut Option<String>,
        expected_total_size: &mut Option<i64>,
    ) -> Result<()> {
        let chunk_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .ok_or_else(|| {
                RemoteError::TableOperation("Invalid column type for chunk_id".to_string())
            })?;

        let chunk_crc32s = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .ok_or_else(|| {
                RemoteError::TableOperation("Invalid column type for chunk_crc32".to_string())
            })?;

        let chunk_datas = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .ok_or_else(|| {
                RemoteError::TableOperation("Invalid column type for chunk_data".to_string())
            })?;

        let total_sizes = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .ok_or_else(|| {
                RemoteError::TableOperation("Invalid column type for total_size".to_string())
            })?;

        let total_sha256s = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| {
                RemoteError::TableOperation("Invalid column type for total_sha256".to_string())
            })?;

        for i in 0..batch.num_rows() {
            let chunk_id = chunk_ids.value(i);
            let expected_crc = chunk_crc32s.value(i) as u32; // Cast back to u32
            let chunk_data = chunk_datas.value(i);

            // Verify chunk sequence
            if chunk_id != *expected_chunk_id {
                return Err(RemoteError::InvalidChunkSequence {
                    expected: *expected_chunk_id,
                    actual: chunk_id,
                });
            }

            // Verify CRC32
            let actual_crc = crc32fast::hash(chunk_data);
            if actual_crc != expected_crc {
                return Err(RemoteError::ChunkIntegrityFailed {
                    bundle_id: self.bundle_id.clone(),
                    chunk_id,
                    expected: expected_crc as i64,
                    actual: actual_crc as i64,
                });
            }

            // Update file hash
            file_hasher.update(chunk_data);
            *total_bytes_read += chunk_data.len() as u64;

            // Write chunk to output
            writer.write_all(chunk_data).await?;

            // Store expected values from first chunk
            if expected_chunk_id == &0 {
                *expected_total_size = Some(total_sizes.value(i));
                *expected_sha256 = Some(total_sha256s.value(i).to_string());
            }

            *expected_chunk_id += 1;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FileType, RemoteTable};
    use std::io::Cursor;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_read_write_roundtrip() {
        let _ = env_logger::try_init();
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("remote");

        let mut table = RemoteTable::create(&table_path).await.unwrap();

        // Write a test file
        let original_data = vec![42u8; 5 * 1024 * 1024]; // 5MB
        let reader = Cursor::new(original_data.clone());

        let bundle_id = table
            .write_file(
                123,
                "test/roundtrip.dat",
                FileType::LargeFile,
                reader,
                vec!["test".to_string()],
            )
            .await
            .unwrap();

        // Read it back
        let mut output = Vec::new();
        table.read_file(&bundle_id, &mut output).await.unwrap();

        // Verify data matches
        assert_eq!(output, original_data);
    }

    #[tokio::test]
    async fn test_read_nonexistent_file() {
        let _ = env_logger::try_init();
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("remote");

        let table = RemoteTable::create(&table_path).await.unwrap();

        // Try to read a file that doesn't exist
        let mut output = Vec::new();
        let result = table.read_file("nonexistent_bundle_id", &mut output).await;

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

        let mut table = RemoteTable::create(&table_path).await.unwrap();

        // Write empty file
        let reader = Cursor::new(vec![]);
        let bundle_id = table
            .write_file(
                1,
                "test/empty.dat",
                FileType::LargeFile,
                reader,
                vec!["test".to_string()],
            )
            .await
            .unwrap();

        // Read it back
        let mut output = Vec::new();
        table.read_file(&bundle_id, &mut output).await.unwrap();

        assert_eq!(output.len(), 0);
    }

    #[tokio::test]
    async fn test_read_large_file() {
        let _ = env_logger::try_init();
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("remote");

        let mut table = RemoteTable::create(&table_path).await.unwrap();

        // Write a large file with multiple chunks
        let chunk_size = 1024 * 1024; // 1MB chunks
        let data_size = 3 * chunk_size + 512 * 1024; // 3.5MB
        let original_data: Vec<u8> = (0..data_size).map(|i| (i % 256) as u8).collect();

        let reader = Cursor::new(original_data.clone());
        let bundle_id = table
            .write_file(
                456,
                "test/large.dat",
                FileType::PondParquet,
                reader,
                vec!["backup".to_string()],
            )
            .await
            .unwrap();

        // Read it back
        let mut output = Vec::new();
        table.read_file(&bundle_id, &mut output).await.unwrap();

        // Verify data matches exactly
        assert_eq!(output, original_data);
    }
}
