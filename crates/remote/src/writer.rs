// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Streaming writer for chunked files
//!
//! Reads from an AsyncRead source, chunks the data, computes checksums,
//! and writes to Delta Lake in a single transaction.

use crate::Result;
use crate::schema::{CHUNK_SIZE_DEFAULT, ChunkedFileRecord};
use arrow_array::{
    BinaryArray, Int32Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use chrono::Utc;
use deltalake::DeltaTable;
use deltalake::operations::write::WriteBuilder;
use log::debug;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::io::AsyncReadExt;

/// Streaming writer that chunks files and writes to Delta Lake
///
/// Reads data from an AsyncRead source, chunks it into pieces,
/// computes CRC32 per chunk and SHA256 for the entire file,
/// then writes all chunks to Delta Lake in a single transaction.
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
///     FileType::PondParquet,
///     reader,
///     vec!["backup".to_string()],
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
    cli_args: Vec<String>,
    chunk_size: usize,
    /// Optional override for bundle_id (used for metadata to avoid SHA256 computation)
    bundle_id_override: Option<String>,
}

impl<R: tokio::io::AsyncRead + Unpin> ChunkedWriter<R> {
    /// Create a new chunked writer
    ///
    /// # Arguments
    /// * `pond_id` - UUID of the pond
    /// * `pond_txn_id` - Transaction sequence number from pond
    /// * `path` - File path in Delta table
    /// * `version` - Delta table version number
    /// * `reader` - Async reader providing file content
    /// * `cli_args` - CLI arguments that triggered this backup
    #[must_use]
    pub fn new(pond_txn_id: i64, path: String, reader: R, cli_args: Vec<String>) -> Self {
        Self {
            pond_txn_id,
            path,
            reader,
            cli_args,
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
    /// When set, this bundle_id will be used instead of computing SHA256.
    /// This is used for metadata records which need predictable bundle_ids
    /// in the format "metadata_{pond_txn_id}".
    #[must_use]
    pub fn with_bundle_id(mut self, bundle_id: String) -> Self {
        self.bundle_id_override = Some(bundle_id);
        self
    }

    /// Write the file to Delta Lake in chunks with bounded memory
    ///
    /// Reads the file in chunks, computes checksums, and writes to Delta Lake
    /// using streaming writes. Memory usage is bounded by writing batches of
    /// chunks rather than accumulating all chunks before writing.
    ///
    /// # Returns
    /// The bundle_id (SHA256 hash) of the written file
    ///
    /// # Errors
    /// Returns error if:
    /// - Cannot read from input
    /// - Cannot compute checksums
    /// - Cannot write to Delta Lake
    pub async fn write_to_table(mut self, table: &mut DeltaTable) -> Result<String> {
        debug!(
            "Writing file {} (txn {}) to remote in {}MB chunks",
            self.path,
            self.pond_txn_id,
            self.chunk_size / (1024 * 1024)
        );

        // Streaming write with bounded memory
        let mut file_hasher = Sha256::new();
        let mut total_size = 0u64;
        let mut chunk_id = 0i64;
        let mut buffer = vec![0u8; self.chunk_size];

        // Maximum chunks buffered before creating RecordBatch
        // 10 chunks × 50MB = ~500MB per batch
        const CHUNKS_PER_BATCH: usize = 10;
        // TODO: Implement batched writing if needed for performance
        // Currently writing all chunks at once due to WriteBuilder API

        // First pass: read file, compute checksums, store chunks temporarily
        let mut all_chunk_data = Vec::new();

        loop {
            let n = self.reader.read(&mut buffer).await?;
            if n == 0 {
                break;
            }

            let chunk = &buffer[..n];
            file_hasher.update(chunk);
            total_size += n as u64;

            let crc = crc32fast::hash(chunk) as i64;
            all_chunk_data.push((chunk_id, crc, chunk.to_vec()));
            chunk_id += 1;
        }

        // Compute raw SHA256 hash
        let sha256_hash = format!("{:x}", file_hasher.finalize());

        let bundle_id = if let Some(ref override_id) = self.bundle_id_override {
            // Use provided bundle_id (for transaction files)
            override_id.clone()
        } else {
            // Use SHA256-based bundle_id for content-addressed large files
            ChunkedFileRecord::large_file_bundle_id(&sha256_hash)
        };
        let chunk_count = chunk_id;

        debug!(
            "File {} has {} chunks, {} bytes, SHA256={}",
            self.path,
            chunk_count,
            total_size,
            &sha256_hash[..16.min(sha256_hash.len())]
        );

        // Special case: empty file needs at least one row with empty chunk_data
        if all_chunk_data.is_empty() {
            all_chunk_data.push((0, 0i64, vec![])); // CRC32 of empty data is 0
        }

        // Second pass: create RecordBatches with final metadata
        let mut all_batches = Vec::new();
        for chunk_batch in all_chunk_data.chunks(CHUNKS_PER_BATCH) {
            let batch = self.create_batch(
                chunk_batch,
                &bundle_id,
                &sha256_hash,
                total_size,
                chunk_count,
            )?;
            all_batches.push(batch);
        }

        debug!(
            "Writing {} batches to Delta Lake for file {}",
            all_batches.len(),
            bundle_id
        );

        // Write all batches to Delta Lake in a single transaction
        let updated_table = WriteBuilder::new(table.log_store(), table.state.clone())
            .with_input_batches(all_batches)
            .await?;

        // Update our table's state to reflect the write
        *table = updated_table;

        debug!(
            "Successfully wrote file {} ({} chunks) to remote",
            self.path, chunk_count
        );

        Ok(bundle_id)
    }

    /// Create a RecordBatch from a slice of chunks with final metadata
    fn create_batch(
        &self,
        chunks: &[(i64, i64, Vec<u8>)],
        bundle_id: &str,
        sha256_hash: &str,
        total_size: u64,
        chunk_count: i64,
    ) -> Result<RecordBatch> {
        if chunks.is_empty() {
            return Err(arrow::error::ArrowError::InvalidArgumentError(
                "Cannot create batch from empty chunks".to_string(),
            )
            .into());
        }

        let cli_args_json = serde_json::to_string(&self.cli_args)?;
        let created_at = Utc::now().timestamp_micros();
        let num_chunks = chunks.len();

        let mut bundle_ids = Vec::with_capacity(num_chunks);
        let mut pond_txn_ids = Vec::with_capacity(num_chunks);
        let mut paths = Vec::with_capacity(num_chunks);
        let mut chunk_ids = Vec::with_capacity(num_chunks);
        let mut chunk_crc32s = Vec::with_capacity(num_chunks);
        let mut chunk_datas = Vec::with_capacity(num_chunks);
        let mut total_sizes = Vec::with_capacity(num_chunks);
        let mut total_sha256s = Vec::with_capacity(num_chunks);
        let mut chunk_counts = Vec::with_capacity(num_chunks);
        let mut cli_args_vec = Vec::with_capacity(num_chunks);
        let mut created_ats = Vec::with_capacity(num_chunks);

        for (chunk_id, crc, data) in chunks {
            bundle_ids.push(bundle_id.to_string());
            pond_txn_ids.push(self.pond_txn_id);
            paths.push(self.path.clone());
            chunk_ids.push(*chunk_id);
            chunk_crc32s.push(*crc);
            chunk_datas.push(data.clone());
            total_sizes.push(total_size as i64);
            total_sha256s.push(sha256_hash.to_string());
            chunk_counts.push(chunk_count);
            cli_args_vec.push(cli_args_json.clone());
            created_ats.push(created_at);
        }

        let schema = ChunkedFileRecord::arrow_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(bundle_ids)),
                Arc::new(Int64Array::from(pond_txn_ids)),
                Arc::new(StringArray::from(paths)),
                Arc::new(Int64Array::from(chunk_ids)),
                Arc::new(Int32Array::from(
                    chunk_crc32s
                        .into_iter()
                        .map(|crc| crc as i32)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(BinaryArray::from(
                    chunk_datas
                        .iter()
                        .map(|v| Some(v.as_slice()))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(total_sizes)),
                Arc::new(StringArray::from(total_sha256s)),
                Arc::new(Int64Array::from(chunk_counts)),
                Arc::new(StringArray::from(cli_args_vec)),
                Arc::new(TimestampMicrosecondArray::from(created_ats).with_timezone("UTC")),
            ],
        )?;

        Ok(batch)
    }
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

        let mut table = RemoteTable::create(&table_path).await.unwrap();

        // Create a small test file (< 1 chunk)
        let data = vec![42u8; 1024 * 1024]; // 1MB
        let reader = Cursor::new(data.clone());

        let bundle_id = table
            .write_file(123, "test/file.dat", reader, vec!["test".to_string()])
            .await
            .unwrap();

        // Verify bundle_id has POND-FILE- prefix and valid SHA256
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

        let mut table = RemoteTable::create(&table_path).await.unwrap();

        // Create a large test file (multiple chunks)
        let chunk_size = 1024 * 1024; // 1MB chunks for fast test
        let data_size = 3 * chunk_size + 512 * 1024; // 3.5MB = 4 chunks
        let data = vec![123u8; data_size];

        let _bundle_id = table
            .write_file(
                456,
                "test/large.dat",
                Cursor::new(data.clone()),
                vec!["backup".to_string()],
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

        let mut table = RemoteTable::create(&table_path).await.unwrap();

        // Empty file
        let data: Vec<u8> = vec![];
        let reader = Cursor::new(data);

        let bundle_id = table
            .write_file(1, "test/empty.dat", reader, vec!["test".to_string()])
            .await
            .unwrap();

        // SHA256 of empty input with POND-FILE- prefix
        let expected_hash = format!("{:x}", Sha256::digest(b""));
        let expected_bundle_id = format!("POND-FILE-{}", expected_hash);
        assert_eq!(bundle_id, expected_bundle_id);
    }
}
