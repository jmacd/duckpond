// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! AsyncFileReader implementation over chunked backup storage.
//!
//! Enables reading parquet files directly from a remote backup without
//! materializing them to disk. Used by the cross-pond import mechanism
//! to discover foreign pond directory structure and partition mappings.
//!
//! The reader reassembles chunks from the backup's Delta Lake table and
//! serves random byte-range reads as required by parquet's AsyncFileReader.

use bytes::Bytes;
use futures::FutureExt;
use log::debug;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::file::metadata::ParquetMetaData;
use std::ops::Range;
use std::sync::Arc;

/// A parquet AsyncFileReader that reads from chunked backup storage.
///
/// Reassembles a backed-up file from its chunks and caches the result
/// in memory. Serves random byte-range reads from the cached bytes.
///
/// This is suitable for reading OpLog parquet files from foreign backups
/// (typically a few hundred KB to a few MB each). For large files, a
/// true chunk-level random-access reader would be more memory-efficient.
pub struct ChunkedAsyncFileReader {
    /// Cached file content (reassembled from chunks)
    data: Arc<Bytes>,
}

impl ChunkedAsyncFileReader {
    /// Create a reader from pre-fetched file bytes.
    ///
    /// Use `from_remote` to fetch from a RemoteTable.
    pub fn new(data: Bytes) -> Self {
        Self {
            data: Arc::new(data),
        }
    }

    /// Fetch a file from remote backup and create a reader.
    ///
    /// Reads all chunks for the specified file, verifies BLAKE3 integrity,
    /// and caches the result for random-access reads.
    pub async fn from_remote(
        remote_table: &crate::RemoteTable,
        bundle_id: &str,
        path: &str,
        pond_txn_id: i64,
    ) -> crate::Result<Self> {
        let mut buffer = Vec::new();
        remote_table
            .read_file(bundle_id, path, pond_txn_id, &mut buffer)
            .await?;
        debug!(
            "ChunkedAsyncFileReader: loaded {} bytes for {}",
            buffer.len(),
            path
        );
        Ok(Self::new(Bytes::from(buffer)))
    }

    /// Get the total size of the file
    pub fn len(&self) -> u64 {
        self.data.len() as u64
    }

    /// Returns true if the file is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl AsyncFileReader for ChunkedAsyncFileReader {
    fn get_bytes(
        &mut self,
        range: Range<u64>,
    ) -> futures::future::BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let data = Arc::clone(&self.data);
        async move {
            let start = range.start as usize;
            let end = range.end as usize;
            if end > data.len() {
                return Err(parquet::errors::ParquetError::IndexOutOfBound(
                    end,
                    data.len(),
                ));
            }
            Ok(data.slice(start..end))
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> futures::future::BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let data = Arc::clone(&self.data);
        async move {
            // Parse parquet footer from the cached bytes using the footer reader
            let len = data.len() as u64;
            let mut reader = parquet::file::metadata::ParquetMetaDataReader::new();
            reader.try_parse_sized(&*data, len)?;
            let metadata = reader.finish()?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunked_reader_byte_ranges() {
        let data = Bytes::from(vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let mut reader = ChunkedAsyncFileReader::new(data);

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(reader.get_bytes(2..5)).unwrap();
        assert_eq!(&result[..], &[2, 3, 4]);
    }

    #[test]
    fn test_chunked_reader_full_range() {
        let data = Bytes::from(vec![10u8, 20, 30]);
        let mut reader = ChunkedAsyncFileReader::new(data);

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(reader.get_bytes(0..3)).unwrap();
        assert_eq!(&result[..], &[10, 20, 30]);
    }

    #[test]
    fn test_chunked_reader_out_of_bounds() {
        let data = Bytes::from(vec![1u8, 2, 3]);
        let mut reader = ChunkedAsyncFileReader::new(data);

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(reader.get_bytes(0..10));
        assert!(result.is_err());
    }
}
