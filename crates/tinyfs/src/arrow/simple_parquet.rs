//! Simple Parquet integration test for TinyFS
//!
//! This is a basic implementation to verify Arrow/Parquet functionality works.

use crate::{EntryType, Result, WD};
use arrow_array::RecordBatch;
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use parquet::file::properties::WriterProperties;
use std::io::Cursor;
use std::path::Path;
use tokio_util::bytes::Bytes;

/// Simple extension trait for basic Parquet operations on WD
/// This provides synchronous-style API for basic testing and simple use cases
#[async_trait::async_trait]
pub trait SimpleParquetExt {
    /// Write a RecordBatch to a Parquet file
    async fn write_parquet<P: AsRef<Path> + Send + Sync>(
        &self,
        path: P,
        batch: &RecordBatch,
        entry_type: EntryType,
    ) -> Result<()>;

    /// Read a Parquet file as a single RecordBatch
    async fn read_parquet<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<RecordBatch>;
}

#[async_trait::async_trait]
impl SimpleParquetExt for WD {
    async fn write_parquet<P: AsRef<Path> + Send + Sync>(
        &self,
        path: P,
        batch: &RecordBatch,
        entry_type: EntryType,
    ) -> Result<()> {
        // Create an in-memory buffer first
        let mut buffer = Vec::new();

        // Write to the buffer using sync parquet writer
        {
            let cursor = Cursor::new(&mut buffer);
            let props = WriterProperties::builder().build();
            let mut writer = ArrowWriter::try_new(cursor, batch.schema(), Some(props))
                .map_err(|e| crate::Error::Other(format!("Arrow writer error: {}", e)))?;

            writer
                .write(batch)
                .map_err(|e| crate::Error::Other(format!("Write batch error: {}", e)))?;

            let _ = writer
                .close()
                .map_err(|e| crate::Error::Other(format!("Close writer error: {}", e)))?;
        }

        // Now write the buffer to TinyFS using create_file_path_streaming_with_type
        let (_, mut writer) = self
            .create_file_path_streaming_with_type(&path, entry_type)
            .await?;
        use tokio::io::AsyncWriteExt;
        writer
            .write_all(&buffer)
            .await
            .map_err(|e| crate::Error::Other(format!("Write to TinyFS error: {}", e)))?;
        writer
            .shutdown()
            .await
            .map_err(|e| crate::Error::Other(format!("Shutdown writer error: {}", e)))?;

        Ok(())
    }

    async fn read_parquet<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<RecordBatch> {
        // Read the entire file into memory first
        let data = self.read_file_path_to_vec(&path).await?;

        // Convert to Bytes for ChunkReader
        let bytes = Bytes::from(data);
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| crate::Error::Other(format!("Parquet reader error: {}", e)))?
            .build()
            .map_err(|e| crate::Error::Other(format!("Build reader error: {}", e)))?;

        // Read all batches and combine them
        let mut batches = Vec::new();
        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| crate::Error::Other(format!("Read batch error: {}", e)))?;
            batches.push(batch);
        }

        if batches.is_empty() {
            return Err(crate::Error::Other("Empty Parquet file".to_string()));
        }

        // If only one batch, return it directly
        if batches.len() == 1 {
            Ok(batches.into_iter().next().expect("not empty"))
        } else {
            // Concatenate multiple batches
            let schema = batches[0].schema();
            let batch_refs: Vec<&RecordBatch> = batches.iter().collect();
            arrow::compute::concat_batches(&schema, batch_refs)
                .map_err(|e| crate::Error::Other(format!("Concat batches error: {}", e)))
        }
    }
}
