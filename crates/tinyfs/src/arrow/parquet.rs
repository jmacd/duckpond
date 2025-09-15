//! Full Parquet integration for TinyFS following the original pond pattern
//!
//! This module provides high-level ForArrow integration for reading/writing
//! Vec<T> where T: Serialize + Deserialize + ForArrow.

use std::path::Path;
use std::io::Cursor;
use arrow_array::RecordBatch;
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use parquet::file::properties::WriterProperties;
use serde::{Serialize, Deserialize};
use tokio_util::bytes::Bytes;
use tokio::io::AsyncWriteExt;
use crate::{Result, EntryType, WD};
use super::schema::ForArrow;

/// Default batch size for processing large datasets
pub const DEFAULT_BATCH_SIZE: usize = 1000;

#[async_trait::async_trait]
pub trait ParquetExt {
    /// Write a Vec<T> to a Parquet file, where T implements ForArrow
    async fn create_table_from_items<T, P>(
        &self,
        path: P,
        items: &Vec<T>,
        entry_type: EntryType,
    ) -> Result<()>
    where
        T: Serialize + ForArrow + Send + Sync,
        P: AsRef<Path> + Send + Sync;

    /// Read a Parquet file as Vec<T>, where T implements ForArrow
    async fn read_table_as_items<T, P>(&self, path: P) -> Result<Vec<T>>
    where
        T: for<'a> Deserialize<'a> + ForArrow + Send + Sync,
        P: AsRef<Path> + Send + Sync;

    /// Low-level: Write a RecordBatch directly
    async fn create_table_from_batch<P>(
        &self,
        path: P,
        batch: &RecordBatch,
        entry_type: EntryType,
    ) -> Result<()>
    where
        P: AsRef<Path> + Send + Sync;

    /// Low-level: Write a RecordBatch directly with temporal metadata (for FileSeries)
    async fn create_table_from_batch_with_metadata<P>(
        &self,
        path: P,
        batch: &RecordBatch,
        entry_type: EntryType,
        min_event_time: Option<i64>,
        max_event_time: Option<i64>,
        timestamp_column: Option<&str>,
    ) -> Result<()>
    where
        P: AsRef<Path> + Send + Sync;

    /// Low-level: Read a RecordBatch directly
    async fn read_table_as_batch<P>(&self, path: P) -> Result<RecordBatch>
    where
        P: AsRef<Path> + Send + Sync;

    /// Create a FileSeries from RecordBatch with temporal metadata extraction
    /// This method extracts min/max timestamps from the specified time column
    async fn create_series_from_batch<P>(
        &self,
        path: P,
        batch: &RecordBatch,
        timestamp_column: Option<&str>,
    ) -> Result<(i64, i64)>
    where
        P: AsRef<Path> + Send + Sync;

    /// Create a FileSeries from items with automatic temporal extraction
    async fn create_series_from_items<T, P>(
        &self,
        path: P,
        items: &Vec<T>,
        timestamp_column: Option<&str>,
    ) -> Result<(i64, i64)>
    where
        T: Serialize + ForArrow + Send + Sync,
        P: AsRef<Path> + Send + Sync;
}

#[async_trait::async_trait]
impl ParquetExt for WD {
    async fn create_table_from_items<T, P>(
        &self,
        path: P,
        items: &Vec<T>,
        entry_type: EntryType,
    ) -> Result<()>
    where
        T: Serialize + ForArrow + Send + Sync,
        P: AsRef<Path> + Send + Sync,
    {
        // Convert Vec<T> to RecordBatch using serde_arrow
        let fields = T::for_arrow();
        let batch = serde_arrow::to_record_batch(&fields, items)
            .map_err(|e| crate::Error::Other(format!("Failed to serialize to arrow: {}", e)))?;

        // Write the batch using the low-level method
        self.create_table_from_batch(path, &batch, entry_type).await
    }

    async fn read_table_as_items<T, P>(&self, path: P) -> Result<Vec<T>>
    where
        T: for<'a> Deserialize<'a> + ForArrow + Send + Sync,
        P: AsRef<Path> + Send + Sync,
    {
        // Read as RecordBatch first
        let batch = self.read_table_as_batch(path).await?;

        // Convert RecordBatch to Vec<T> using serde_arrow
        let items = serde_arrow::from_record_batch(&batch)
            .map_err(|e| crate::Error::Other(format!("Failed to deserialize from arrow: {}", e)))?;

        Ok(items)
    }

    async fn create_table_from_batch<P>(
        &self,
        path: P,
        batch: &RecordBatch,
        entry_type: EntryType,
    ) -> Result<()>
    where
        P: AsRef<Path> + Send + Sync,
    {
        // Create an in-memory buffer first
        let mut buffer = Vec::new();
        
        // Write to the buffer using sync parquet writer
        {
            let cursor = Cursor::new(&mut buffer);
            let props = WriterProperties::builder().build();
            let mut writer = ArrowWriter::try_new(cursor, batch.schema(), Some(props))
                .map_err(|e| crate::Error::Other(format!("Arrow writer error: {}", e)))?;
                
            writer.write(batch)
                .map_err(|e| crate::Error::Other(format!("Write batch error: {}", e)))?;
                
            writer.close()
                .map_err(|e| crate::Error::Other(format!("Close writer error: {}", e)))?;
        }
        
        // Now write the buffer to TinyFS
        let (_, mut writer) = self.create_file_path_streaming_with_type(&path, entry_type).await?;
        writer.write_all(&buffer).await
            .map_err(|e| crate::Error::Other(format!("Write to TinyFS error: {}", e)))?;
        writer.shutdown().await
            .map_err(|e| crate::Error::Other(format!("Shutdown writer error: {}", e)))?;
        
        Ok(())
    }

    async fn create_table_from_batch_with_metadata<P>(
        &self,
        path: P,
        batch: &RecordBatch,
        entry_type: EntryType,
        _min_event_time: Option<i64>,
        _max_event_time: Option<i64>,
        _timestamp_column: Option<&str>,
    ) -> Result<()>
    where
        P: AsRef<Path> + Send + Sync,
    {
        // Create an in-memory buffer first
        let mut buffer = Vec::new();
        
        // Write to the buffer using sync parquet writer
        {
            let cursor = Cursor::new(&mut buffer);
            let props = WriterProperties::builder().build();
            let mut writer = ArrowWriter::try_new(cursor, batch.schema(), Some(props))
                .map_err(|e| crate::Error::Other(format!("Arrow writer error: {}", e)))?;
                
            writer.write(batch)
                .map_err(|e| crate::Error::Other(format!("Write batch error: {}", e)))?;
                
            writer.close()
                .map_err(|e| crate::Error::Other(format!("Close writer error: {}", e)))?;
        }
        
        // SIMPLIFIED: Use unified streaming write for all entry types
        // TLogFS will handle temporal metadata extraction automatically for FileSeries
        let (_, mut writer) = self.create_file_path_streaming_with_type(&path, entry_type).await?;
        writer.write_all(&buffer).await
            .map_err(|e| crate::Error::Other(format!("Write to TinyFS error: {}", e)))?;
        writer.shutdown().await
            .map_err(|e| crate::Error::Other(format!("Shutdown writer error: {}", e)))?;
        
        Ok(())
    }

    async fn read_table_as_batch<P>(&self, path: P) -> Result<RecordBatch>
    where
        P: AsRef<Path> + Send + Sync,
    {
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
        
        // Return single batch or concatenate multiple
        if batches.is_empty() {
            return Err(crate::Error::Other("No data in parquet file".to_string()));
        } else if batches.len() == 1 {
            Ok(batches.into_iter().next().unwrap())
        } else {
            // Concatenate multiple batches
            let schema = batches[0].schema();
            let batch_refs: Vec<&RecordBatch> = batches.iter().collect();
            arrow::compute::concat_batches(&schema, batch_refs)
                .map_err(|e| crate::Error::Other(format!("Concat batches error: {}", e)))
        }
    }

    async fn create_series_from_batch<P>(
        &self,
        path: P,
        batch: &RecordBatch,
        _timestamp_column: Option<&str>,
    ) -> Result<(i64, i64)>
    where
        P: AsRef<Path> + Send + Sync,
    {
        // SIMPLIFIED: Use unified streaming write, let TLogFS handle temporal metadata extraction
        // Write the FileSeries using streaming approach
        self.create_table_from_batch_with_metadata(&path, batch, EntryType::FileSeries, None, None, None).await?;

        // Return placeholder values since TLogFS now handles temporal metadata extraction
        // In the future, we could query TLogFS for the extracted metadata if needed
        Ok((0, 0))
    }

    async fn create_series_from_items<T, P>(
        &self,
        path: P,
        items: &Vec<T>,
        timestamp_column: Option<&str>,
    ) -> Result<(i64, i64)>
    where
        T: Serialize + ForArrow + Send + Sync,
        P: AsRef<Path> + Send + Sync,
    {
        // Convert Vec<T> to RecordBatch using serde_arrow
        let fields = T::for_arrow();
        let batch = serde_arrow::to_record_batch(&fields, items)
            .map_err(|e| crate::Error::Other(format!("Failed to serialize to arrow: {}", e)))?;

        // Use the batch method
        self.create_series_from_batch(path, &batch, timestamp_column).await
    }
}
