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
        timestamp_column: Option<&str>,
    ) -> Result<(i64, i64)>
    where
        P: AsRef<Path> + Send + Sync,
    {
        // Determine timestamp column
        let time_col = match timestamp_column {
            Some(col) => col.to_string(),
            None => {
                // Auto-detect timestamp column using our utility function
                // We need to access the TLogFS schema functions, so we'll import them
                use crate::Error;
                
                // Try common timestamp column names
                let candidates = ["timestamp", "Timestamp", "event_time", "time", "ts", "datetime"];
                let mut found_col = None;
                
                for candidate in candidates {
                    if batch.schema().column_with_name(candidate).is_some() {
                        found_col = Some(candidate.to_string());
                        break;
                    }
                }
                
                found_col.ok_or_else(|| Error::Other("No timestamp column found in batch".to_string()))?
            }
        };

        // Extract temporal range from the batch
        // We need to access the TLogFS temporal extraction function
        // For now, let's implement a basic version here until we can properly import it
        let time_array = batch
            .column_by_name(&time_col)
            .ok_or_else(|| crate::Error::Other(format!("Time column '{}' not found", time_col)))?;

        // Handle different timestamp types (simplified version)
        use arrow::array::Array;
        use arrow::datatypes::{DataType, TimeUnit};
        
        let (min_time, max_time) = match time_array.data_type() {
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                let array = time_array.as_any().downcast_ref::<arrow::array::TimestampMillisecondArray>()
                    .ok_or_else(|| crate::Error::Other("Failed to downcast timestamp array".to_string()))?;
                let min = array.iter().flatten().min().unwrap_or(0);
                let max = array.iter().flatten().max().unwrap_or(0);
                (min, max)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let array = time_array.as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                    .ok_or_else(|| crate::Error::Other("Failed to downcast timestamp array".to_string()))?;
                let min = array.iter().flatten().min().unwrap_or(0);
                let max = array.iter().flatten().max().unwrap_or(0);
                (min, max)
            }
            DataType::Int64 => {
                let array = time_array.as_any().downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| crate::Error::Other("Failed to downcast int64 array".to_string()))?;
                let min = array.iter().flatten().min().unwrap_or(0);
                let max = array.iter().flatten().max().unwrap_or(0);
                (min, max)
            }
            _ => return Err(crate::Error::Other(format!("Unsupported timestamp type: {:?}", time_array.data_type())))
        };

        // Write the batch as FileSeries
        self.create_table_from_batch(path, batch, EntryType::FileSeries).await?;

        Ok((min_time, max_time))
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
