// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Parquet integration for TinyFS
//!
//! This module provides:
//! - High-level ForArrow integration for writing/reading `Vec<T>`
//! - Low-level RecordBatch operations  
//! - Temporal bounds extraction using Arrow compute kernels
//! - Parquet metadata parsing for existing files (used by copy command)

use super::schema::ForArrow;
use crate::{EntryType, Result, WD};
use arrow_array::{Array, RecordBatch};
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::path::Path;
use tokio::io::AsyncWriteExt;
use tokio_util::bytes::Bytes;

// ============================================================================
// Temporal Bounds Extraction
// ============================================================================

/// Extract min/max temporal bounds from a RecordBatch column using Arrow compute kernels.
/// This is efficient - no serialization needed, just array aggregation.
pub fn extract_temporal_bounds_from_batch(
    batch: &RecordBatch,
    timestamp_column: &str,
) -> Result<(i64, i64)> {
    use arrow_array::types::{
        TimestampSecondType, TimestampMillisecondType, 
        TimestampMicrosecondType, TimestampNanosecondType,
    };
    
    // Find the timestamp column
    let col_idx = batch
        .schema()
        .index_of(timestamp_column)
        .map_err(|_| crate::Error::Other(format!(
            "Timestamp column '{}' not found in schema", timestamp_column
        )))?;
    
    let column = batch.column(col_idx);
    
    // Try to get as Int64 array (raw i64 timestamps)
    if let Some(int64_array) = column.as_any().downcast_ref::<arrow_array::Int64Array>() {
        let min = arrow::compute::min(int64_array)
            .ok_or_else(|| crate::Error::Other("No min value in timestamp column".into()))?;
        let max = arrow::compute::max(int64_array)
            .ok_or_else(|| crate::Error::Other("No max value in timestamp column".into()))?;
        return Ok((min, max));
    }
    
    // Try TimestampSecondArray
    if let Some(ts_array) = column.as_any().downcast_ref::<arrow_array::TimestampSecondArray>() {
        let min = arrow::compute::min::<TimestampSecondType>(ts_array)
            .ok_or_else(|| crate::Error::Other("No min value in timestamp column".into()))?;
        let max = arrow::compute::max::<TimestampSecondType>(ts_array)
            .ok_or_else(|| crate::Error::Other("No max value in timestamp column".into()))?;
        return Ok((min, max));
    }
    
    // Try TimestampMillisecondArray  
    if let Some(ts_array) = column.as_any().downcast_ref::<arrow_array::TimestampMillisecondArray>() {
        let min = arrow::compute::min::<TimestampMillisecondType>(ts_array)
            .ok_or_else(|| crate::Error::Other("No min value in timestamp column".into()))?;
        let max = arrow::compute::max::<TimestampMillisecondType>(ts_array)
            .ok_or_else(|| crate::Error::Other("No max value in timestamp column".into()))?;
        return Ok((min, max));
    }
    
    // Try TimestampMicrosecondArray
    if let Some(ts_array) = column.as_any().downcast_ref::<arrow_array::TimestampMicrosecondArray>() {
        let min = arrow::compute::min::<TimestampMicrosecondType>(ts_array)
            .ok_or_else(|| crate::Error::Other("No min value in timestamp column".into()))?;
        let max = arrow::compute::max::<TimestampMicrosecondType>(ts_array)
            .ok_or_else(|| crate::Error::Other("No max value in timestamp column".into()))?;
        return Ok((min, max));
    }
    
    // Try TimestampNanosecondArray
    if let Some(ts_array) = column.as_any().downcast_ref::<arrow_array::TimestampNanosecondArray>() {
        let min = arrow::compute::min::<TimestampNanosecondType>(ts_array)
            .ok_or_else(|| crate::Error::Other("No min value in timestamp column".into()))?;
        let max = arrow::compute::max::<TimestampNanosecondType>(ts_array)
            .ok_or_else(|| crate::Error::Other("No max value in timestamp column".into()))?;
        return Ok((min, max));
    }
    
    Err(crate::Error::Other(format!(
        "Timestamp column '{}' has unsupported type: {:?}",
        timestamp_column,
        column.data_type()
    )))
}

/// Extract temporal bounds from ParquetMetaData (high-level API).
/// Used when parsing existing parquet files (e.g., copy command).
pub fn extract_temporal_bounds_from_parquet_metadata(
    parquet_meta: &parquet::file::metadata::ParquetMetaData,
    schema: &arrow_schema::SchemaRef,
    ts_column: &str,
) -> Result<(i64, i64)> {
    use parquet::file::statistics::Statistics;

    let ts_col_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == ts_column)
        .ok_or_else(|| crate::Error::Other(format!(
            "Timestamp column '{}' not found in schema", ts_column
        )))?;

    let mut global_min: Option<i64> = None;
    let mut global_max: Option<i64> = None;

    for row_group in parquet_meta.row_groups() {
        if ts_col_idx >= row_group.columns().len() {
            continue;
        }

        let column_chunk = &row_group.columns()[ts_col_idx];
        if let Some(stats) = column_chunk.statistics() {
            if let Statistics::Int64(int64_stats) = stats {
                if let (Some(&min_val), Some(&max_val)) =
                    (int64_stats.min_opt(), int64_stats.max_opt())
                {
                    global_min = Some(global_min.map_or(min_val, |v| v.min(min_val)));
                    global_max = Some(global_max.map_or(max_val, |v| v.max(max_val)));
                }
            }
        }
    }

    match (global_min, global_max) {
        (Some(min), Some(max)) => Ok((min, max)),
        _ => Err(crate::Error::Other(format!(
            "No statistics found for timestamp column '{}'", ts_column
        ))),
    }
}

// ============================================================================
// Write Options (Anti-Duplication Pattern)
// ============================================================================

/// Options for writing parquet files
#[derive(Default, Clone)]
pub struct WriteOptions<'a> {
    /// For series files: the timestamp column name (defaults to "timestamp")
    pub timestamp_column: Option<&'a str>,
}

// ============================================================================
// Core Write/Read Functions
// ============================================================================

/// Serialize a RecordBatch to parquet bytes in memory
fn serialize_batch_to_parquet(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
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
    Ok(buffer)
}

/// Parse parquet bytes into concatenated RecordBatch
fn parse_parquet_to_batch(data: Vec<u8>) -> Result<RecordBatch> {
    let bytes = Bytes::from(data);
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| crate::Error::Other(format!("Parquet reader error: {}", e)))?
        .build()
        .map_err(|e| crate::Error::Other(format!("Build reader error: {}", e)))?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| crate::Error::Other(format!("Read batch error: {}", e)))?;
        batches.push(batch);
    }

    if batches.is_empty() {
        return Err(crate::Error::Other("No data in parquet file".to_string()));
    } else if batches.len() == 1 {
        Ok(batches.into_iter().next().expect("not empty"))
    } else {
        let schema = batches[0].schema();
        let batch_refs: Vec<&RecordBatch> = batches.iter().collect();
        arrow::compute::concat_batches(&schema, batch_refs)
            .map_err(|e| crate::Error::Other(format!("Concat batches error: {}", e)))
    }
}

// ============================================================================
// ParquetExt Trait - Clean API
// ============================================================================

#[async_trait::async_trait]
pub trait ParquetExt {
    /// Write items to a parquet file (high-level ForArrow API)
    async fn create_table_from_items<T, P>(
        &self,
        path: P,
        items: &[T],
        entry_type: EntryType,
    ) -> Result<()>
    where
        T: Serialize + ForArrow + Send + Sync,
        P: AsRef<Path> + Send + Sync;

    /// Read a parquet file as items (high-level ForArrow API)
    async fn read_table_as_items<T, P>(&self, path: P) -> Result<Vec<T>>
    where
        T: for<'a> Deserialize<'a> + ForArrow + Send + Sync,
        P: AsRef<Path> + Send + Sync;

    /// Write a RecordBatch to a parquet file
    async fn create_table_from_batch<P>(
        &self,
        path: P,
        batch: &RecordBatch,
        entry_type: EntryType,
    ) -> Result<()>
    where
        P: AsRef<Path> + Send + Sync;

    /// Read a parquet file as RecordBatch
    async fn read_table_as_batch<P>(&self, path: P) -> Result<RecordBatch>
    where
        P: AsRef<Path> + Send + Sync;

    /// Create a FileSeries from RecordBatch with automatic temporal extraction.
    /// Uses Arrow compute kernels to extract min/max from the timestamp column.
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
        items: &[T],
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
        items: &[T],
        entry_type: EntryType,
    ) -> Result<()>
    where
        T: Serialize + ForArrow + Send + Sync,
        P: AsRef<Path> + Send + Sync,
    {
        let fields = T::for_arrow();
        let batch = serde_arrow::to_record_batch(&fields, &items)
            .map_err(|e| crate::Error::Other(format!("Failed to serialize to arrow: {}", e)))?;
        self.create_table_from_batch(path, &batch, entry_type).await
    }

    async fn read_table_as_items<T, P>(&self, path: P) -> Result<Vec<T>>
    where
        T: for<'a> Deserialize<'a> + ForArrow + Send + Sync,
        P: AsRef<Path> + Send + Sync,
    {
        let batch = self.read_table_as_batch(path).await?;
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
        let buffer = serialize_batch_to_parquet(batch)?;

        let (_, mut writer) = self
            .create_file_path_streaming_with_type(&path, entry_type)
            .await?;
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

    async fn read_table_as_batch<P>(&self, path: P) -> Result<RecordBatch>
    where
        P: AsRef<Path> + Send + Sync,
    {
        let data = self.read_file_path_to_vec(&path).await?;
        parse_parquet_to_batch(data)
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
        let ts_col = timestamp_column.unwrap_or("timestamp");
        
        // Extract temporal bounds directly from the RecordBatch using Arrow kernels
        let (min_time, max_time) = extract_temporal_bounds_from_batch(batch, ts_col)?;
        
        // Serialize batch to parquet
        let buffer = serialize_batch_to_parquet(batch)?;
        
        // Write to TinyFS with temporal metadata
        let (_, mut writer) = self
            .create_file_path_streaming_with_type(&path, EntryType::FileSeriesPhysical)
            .await?;

        writer
            .write_all(&buffer)
            .await
            .map_err(|e| crate::Error::Other(format!("Write to TinyFS error: {}", e)))?;

        // Set temporal metadata from Arrow kernels (no parquet parsing needed!)
        writer.set_temporal_metadata(min_time, max_time, ts_col.to_string());

        writer
            .shutdown()
            .await
            .map_err(|e| crate::Error::Other(format!("Shutdown writer error: {}", e)))?;

        Ok((min_time, max_time))
    }

    async fn create_series_from_items<T, P>(
        &self,
        path: P,
        items: &[T],
        timestamp_column: Option<&str>,
    ) -> Result<(i64, i64)>
    where
        T: Serialize + ForArrow + Send + Sync,
        P: AsRef<Path> + Send + Sync,
    {
        let fields = T::for_arrow();
        let batch = serde_arrow::to_record_batch::<&[T]>(&fields, &items)
            .map_err(|e| crate::Error::Other(format!("Failed to serialize to arrow: {}", e)))?;
        self.create_series_from_batch(path, &batch, timestamp_column).await
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use crate::EntryType;
    use crate::arrow::{ForArrow, ParquetExt};
    use crate::arrow::parquet::extract_temporal_bounds_from_batch;
    use crate::memory::new_fs;
    use arrow::datatypes::{DataType, Field, FieldRef};
    use arrow_array::{record_batch, RecordBatch};
    use log::debug;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestRecord {
        id: i64,
        name: String,
        score: Option<f64>,
    }

    impl ForArrow for TestRecord {
        fn for_arrow() -> Vec<FieldRef> {
            vec![
                Arc::new(Field::new("id", DataType::Int64, false)),
                Arc::new(Field::new("name", DataType::Utf8, false)),
                Arc::new(Field::new("score", DataType::Float64, true)),
            ]
        }
    }

    #[tokio::test]
    async fn test_full_parquet_roundtrip_with_forarrow() -> TestResult {
        let fs = new_fs().await;
        let wd = fs.root().await?;

        let test_data = vec![
            TestRecord { id: 1, name: "Alice".to_string(), score: Some(95.5) },
            TestRecord { id: 2, name: "Bob".to_string(), score: None },
            TestRecord { id: 3, name: "Charlie".to_string(), score: Some(87.3) },
        ];

        let test_path = "test_records.parquet";
        wd.create_table_from_items(test_path, &test_data, EntryType::FileTablePhysical).await?;
        let read_data: Vec<TestRecord> = wd.read_table_as_items(test_path).await?;

        assert_eq!(test_data.len(), read_data.len());
        for (original, read) in test_data.iter().zip(read_data.iter()) {
            assert_eq!(original, read);
        }

        debug!("✅ Full ParquetExt ForArrow roundtrip successful!");
        Ok(())
    }

    #[tokio::test]
    async fn test_low_level_recordbatch_operations() -> Result<(), Box<dyn std::error::Error>> {
        let fs = new_fs().await;
        let wd = fs.root().await?;

        let batch = record_batch!(
            ("product", Utf8, ["Widget A", "Widget B", "Widget C"]),
            ("quantity", Int64, [100_i64, 250_i64, 75_i64]),
            ("price", Float64, [19.99, 15.50, 8.25])
        )?;

        let test_path = "products.parquet";
        wd.create_table_from_batch(test_path, &batch, EntryType::FileTablePhysical).await?;
        let read_batch = wd.read_table_as_batch(test_path).await?;

        assert_eq!(batch.schema(), read_batch.schema());
        assert_eq!(batch.num_rows(), read_batch.num_rows());
        assert_eq!(batch.num_columns(), read_batch.num_columns());

        debug!("✅ Low-level RecordBatch operations successful!");
        Ok(())
    }

    #[tokio::test]
    async fn test_extract_temporal_bounds_from_batch() -> Result<(), Box<dyn std::error::Error>> {
        use arrow_array::{Int64Array, Float64Array};
        use arrow_schema::{DataType, Field, Schema};
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![100_i64, 500_i64, 200_i64, 300_i64])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
            ],
        )?;

        let (min, max) = extract_temporal_bounds_from_batch(&batch, "timestamp")?;
        assert_eq!(min, 100);
        assert_eq!(max, 500);

        debug!("✅ Temporal bounds extraction using Arrow kernels works!");
        Ok(())
    }

    #[tokio::test]
    async fn test_large_dataset_batching() -> Result<(), Box<dyn std::error::Error>> {
        let fs = new_fs().await;
        let wd = fs.root().await?;

        let large_data: Vec<TestRecord> = (0..2500)
            .map(|i| TestRecord {
                id: i,
                name: format!("User_{}", i),
                score: if i % 3 == 0 { None } else { Some((i as f64) * 0.1) },
            })
            .collect();

        let test_path = "large_dataset.parquet";
        wd.create_table_from_items(test_path, &large_data, EntryType::FileTablePhysical).await?;
        let read_data: Vec<TestRecord> = wd.read_table_as_items(test_path).await?;

        assert_eq!(large_data.len(), read_data.len());
        assert_eq!(large_data[0], read_data[0]);
        assert_eq!(large_data[1000], read_data[1000]);
        assert_eq!(large_data[2499], read_data[2499]);

        debug!("✅ Large dataset batching successful!");
        Ok(())
    }

    #[tokio::test]
    async fn test_entry_type_integration() -> Result<(), Box<dyn std::error::Error>> {
        let fs = new_fs().await;
        let wd = fs.root().await?;

        let test_data = vec![
            TestRecord { id: 1, name: "Entry1".to_string(), score: Some(100.0) },
            TestRecord { id: 2, name: "Entry2".to_string(), score: Some(200.0) },
        ];

        // Test with FileTable entry type
        let table_path = "table_entries.parquet";
        wd.create_table_from_items(table_path, &test_data, EntryType::FileTablePhysical).await?;

        // Test with FileData entry type
        let data_path = "data_entries.parquet";
        wd.create_table_from_items(data_path, &test_data, EntryType::FileDataPhysical).await?;

        let table_data: Vec<TestRecord> = wd.read_table_as_items(table_path).await?;
        let data_data: Vec<TestRecord> = wd.read_table_as_items(data_path).await?;

        assert_eq!(test_data, table_data);
        assert_eq!(test_data, data_data);

        debug!("✅ Entry type integration successful!");
        Ok(())
    }
}
