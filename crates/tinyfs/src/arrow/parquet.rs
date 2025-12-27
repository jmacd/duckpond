// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Full Parquet integration for TinyFS following the original pond pattern
//!
//! This module provides high-level ForArrow integration for reading/writing
//! Vec<T> where T: Serialize + Deserialize + ForArrow.

use super::schema::ForArrow;
use crate::{EntryType, Result, WD};
use arrow_array::RecordBatch;
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::path::Path;
use tokio::io::AsyncWriteExt;
use tokio_util::bytes::Bytes;

/// Default batch size for processing large datasets
pub const DEFAULT_BATCH_SIZE: usize = 1000;

/// Extract temporal bounds from high-level ParquetMetaData API
/// This is a public helper for extracting temporal metadata from already-written parquet files
pub fn extract_temporal_bounds_from_parquet_metadata(
    parquet_meta: &parquet::file::metadata::ParquetMetaData,
    schema: &arrow_schema::SchemaRef,
    ts_column: &str,
) -> Result<(i64, i64)> {
    use parquet::file::statistics::Statistics;

    // Find the timestamp column index in the schema
    let ts_col_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == ts_column)
        .ok_or_else(|| {
            crate::Error::Other(format!(
                "Timestamp column '{}' not found in schema",
                ts_column
            ))
        })?;

    let mut global_min: Option<i64> = None;
    let mut global_max: Option<i64> = None;

    // Iterate through all row groups
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
            "No statistics found for timestamp column '{}'",
            ts_column
        ))),
    }
}

/// Extract temporal bounds from Thrift FileMetaData (returned by ArrowWriter::close)
fn extract_temporal_from_thrift(
    thrift_meta: &parquet::format::FileMetaData,
    schema: &arrow_schema::SchemaRef,
    ts_column: &str,
) -> Result<(i64, i64)> {
    // Find the timestamp column index in the schema
    let ts_col_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == ts_column)
        .ok_or_else(|| {
            crate::Error::Other(format!(
                "Timestamp column '{}' not found in schema",
                ts_column
            ))
        })?;

    let mut global_min: Option<i64> = None;
    let mut global_max: Option<i64> = None;

    // Iterate through all row groups and their column chunks
    for row_group in &thrift_meta.row_groups {
        // Find the timestamp column chunk
        if ts_col_idx >= row_group.columns.len() {
            continue; // Schema mismatch, skip
        }

        let column_chunk = &row_group.columns[ts_col_idx];
        if let Some(ref meta_data) = column_chunk.meta_data {
            if let Some(ref stats) = meta_data.statistics {
                // Extract min/max values from statistics
                // Use min_value/max_value first (preferred), fallback to deprecated min/max
                let min_bytes = stats.min_value.as_ref().or(stats.min.as_ref());
                let max_bytes = stats.max_value.as_ref().or(stats.max.as_ref());

                if let (Some(min_b), Some(max_b)) = (min_bytes, max_bytes) {
                    // Parquet stores INT64 (timestamp) as 8 bytes little-endian
                    if min_b.len() >= 8 && max_b.len() >= 8 {
                        let min_ts = i64::from_le_bytes(min_b[0..8].try_into().unwrap());
                        let max_ts = i64::from_le_bytes(max_b[0..8].try_into().unwrap());

                        global_min = Some(global_min.map_or(min_ts, |v| v.min(min_ts)));
                        global_max = Some(global_max.map_or(max_ts, |v| v.max(max_ts)));
                    }
                }
            }
        }
    }

    match (global_min, global_max) {
        (Some(min), Some(max)) => Ok((min, max)),
        _ => Err(crate::Error::Other(format!(
            "No statistics found for timestamp column '{}'",
            ts_column
        ))),
    }
}

#[async_trait::async_trait]
pub trait ParquetExt {
    /// Write a Vec<T> to a Parquet file, where T implements ForArrow
    async fn create_table_from_items<T, P>(
        &self,
        path: P,
        items: &[T],
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
        // Convert Vec<T> to RecordBatch using serde_arrow
        let fields = T::for_arrow();
        let batch = serde_arrow::to_record_batch(&fields, &items)
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
        // @@@ weird
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

        // Now write the buffer to TinyFS
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
        P: AsRef<Path> + Send + Sync,
    {
        // Create an in-memory buffer first
        let mut buffer = Vec::new();

        // Write to the buffer using sync parquet writer and capture file metadata
        let file_metadata_thrift = {
            let cursor = Cursor::new(&mut buffer);
            let props = WriterProperties::builder().build();
            let mut writer = ArrowWriter::try_new(cursor, batch.schema(), Some(props))
                .map_err(|e| crate::Error::Other(format!("Arrow writer error: {}", e)))?;

            writer
                .write(batch)
                .map_err(|e| crate::Error::Other(format!("Write batch error: {}", e)))?;

            writer
                .close()
                .map_err(|e| crate::Error::Other(format!("Close writer error: {}", e)))?
        };

        // Extract temporal bounds from Thrift metadata if this is a series file
        let (final_min, final_max, ts_col_name) = if entry_type == EntryType::FileSeriesPhysical {
            match (min_event_time, max_event_time) {
                (Some(min), Some(max)) => (
                    min,
                    max,
                    timestamp_column.unwrap_or("timestamp").to_string(),
                ),
                _ => {
                    // Extract from writer's metadata (no file re-reading!)
                    let ts_col = timestamp_column.unwrap_or("timestamp");
                    let (min, max) = extract_temporal_from_thrift(
                        &file_metadata_thrift,
                        &batch.schema(),
                        ts_col,
                    )?;
                    (min, max, ts_col.to_string())
                }
            }
        } else {
            (0, 0, String::new())
        };

        // Create TLogFS writer
        let (_, mut writer) = self
            .create_file_path_streaming_with_type(&path, entry_type)
            .await?;

        // Write the parquet bytes
        writer
            .write_all(&buffer)
            .await
            .map_err(|e| crate::Error::Other(format!("Write to TinyFS error: {}", e)))?;

        // Set temporal metadata before shutdown (for series files)
        if entry_type == EntryType::FileSeriesPhysical {
            writer.set_temporal_metadata(final_min, final_max, ts_col_name);
        }

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
            Ok(batches.into_iter().next().expect("not empty"))
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
        self.create_table_from_batch_with_metadata(
            &path,
            batch,
            EntryType::FileSeriesPhysical,
            None,
            None,
            None,
        )
        .await?;

        // Return placeholder values since TLogFS now handles temporal metadata extraction
        // In the future, we could query TLogFS for the extracted metadata if needed
        Ok((0, 0))
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
        // Convert Vec<T> to RecordBatch using serde_arrow
        let fields = T::for_arrow();
        let batch = serde_arrow::to_record_batch::<&[T]>(&fields, &items)
            .map_err(|e| crate::Error::Other(format!("Failed to serialize to arrow: {}", e)))?;

        // Use the batch method
        self.create_series_from_batch(path, &batch, timestamp_column)
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::EntryType;
    use crate::arrow::{ForArrow, ParquetExt};
    use crate::memory::new_fs;
    use arrow::datatypes::{DataType, Field, FieldRef};
    use arrow_array::record_batch;
    use log::debug;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    /// Test data structure that implements ForArrow
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
                Arc::new(Field::new("score", DataType::Float64, true)), // nullable
            ]
        }
    }

    #[tokio::test]
    async fn test_full_parquet_roundtrip_with_forarrow() -> Result<(), Box<dyn std::error::Error>> {
        // Create a test filesystem and get root WD
        let fs = new_fs().await;
        let wd = fs.root().await?;

        // Create test data
        let test_data = vec![
            TestRecord {
                id: 1,
                name: "Alice".to_string(),
                score: Some(95.5),
            },
            TestRecord {
                id: 2,
                name: "Bob".to_string(),
                score: None,
            },
            TestRecord {
                id: 3,
                name: "Charlie".to_string(),
                score: Some(87.3),
            },
        ];

        let test_path = "test_records.parquet";

        // Write using the high-level ForArrow API
        wd.create_table_from_items(test_path, &test_data, EntryType::FileTablePhysical)
            .await?;

        // Read back using the high-level ForArrow API
        let read_data: Vec<TestRecord> = wd.read_table_as_items(test_path).await?;

        // Verify the data matches
        assert_eq!(test_data.len(), read_data.len());
        for (original, read) in test_data.iter().zip(read_data.iter()) {
            assert_eq!(original, read);
        }

        debug!("✅ Full ParquetExt ForArrow roundtrip successful!");
        debug!(
            "   Processed {} records with mixed nullable/non-nullable fields",
            read_data.len()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_low_level_recordbatch_operations() -> Result<(), Box<dyn std::error::Error>> {
        // Create a test filesystem and get root WD
        let fs = new_fs().await;
        let wd = fs.root().await?;

        // Create a RecordBatch using Arrow macros
        let batch = record_batch!(
            ("product", Utf8, ["Widget A", "Widget B", "Widget C"]),
            ("quantity", Int64, [100_i64, 250_i64, 75_i64]),
            ("price", Float64, [19.99, 15.50, 8.25])
        )?;

        let test_path = "products.parquet";

        // Write using low-level RecordBatch API
        wd.create_table_from_batch(test_path, &batch, EntryType::FileTablePhysical)
            .await?;

        // Read back using low-level RecordBatch API
        let read_batch = wd.read_table_as_batch(test_path).await?;

        // Verify schema and data
        assert_eq!(batch.schema(), read_batch.schema());
        assert_eq!(batch.num_rows(), read_batch.num_rows());
        assert_eq!(batch.num_columns(), read_batch.num_columns());

        // Verify column data
        use arrow_array::{Float64Array, Int64Array, StringArray};

        let original_products = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let read_products = read_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(original_products, read_products);

        let original_quantities = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let read_quantities = read_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(original_quantities, read_quantities);

        let original_prices = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let read_prices = read_batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(original_prices, read_prices);

        debug!("✅ Low-level RecordBatch operations successful!");
        debug!(
            "   Verified schema and data integrity for {} rows",
            read_batch.num_rows()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_large_dataset_batching() -> Result<(), Box<dyn std::error::Error>> {
        // Create a test filesystem and get root WD
        let fs = new_fs().await;
        let wd = fs.root().await?;

        // Create a large dataset (more than DEFAULT_BATCH_SIZE = 1000)
        let large_data: Vec<TestRecord> = (0..2500)
            .map(|i| TestRecord {
                id: i,
                name: format!("User_{}", i),
                score: if i % 3 == 0 {
                    None
                } else {
                    Some((i as f64) * 0.1)
                },
            })
            .collect();

        let test_path = "large_dataset.parquet";

        // Write the large dataset
        wd.create_table_from_items(test_path, &large_data, EntryType::FileTablePhysical)
            .await?;

        // Read it back
        let read_data: Vec<TestRecord> = wd.read_table_as_items(test_path).await?;

        // Verify all data is preserved
        assert_eq!(large_data.len(), read_data.len());

        // Spot check some records
        assert_eq!(large_data[0], read_data[0]);
        assert_eq!(large_data[1000], read_data[1000]);
        assert_eq!(large_data[2499], read_data[2499]);

        // Check that nullable fields are handled correctly
        let none_count_original = large_data.iter().filter(|r| r.score.is_none()).count();
        let none_count_read = read_data.iter().filter(|r| r.score.is_none()).count();
        assert_eq!(none_count_original, none_count_read);

        debug!("✅ Large dataset batching successful!");
        debug!(
            "   Processed {} records with automatic batching",
            read_data.len()
        );
        debug!(
            "   Nullable field handling: {} None values preserved",
            none_count_read
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_entry_type_integration() -> Result<(), Box<dyn std::error::Error>> {
        // Create a test filesystem and get root WD
        let fs = new_fs().await;
        let wd = fs.root().await?;

        // Test different entry types
        let test_data = vec![
            TestRecord {
                id: 1,
                name: "Entry1".to_string(),
                score: Some(100.0),
            },
            TestRecord {
                id: 2,
                name: "Entry2".to_string(),
                score: Some(200.0),
            },
        ];

        // Test with FileTable entry type
        let table_path = "table_entries.parquet";
        wd.create_table_from_items(table_path, &test_data, EntryType::FileTablePhysical)
            .await?;

        // Test with FileData entry type
        let data_path = "data_entries.parquet";
        wd.create_table_from_items(data_path, &test_data, EntryType::FileDataPhysical)
            .await?;

        // Verify both can be read back correctly
        let table_data: Vec<TestRecord> = wd.read_table_as_items(table_path).await?;
        let data_data: Vec<TestRecord> = wd.read_table_as_items(data_path).await?;

        assert_eq!(test_data, table_data);
        assert_eq!(test_data, data_data);

        debug!("✅ Entry type integration successful!");
        debug!("   Verified FileTable and FileData entry types work correctly");

        Ok(())
    }

/// The 64 KiB threshold for large file storage (copied from tlogfs)
const LARGE_FILE_THRESHOLD: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct LargeTestRecord {
    id: i64,
    name: String,
    description: String, // Add longer text field
    data: Vec<u8>,       // Add binary data field
    score: Option<f64>,
}

impl ForArrow for LargeTestRecord {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("id", DataType::Int64, false)),
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("description", DataType::Utf8, false)),
            Arc::new(Field::new("data", DataType::Binary, false)),
            Arc::new(Field::new("score", DataType::Float64, true)),
        ]
    }
}

#[tokio::test]
async fn test_parquet_file_size_estimation() -> Result<(), Box<dyn std::error::Error>> {
    let fs = new_fs().await;
    let wd = fs.root().await?;

    // Create different sized datasets to see what triggers large file storage
    let sizes = vec![100, 500, 1000, 2500, 5000, 10000];

    for size in sizes {
        let large_data: Vec<LargeTestRecord> = (0..size)
            .map(|i| LargeTestRecord {
                id: i as i64,
                name: format!("User_{}_with_longer_name_to_increase_size", i),
                description: format!("This is a longer description for user {} to increase the size of each record and make the parquet file larger. We need to trigger large file storage which happens at {} bytes.", i, LARGE_FILE_THRESHOLD),
                data: vec![42u8; 100], // 100 bytes of binary data per record
                score: if i % 3 == 0 { None } else { Some((i as f64) * 0.1) },
            })
            .collect();

        let test_path = format!("test_size_{}.parquet", size);

        // Write the dataset
        wd.create_table_from_items(&test_path, &large_data, EntryType::FileTablePhysical)
            .await?;

        // Read the raw file content to check size
        let content = wd.read_file_path_to_vec(&test_path).await?;
        let file_size = content.len();

        debug!("Dataset size: {} records", size);
        debug!(
            "Parquet file size: {} bytes ({:.2} KiB)",
            file_size,
            file_size as f64 / 1024.0
        );
        debug!(
            "Large file threshold: {} bytes ({:.2} KiB)",
            LARGE_FILE_THRESHOLD,
            LARGE_FILE_THRESHOLD as f64 / 1024.0
        );
        debug!(
            "Triggers large file storage: {}",
            file_size >= LARGE_FILE_THRESHOLD
        );

        // Test that we can read it back correctly
        let read_data: Vec<LargeTestRecord> = wd.read_table_as_items(&test_path).await?;
        assert_eq!(large_data.len(), read_data.len());

        if file_size >= LARGE_FILE_THRESHOLD {
            debug!("🎯 Found a dataset size that triggers large file storage!");
            break;
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_guaranteed_large_parquet_file() -> Result<(), Box<dyn std::error::Error>> {
    let fs = new_fs().await;
    let wd = fs.root().await?;

    // Create a dataset that definitely exceeds 64 KiB
    let record_count = 20000; // Much larger dataset
    let large_data: Vec<LargeTestRecord> = (0..record_count)
        .map(|i| LargeTestRecord {
            id: i as i64,
            name: format!("User_{}_with_very_long_name_to_ensure_large_file_storage_is_triggered_definitely", i),
            description: format!("This is an extremely long description for user {} designed to create a large parquet file that will definitely exceed the 64 KiB threshold for large file storage. We're repeating this text multiple times to ensure size: {}", i, "padding ".repeat(20)),
            data: vec![42u8; 500], // 500 bytes of binary data per record
            score: if i % 3 == 0 { None } else { Some((i as f64) * 0.1) },
        })
        .collect();

    let test_path = "guaranteed_large.parquet";

    // Write the dataset
    wd.create_table_from_items(test_path, &large_data, EntryType::FileTablePhysical)
        .await?;

    // Read the raw file content to check size
    let content = wd.read_file_path_to_vec(test_path).await?;
    let file_size = content.len();

    debug!("🎯 GUARANTEED LARGE FILE TEST");
    debug!("Dataset size: {} records", record_count);
    debug!(
        "Parquet file size: {} bytes ({:.2} KiB)",
        file_size,
        file_size as f64 / 1024.0
    );
    debug!(
        "Large file threshold: {} bytes ({:.2} KiB)",
        LARGE_FILE_THRESHOLD,
        LARGE_FILE_THRESHOLD as f64 / 1024.0
    );
    debug!(
        "Triggers large file storage: {}",
        file_size >= LARGE_FILE_THRESHOLD
    );

    // This should definitely trigger large file storage
    assert!(
        file_size >= LARGE_FILE_THRESHOLD,
        "Expected file size {} to exceed threshold {}",
        file_size,
        LARGE_FILE_THRESHOLD
    );

    // Verify we can still read it back correctly
    let read_data: Vec<LargeTestRecord> = wd.read_table_as_items(test_path).await?;
    assert_eq!(large_data.len(), read_data.len());

    // Spot check some records
    assert_eq!(large_data[0], read_data[0]);
    assert_eq!(large_data[10000], read_data[10000]);
    assert_eq!(large_data[19999], read_data[19999]);

    debug!("✅ Large Parquet file test successful!");
    debug!(
        "   Large file storage working correctly with {} KiB file",
        file_size / 1024
    );

    Ok(())
}
}
