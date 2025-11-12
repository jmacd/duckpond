/// Unified Temporal Filtering for TLogFS
///
/// This module provides consistent temporal override filtering that can be applied
/// at different levels: predicate pushdown, post-read filtering, and stream filtering.
use arrow::array::{
    Array, BooleanArray, Int64Array, RecordBatch, TimestampMicrosecondArray, TimestampSecondArray,
};
use arrow::compute;

use datafusion::common::DataFusionError;
use log::debug;

/// Temporal override bounds (min_time, max_time) in milliseconds
pub type TemporalBounds = (i64, i64);

/// Apply temporal filtering to a RecordBatch
/// This is the unified filtering mechanism that works on any RecordBatch
pub fn apply_temporal_filter_to_batch(
    batch: &RecordBatch,
    min_time: i64,
    max_time: i64,
) -> Result<RecordBatch, DataFusionError> {
    // Find timestamp column
    let timestamp_col_idx = batch
        .schema()
        .index_of("timestamp")
        .map_err(|e| DataFusionError::Internal(format!("No timestamp column found: {}", e)))?;

    let timestamp_column = batch.column(timestamp_col_idx);

    // Handle different timestamp column types and convert to milliseconds
    let timestamp_values: Vec<i64> = if let Some(ts_array) = timestamp_column
        .as_any()
        .downcast_ref::<TimestampSecondArray>()
    {
        // Convert second timestamps to milliseconds
        (0..ts_array.len())
            .map(|i| {
                if ts_array.is_null(i) {
                    0 // Will be filtered out anyway
                } else {
                    ts_array.value(i) * 1000 // Convert seconds to milliseconds
                }
            })
            .collect()
    } else if let Some(ts_array) = timestamp_column
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
    {
        // Convert microsecond timestamps to milliseconds
        (0..ts_array.len())
            .map(|i| {
                if ts_array.is_null(i) {
                    0 // Will be filtered out anyway
                } else {
                    ts_array.value(i) / 1000 // Convert microseconds to milliseconds
                }
            })
            .collect()
    } else if let Some(int64_array) = timestamp_column.as_any().downcast_ref::<Int64Array>() {
        // Assume Int64 timestamps are already in milliseconds
        (0..int64_array.len())
            .map(|i| {
                if int64_array.is_null(i) {
                    0 // Will be filtered out anyway
                } else {
                    int64_array.value(i)
                }
            })
            .collect()
    } else {
        return Err(DataFusionError::Internal(format!(
            "Timestamp column has unsupported type: {:?}",
            timestamp_column.data_type()
        )));
    };

    // Create boolean mask for filtering: timestamp >= min_time AND timestamp <= max_time
    let mut filter_mask = Vec::with_capacity(timestamp_values.len());
    for (i, &ts) in timestamp_values.iter().enumerate() {
        if timestamp_column.is_null(i) {
            filter_mask.push(false);
        } else {
            filter_mask.push(ts >= min_time && ts <= max_time);
        }
    }

    let original_rows = batch.num_rows();
    let filtered_rows = filter_mask.iter().filter(|&&x| x).count();
    let filter_array = BooleanArray::from(filter_mask);

    debug!(
        "Temporal filter: {original_rows} → {filtered_rows} rows (bounds: {min_time} to {max_time})"
    );

    // Apply filter to all columns
    let filtered_columns: Result<Vec<_>, _> = batch
        .columns()
        .iter()
        .map(|col| compute::filter(col, &filter_array))
        .collect();

    let filtered_columns =
        filtered_columns.map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    RecordBatch::try_new(batch.schema(), filtered_columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Create a Parquet predicate for temporal filtering
/// This enables predicate pushdown for large datasets
#[must_use]
pub fn create_temporal_predicate(
    _min_time: i64,
    _max_time: i64,
) -> Box<dyn Fn(&parquet::file::metadata::RowGroupMetaData) -> bool + Send + Sync> {
    // @@@ WOW WOW WHAT IS IT
    Box::new(move |row_group_metadata| {
        // Try to get timestamp column statistics
        if let Some(timestamp_col) = row_group_metadata
            .columns()
            .iter()
            .find(|col| col.column_path().string() == "timestamp")
        {
            if let Some(_statistics) = timestamp_col.statistics() {
                // Check if this row group overlaps with our temporal bounds
                // Note: This is a simplified check - real implementation would need
                // to handle different timestamp types and units properly

                // For now, conservatively return true (include row group)
                // TODO: Implement proper statistics-based filtering
                true
            } else {
                // No statistics available, include the row group
                true
            }
        } else {
            // No timestamp column found, include the row group
            true
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Builder, TimestampSecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    #[test]
    fn test_temporal_filter_timestamp_seconds() {
        // Create test data with TimestampSecondArray with explicit timezone
        let timestamp_data = TimestampSecondArray::from(vec![
            Some(1704067200), // 2024-01-01 00:00:00 UTC (in bounds)
            Some(1609459200), // 2021-01-01 00:00:00 UTC (out of bounds - before)
            Some(1717113599), // 2024-05-30 23:59:59 UTC (in bounds)
            Some(1735689600), // 2025-01-01 00:00:00 UTC (out of bounds - after)
        ])
        .with_timezone("+00:00");

        let mut value_builder = Int64Builder::new();
        value_builder.append_value(100);
        value_builder.append_value(200);
        value_builder.append_value(300);
        value_builder.append_value(400);
        let value_data = value_builder.finish();

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, Some("+00:00".into())),
                false,
            ),
            Field::new("value", DataType::Int64, false),
        ]));

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(timestamp_data), Arc::new(value_data)])
                .unwrap();

        // Apply temporal filter: 2024-01-01 to 2024-05-30 (in milliseconds)
        let min_time = 1704067200000; // 2024-01-01 00:00:00 UTC in milliseconds
        let max_time = 1717113599000; // 2024-05-30 23:59:59 UTC in milliseconds

        let filtered_batch = apply_temporal_filter_to_batch(&batch, min_time, max_time).unwrap();

        // Should have 2 rows (indices 0 and 2)
        assert_eq!(filtered_batch.num_rows(), 2);

        // Check that the right values remain
        let filtered_values = filtered_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(filtered_values.value(0), 100); // From 2024-01-01
        assert_eq!(filtered_values.value(1), 300); // From 2024-05-30
    }
}
