//! Consolidated test helpers for all DuckPond crates
//!
//! This module provides reusable test utilities to eliminate duplication across
//! tinyfs, tlogfs, provider, and other crates. It includes:
//!
//! - Parquet data generation utilities
//! - RecordBatch builders for common patterns
//! - Test environment setup helpers
//! - Provider context creation for testing
//!
//! # Usage
//!
//! Add to your test dependencies in Cargo.toml:
//! ```toml
//! [dev-dependencies]
//! utilities = { path = "../utilities" }
//! ```

use arrow::array::{
    Float64Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::io::Cursor;
use std::sync::Arc;

/// Test helper error type for better error chaining
#[derive(Debug, thiserror::Error)]
pub enum TestError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("General error: {0}")]
    General(String),
}

pub type TestResult<T> = Result<T, TestError>;

// ============================================================================
// PARQUET GENERATION UTILITIES
// ============================================================================

/// Generate Parquet bytes from a RecordBatch
///
/// # Example
/// ```ignore
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("id", DataType::Int32, false),
/// ]));
/// let batch = RecordBatch::try_new(
///     schema.clone(),
///     vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
/// ).unwrap();
/// let parquet_bytes = generate_parquet_from_batch(batch).unwrap();
/// ```
pub fn generate_parquet_from_batch(batch: RecordBatch) -> TestResult<Vec<u8>> {
    use parquet::arrow::ArrowWriter;

    let schema = batch.schema();
    let mut buffer = Vec::new();
    {
        let cursor = Cursor::new(&mut buffer);
        let mut writer = ArrowWriter::try_new(cursor, schema, None)?;
        writer.write(&batch)?;
        _ = writer.close()?;
    }
    Ok(buffer)
}

/// Generate Parquet bytes with timestamp and value columns
///
/// Creates a Parquet file with columns: timestamp (milliseconds), value (f64)
///
/// # Example
/// ```ignore
/// let data = vec![
///     (1000, 23.5),
///     (2000, 24.1),
///     (3000, 25.2),
/// ];
/// let parquet_bytes = generate_parquet_with_timestamps(data).unwrap();
/// ```
pub fn generate_parquet_with_timestamps(data: Vec<(i64, f64)>) -> TestResult<Vec<u8>> {
    let (timestamps, values): (Vec<i64>, Vec<f64>) = data.into_iter().unzip();

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("value", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMillisecondArray::from(timestamps)),
            Arc::new(Float64Array::from(values)),
        ],
    )?;

    generate_parquet_from_batch(batch)
}

/// Generate Parquet bytes with timestamp, sensor_id, and value columns
///
/// Creates a Parquet file with columns: timestamp (ms), sensor_id (string), value (f64)
///
/// # Example
/// ```ignore
/// let data = vec![
///     (1000, "sensor1", 23.5),
///     (2000, "sensor2", 24.1),
/// ];
/// let parquet_bytes = generate_parquet_with_sensor_data(data).unwrap();
/// ```
pub fn generate_parquet_with_sensor_data(data: Vec<(i64, &str, f64)>) -> TestResult<Vec<u8>> {
    let (timestamps, sensor_ids, values): (Vec<i64>, Vec<String>, Vec<f64>) = data
        .into_iter()
        .map(|(t, s, v)| (t, s.to_string(), v))
        .fold(
            (Vec::new(), Vec::new(), Vec::new()),
            |(mut ts, mut ids, mut vals), (t, id, v)| {
                ts.push(t);
                ids.push(id);
                vals.push(v);
                (ts, ids, vals)
            },
        );

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("sensor_id", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMillisecondArray::from(timestamps)),
            Arc::new(StringArray::from(sensor_ids)),
            Arc::new(Float64Array::from(values)),
        ],
    )?;

    generate_parquet_from_batch(batch)
}

/// Generate simple Parquet bytes with id, name, value columns
///
/// Useful for testing basic table operations without temporal data
///
/// # Example
/// ```ignore
/// let data = vec![
///     (1, "Alice", 100),
///     (2, "Bob", 200),
/// ];
/// let parquet_bytes = generate_simple_table_parquet(data).unwrap();
/// ```
pub fn generate_simple_table_parquet(data: Vec<(i32, &str, i32)>) -> TestResult<Vec<u8>> {
    let (ids, names, values): (Vec<i32>, Vec<String>, Vec<i32>) = data
        .into_iter()
        .map(|(id, name, val)| (id, name.to_string(), val))
        .fold(
            (Vec::new(), Vec::new(), Vec::new()),
            |(mut ids, mut names, mut vals), (id, name, val)| {
                ids.push(id);
                names.push(name);
                vals.push(val);
                (ids, names, vals)
            },
        );

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int32Array::from(values)),
        ],
    )?;

    generate_parquet_from_batch(batch)
}

// ============================================================================
// RECORDBATCH BUILDERS
// ============================================================================

/// Builder for creating test RecordBatches with sensor data
///
/// Eliminates duplication in RecordBatch creation patterns across tests.
///
/// # Example
/// ```ignore
/// let (batch, min_time, max_time) = SensorBatchBuilder::new()
///     .add_reading(0, "sensor1", 23.5, 45.2)
///     .add_reading(1000, "sensor1", 24.1, 46.8)
///     .build()
///     .unwrap();
/// ```
pub struct SensorBatchBuilder {
    base_time: i64,
    timestamps: Vec<i64>,
    sensor_ids: Vec<String>,
    temperatures: Vec<f64>,
    humidity: Vec<f64>,
}

impl Default for SensorBatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SensorBatchBuilder {
    /// Create a new builder with current timestamp as base
    #[must_use]
    pub fn new() -> Self {
        Self::with_base_time(chrono::Utc::now().timestamp_millis())
    }

    /// Create a new builder with a specific base timestamp
    #[must_use]
    pub fn with_base_time(base_time: i64) -> Self {
        Self {
            base_time,
            timestamps: Vec::new(),
            sensor_ids: Vec::new(),
            temperatures: Vec::new(),
            humidity: Vec::new(),
        }
    }

    /// Add a sensor reading with offset from base time
    #[must_use]
    pub fn add_reading(
        mut self,
        offset_ms: i64,
        sensor_id: &str,
        temperature: f64,
        humidity: f64,
    ) -> Self {
        self.timestamps.push(self.base_time + offset_ms);
        self.sensor_ids.push(sensor_id.to_string());
        self.temperatures.push(temperature);
        self.humidity.push(humidity);
        self
    }

    /// Add a simple reading with auto-incrementing sensor ID
    #[must_use]
    pub fn add_simple_reading(self, offset_ms: i64, temperature: f64) -> Self {
        let sensor_id = format!("sensor{}", self.sensor_ids.len() + 1);
        self.add_reading(offset_ms, &sensor_id, temperature, 45.0 + temperature * 0.5)
    }

    /// Build the RecordBatch with the standard sensor schema and time range
    pub fn build(self) -> TestResult<(RecordBatch, i64, i64)> {
        if self.timestamps.is_empty() {
            return Err(TestError::General("No data added to builder".to_string()));
        }

        let min_time = *self.timestamps.iter().min().unwrap();
        let max_time = *self.timestamps.iter().max().unwrap();

        let schema = Self::standard_sensor_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(self.timestamps)),
                Arc::new(StringArray::from(self.sensor_ids)),
                Arc::new(Float64Array::from(self.temperatures)),
                Arc::new(Float64Array::from(self.humidity)),
            ],
        )?;

        Ok((batch, min_time, max_time))
    }

    /// Build just the RecordBatch without time range
    pub fn build_batch(self) -> TestResult<RecordBatch> {
        let (batch, _, _) = self.build()?;
        Ok(batch)
    }

    /// Build the RecordBatch and convert to Parquet bytes
    pub fn build_parquet(self) -> TestResult<Vec<u8>> {
        let batch = self.build_batch()?;
        generate_parquet_from_batch(batch)
    }

    /// Standard sensor schema used across tests
    #[must_use]
    pub fn standard_sensor_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("sensor_id", DataType::Utf8, false),
            Field::new("temperature", DataType::Float64, false),
            Field::new("humidity", DataType::Float64, false),
        ]))
    }

    /// Create a simple 4-reading test batch (common pattern)
    pub fn default_test_batch() -> TestResult<(RecordBatch, i64, i64)> {
        Self::new()
            .add_reading(0, "sensor1", 23.5, 45.2)
            .add_reading(1000, "sensor1", 24.1, 46.8)
            .add_reading(2000, "sensor2", 22.8, 44.1)
            .add_reading(3000, "sensor2", 25.2, 48.9)
            .build()
    }
}

/// Builder for creating test RecordBatches with different timestamp types
///
/// Useful for testing timestamp conversion and compatibility
pub struct TimestampBatchBuilder;

impl TimestampBatchBuilder {
    /// Create batch with microsecond timestamps
    pub fn microseconds(timestamps: Vec<i64>) -> TestResult<RecordBatch> {
        let timestamps_array = TimestampMicrosecondArray::from(timestamps);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )]));
        Ok(RecordBatch::try_new(
            schema,
            vec![Arc::new(timestamps_array)],
        )?)
    }

    /// Create batch with millisecond timestamps
    pub fn milliseconds(timestamps: Vec<i64>) -> TestResult<RecordBatch> {
        let timestamps_array = TimestampMillisecondArray::from(timestamps);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )]));
        Ok(RecordBatch::try_new(
            schema,
            vec![Arc::new(timestamps_array)],
        )?)
    }

    /// Create batch with raw Int64 timestamps
    pub fn int64(timestamps: Vec<i64>) -> TestResult<RecordBatch> {
        let timestamps_array = Int64Array::from(timestamps);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Int64,
            false,
        )]));
        Ok(RecordBatch::try_new(
            schema,
            vec![Arc::new(timestamps_array)],
        )?)
    }

    /// Create all three variants for comprehensive testing
    pub fn all_variants() -> TestResult<Vec<RecordBatch>> {
        Ok(vec![
            Self::microseconds(vec![1000000, 2000000, 3000000])?,
            Self::milliseconds(vec![1000, 2000, 3000])?,
            Self::int64(vec![1000, 2000, 3000])?,
        ])
    }
}

// ============================================================================
// SCHEMA UTILITIES
// ============================================================================

/// Common schema patterns used across tests
pub struct TestSchemas;

impl TestSchemas {
    /// Sensor data schema: timestamp, sensor_id, temperature, humidity
    #[must_use]
    pub fn sensor_data() -> Arc<Schema> {
        SensorBatchBuilder::standard_sensor_schema()
    }

    /// Simple table schema: id, name, value
    #[must_use]
    pub fn simple_table() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]))
    }

    /// Timeseries schema: timestamp, value
    #[must_use]
    pub fn timeseries() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, false),
        ]))
    }

    /// Multi-sensor schema: timestamp, site_id, sensor_id, value
    #[must_use]
    pub fn multi_sensor() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("site_id", DataType::Utf8, false),
            Field::new("sensor_id", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_parquet_with_timestamps() {
        let data = vec![(1000, 23.5), (2000, 24.1), (3000, 25.2)];
        let result = generate_parquet_with_timestamps(data);
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(!bytes.is_empty());
        // Parquet magic number check
        assert_eq!(&bytes[0..4], b"PAR1");
    }

    #[test]
    fn test_generate_parquet_with_sensor_data() {
        let data = vec![
            (1000, "sensor1", 23.5),
            (2000, "sensor2", 24.1),
            (3000, "sensor1", 25.2),
        ];
        let result = generate_parquet_with_sensor_data(data);
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(!bytes.is_empty());
        assert_eq!(&bytes[0..4], b"PAR1");
    }

    #[test]
    fn test_generate_simple_table_parquet() {
        let data = vec![(1, "Alice", 100), (2, "Bob", 200), (3, "Charlie", 150)];
        let result = generate_simple_table_parquet(data);
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(!bytes.is_empty());
        assert_eq!(&bytes[0..4], b"PAR1");
    }

    #[test]
    fn test_sensor_batch_builder() {
        let result = SensorBatchBuilder::new()
            .add_reading(0, "sensor1", 23.5, 45.2)
            .add_reading(1000, "sensor1", 24.1, 46.8)
            .build();

        assert!(result.is_ok());
        let (batch, min_time, max_time) = result.unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);
        assert!(max_time > min_time);
    }

    #[test]
    fn test_sensor_batch_builder_default() {
        let result = SensorBatchBuilder::default_test_batch();
        assert!(result.is_ok());
        let (batch, min_time, max_time) = result.unwrap();
        assert_eq!(batch.num_rows(), 4);
        assert!(max_time > min_time);
    }

    #[test]
    fn test_sensor_batch_to_parquet() {
        let result = SensorBatchBuilder::new()
            .add_simple_reading(0, 25.0)
            .add_simple_reading(1000, 26.0)
            .build_parquet();

        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(!bytes.is_empty());
        assert_eq!(&bytes[0..4], b"PAR1");
    }

    #[test]
    fn test_timestamp_batch_variants() {
        let result = TimestampBatchBuilder::all_variants();
        assert!(result.is_ok());
        let batches = result.unwrap();
        assert_eq!(batches.len(), 3);
        for batch in batches {
            assert_eq!(batch.num_rows(), 3);
            assert_eq!(batch.num_columns(), 1);
        }
    }

    #[test]
    fn test_test_schemas() {
        let sensor_schema = TestSchemas::sensor_data();
        assert_eq!(sensor_schema.fields().len(), 4);

        let simple_schema = TestSchemas::simple_table();
        assert_eq!(simple_schema.fields().len(), 3);

        let timeseries_schema = TestSchemas::timeseries();
        assert_eq!(timeseries_schema.fields().len(), 2);

        let multi_sensor_schema = TestSchemas::multi_sensor();
        assert_eq!(multi_sensor_schema.fields().len(), 4);
    }

    #[test]
    fn test_empty_builder_fails() {
        let result = SensorBatchBuilder::new().build();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("No data added to builder")
        );
    }
}
