// Test utilities for DRY test code - eliminates duplication across test files

use crate::persistence::OpLogPersistence;
use crate::transaction_guard::TransactionGuard;
use arrow::array::{
    Float64Array, Int64Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use log::info;
use std::sync::Arc;
use tempfile::{TempDir, tempdir};

/// Test helper error type for better error chaining
#[derive(Debug, thiserror::Error)]
pub enum TestError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("TLogFS error: {0}")]
    TLogFS(#[from] crate::TLogFSError),
    #[error("General error: {0}")]
    General(String),
}

pub type TestResult<T> = Result<T, TestError>;

/// Standard test result type for consistency across all tests
pub type StdTestResult = TestResult<()>;

/// Builder for creating test RecordBatches with sensor data
/// Eliminates duplication in RecordBatch creation patterns
pub struct TestRecordBatchBuilder {
    base_time: i64,
    timestamps: Vec<i64>,
    sensor_ids: Vec<String>,
    temperatures: Vec<f64>,
    humidity: Vec<f64>,
}

impl TestRecordBatchBuilder {
    /// Create a new builder with current timestamp as base
    pub fn new() -> Self {
        let base_time = Utc::now().timestamp_millis();
        Self {
            base_time,
            timestamps: Vec::new(),
            sensor_ids: Vec::new(),
            temperatures: Vec::new(),
            humidity: Vec::new(),
        }
    }

    /// Create a new builder with a specific base timestamp
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
    pub fn add_simple_reading(self, offset_ms: i64, temperature: f64) -> Self {
        let sensor_id = format!("sensor{}", self.sensor_ids.len() + 1);
        self.add_reading(offset_ms, &sensor_id, temperature, 45.0 + temperature * 0.5)
    }

    /// Build the RecordBatch with the standard sensor schema
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

    /// Standard sensor schema used across tests
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

    /// Create batch with different timestamp types for testing
    pub fn timestamp_types_test_batches() -> TestResult<Vec<RecordBatch>> {
        let mut batches = Vec::new();

        // Microsecond timestamps
        let timestamps_micro = TimestampMicrosecondArray::from(vec![1000000, 2000000, 3000000]);
        let schema_micro = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )]));
        batches.push(RecordBatch::try_new(
            schema_micro,
            vec![Arc::new(timestamps_micro)],
        )?);

        // Raw Int64 timestamps
        let timestamps_int64 = Int64Array::from(vec![1000, 2000, 3000]);
        let schema_int64 = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Int64,
            false,
        )]));
        batches.push(RecordBatch::try_new(
            schema_int64,
            vec![Arc::new(timestamps_int64)],
        )?);

        Ok(batches)
    }
}

/// Test environment setup that encapsulates common test patterns
/// Eliminates duplication in test setup across multiple test functions
pub struct TestEnvironment {
    pub temp_dir: TempDir,
    pub persistence: OpLogPersistence,
}

impl TestEnvironment {
    /// Create a new test environment with temp directory and persistence
    pub async fn new() -> TestResult<Self> {
        let temp_dir = tempdir()
            .map_err(|e| TestError::General(format!("Failed to create temp directory: {}", e)))?;
        let store_path = temp_dir.path().join("test_store");

        let persistence = OpLogPersistence::create_test(store_path.to_str().unwrap())
            .await
            .map_err(|e| {
                TestError::General(format!("Failed to create persistence layer: {}", e))
            })?;

        Ok(Self {
            temp_dir,
            persistence,
        })
    }

    /// Complete transaction pattern: begin, execute closure, commit
    pub async fn transaction<F, Fut, T>(&mut self, f: F) -> TestResult<T>
    where
        F: FnOnce(&mut TransactionGuard<'_>) -> Fut,
        Fut: std::future::Future<Output = TestResult<T>>,
    {
        let mut guard = self.persistence.begin_test().await?;
        let result = f(&mut guard).await?;
        guard
            .commit_test()
            .await
            .map_err(|e| TestError::General(format!("Failed to commit transaction: {}", e)))?;
        Ok(result)
    }

    // Store test FileSeries with metadata (common pattern)
    // pub async fn store_test_file_series(
    //     &self,
    //     content: &[u8],
    //     min_time: i64,
    //     max_time: i64,
    //     timestamp_column: &str,
    // ) -> TestResult<(NodeID, NodeID)> {
    //     let node_id = NodeID::generate();
    //     let part_id = NodeID::generate();

    //     self.persistence
    //         .store_file_series_with_metadata(node_id, part_id, content, min_time, max_time, timestamp_column)
    //         .await
    //         .map_err(|e| TestError::General(format!("Failed to store FileSeries: {}", e)))?;

    //     Ok((node_id, part_id))
    // }
}

/// Default implementation for convenient usage
impl Default for TestRecordBatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_record_batch_builder() -> StdTestResult {
        let (batch, min_time, max_time) = TestRecordBatchBuilder::default_test_batch()?;

        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.num_columns(), 4);
        assert!(max_time > min_time);

        Ok(())
    }

    #[tokio::test]
    async fn test_environment_setup() -> StdTestResult {
        let mut env = TestEnvironment::new().await?;

        info!("starting test");
        // Test transaction pattern with transaction guard
        {
            let tx =
                env.persistence.begin_test().await.map_err(|e| {
                    TestError::General(format!("Failed to begin transaction: {}", e))
                })?;

            // // Initialize root directory to make the transaction non-empty
            // tx.initialize_root_directory().await
            //     .map_err(|e| TestError::General(format!("Failed to initialize root: {}", e)))?;

            tx.commit_test()
                .await
                .map_err(|e| TestError::General(format!("Failed to commit transaction: {}", e)))?;
        }

        Ok(())
    }

    #[test]
    fn test_builder_pattern() {
        let result = TestRecordBatchBuilder::new()
            .add_simple_reading(0, 25.0)
            .add_simple_reading(1000, 26.0)
            .build_batch();

        assert!(result.is_ok());
    }
}
