// Test utilities for DRY test code - eliminates duplication across test files
//
// NOTE: RecordBatch builders and Parquet generation utilities have been moved
// to the utilities crate (utilities::test_helpers) for use across all DuckPond
// crates. Import from there instead of duplicating here.

use crate::persistence::OpLogPersistence;
use crate::transaction_guard::TransactionGuard;
use log::info;
use std::future::Future;
use tempfile::{TempDir, tempdir};

// Re-export commonly used test helpers from utilities crate
pub use utilities::test_helpers::{
    SensorBatchBuilder, TestSchemas, TimestampBatchBuilder, generate_parquet_from_batch,
    generate_parquet_with_sensor_data, generate_parquet_with_timestamps,
    generate_simple_table_parquet,
};

/// Test helper error type for tlogfs tests (supports both utilities and tlogfs errors)
#[derive(Debug, thiserror::Error)]
pub enum TestError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("TLogFS error: {0}")]
    TLogFS(#[from] crate::TLogFSError),
    #[error("TinyFS error: {0}")]
    TinyFS(#[from] tinyfs::Error),
    #[error("Utilities test error: {0}")]
    Utilities(#[from] utilities::test_helpers::TestError),
    #[error("General error: {0}")]
    General(String),
}

pub type TestResult<T> = Result<T, TestError>;

/// Standard test result type for consistency across all tests
pub type StdTestResult = TestResult<()>;

// NOTE: RecordBatch builders have been moved to utilities::test_helpers
// Re-export for backward compatibility
pub use utilities::test_helpers::SensorBatchBuilder as TestRecordBatchBuilder;

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
        Fut: Future<Output = TestResult<T>>,
    {
        let mut guard = self.persistence.begin_test().await?;
        let result = f(&mut guard).await?;
        guard
            .commit_test()
            .await
            .map_err(|e| TestError::General(format!("Failed to commit transaction: {}", e)))?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_record_batch_builder() -> StdTestResult {
        let (batch, min_time, max_time) = SensorBatchBuilder::default_test_batch()?;

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
            let tx = env.persistence.begin_test().await?;

            // // Initialize root directory to make the transaction non-empty
            // tx.initialize_root_directory().await?;

            tx.commit_test().await?;
        }

        Ok(())
    }

    #[test]
    fn test_builder_pattern() {
        let result = SensorBatchBuilder::new()
            .add_simple_reading(0, 25.0)
            .add_simple_reading(1000, 26.0)
            .build_batch();

        assert!(result.is_ok());
    }
}
