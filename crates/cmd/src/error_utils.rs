// Error handling utilities to eliminate repetitive error mapping patterns

use anyhow::{Result, anyhow};

/// Extension trait for Results to provide standardized error mapping
/// Eliminates repetitive `.map_err(|e| anyhow!("Failed to ..."))` patterns
pub trait ErrorContext<T> {
    /// Map error with a context message for common "Failed to..." patterns
    fn with_context_msg(self, msg: &str) -> Result<T>;

    /// Map error with a formatted context message
    fn with_context_fmt(self, msg: &str, args: &dyn std::fmt::Display) -> Result<T>;

    /// Common error mapping for persistence operations
    fn persist_context(self, operation: &str) -> Result<T>;

    /// Common error mapping for file operations
    fn file_context(self, operation: &str, path: &str) -> Result<T>;

    /// Common error mapping for transaction operations
    fn transaction_context(self, operation: &str) -> Result<T>;

    /// Common error mapping for ship operations
    fn ship_context(self, operation: &str) -> Result<T>;

    /// Common error mapping for Parquet operations
    fn parquet_context(self, operation: &str) -> Result<T>;

    /// Common error mapping for CSV operations
    fn csv_context(self, operation: &str) -> Result<T>;

    /// Common error mapping for Delta table operations
    fn delta_context(self, operation: &str) -> Result<T>;
}

impl<T, E> ErrorContext<T> for std::result::Result<T, E>
where
    E: std::fmt::Display,
{
    fn with_context_msg(self, msg: &str) -> Result<T> {
        self.map_err(|e| anyhow!("{}: {}", msg, e))
    }

    fn with_context_fmt(self, msg: &str, args: &dyn std::fmt::Display) -> Result<T> {
        self.map_err(|e| anyhow!("{} {}: {}", msg, args, e))
    }

    fn persist_context(self, operation: &str) -> Result<T> {
        self.with_context_msg(&format!("Failed to {}", operation))
    }

    fn file_context(self, operation: &str, path: &str) -> Result<T> {
        self.with_context_fmt(&format!("Failed to {}", operation), &path)
    }

    fn transaction_context(self, operation: &str) -> Result<T> {
        self.with_context_msg(&format!("Failed to {} transaction", operation))
    }

    fn ship_context(self, operation: &str) -> Result<T> {
        self.with_context_msg(&format!("Failed to {} ship", operation))
    }

    fn parquet_context(self, operation: &str) -> Result<T> {
        self.with_context_msg(&format!("Failed to {} Parquet", operation))
    }

    fn csv_context(self, operation: &str) -> Result<T> {
        self.with_context_msg(&format!("Failed to {} CSV", operation))
    }

    fn delta_context(self, operation: &str) -> Result<T> {
        self.with_context_msg(&format!("Failed to {} Delta table", operation))
    }
}

/// Common error mapping patterns used across the codebase
pub struct CommonErrors;

impl CommonErrors {
    /// Standard "Failed to initialize ship" error
    pub fn init_ship<E: std::fmt::Display>(err: E) -> anyhow::Error {
        anyhow!("Failed to initialize ship: {}", err)
    }

    /// Standard "Failed to begin transaction" error
    pub fn begin_transaction<E: std::fmt::Display>(err: E) -> anyhow::Error {
        anyhow!("Failed to begin transaction: {}", err)
    }

    /// Standard "Failed to commit transaction" error
    pub fn commit_transaction<E: std::fmt::Display>(err: E) -> anyhow::Error {
        anyhow!("Failed to commit transaction: {}", err)
    }

    /// Standard "Failed to initialize pond" error
    pub fn init_pond<E: std::fmt::Display>(err: E) -> anyhow::Error {
        anyhow!("Failed to initialize pond: {}", err)
    }

    /// Standard "Failed to get Delta table" error
    pub fn get_delta_table<E: std::fmt::Display>(err: E) -> anyhow::Error {
        anyhow!("Failed to get Delta table: {}", err)
    }

    /// Standard "Failed to open table at version" error
    pub fn open_table_version<E: std::fmt::Display>(err: E, version: i64) -> anyhow::Error {
        anyhow!("Failed to open table at version {}: {}", version, err)
    }

    /// Standard "Failed to open Parquet file" error
    pub fn open_parquet_file<E: std::fmt::Display>(err: E, file_path: &str) -> anyhow::Error {
        anyhow!("Failed to open Parquet file {}: {}", file_path, err)
    }

    /// Standard "Failed to create Parquet reader" error
    pub fn create_parquet_reader<E: std::fmt::Display>(err: E) -> anyhow::Error {
        anyhow!("Failed to create Parquet reader: {}", err)
    }

    /// Standard "Failed to build Parquet reader" error
    pub fn build_parquet_reader<E: std::fmt::Display>(err: E) -> anyhow::Error {
        anyhow!("Failed to build Parquet reader: {}", err)
    }

    /// Standard "Failed to read Parquet batch" error
    pub fn read_parquet_batch<E: std::fmt::Display>(err: E) -> anyhow::Error {
        anyhow!("Failed to read Parquet batch: {}", err)
    }

    /// Standard "Failed to open CSV file" error
    pub fn open_csv_file<E: std::fmt::Display>(err: E) -> anyhow::Error {
        anyhow!("Failed to open CSV file: {}", err)
    }

    /// Standard "Failed to infer CSV schema" error
    pub fn infer_csv_schema<E: std::fmt::Display>(err: E) -> anyhow::Error {
        anyhow!("Failed to infer CSV schema: {}", err)
    }

    /// Standard "Failed to create CSV reader" error
    pub fn create_csv_reader<E: std::fmt::Display>(err: E) -> anyhow::Error {
        anyhow!("Failed to create CSV reader: {}", err)
    }

    /// Standard "Failed to read CSV batch" error
    pub fn read_csv_batch<E: std::fmt::Display>(err: E) -> anyhow::Error {
        anyhow!("Failed to read CSV batch: {}", err)
    }

    /// Standard "Failed to create Parquet writer" error
    pub fn create_parquet_writer<E: std::fmt::Display>(err: E) -> anyhow::Error {
        anyhow!("Failed to create Parquet writer: {}", err)
    }

    /// Standard "Failed to write Parquet data" error
    pub fn write_parquet_data<E: std::fmt::Display>(err: E) -> anyhow::Error {
        anyhow!("Failed to write Parquet data: {}", err)
    }

    /// Standard "Failed to close Parquet writer" error
    pub fn close_parquet_writer<E: std::fmt::Display>(err: E) -> anyhow::Error {
        anyhow!("Failed to close Parquet writer: {}", err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error as IoError, ErrorKind};

    #[test]
    fn test_error_context_extension() {
        let io_error = IoError::new(ErrorKind::NotFound, "file not found");
        let result: std::result::Result<(), _> = Err(io_error);

        let mapped = result.file_context("open", "/test/path");
        assert!(mapped.is_err());
        assert!(
            mapped
                .unwrap_err()
                .to_string()
                .contains("Failed to open /test/path")
        );
    }

    #[test]
    fn test_transaction_context() {
        let error = "connection lost";
        let result: std::result::Result<(), _> = Err(error);

        let mapped = result.transaction_context("begin");
        assert!(mapped.is_err());
        assert!(
            mapped
                .unwrap_err()
                .to_string()
                .contains("Failed to begin transaction")
        );
    }

    #[test]
    fn test_common_errors() {
        let err = CommonErrors::init_ship("connection failed");
        assert!(err.to_string().contains("Failed to initialize ship"));

        let err = CommonErrors::open_table_version("corrupt data", 42);
        assert!(
            err.to_string()
                .contains("Failed to open table at version 42")
        );
    }
}
