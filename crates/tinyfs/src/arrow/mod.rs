//! Arrow integration for TinyFS
//!
//! This module provides Arrow ecosystem integration, including:
//! - Schema generation from Rust types via ForArrow trait
//! - Parquet file I/O with async support
//! - Integration with the serde_arrow ecosystem

pub mod schema;
// pub mod parquet; // Temporarily disabled due to async trait issues
pub mod simple_parquet;

#[cfg(test)]
mod simple_parquet_tests;

pub use schema::ForArrow;
// pub use parquet::ParquetExt; // Temporarily disabled
pub use simple_parquet::SimpleParquetExt;
