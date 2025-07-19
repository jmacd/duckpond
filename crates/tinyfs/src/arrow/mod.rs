//! Arrow integration for TinyFS
//!
//! This module provides Arrow ecosystem integration, including:
//! - Schema generation from Rust types via ForArrow trait
//! - Parquet file I/O with async support
//! - Integration with the serde_arrow ecosystem

pub mod schema;
pub mod parquet; // Full ParquetExt trait - fixing async trait compatibility issues
pub mod simple_parquet;

#[cfg(test)]
mod simple_parquet_tests;

#[cfg(test)]
mod parquet_tests;

#[cfg(test)]
mod large_parquet_tests;

pub use schema::ForArrow;
pub use parquet::ParquetExt; // Full ParquetExt trait - fixing async trait compatibility issues  
pub use simple_parquet::SimpleParquetExt;
