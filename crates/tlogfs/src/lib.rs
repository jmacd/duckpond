//! TLogFS - A filesystem implementation using Delta Lake for storage
//! 
//! Set DUCKPOND_LOG environment variable to control logging:
//! - DUCKPOND_LOG=off (default) - silent
//! - DUCKPOND_LOG=info - basic operations  
//! - DUCKPOND_LOG=debug - detailed diagnostics

// Core schema and data structures
pub mod schema;

// Delta Lake integration
pub mod delta;

// Large file storage utilities
pub mod large_files;

// Persistence layer implementation
pub mod persistence;

// Transaction guard implementation
pub mod transaction_guard;

// File writer implementation with clean write path
pub mod file_writer;

// DataFusion query interfaces
pub mod query;

// Arrow-backed filesystem object implementations  
pub mod file;
pub mod directory;
pub mod symlink;

// Error types
pub mod error;

// Dynamic factory system
pub mod factory;

// Hostmount dynamic directory
pub mod hostmount;

// SQL-derived dynamic node factory
pub mod sql_derived;

// CSV directory dynamic factory
pub mod csv_directory;

// TinyFS ObjectStore implementation for DataFusion ListingTable integration
pub mod tinyfs_object_store;

// Dynamic directory factory for composing other factories
pub mod dynamic_dir;

// File-table duality integration for TinyFS and DataFusion
pub mod file_table;

// Re-export key types
pub use error::TLogFSError;
pub use persistence::{
    OpLogPersistence,
};
pub use schema::{OplogEntry, VersionedDirectoryEntry};

// Re-export query interfaces for DataFusion integration
pub use query::{DirectoryTable, MetadataTable, SeriesTable, SeriesExt, SeriesStream, FileInfo};

// Test utilities for DRY test patterns
#[cfg(test)]
pub mod test_utils;

#[cfg(test)]
mod tests;
