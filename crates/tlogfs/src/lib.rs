#![allow(tail_expr_drop_order)]
#![allow(missing_docs)]

//! TLogFS - A filesystem implementation using Delta Lake for storage
//!
//! Set RUST_LOG environment variable to control logging:
//! - RUST_LOG=off (default) - silent
//! - RUST_LOG=info - basic operations  
//! - RUST_LOG=debug - detailed diagnostics
//! - RUST_LOG=tlogfs=debug - debug only tlogfs crate

/// Core schema and data structures
pub mod schema;

/// Delta Lake integration
pub mod delta;

/// Large file storage utilities
pub mod large_files;

/// Persistence layer implementation
pub mod persistence;

// Transaction guard implementation
pub mod transaction_guard;

// File writer implementation with clean write path
pub mod file_writer;

// DataFusion query interfaces
pub mod query;

// Arrow-backed filesystem object implementations
pub mod directory;
pub mod file;
pub mod symlink;

// Error types
pub mod error;

// Transaction metadata - required for all commits
pub mod txn_metadata;

// Dynamic factory system
pub mod factory;

// Data taxonomy for sensitive configuration fields
pub mod data_taxonomy;

// SQL-derived dynamic node factory
pub mod sql_derived;

// Temporal reduce dynamic factory
pub mod temporal_reduce;

// Timeseries join dynamic factory
pub mod timeseries_join;

// Timeseries pivot dynamic factory
pub mod timeseries_pivot;

// Schema validation utilities
pub mod schema_validation;

// Template dynamic factory
pub mod template_factory;

// TinyFS ObjectStore implementation for DataFusion ListingTable integration
pub mod tinyfs_object_store;

// File-table duality integration for TinyFS and DataFusion
pub mod file_table;

// Re-export key types
pub use error::TLogFSError;
pub use persistence::OpLogPersistence;
pub use schema::{DirectoryEntry, OplogEntry};
// Backward compatibility alias
#[allow(deprecated)]
pub use schema::VersionedDirectoryEntry;
pub use transaction_guard::TransactionGuard;
pub use txn_metadata::{PondTxnMetadata, PondUserMetadata};

// Re-export query interfaces for DataFusion integration
pub use query::{execute_sql_on_file, get_file_schema};

// Re-export factory types for easy access
pub use factory::{
    ConfigFile, DYNAMIC_FACTORIES, DynamicFactory, FactoryContext, FactoryRegistry, PondMetadata,
};

// Note: Macros are #[macro_export] so they're automatically available at crate::macro_name
// No need to re-export them here

// Remote storage factory for S3-compatible object stores
pub mod remote_factory;

// Bundle creation for remote backups (tar+zstd streaming compression)
pub mod bundle;

// Test utilities for DRY test patterns
#[cfg(test)]
pub mod test_utils;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod change_detection_tests;
