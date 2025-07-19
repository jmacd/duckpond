//! TLogFS - A filesystem implementation using Delta Lake for storage
//! 
//! Set DUCKPOND_LOG environment variable to control logging:
//! - DUCKPOND_LOG=off (default) - silent
//! - DUCKPOND_LOG=info - basic operations  
//! - DUCKPOND_LOG=debug - detailed diagnostics

// Re-export diagnostics for convenience
pub use diagnostics::{log_info, log_debug, init_diagnostics};

// Core schema and data structures
pub mod schema;

// Delta Lake integration
pub mod delta;

// Large file storage utilities
pub mod large_files;

// Persistence layer implementation
pub mod persistence;

// DataFusion query interfaces
pub mod query;

// Arrow-backed filesystem object implementations  
pub mod file;
pub mod directory;
pub mod symlink;

// Error types
pub mod error;

// Re-export key types
pub use error::TLogFSError;
pub use persistence::{OpLogPersistence, create_oplog_fs};
pub use schema::{OplogEntry, VersionedDirectoryEntry, create_oplog_table};
pub use delta::DeltaTableManager;

// Integration tests - now enabled with updated architecture
#[cfg(test)]
mod tests;

// Backend query testing
#[cfg(test)]
mod test_backend_query;

// Phase 4 integration tests
#[cfg(test)]
mod test_phase4;

// Persistence layer debug test
#[cfg(test)]
mod test_persistence_debug;

#[cfg(test)]
mod serde_arrow_test;

#[cfg(test)]
mod versioned_directory_test;

#[cfg(test)]
mod oplog_entry_test;

#[cfg(test)]
mod delta_lake_test;

#[cfg(test)]
mod create_oplog_table_debug_test;

//#[cfg(test)]
//mod debug_integration_test;

#[cfg(test)]
mod large_files_tests;
