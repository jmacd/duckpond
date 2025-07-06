//! TinyLogFS - A filesystem implementation using Delta Lake for storage
//! 
//! Set DUCKPOND_LOG environment variable to control logging:
//! - DUCKPOND_LOG=off (default) - silent
//! - DUCKPOND_LOG=info - basic operations  
//! - DUCKPOND_LOG=debug - detailed diagnostics

// Re-export diagnostics for convenience
pub use diagnostics::{log_info, log_debug, init_diagnostics};

// Core schema and data structures
pub mod schema;

// Persistence layer implementation
pub mod persistence;

// Delta table management and caching
pub mod delta_manager;

// DataFusion query interfaces
pub mod query;

// Arrow-backed filesystem object implementations  
pub mod file;
pub mod directory;
pub mod symlink;

// Error types
pub mod error;

// Re-export key types
pub use error::TinyLogFSError;
pub use persistence::{OpLogPersistence, create_oplog_fs};
pub use schema::{OplogEntry, VersionedDirectoryEntry, create_oplog_table};
pub use delta_manager::DeltaTableManager;

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
