// TinyLogFS - Refined Hybrid Filesystem Implementation
// Phase 2: Single-threaded architecture with Arrow builder transaction state

// Phase 1 - Schema and utilities (working implementation)
pub mod schema;

// Phase 2 - Refined implementation modules
pub mod filesystem;
pub mod transaction;
pub mod directory;
pub mod error;

// Integration tests for Phase 2
#[cfg(test)]
mod tests;

// Re-export Phase 1 components for backward compatibility
pub use schema::{OplogEntry, DirectoryEntry, create_oplog_table};

// Re-export Phase 2 components
pub use filesystem::TinyLogFS;
pub use transaction::{TransactionState, FilesystemOperation};
pub use directory::OpLogDirectory;
pub use error::TinyLogFSError;
