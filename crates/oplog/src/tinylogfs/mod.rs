// TinyLogFS - Arrow-Native Filesystem Backend Implementation
// Direct implementation of TinyFS backend using low-level Arrow and DataFusion

// Core schema and data structures
pub mod schema;

// Persistence layer implementation
pub mod persistence;

// OLD: Arrow-native backend implementation (Phase 5: Removed old backend system)
// pub mod backend;

// Arrow-backed filesystem object implementations  
pub mod file;
pub mod directory;
pub mod symlink;

// Error types
pub mod error;

// Re-export core components for public API
pub use persistence::{OpLogPersistence, create_oplog_fs}; // Factory function now from persistence layer
pub use schema::{OplogEntry, DirectoryEntry, create_oplog_table};
pub use file::OpLogFile;
// pub use directory::OpLogDirectory; // Temporarily disabled until compilation is fixed
pub use symlink::OpLogSymlink;
pub use error::TinyLogFSError;

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
