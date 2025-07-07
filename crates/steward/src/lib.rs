//! Steward - Secondary filesystem for monitoring and post-commit actions
//!
//! The steward manages a primary "data" filesystem and a secondary "control" filesystem,
//! both implemented using tlogfs. It sequences post-commit actions and maintains
//! transaction metadata in the control filesystem.

use std::path::{Path, PathBuf};
use thiserror::Error;

mod ship;

pub use ship::Ship;

#[derive(Debug, Error)]
pub enum StewardError {
    #[error("Failed to initialize data filesystem: {0}")]
    DataInit(#[from] tlogfs::TLogFSError),
    
    #[error("Failed to initialize control filesystem: {0}")]
    ControlInit(tlogfs::TLogFSError),
    
    #[error("Transaction sequence mismatch: expected {expected}, found {actual}")]
    TransactionSequenceMismatch { expected: u64, actual: u64 },
    
    #[error("Recovery needed: missing transaction file /txn/{sequence}")]
    RecoveryNeeded { sequence: u64 },
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Get the data filesystem path under the pond
pub fn get_data_path(pond_path: &Path) -> PathBuf {
    pond_path.join("data")
}

/// Get the control filesystem path under the pond
pub fn get_control_path(pond_path: &Path) -> PathBuf {
    pond_path.join("control")
}
