//! Steward - Secondary filesystem for monitoring and post-commit actions
//!
//! The steward manages a primary "data" filesystem and a secondary "control" filesystem,
//! both implemented using tlogfs. It sequences post-commit actions and maintains
//! transaction metadata in the control filesystem.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;

mod control_table;
mod guard;
mod ship;

pub use control_table::ControlTable;
pub use guard::StewardTransactionGuard;
pub use ship::Ship;
pub use tlogfs::{PondUserMetadata, PondTxnMetadata, PondMetadata};

/// Pond identity metadata - immutable information about the pond's origin
/// This metadata is created once when the pond is initialized and preserved across replicas
/// This is a MESS! Look at consolidating with PondTxnMetadata.

/// Recovery command result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryResult {
    /// Number of transactions recovered
    pub recovered_count: u64,
    /// Whether recovery was needed
    pub was_needed: bool,
}

#[derive(Debug, Error)]
pub enum StewardError {
    #[error("TinyFS error: {0}")]
    DataInit(#[from] tlogfs::TLogFSError),

    #[error("Control table error: {0}")]
    ControlTable(String),

    #[error("Transaction aborted: {0}")]
    Aborted(String),

    #[error("Transaction sequence mismatch: expected {expected}, found {actual}")]
    TransactionSequenceMismatch { expected: i64, actual: i64 },

    #[error(
        "Recovery needed: incomplete transaction {txn_meta:?}. Run 'recover' command."
    )]
    RecoveryNeeded {
        txn_meta: PondTxnMetadata,
    },

    #[error(
        "Transaction mode violation: write transaction made no changes (should have been read transaction)"
    )]
    WriteTransactionNoChanges,

    #[error("Transaction mode violation: read transaction attempted to write data")]
    ReadTransactionAttemptedWrite,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("uuid7 parse error: {0}")]
    Uuid(#[from] uuid7::ParseError),
    
    #[error("Delta Lake error: {0}")]
    DeltaLake(String),

    #[error("Dynamic error: {0}")]
    Dyn(Box<dyn std::error::Error + Send + Sync>),
}

/// Get the data filesystem path under the pond
pub fn get_data_path(pond_path: &Path) -> PathBuf {
    pond_path.join("data")
}

/// Get the control filesystem path under the pond
pub fn get_control_path(pond_path: &Path) -> PathBuf {
    pond_path.join("control")
}
