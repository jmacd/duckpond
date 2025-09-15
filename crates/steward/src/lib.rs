//! Steward - Secondary filesystem for monitoring and post-commit actions
//!
//! The steward manages a primary "data" filesystem and a secondary "control" filesystem,
//! both implemented using tlogfs. It sequences post-commit actions and maintains
//! transaction metadata in the control filesystem.

use std::path::{Path, PathBuf};
use thiserror::Error;
use serde::{Deserialize, Serialize};

mod ship;
mod guard;

pub use ship::Ship;
pub use guard::StewardTransactionGuard;

/// Transaction descriptor containing command information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxDesc {
    pub txn_id: String,
    pub args: Vec<String>,
}

/// Recovery command result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryResult {
    /// Number of transactions recovered
    pub recovered_count: u64,
    /// Whether recovery was needed
    pub was_needed: bool,
}

impl TxDesc {
    /// Create a new transaction descriptor from command arguments
    pub fn new(txn_id: &str, args: Vec<String>) -> Self {
        Self { txn_id: txn_id.into(), args }
    }
    
    /// Get the command name (first argument)
    pub fn command_name(&self) -> Option<&str> {
        self.args.first().map(|s| s.as_str())
    }
    
    /// Serialize to JSON string for storage in /txn/* files
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
    
    /// Deserialize from JSON string
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

#[derive(Debug, Error)]
pub enum StewardError {
    #[error("Failed to initialize data filesystem: {0}")]
    DataInit(#[from] tlogfs::TLogFSError),
    
    #[error("Failed to initialize control filesystem: {0}")]
    ControlInit(tlogfs::TLogFSError),
    
    #[error("Transaction sequence mismatch: expected {expected}, found {actual}")]
    TransactionSequenceMismatch { expected: u64, actual: u64 },
    
    #[error("Recovery needed: missing transaction file /txn/{txn_id}. Run 'recover' command.")]
    RecoveryNeeded { txn_id: String, tx_desc: TxDesc },
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
    
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
