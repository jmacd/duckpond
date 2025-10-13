//! Steward - Secondary filesystem for monitoring and post-commit actions
//!
//! The steward manages a primary "data" filesystem and a secondary "control" filesystem,
//! both implemented using tlogfs. It sequences post-commit actions and maintains
//! transaction metadata in the control filesystem.

use std::path::{Path, PathBuf};
use std::collections::HashMap;
use thiserror::Error;
use serde::{Deserialize, Serialize};

mod ship;
mod guard;
mod control_table;

pub use ship::Ship;
pub use guard::StewardTransactionGuard;
pub use control_table::ControlTable;

/// Options for beginning a transaction
#[derive(Debug, Clone, Default)]
pub struct TransactionOptions {
    /// Command-line arguments for transaction metadata
    pub args: Vec<String>,
    /// Template variables for dynamic configuration
    pub variables: HashMap<String, String>,
    /// Whether this is a write transaction (true) or read transaction (false)
    pub is_write: bool,
}

impl TransactionOptions {
    /// Create options for a read transaction
    pub fn read(args: Vec<String>) -> Self {
        Self {
            args,
            variables: HashMap::new(),
            is_write: false,
        }
    }

    /// Create options for a write transaction
    pub fn write(args: Vec<String>) -> Self {
        Self {
            args,
            variables: HashMap::new(),
            is_write: true,
        }
    }

    /// Add template variables (builder pattern)
    pub fn with_variables(mut self, variables: HashMap<String, String>) -> Self {
        self.variables = variables;
        self
    }
}

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
    
    #[error("Control table error: {0}")]
    ControlTable(String),
    
    #[error("Transaction sequence mismatch: expected {expected}, found {actual}")]
    TransactionSequenceMismatch { expected: i64, actual: i64 },
    
    #[error("Recovery needed: incomplete transaction seq={txn_seq:?}, id={txn_id}. Run 'recover' command.")]
    RecoveryNeeded { txn_seq: Option<i64>, txn_id: String, tx_desc: TxDesc },
    
    #[error("Transaction mode violation: write transaction made no changes (should have been read transaction)")]
    WriteTransactionNoChanges,
    
    #[error("Transaction mode violation: read transaction attempted to write data")]
    ReadTransactionAttemptedWrite,
    
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
