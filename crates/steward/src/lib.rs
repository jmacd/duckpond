// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Steward - Transaction coordination and audit logging
//!
//! Steward manages transaction lifecycle for the data filesystem (TLogFS),
//! recording transaction metadata in a separate control table (Delta Lake).
//! It sequences post-commit actions and enables recovery from incomplete transactions.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;

mod control_table;
mod guard;
mod ship;

pub use control_table::ControlTable;
pub use guard::StewardTransactionGuard;
pub use ship::Ship;
pub use tlogfs::{PondMetadata, PondTxnMetadata, PondUserMetadata};

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

    #[error("Recovery needed: incomplete transaction {txn_meta:?}. Run 'recover' command.")]
    RecoveryNeeded { txn_meta: PondTxnMetadata },

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
#[must_use]
pub fn get_data_path(pond_path: &Path) -> PathBuf {
    pond_path.join("data")
}

/// Get the control table path under the pond
#[must_use]
pub fn get_control_path(pond_path: &Path) -> PathBuf {
    pond_path.join("control")
}
