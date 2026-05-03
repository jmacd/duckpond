// SPDX-License-Identifier: Apache-2.0

//! Errors returned by the [`crate`] crate.

/// Errors returned from the remote-sync layer.
#[derive(Debug, thiserror::Error)]
pub enum RemoteError {
    /// IO error from the local filesystem.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Underlying store error.
    #[error("store error: {0}")]
    Store(#[from] sandbox_store::StoreError),

    /// Underlying steward error.
    #[error("steward error: {0}")]
    Steward(#[from] sandbox_steward::StewardError),

    /// Delta Lake error.
    #[error("delta error: {0}")]
    Delta(#[from] deltalake::DeltaTableError),

    /// Arrow error from schema/batch construction.
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// JSON encoding/decoding error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// A row violated the union shape implied by `partition_kind`
    /// (e.g., a `manifest` row missing `commit_kind`).  Indicates a
    /// foreign writer or schema corruption.
    #[error("schema violation: {0}")]
    Schema(String),

    /// The remote was opened but its `sandbox.store_id` Delta table
    /// property is missing or unparseable.
    #[error("invalid remote: {0}")]
    InvalidRemote(String),

    /// The remote's `store_id` does not match the steward attempting
    /// to push or pull.
    #[error("store_id mismatch: remote={remote}, steward={steward}")]
    StoreIdMismatch {
        /// The remote's recorded store_id.
        remote: uuid::Uuid,
        /// The steward's store_id.
        steward: uuid::Uuid,
    },

    /// Push or pull was asked about a `txn_seq` for which no
    /// `DataCommitted` record exists locally.
    #[error("no DataCommitted at txn_seq {0}")]
    NoSuchCommit(i64),
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, RemoteError>;
