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

    /// DataFusion error.
    #[error("datafusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

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

    /// The consumer's `last_pulled_seq` is below the remote's oldest
    /// available bundle: bundles between them have been pruned by
    /// retention.  Recovery requires `restart-from-compact`.
    #[error(
        "consumer is below retention horizon: last_pulled={last_pulled}, oldest_available={oldest_available}"
    )]
    BehindRetention {
        /// The consumer's last_pulled_seq before the failed pull.
        last_pulled: i64,
        /// The remote's oldest available bundle seq.
        oldest_available: i64,
    },

    /// `Remote::maintain` was called with `keep_compact_bundles == 0`,
    /// which would prune away all restart points.  Refused.
    #[error("invalid retention: keep_compact_bundles must be >= 1, got {0}")]
    InvalidRetention(usize),

    /// `Remote::maintain` requires at least `need` compact bundles to
    /// retain, but the remote has only `have`.  Refused (would leave
    /// no restart point).
    #[error("insufficient compact bundles: have {have}, need {need}")]
    InsufficientCompactBundles {
        /// How many compact bundles the remote currently has.
        have: usize,
        /// How many were requested for retention.
        need: usize,
    },

    /// `Remote::restart_from_compact` was called but the remote has
    /// no compact bundles to bootstrap from.
    #[error("no restart point: remote has no compact bundles")]
    NoRestartPoint,

    /// `Remote::restart_from_compact` was asked to wipe a path that
    /// is not a pond directory (or the pond there can't be opened).
    /// Refuses to nuke arbitrary user files.
    #[error("restart path is not a pond: {path}: {reason}")]
    RestartPathNotPond {
        /// The path that was checked.
        path: String,
        /// Why it failed validation.
        reason: String,
    },
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, RemoteError>;
