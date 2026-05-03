// SPDX-License-Identifier: Apache-2.0

//! Errors returned by the steward crate.

/// Errors returned from the [`crate::Steward`] and related types.
#[derive(Debug, thiserror::Error)]
pub enum StewardError {
    /// The pond path was invalid.
    #[error("invalid path: {0}")]
    InvalidPath(String),

    /// IO error from the local filesystem.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Underlying store error.
    #[error("store error: {0}")]
    Store(#[from] sandbox_store::StoreError),

    /// Delta Lake error from the control table.
    #[error("delta error: {0}")]
    Delta(#[from] deltalake::DeltaTableError),

    /// Arrow error.
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// DataFusion error.
    #[error("datafusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    /// JSON encoding/decoding error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// A logical invariant was violated.
    #[error("invariant violation: {0}")]
    Invariant(String),

    /// Recovery is required (incomplete transaction detected).  The
    /// prototype reports but does not auto-roll-back; an operator can
    /// choose how to proceed.
    #[error("recovery needed: {0}")]
    RecoveryNeeded(String),

    /// A read guard tried to mutate state, or some similar API
    /// misuse caught at runtime.
    #[error("api misuse: {0}")]
    ApiMisuse(String),

    /// The pond was created without a `store_id` setting, so it
    /// predates the identity invariant.  Sandbox does not perform
    /// silent backfill; the operator must explicitly migrate or
    /// recreate the pond.
    #[error("legacy pond: {0}")]
    LegacyPond(String),
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, StewardError>;
