// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

/// Provider error types
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid URL format
    #[error("Invalid URL: {0}")]
    InvalidUrl(String),

    /// Decompression error
    #[error("Decompression error: {0}")]
    DecompressionError(String),

    /// Invalid cache key type
    #[error("Invalid cache key type")]
    InvalidCacheKey,

    /// Mutex poisoned error
    #[error("Mutex poisoned: {0}")]
    MutexPoisoned(String),

    /// Session context error
    #[error("Session context error: {0}")]
    SessionContext(String),

    /// DataFusion error
    #[error("DataFusion error: {0}")]
    DataFusionError(#[from] datafusion::error::DataFusionError),

    /// Arrow error
    #[error("Arrow error: {0}")]
    Arrow(String),

    /// State handle error (invalid downcast)
    #[error("State handle error: {0}")]
    StateHandle(String),

    /// TLogFS error (for implementations in tlogfs crate)
    #[error("TLogFS error: {0}")]
    TLogFS(String),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// TinyFS error
    #[error("TinyFS error: {0}")]
    TinyFs(#[from] tinyfs::Error),

    /// URL parse
    #[error("URL parse error: {0}")]
    ParseUrl(#[from] url::ParseError),
}

/// Result type for provider operations
pub type Result<T> = std::result::Result<T, Error>;
