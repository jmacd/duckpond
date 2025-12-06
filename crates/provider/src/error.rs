
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
