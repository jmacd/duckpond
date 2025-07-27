// Error types for TLogFS operations
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum TLogFSError {
    #[error("Delta Lake error: {0}")]
    Delta(#[from] deltalake::DeltaTableError),
    
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    
    #[error("TinyFS error: {0}")]
    TinyFS(#[from] tinyfs::Error),
    
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    
    #[error("Node not found: {path}")]
    NodeNotFound { path: PathBuf },
    
    #[error("Transaction error: {message}")]
    Transaction { message: String },
    
    #[error("Missing data")]
    Missing,
    
    #[error("Commit error: {message}")]
    Commit { message: String },
    
    #[error("Restore error: {message}")]
    Restore { message: String },
    
    #[error("Arrow error: {0}")]
    ArrowSchema(#[from] arrow_schema::ArrowError),
    
    #[error("Arrow error: {0}")]
    ArrowMessage(String),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_arrow::Error),
    
    #[error("Large file not found: {sha256} at path {path}")]
    LargeFileNotFound {
        sha256: String,
        path: String,
        #[source]
        source: std::io::Error,
    },
    
    #[error("Large file integrity check failed: expected {expected}, got {actual}")]
    LargeFileIntegrityError {
        expected: String,
        actual: String,
    },
}

