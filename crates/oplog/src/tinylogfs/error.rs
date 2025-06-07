// Error types for TinyLogFS operations
use std::path::PathBuf;
use std::time::SystemTime;

#[derive(Debug, thiserror::Error)]
pub enum TinyLogFSError {
    #[error("OpLog error: {0}")]
    OpLog(#[from] crate::error::Error),
    
    #[error("TinyFS error: {0}")]
    TinyFS(#[from] tinyfs::Error),
    
    #[error("Node not found: {path}")]
    NodeNotFound { path: PathBuf },
    
    #[error("Transaction error: {message}")]
    Transaction { message: String },
    
    #[error("Commit error: {message}")]
    Commit { message: String },
    
    #[error("Restore error: {message}")]
    Restore { message: String },
    
    #[error("Arrow error: {0}")]
    Arrow(String),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_arrow::Error),
    
    #[error("UUID error: {0}")]
    Uuid(#[from] uuid::Error),
}
