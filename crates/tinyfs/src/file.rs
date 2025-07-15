use super::error;
use super::metadata::Metadata;
use async_trait::async_trait;
use std::sync::Arc;
use std::pin::Pin;
use tokio::sync::Mutex;
use tokio::io::{AsyncRead, AsyncWrite};


/// Simple handle wrapper - no external state management
#[derive(Clone)]
pub struct Handle(Arc<tokio::sync::Mutex<Box<dyn File>>>);

/// Represents a file with binary content.
/// This design uses streaming I/O as the fundamental operations.
/// Implementations handle their own state management and write protection.
#[async_trait]
pub trait File: Metadata + Send + Sync {
    /// Create an AsyncRead stream for the file content
    /// Implementations handle their own concurrent read protection
    async fn async_reader(&self) -> error::Result<Pin<Box<dyn AsyncRead + Send>>>;
    
    /// Create an AsyncWrite stream for the file content  
    /// Implementations handle their own write exclusivity
    async fn async_writer(&self) -> error::Result<Pin<Box<dyn AsyncWrite + Send>>>;
    
    /// Check if file is currently being written (optional)
    async fn is_being_written(&self) -> bool {
        false // Default implementation for simple files
    }
}

impl Handle {
    pub fn new(file: Arc<tokio::sync::Mutex<Box<dyn File>>>) -> Self {
        Self(file)
    }
    
    /// Get an async reader - delegated to implementation
    pub async fn async_reader(&self) -> error::Result<Pin<Box<dyn AsyncRead + Send>>> {
        let file = self.0.lock().await;
        file.async_reader().await
    }
    
    /// Get an async writer - delegated to implementation  
    pub async fn async_writer(&self) -> error::Result<Pin<Box<dyn AsyncWrite + Send>>> {
        let file = self.0.lock().await;
        file.async_writer().await
    }

    /// Get metadata through the file handle
    pub async fn metadata_u64(&self, name: &str) -> error::Result<Option<u64>> {
        let file = self.0.lock().await;
        file.metadata_u64(name).await
    }

    /// Internal method: Write file content directly (used by buffer helpers)
    pub async fn write_file(&self, content: &[u8]) -> error::Result<()> {
        let mut writer = self.async_writer().await?;
        use tokio::io::AsyncWriteExt;
        writer.write_all(content).await.map_err(|e| {
            error::Error::Other(format!("Failed to write file content: {}", e))
        })?;
        writer.shutdown().await.map_err(|e| {
            error::Error::Other(format!("Failed to complete file write: {}", e))
        })
    }
}


