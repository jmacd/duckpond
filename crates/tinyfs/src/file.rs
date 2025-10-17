use super::error;
use super::metadata::Metadata;
use async_trait::async_trait;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};
use tokio::sync::Mutex;

/// Trait that combines AsyncRead and AsyncSeek for random access file operations
pub trait AsyncReadSeek: AsyncRead + AsyncSeek + Send + Unpin {}

/// Blanket implementation for types that implement both AsyncRead and AsyncSeek
impl<T: AsyncRead + AsyncSeek + Send + Unpin> AsyncReadSeek for T {}

/// Simple handle wrapper - no external state management
#[derive(Clone)]
pub struct Handle(Arc<tokio::sync::Mutex<Box<dyn File>>>);

/// Represents a file with binary content.
/// This design uses streaming I/O as the fundamental operations.
/// Implementations handle their own state management and write protection.
#[async_trait]
pub trait File: Metadata + Send + Sync {
    /// Create a reader stream - implementation specific
    async fn async_reader(&self) -> error::Result<Pin<Box<dyn AsyncReadSeek>>>;

    /// Create a writer stream - implementation specific  
    async fn async_writer(&self) -> error::Result<Pin<Box<dyn AsyncWrite + Send>>>;

    /// Allow downcasting to concrete file types
    fn as_any(&self) -> &dyn std::any::Any;
}

impl Handle {
    pub fn new(file: Arc<tokio::sync::Mutex<Box<dyn File>>>) -> Self {
        Self(file)
    }

    /// Get an async reader - delegated to implementation
    pub async fn async_reader(&self) -> error::Result<Pin<Box<dyn AsyncReadSeek>>> {
        let file = self.0.lock().await;
        file.async_reader().await
    }

    /// Get an async writer - delegated to implementation  
    pub async fn async_writer(&self) -> error::Result<Pin<Box<dyn AsyncWrite + Send>>> {
        let file = self.0.lock().await;
        file.async_writer().await
    }

    /// Get metadata through the file handle
    pub async fn metadata(&self) -> error::Result<crate::NodeMetadata> {
        let file = self.0.lock().await;
        file.metadata().await
    }

    /// Internal method: Write file content directly (used by buffer helpers)
    pub async fn write_file(&self, content: &[u8]) -> error::Result<()> {
        let mut writer = self.async_writer().await?;
        use tokio::io::AsyncWriteExt;
        writer
            .write_all(content)
            .await
            .map_err(|e| error::Error::Other(format!("Failed to write file content: {}", e)))?;
        writer
            .shutdown()
            .await
            .map_err(|e| error::Error::Other(format!("Failed to complete file write: {}", e)))
    }

    /// Access the underlying file for downcasting (clones the Arc)
    pub async fn get_file(&self) -> Arc<tokio::sync::Mutex<Box<dyn File>>> {
        self.0.clone()
    }
}
