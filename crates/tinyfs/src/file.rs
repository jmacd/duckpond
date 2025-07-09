use super::error;
use super::metadata::Metadata;
use async_trait::async_trait;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::Mutex;

/// A handle for a refcounted file.
#[derive(Clone)]
pub struct Handle(Arc<tokio::sync::Mutex<Box<dyn File>>>);

/// Represents a file with binary content.
/// This design eliminates local state by delegating all operations
/// to the persistence layer as the single source of truth.
#[async_trait]
pub trait File: Metadata + Send + Sync {
    /// Read the entire file content into a Vec<u8>
    /// This is the fundamental operation - all content flows through the persistence layer
    async fn read_to_vec(&self) -> error::Result<Vec<u8>>;
    
    /// Write content to the file from a byte slice
    /// This stores content via the persistence layer immediately
    async fn write_from_slice(&mut self, content: &[u8]) -> error::Result<()>;
}

impl Handle {
    pub fn new(r: Arc<tokio::sync::Mutex<Box<dyn File>>>) -> Self {
        Self(r)
    }

    pub async fn content(&self) -> error::Result<Vec<u8>> {
        let file = self.0.lock().await;
        file.read_to_vec().await
    }
    
    pub async fn write_file(&self, content: &[u8]) -> error::Result<()> {
        let mut file = self.0.lock().await;
        file.write_from_slice(content).await
    }

    /// Get metadata through the file handle
    pub async fn metadata_u64(&self, name: &str) -> error::Result<Option<u64>> {
        let file = self.0.lock().await;
        file.metadata_u64(name).await
    }
}

impl Deref for Handle {
    type Target = Arc<Mutex<Box<dyn File>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
