use super::error;
use async_trait::async_trait;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::Mutex;

/// A handle for a refcounted file.
#[derive(Clone)]
pub struct Handle(Arc<tokio::sync::Mutex<Box<dyn File>>>);

/// Represents a file with binary content
#[async_trait]
pub trait File: Send + Sync {
    async fn content(&self) -> error::Result<&[u8]>;
    async fn write_content(&mut self, content: &[u8]) -> error::Result<()>;
}

impl Handle {
    pub fn new(r: Arc<tokio::sync::Mutex<Box<dyn File>>>) -> Self {
        Self(r)
    }

    pub async fn content(&self) -> error::Result<Vec<u8>> {
        let file = self.0.lock().await;
        Ok(file.content().await?.to_vec())
    }
    
    pub async fn write_file(&self, content: &[u8]) -> error::Result<()> {
        let mut file = self.0.lock().await;
        file.write_content(content).await
    }
}

impl Deref for Handle {
    type Target = Arc<Mutex<Box<dyn File>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
