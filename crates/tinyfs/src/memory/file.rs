use crate::error;
use crate::file::{File, Handle};
use crate::metadata::Metadata;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Represents a file backed by memory
/// This implementation stores file content in a Vec<u8> and is suitable for
/// testing, development, and lightweight filesystem operations.
pub struct MemoryFile {
    content: Vec<u8>,
}

#[async_trait]
impl Metadata for MemoryFile {
    async fn metadata_u64(&self, _name: &str) -> error::Result<Option<u64>> {
        // Memory files don't have persistent metadata
        Ok(None)
    }
}

#[async_trait]
impl File for MemoryFile {
    async fn read_to_vec(&self) -> error::Result<Vec<u8>> {
        Ok(self.content.clone())
    }
    
    async fn write_from_slice(&mut self, content: &[u8]) -> error::Result<()> {
        self.content = content.to_vec();
        Ok(())
    }
    
    async fn async_reader(&self) -> error::Result<std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>> {
        use std::io::Cursor;
        Ok(Box::pin(Cursor::new(self.content.clone())))
    }
    
    async fn async_writer(&mut self) -> error::Result<std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send>>> {
        // For MemoryFile, we'll use the default implementation which errors
        // The Handle wrapper provides the actual async_writer functionality
        Err(error::Error::Other("async_writer not supported at trait level - use Handle".to_string()))
    }
}

impl MemoryFile {
    /// Create a new MemoryFile handle with the given content
    pub fn new_handle<T: AsRef<[u8]>>(content: T) -> Handle {
        Handle::new(Arc::new(tokio::sync::Mutex::new(Box::new(MemoryFile {
            content: content.as_ref().to_vec(),
        }))))
    }
}
