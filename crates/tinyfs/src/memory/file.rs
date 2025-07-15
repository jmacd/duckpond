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
    content: Arc<tokio::sync::Mutex<Vec<u8>>>,
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
    async fn async_reader(&self) -> error::Result<std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>> {
        use std::io::Cursor;
        let content = self.content.lock().await;
        Ok(Box::pin(Cursor::new(content.clone())))
    }
    
    async fn async_writer(&mut self) -> error::Result<std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send>>> {
        let content_ref = self.content.clone();
        let completion_fn = move |buffer: Vec<u8>| {
            Box::pin(async move {
                let mut content = content_ref.lock().await;
                *content = buffer;
                Ok(())
            }) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), std::io::Error>> + Send>>
        };
        
        Ok(Box::pin(crate::async_helpers::SimpleBufferedWriter::new(completion_fn)))
    }
}

impl MemoryFile {
    /// Create a new MemoryFile handle with the given content
    pub fn new_handle<T: AsRef<[u8]>>(content: T) -> Handle {
        Handle::new(Arc::new(tokio::sync::Mutex::new(Box::new(MemoryFile {
            content: Arc::new(tokio::sync::Mutex::new(content.as_ref().to_vec())),
        }))))
    }
}


