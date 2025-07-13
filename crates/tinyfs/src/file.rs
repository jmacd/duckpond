use super::error;
use super::metadata::Metadata;
use async_trait::async_trait;
use std::ops::Deref;
use std::sync::Arc;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::Mutex;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::fs::File as TokioFile;
use tempfile::NamedTempFile;

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
    
    /// Create an AsyncRead stream for the file content
    /// Default implementation wraps read_to_vec() for backward compatibility
    async fn async_reader(&self) -> error::Result<Pin<Box<dyn AsyncRead + Send>>> {
        let content = self.read_to_vec().await?;
        Ok(Box::pin(std::io::Cursor::new(content)))
    }
    
    /// Create an AsyncWrite stream for the file content
    /// Default implementation: not supported at trait level, handle at Handle level
    async fn async_writer(&mut self) -> error::Result<Pin<Box<dyn AsyncWrite + Send>>> {
        Err(error::Error::Other("async_writer not supported at trait level - use Handle".to_string()))
    }
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

    /// Get an async reader for streaming the file content
    pub async fn async_reader(&self) -> error::Result<Pin<Box<dyn AsyncRead + Send>>> {
        let file = self.0.lock().await;
        file.async_reader().await
    }
    
    /// Get an async writer for streaming content to the file
    pub async fn async_writer(&self) -> error::Result<FileWriter> {
        Ok(FileWriter::new(self.clone()))
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

/// Memory threshold for switching to file-based buffering (e.g., 1MB)
const MEMORY_BUFFER_THRESHOLD: usize = 1024 * 1024;

/// Hybrid writer that buffers in memory up to threshold, then spills to temp file
pub struct HybridWriter {
    memory_buffer: Option<Vec<u8>>,
    temp_file: Option<TokioFile>,
    temp_path: Option<std::path::PathBuf>,
    total_written: usize,
}

impl HybridWriter {
    pub fn new() -> Self {
        Self {
            memory_buffer: Some(Vec::new()),
            temp_file: None,
            temp_path: None,
            total_written: 0,
        }
    }
    
    /// Get the final data - either from memory buffer or temp file
    pub async fn into_data(self) -> std::io::Result<Vec<u8>> {
        if let Some(buffer) = self.memory_buffer {
            // Small file - return memory buffer
            Ok(buffer)
        } else if let Some(temp_path) = self.temp_path {
            // Large file - read from temp file
            let data = tokio::fs::read(&temp_path).await?;
            // Clean up temp file
            let _ = tokio::fs::remove_file(&temp_path).await;
            Ok(data)
        } else {
            Ok(Vec::new())
        }
    }
    
    /// Spill memory buffer to temporary file
    async fn spill_to_temp_file(&mut self) -> std::io::Result<()> {
        if self.temp_file.is_some() {
            return Ok(()); // Already spilled
        }
        
        // Create temporary file
        let temp_file = NamedTempFile::new()?;
        let temp_path = temp_file.path().to_path_buf();
        let mut tokio_file = TokioFile::create(&temp_path).await?;
        
        // Write existing buffer to temp file
        if let Some(buffer) = self.memory_buffer.take() {
            use tokio::io::AsyncWriteExt;
            tokio_file.write_all(&buffer).await?;
            tokio_file.flush().await?;
        }
        
        self.temp_file = Some(tokio_file);
        self.temp_path = Some(temp_path);
        
        Ok(())
    }
}

impl AsyncWrite for HybridWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let this = &mut *self;
        
        // Check if we need to spill to temp file
        if this.memory_buffer.is_some() && 
           this.total_written + buf.len() > MEMORY_BUFFER_THRESHOLD {
            
            // Need to spill - but we can't do async work in poll_write
            // So we'll use a waker-based approach
            let mut spill_future = Box::pin(this.spill_to_temp_file());
            match spill_future.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    // Successfully spilled, continue with file write
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
        
        // Write to appropriate destination
        if let Some(ref mut buffer) = this.memory_buffer {
            // Still in memory mode
            buffer.extend_from_slice(buf);
            this.total_written += buf.len();
            Poll::Ready(Ok(buf.len()))
        } else if let Some(ref mut temp_file) = this.temp_file {
            // In temp file mode
            use tokio::io::AsyncWriteExt;
            let mut write_future = Box::pin(temp_file.write_all(buf));
            match write_future.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    this.total_written += buf.len();
                    Poll::Ready(Ok(buf.len()))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Writer in invalid state"
            )))
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        if let Some(ref mut temp_file) = self.temp_file {
            use tokio::io::AsyncWriteExt;
            let mut flush_future = Box::pin(temp_file.flush());
            flush_future.as_mut().poll(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        if let Some(ref mut temp_file) = self.temp_file {
            use tokio::io::AsyncWriteExt;
            let mut shutdown_future = Box::pin(temp_file.shutdown());
            shutdown_future.as_mut().poll(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

/// File writer that uses hybrid buffering strategy
pub struct FileWriter {
    handle: Handle,
    writer: Option<HybridWriter>,
}

impl FileWriter {
    pub fn new(handle: Handle) -> Self {
        Self {
            handle,
            writer: Some(HybridWriter::new()),
        }
    }
}

impl AsyncWrite for FileWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        if let Some(ref mut writer) = self.writer {
            Pin::new(writer).poll_write(cx, buf)
        } else {
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Writer already closed"
            )))
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        if let Some(ref mut writer) = self.writer {
            Pin::new(writer).poll_flush(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        let this = &mut *self;
        
        // First shutdown the hybrid writer
        if let Some(ref mut writer) = this.writer {
            if let Poll::Pending = Pin::new(writer).poll_shutdown(cx)? {
                return Poll::Pending;
            }
        }
        
        // Extract data and write to file
        if let Some(writer) = this.writer.take() {
            let handle = this.handle.clone();
            
            // Spawn task to extract data and write to file
            tokio::spawn(async move {
                match writer.into_data().await {
                    Ok(data) => {
                        if let Err(e) = handle.write_file(&data).await {
                            eprintln!("Failed to write file: {}", e);
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to extract writer data: {}", e);
                    }
                }
            });
        }
        
        Poll::Ready(Ok(()))
    }
}
