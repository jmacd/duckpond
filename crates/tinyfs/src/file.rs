use super::error;
use super::metadata::Metadata;
use async_trait::async_trait;
use std::ops::Deref;
use std::sync::Arc;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::Mutex;
use tokio::io::{AsyncRead, AsyncWrite};


/// A handle for a refcounted file with write protection.
#[derive(Clone)]
pub struct Handle {
    inner: Arc<tokio::sync::Mutex<Box<dyn File>>>,
    state: Arc<tokio::sync::RwLock<FileState>>,
}

/// Tracks whether a file is currently being written to prevent concurrent access.
#[derive(Debug, Clone, PartialEq)]
enum FileState {
    Ready,   // Available for read/write operations
    Writing, // Being written via streaming - reads return error
}

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
}

impl Handle {
    pub fn new(r: Arc<tokio::sync::Mutex<Box<dyn File>>>) -> Self {
        Self {
            inner: r,
            state: Arc::new(tokio::sync::RwLock::new(FileState::Ready)),
        }
    }

    pub async fn content(&self) -> error::Result<Vec<u8>> {
        // Check if file is being written
        let state = self.state.read().await;
        if *state == FileState::Writing {
            return Err(error::Error::Other("File is currently being written".to_string()));
        }
        drop(state);
        
        let file = self.inner.lock().await;
        file.read_to_vec().await
    }
    
    pub async fn write_file(&self, content: &[u8]) -> error::Result<()> {
        // Direct write - used internally by StreamingFileWriter
        // No state check needed since this is called from within the writer
        let mut file = self.inner.lock().await;
        file.write_from_slice(content).await
    }
    
    /// Write file content with state checking (public API)
    pub async fn write_file_checked(&self, content: &[u8]) -> error::Result<()> {
        // Check if file is being written
        let state = self.state.read().await;
        if *state == FileState::Writing {
            return Err(error::Error::Other("File is currently being written".to_string()));
        }
        drop(state);
        
        self.write_file(content).await
    }

    /// Get an async reader for streaming the file content
    pub async fn async_reader(&self) -> error::Result<Pin<Box<dyn AsyncRead + Send>>> {
        // Check if file is being written
        let state = self.state.read().await;
        if *state == FileState::Writing {
            return Err(error::Error::Other("File is currently being written".to_string()));
        }
        drop(state);
        
        let file = self.inner.lock().await;
        file.async_reader().await
    }
    
    /// Get an async writer for streaming content to the file
    /// This creates a StreamingFileWriter that holds a write lock
    pub async fn async_writer(&self) -> error::Result<StreamingFileWriter> {
        // Acquire write lock - this will fail if already writing
        let mut state = self.state.write().await;
        if *state == FileState::Writing {
            return Err(error::Error::Other("File is already being written".to_string()));
        }
        *state = FileState::Writing;
        
        Ok(StreamingFileWriter::new(self.clone(), WriteGuard::new(self.state.clone())))
    }

    /// Get metadata through the file handle
    pub async fn metadata_u64(&self, name: &str) -> error::Result<Option<u64>> {
        let file = self.inner.lock().await;
        file.metadata_u64(name).await
    }
}

impl Deref for Handle {
    type Target = Arc<Mutex<Box<dyn File>>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// A guard that ensures the file state is reset to Ready when dropped
struct WriteGuard {
    state: Arc<tokio::sync::RwLock<FileState>>,
}

impl WriteGuard {
    fn new(state: Arc<tokio::sync::RwLock<FileState>>) -> Self {
        Self { state }
    }
}

impl Drop for WriteGuard {
    fn drop(&mut self) {
        // Reset state to Ready when writer is dropped
        // We use try_write to avoid blocking in Drop
        if let Ok(mut state) = self.state.try_write() {
            *state = FileState::Ready;
        }
        // If we can't acquire the lock immediately, we're in a problematic state
        // but there's not much we can do in Drop
    }
}

/// Streaming file writer that holds a write lock to prevent concurrent access
pub struct StreamingFileWriter {
    handle: Handle,
    buffer: Vec<u8>,
    _write_guard: WriteGuard, // Holds the write lock
    write_result_rx: Option<tokio::sync::oneshot::Receiver<Result<(), crate::error::Error>>>,
}

/// Type alias for backward compatibility
pub type FileWriter = StreamingFileWriter;

impl StreamingFileWriter {
    fn new(handle: Handle, write_guard: WriteGuard) -> Self {
        Self {
            handle,
            buffer: Vec::new(),
            _write_guard: write_guard,
            write_result_rx: None,
        }
    }
}

impl AsyncWrite for StreamingFileWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        // Simply append to our buffer
        self.buffer.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        // Nothing to flush for memory buffer
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        let this = &mut *self;
        
        // Start the write task if not already started
        if this.write_result_rx.is_none() {
            let data = std::mem::take(&mut this.buffer);
            if !data.is_empty() {
                let handle = this.handle.clone();
                let (tx, rx) = tokio::sync::oneshot::channel();
                this.write_result_rx = Some(rx);
                
                // Spawn the write task
                tokio::spawn(async move {
                    let result = handle.write_file(&data).await;
                    let _ = tx.send(result); // Ignore send errors (receiver dropped)
                });
            } else {
                // Empty buffer, nothing to write
                return Poll::Ready(Ok(()));
            }
        }
        
        // Poll the receiver for the write result
        if let Some(ref mut rx) = this.write_result_rx {
            match Pin::new(rx).poll(cx) {
                Poll::Ready(Ok(Ok(()))) => {
                    this.write_result_rx = None;
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Ok(Err(e))) => {
                    this.write_result_rx = None;
                    Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, e)))
                }
                Poll::Ready(Err(_)) => {
                    // Sender was dropped - treat as cancelled
                    this.write_result_rx = None;
                    Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::Other, 
                        "Write task was cancelled"
                    )))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }
}
