use crate::error;
use crate::file::{File, Handle};
use crate::metadata::Metadata;
use async_trait::async_trait;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::pin::Pin;
use std::future::Future;
use tokio::sync::{Mutex, RwLock};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

/// Represents a file backed by memory with integrated write protection
/// This implementation stores file content in a Vec<u8> and manages
/// its own write state to prevent concurrent writes.
pub struct MemoryFile {
    content: Arc<Mutex<Vec<u8>>>,
    write_state: Arc<RwLock<WriteState>>,
}

#[derive(Debug, Clone, PartialEq)]
enum WriteState {
    Ready,
    Writing,
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
    async fn async_reader(&self) -> error::Result<Pin<Box<dyn AsyncRead + Send>>> {
        // Check write state
        let state = self.write_state.read().await;
        if *state == WriteState::Writing {
            return Err(error::Error::Other("File is currently being written".to_string()));
        }
        drop(state);
        
        let content = self.content.lock().await;
        Ok(Box::pin(std::io::Cursor::new(content.clone())))
    }
    
    async fn async_writer(&self) -> error::Result<Pin<Box<dyn AsyncWrite + Send>>> {
        // Acquire write lock
        let mut state = self.write_state.write().await;
        if *state == WriteState::Writing {
            return Err(error::Error::Other("File is already being written".to_string()));
        }
        *state = WriteState::Writing;
        drop(state);
        
        Ok(Box::pin(MemoryFileWriter::new(
            self.content.clone(),
            self.write_state.clone()
        )))
    }
    
    async fn is_being_written(&self) -> bool {
        let state = self.write_state.read().await;
        *state == WriteState::Writing
    }
}

impl MemoryFile {
    /// Create a new MemoryFile handle with the given content
    pub fn new_handle<T: AsRef<[u8]>>(content: T) -> Handle {
        let memory_file = MemoryFile {
            content: Arc::new(Mutex::new(content.as_ref().to_vec())),
            write_state: Arc::new(RwLock::new(WriteState::Ready)),
        };
        Handle::new(Arc::new(Mutex::new(Box::new(memory_file))))
    }
}

/// Writer that resets state on drop
struct MemoryFileWriter {
    content: Arc<Mutex<Vec<u8>>>,
    write_state: Arc<RwLock<WriteState>>,
    buffer: Vec<u8>,
    completed: bool,
    completion_future: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl MemoryFileWriter {
    fn new(content: Arc<Mutex<Vec<u8>>>, write_state: Arc<RwLock<WriteState>>) -> Self {
        Self {
            content,
            write_state,
            buffer: Vec::new(),
            completed: false,
            completion_future: None,
        }
    }
}

impl Drop for MemoryFileWriter {
    fn drop(&mut self) {
        if !self.completed {
            // Reset write state on drop (panic safety)
            if let Ok(mut state) = self.write_state.try_write() {
                *state = WriteState::Ready;
            }
        }
    }
}

impl AsyncWrite for MemoryFileWriter {
    fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, std::io::Error>> {
        if self.completed {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Writer already completed"
            )));
        }
        self.buffer.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }
    
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        if self.completed {
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Writer already completed"
            )))
        } else {
            Poll::Ready(Ok(()))
        }
    }
    
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        if self.completed {
            return Poll::Ready(Ok(()));
        }
        
        // Create completion future if not already created
        if self.completion_future.is_none() {
            let content = self.content.clone();
            let write_state = self.write_state.clone();
            let buffer = std::mem::take(&mut self.buffer);
            
            let future = Box::pin(async move {
                // Update content
                {
                    let mut content_guard = content.lock().await;
                    *content_guard = buffer;
                }
                
                // Reset write state
                {
                    let mut state = write_state.write().await;
                    *state = WriteState::Ready;
                }
            });
            self.completion_future = Some(future);
        }
        
        // Poll the completion future
        match self.completion_future.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Ready(()) => {
                self.completed = true;
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}


