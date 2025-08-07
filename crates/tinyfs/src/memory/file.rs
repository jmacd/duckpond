use crate::error;
use crate::file::{File, Handle, AsyncReadSeek};
use crate::metadata::{Metadata, NodeMetadata};
use crate::EntryType;
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
    entry_type: EntryType,
}

#[derive(Debug, Clone, PartialEq)]
enum WriteState {
    Ready,
    Writing,
}

#[async_trait]
impl Metadata for MemoryFile {
    async fn metadata(&self) -> error::Result<NodeMetadata> {
        let content = self.content.lock().await;
        let size = content.len() as u64;
        
        // For memory files, we'll compute a simple hash for now
        // In a real implementation, we'd use SHA256
        let sha256 = format!("{:016x}", simple_hash(&content));
        
        Ok(NodeMetadata {
            version: 1, // Memory files don't track versions
            size: Some(size),
            sha256: Some(sha256),
            entry_type: self.entry_type, // Use the stored entry type
	    timestamp: 0, // TODO
        })
    }
}

/// Simple hash function for memory files (not cryptographically secure)
fn simple_hash(data: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    data.hash(&mut hasher);
    hasher.finish()
}

#[async_trait]
impl File for MemoryFile {
    async fn async_reader(&self) -> error::Result<Pin<Box<dyn AsyncReadSeek>>> {
        // Check write state
        let state = self.write_state.read().await;
        if *state == WriteState::Writing {
            return Err(error::Error::Other("File is currently being written".to_string()));
        }
        drop(state);
        
        let content = self.content.lock().await;
        // std::io::Cursor implements both AsyncRead and AsyncSeek
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
}

impl MemoryFile {
    /// Create a new MemoryFile handle with the given content
    pub fn new_handle<T: AsRef<[u8]>>(content: T) -> Handle {
        let memory_file = MemoryFile {
            content: Arc::new(Mutex::new(content.as_ref().to_vec())),
            write_state: Arc::new(RwLock::new(WriteState::Ready)),
            entry_type: EntryType::FileData, // Default for memory files
        };
        Handle::new(Arc::new(Mutex::new(Box::new(memory_file))))
    }
    
    /// Create a new MemoryFile handle with the given content and entry type
    pub fn new_handle_with_entry_type<T: AsRef<[u8]>>(content: T, entry_type: EntryType) -> Handle {
        let memory_file = MemoryFile {
            content: Arc::new(Mutex::new(content.as_ref().to_vec())),
            write_state: Arc::new(RwLock::new(WriteState::Ready)),
            entry_type,
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


