// Clean architecture File implementation for TinyFS
use tinyfs::{File, Metadata, persistence::PersistenceLayer, NodeID};
use std::sync::Arc;
use std::pin::Pin;
use std::future::Future;
use async_trait::async_trait;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::RwLock;

/// TLogFS file with transaction-integrated state management
/// - Integrates write state with Delta Lake transaction lifecycle
/// - Single source of truth via persistence layer
/// - Proper separation of concerns
pub struct OpLogFile {
    /// Unique node identifier for this file
    node_id: NodeID,
    
    /// Parent directory node ID (for persistence operations)
    parent_node_id: NodeID,
    
    /// Reference to persistence layer (single source of truth)
    persistence: Arc<dyn PersistenceLayer>,
    
    /// Transaction-bound write state
    transaction_state: Arc<RwLock<TransactionWriteState>>,
}

#[derive(Debug, Clone, PartialEq)]
enum TransactionWriteState {
    Ready,
    WritingInTransaction(i64), // Transaction ID
}

impl OpLogFile {
    /// Create new file instance with persistence layer dependency injection
    pub fn new(
        node_id: NodeID,
        parent_node_id: NodeID,
        persistence: Arc<dyn PersistenceLayer>
    ) -> Self {
        let node_id_debug = format!("{:?}", node_id);
        let parent_node_id_debug = format!("{:?}", parent_node_id);
        diagnostics::log_debug!("OpLogFile::new() - creating file with node_id: {node_id}, parent: {parent_node_id}", 
                                node_id: node_id_debug, parent_node_id: parent_node_id_debug);
        
        Self {
            node_id,
            parent_node_id,
            persistence,
            transaction_state: Arc::new(RwLock::new(TransactionWriteState::Ready)),
        }
    }
    
    /// Create a file handle for TinyFS integration
    pub fn create_handle(oplog_file: OpLogFile) -> tinyfs::FileHandle {
        tinyfs::FileHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(oplog_file))))
    }
}

#[async_trait]
impl Metadata for OpLogFile {
    async fn metadata_u64(&self, name: &str) -> tinyfs::Result<Option<u64>> {
        // For files, the partition is the parent directory (parent_node_id)
        self.persistence.metadata_u64(self.node_id, self.parent_node_id, name).await
    }
}

#[async_trait]
impl File for OpLogFile {
    async fn async_reader(&self) -> tinyfs::Result<Pin<Box<dyn AsyncRead + Send + 'static>>> {
        // Check transaction state
        let state = self.transaction_state.read().await;
        if let TransactionWriteState::WritingInTransaction(_) = *state {
            return Err(tinyfs::Error::Other("File is being written in active transaction".to_string()));
        }
        drop(state);
        
        diagnostics::log_debug!("OpLogFile::async_reader() - loading content via persistence layer");
        
        // Load file content directly from persistence layer (avoids recursion)
        let content = self.persistence.load_file_content(self.node_id.clone(), self.parent_node_id.clone()).await?;
        let content_len = content.len();
        diagnostics::log_debug!("OpLogFile::async_reader() - loaded {content_len} bytes", content_len: content_len);
        
        Ok(Box::pin(std::io::Cursor::new(content)))
    }
    
    async fn async_writer(&self) -> tinyfs::Result<Pin<Box<dyn AsyncWrite + Send + 'static>>> {
        // For now, we'll use a simple approach without transaction ID integration
        // In a full implementation, you'd get the current transaction ID from persistence layer
        
        // Acquire write lock
        let mut state = self.transaction_state.write().await;
        if let TransactionWriteState::WritingInTransaction(_) = *state {
            return Err(tinyfs::Error::Other("File is already being written in a transaction".to_string()));
        }
        *state = TransactionWriteState::WritingInTransaction(0); // Simplified for now
        drop(state);
        
        diagnostics::log_debug!("OpLogFile::async_writer() - creating writer for persistence layer");
        
        // Create a simple buffering writer that will store content via persistence layer
        let persistence = self.persistence.clone();
        let node_id = self.node_id.clone(); 
        let parent_node_id = self.parent_node_id.clone();
        let transaction_state = self.transaction_state.clone();
        
        Ok(Box::pin(OpLogFileWriter::new(persistence, node_id, parent_node_id, transaction_state)))
    }
    
    async fn is_being_written(&self) -> bool {
        let state = self.transaction_state.read().await;
        matches!(*state, TransactionWriteState::WritingInTransaction(_))
    }
}

/// Writer integrated with Delta Lake transactions
struct OpLogFileWriter {
    buffer: Vec<u8>,
    persistence: Arc<dyn PersistenceLayer>,
    node_id: NodeID,
    parent_node_id: NodeID,
    transaction_state: Arc<RwLock<TransactionWriteState>>,
    completed: bool,
    completion_future: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl OpLogFileWriter {
    fn new(
        persistence: Arc<dyn PersistenceLayer>, 
        node_id: NodeID, 
        parent_node_id: NodeID,
        transaction_state: Arc<RwLock<TransactionWriteState>>
    ) -> Self {
        Self {
            buffer: Vec::new(),
            persistence,
            node_id,
            parent_node_id,
            transaction_state,
            completed: false,
            completion_future: None,
        }
    }
}

impl Drop for OpLogFileWriter {
    fn drop(&mut self) {
        if !self.completed {
            // Reset transaction state on drop (panic safety)
            if let Ok(mut state) = self.transaction_state.try_write() {
                *state = TransactionWriteState::Ready;
            }
        }
    }
}

#[async_trait]
impl AsyncWrite for OpLogFileWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let this = unsafe { self.get_unchecked_mut() };
        if this.completed {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe, 
                "Writer already completed"
            )));
        }
        this.buffer.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        let this = unsafe { self.get_unchecked_mut() };
        if this.completed {
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Writer already completed"
            )))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        let this = unsafe { self.get_unchecked_mut() };
        
        if this.completed {
            return Poll::Ready(Ok(()));
        }
        
        // Create completion future if not already created
        if this.completion_future.is_none() {
            let content = std::mem::take(&mut this.buffer);
            let persistence = this.persistence.clone();
            let node_id = this.node_id.clone();
            let parent_node_id = this.parent_node_id.clone();
            let transaction_state = this.transaction_state.clone();
            
            let content_len = content.len();
            diagnostics::log_debug!("OpLogFileWriter::poll_shutdown() - storing {content_len} bytes via persistence layer", content_len: content_len);
            
            let future = Box::pin(async move {
                // Store content 
                let _ = persistence.store_file_content(node_id, parent_node_id, &content).await;
                
                // Reset transaction state
                let mut state = transaction_state.write().await;
                *state = TransactionWriteState::Ready;
            });
            this.completion_future = Some(future);
        }
        
        // Poll the completion future
        match this.completion_future.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Ready(()) => {
                this.completed = true;
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
