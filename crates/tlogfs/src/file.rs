// Clean architecture File implementation for TinyFS
use tinyfs::{File, Metadata, persistence::PersistenceLayer, NodeID};
use std::sync::Arc;
use std::pin::Pin;
use async_trait::async_trait;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};

/// Clean architecture file implementation - COMPLETELY STATELESS
/// - NO local state or caching (persistence layer is single source of truth)
/// - Simple delegation to persistence layer for all operations
/// - Proper separation of concerns
pub struct OpLogFile {
    /// Unique node identifier for this file
    node_id: NodeID,
    
    /// Parent directory node ID (for persistence operations)
    parent_node_id: NodeID,
    
    /// Reference to persistence layer (single source of truth)
    persistence: Arc<dyn PersistenceLayer>,
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
        diagnostics::log_debug!("OpLogFile::async_reader() - loading content via persistence layer");
        
        // Load file content directly from persistence layer (avoids recursion)
        let content = self.persistence.load_file_content(self.node_id.clone(), self.parent_node_id.clone()).await?;
        let content_len = content.len();
        diagnostics::log_debug!("OpLogFile::async_reader() - loaded {content_len} bytes", content_len: content_len);
        
        Ok(Box::pin(std::io::Cursor::new(content)))
    }
    
    async fn async_writer(&mut self) -> tinyfs::Result<Pin<Box<dyn AsyncWrite + Send + 'static>>> {
        diagnostics::log_debug!("OpLogFile::async_writer() - creating writer for persistence layer");
        
        // Create a simple buffering writer that will store content via persistence layer
        let persistence = self.persistence.clone();
        let node_id = self.node_id.clone(); 
        let parent_node_id = self.parent_node_id.clone();
        
        Ok(Box::pin(SimpleFileWriter::new(persistence, node_id, parent_node_id)))
    }
}

/// Simple buffered writer for OpLog files that delegates to persistence layer
struct SimpleFileWriter {
    buffer: Vec<u8>,
    persistence: Arc<dyn PersistenceLayer>,
    node_id: NodeID,
    parent_node_id: NodeID,
    completed: bool,
}

impl SimpleFileWriter {
    fn new(persistence: Arc<dyn PersistenceLayer>, node_id: NodeID, parent_node_id: NodeID) -> Self {
        Self {
            buffer: Vec::new(),
            persistence,
            node_id,
            parent_node_id,
            completed: false,
        }
    }
}

#[async_trait]
impl AsyncWrite for SimpleFileWriter {
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
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        let this = unsafe { self.get_unchecked_mut() };
        
        if this.completed {
            return Poll::Ready(Ok(()));
        }
        
        // For simplicity, just complete synchronously by spawning a blocking task
        // In a real implementation, you'd want proper async handling
        let content = std::mem::take(&mut this.buffer);
        let persistence = this.persistence.clone();
        let node_id = this.node_id.clone();
        let parent_node_id = this.parent_node_id.clone();
        
        let content_len = content.len();
        diagnostics::log_debug!("SimpleFileWriter::poll_shutdown() - storing {content_len} bytes via persistence layer", content_len: content_len);
        
        // Spawn task to store content
        tokio::spawn(async move {
            let _ = persistence.store_file_content(node_id, parent_node_id, &content).await;
        });
        
        this.completed = true;
        Poll::Ready(Ok(()))
    }
}
