// Clean architecture File implementation for TinyFS
use tinyfs::{File, Metadata, NodeMetadata, persistence::PersistenceLayer, NodeID, AsyncReadSeek};
use std::sync::Arc;
use std::pin::Pin;
use std::future::Future;
use async_trait::async_trait;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;
use tokio::sync::RwLock;
use diagnostics::*;

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
    WritingInTransaction,
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
        debug!("OpLogFile::new() - creating file with node_id: {node_id}, parent: {parent_node_id}", 
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
    async fn metadata(&self) -> tinyfs::Result<NodeMetadata> {
        // For files, the partition is the parent directory (parent_node_id)
        self.persistence.metadata(self.node_id, self.parent_node_id).await
    }
}

#[async_trait]
impl File for OpLogFile {
    async fn async_reader(&self) -> tinyfs::Result<Pin<Box<dyn AsyncReadSeek>>> {
        // Check transaction state
        let state = self.transaction_state.read().await;
        if let TransactionWriteState::WritingInTransaction = *state {
            return Err(tinyfs::Error::Other("File is being written in active transaction".to_string()));
        }
        drop(state);
        
        debug!("OpLogFile::async_reader() - loading content via persistence layer");
        
        // Load file content directly from persistence layer (avoids recursion)
        let content = self.persistence.load_file_content(self.node_id.clone(), self.parent_node_id.clone()).await?;
        let content_len = content.len();
        debug!("OpLogFile::async_reader() - loaded {content_len} bytes", content_len: content_len);
        
        // std::io::Cursor implements both AsyncRead and AsyncSeek
        Ok(Box::pin(std::io::Cursor::new(content)))
    }
    
    async fn async_writer(&self) -> tinyfs::Result<Pin<Box<dyn AsyncWrite + Send + 'static>>> {
         // Acquire write lock and check for recursive writes
        // The main threat model here is preventing recursive scenarios where 
        // a dynamically synthesized file evaluation tries to write a file 
        // that is already being written in the same execution context
        let mut state = self.transaction_state.write().await;
        match *state {
            TransactionWriteState::WritingInTransaction => {
                return Err(tinyfs::Error::Other("File is already being written in this transaction".to_string()));
            }
            TransactionWriteState::Ready => {
                *state = TransactionWriteState::WritingInTransaction;
            }
        }
        drop(state);
        
        debug!("OpLogFile::async_writer()");
        
        // Get the current entry type from metadata to preserve it
        let metadata = self.persistence.metadata(self.node_id, self.parent_node_id).await?;
        let entry_type = metadata.entry_type;
        
        // Create a simple buffering writer that will store content via persistence layer
        let persistence = self.persistence.clone();
        let node_id = self.node_id.clone(); 
        let parent_node_id = self.parent_node_id.clone();
        let transaction_state = self.transaction_state.clone();
        
        Ok(Box::pin(OpLogFileWriter::new(persistence, node_id, parent_node_id, transaction_state, entry_type)))
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
    entry_type: tinyfs::EntryType, // Store the original entry type
}

impl OpLogFileWriter {
    fn new(
        persistence: Arc<dyn PersistenceLayer>, 
        node_id: NodeID, 
        parent_node_id: NodeID,
        transaction_state: Arc<RwLock<TransactionWriteState>>,
        entry_type: tinyfs::EntryType
    ) -> Self {
        Self {
            buffer: Vec::new(),
            persistence,
            node_id,
            parent_node_id,
            transaction_state,
            completed: false,
            completion_future: None,
            entry_type,
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
            let entry_type = this.entry_type.clone(); // Capture entry type
            
            let content_len = content.len();
            let entry_type_debug = format!("{:?}", entry_type);
            debug!("OpLogFileWriter::poll_shutdown() - storing {content_len} bytes via persistence layer, entry_type: {entry_type}", 
                content_len: content_len, entry_type: entry_type_debug);
            
            debug!("OpLogFileWriter::poll_shutdown() - about to use new FileWriter architecture via store_file_content_ref_transactional");
            
            let future = Box::pin(async move {
                // Phase 4: Use new FileWriter architecture instead of old update methods
                // Get the OpLogPersistence to access transaction guard API
                let result = async {
                    let persistence = persistence.as_any().downcast_ref::<crate::OpLogPersistence>()
                        .ok_or(tinyfs::Error::Other("FileWriter requires OpLogPersistence context".to_string()))?;
                    
                    // Use the new FileWriter pattern through transaction guard API
                    persistence.state().store_file_content_ref(
                        node_id, 
                        parent_node_id, 
                        crate::file_writer::ContentRef::Small(content.clone()),
                        entry_type,
                        match entry_type {
                            tinyfs::EntryType::FileSeries => {
                                // For FileSeries, we need to extract temporal metadata
                                // Create temporary reader for content analysis
                                use std::io::Cursor;
                                let reader = Cursor::new(content);
                                match crate::file_writer::SeriesProcessor::extract_temporal_metadata(reader).await {
                                    Ok(metadata) => metadata,
                                    Err(_) => {
                                        // Fallback to default metadata if extraction fails
                                        crate::file_writer::FileMetadata::Series {
                                            min_timestamp: 0,
                                            max_timestamp: 0,
                                            timestamp_column: "timestamp".to_string(),
                                        }
                                    }
                                }
                            }
                            tinyfs::EntryType::FileTable => {
                                // For FileTable, extract schema
                                use std::io::Cursor;
                                let reader = Cursor::new(content);
                                match crate::file_writer::TableProcessor::validate_schema(reader).await {
                                    Ok(metadata) => metadata,
                                    Err(_) => {
                                        // Fallback to default metadata if validation fails
                                        crate::file_writer::FileMetadata::Table {
                                            schema: r#"{"type": "struct", "fields": []}"#.to_string(),
                                        }
                                    }
                                }
                            }
                            _ => {
                                // For FileData and others, no special processing
                                crate::file_writer::FileMetadata::Data
                            }
                        },
                    ).await
                        .map_err(|e| tinyfs::Error::Other(format!("FileWriter storage failed: {}", e)))
                }.await;
                
                match result {
                    Ok(_) => {
                        if entry_type == tinyfs::EntryType::FileSeries {
                            debug!("OpLogFileWriter::poll_shutdown() - successfully stored FileSeries via new FileWriter architecture");
                        } else {
                            debug!("OpLogFileWriter::poll_shutdown() - successfully stored content via new FileWriter architecture");
                        }
                    }
                    Err(e) => {
                        let error_str = e.to_string();
                        debug!("OpLogFileWriter::poll_shutdown() - failed to store content via FileWriter: {error}", error: error_str);
                    }
                }
                
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
