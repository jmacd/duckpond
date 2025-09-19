use tinyfs::{File, Metadata, NodeMetadata, persistence::PersistenceLayer, NodeID, AsyncReadSeek, Error as TinyFSError};
use crate::persistence::State;
use std::sync::Arc;
use std::pin::Pin;
use std::future::Future;
use async_trait::async_trait;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;
use tokio::sync::RwLock;
use log::debug;

/// TLogFS file with transaction-integrated state management
/// - Integrates write state with Delta Lake transaction lifecycle
/// - Single source of truth via persistence layer
/// - Proper separation of concerns
pub struct OpLogFile {
    /// Unique node identifier for this file
    node_id: NodeID,
    
    /// Parent directory node ID (for persistence operations)
    part_id: NodeID,
    
    /// Reference to persistence layer (single source of truth)
    state: State,
    
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
        part_id: NodeID,
        state: State,
    ) -> Self {
        debug!("OpLogFile::new() - creating file with node_id: {node_id}, parent: {part_id}");        
        Self {
            node_id,
            part_id,
            state,
            transaction_state: Arc::new(RwLock::new(TransactionWriteState::Ready)),
        }
    }
    
    /// Get the node ID for this file
    pub fn get_node_id(&self) -> NodeID {
        self.node_id
    }
    
    /// Get the part ID (parent directory node ID) for this file
    pub fn get_part_id(&self) -> NodeID {
        self.part_id
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
        self.state.metadata(self.node_id, self.part_id).await
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
        
        debug!("OpLogFile::async_reader() - creating streaming reader via persistence layer");
        
        // Use streaming async reader instead of loading entire file into memory
        let reader = self.state.async_file_reader(self.node_id.clone(), self.part_id.clone()).await
                   .map_err(|e| TinyFSError::Other(e.to_string()))?;

        debug!("OpLogFile::async_reader() - created streaming reader successfully");
        
        Ok(reader)
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
        let metadata = self.state.metadata(self.node_id, self.part_id).await?;
        let entry_type = metadata.entry_type;
        
        // Create a simple buffering writer that will store content via persistence layer
        let persistence = self.state.clone();
        let node_id = self.node_id.clone(); 
        let part_id = self.part_id.clone();
        let transaction_state = self.transaction_state.clone();
        
        Ok(Box::pin(OpLogFileWriter::new(persistence, node_id, part_id, transaction_state, entry_type)))
    }
    
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Writer integrated with Delta Lake transactions
struct OpLogFileWriter {
    buffer: Vec<u8>,
    state: State,
    node_id: NodeID,
    part_id: NodeID,
    transaction_state: Arc<RwLock<TransactionWriteState>>,
    completed: bool,
    completion_future: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    entry_type: tinyfs::EntryType, // Store the original entry type
}

impl OpLogFileWriter {
    fn new(
        state: State, 
        node_id: NodeID, 
	part_id: NodeID,
        transaction_state: Arc<RwLock<TransactionWriteState>>,
        entry_type: tinyfs::EntryType
    ) -> Self {
        Self {
            buffer: Vec::new(),
            state,
            node_id,
            part_id,
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
            let mut state = this.state.clone();
            let node_id = this.node_id.clone();
            let part_id = this.part_id.clone();
            let transaction_state = this.transaction_state.clone();
            let entry_type = this.entry_type.clone(); // Capture entry type
            
            let content_len = content.len();
            let entry_type_debug = format!("{:?}", entry_type);
            debug!("OpLogFileWriter::poll_shutdown() - storing {content_len} bytes via persistence layer, entry_type: {entry_type_debug}");
            
            debug!("OpLogFileWriter::poll_shutdown() - about to use new FileWriter architecture via store_file_content_ref_transactional");
            
            let future = Box::pin(async move {
                // Phase 4: Use new FileWriter architecture instead of old update methods
                // Get the OpLogPersistence to access transaction guard API
                let result = async {
                    // Use the new FileWriter pattern through transaction guard API
                    state.store_file_content_ref(
                        node_id, 
                        part_id, 
                        crate::file_writer::ContentRef::Small(content.clone()),
                        entry_type,
                        match entry_type {
                            tinyfs::EntryType::FileSeries => {
                                // Special case: Handle empty FileSeries (0 bytes) for metadata-only versions
                                if content.is_empty() {
                                    debug!("OpLogFileWriter::poll_shutdown() - creating empty FileSeries for metadata-only version");
                                    crate::file_writer::FileMetadata::Series {
                                        min_timestamp: 0, // Will be ignored, temporal overrides in extended attributes take precedence
                                        max_timestamp: 0, // Will be ignored, temporal overrides in extended attributes take precedence
                                        timestamp_column: "timestamp".to_string(), // Default timestamp column
                                    }
                                } else {
                                    // For FileSeries with content, extract temporal metadata
                                    use std::io::Cursor;
                                    let reader = Cursor::new(content.clone());
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
                        debug!("OpLogFileWriter::poll_shutdown() - failed to store content via FileWriter: {error_str}");
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

// QueryableFile trait implementation - follows anti-duplication principles
#[async_trait]
impl crate::query::QueryableFile for OpLogFile {
    /// Create TableProvider for OpLogFile by delegating to existing logic
    /// 
    /// Follows anti-duplication: reuses existing create_listing_table_provider
    /// instead of duplicating DataFusion setup logic.
    async fn as_table_provider(
        &self,
        node_id: tinyfs::NodeID,
        part_id: tinyfs::NodeID,
        tx: &mut crate::transaction_guard::TransactionGuard<'_>,
    ) -> Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>, crate::error::TLogFSError> {
        // Delegate to existing create_listing_table_provider - no duplication
        crate::file_table::create_listing_table_provider(node_id, part_id, tx).await
    }
}

/// Create a table provider from multiple file URLs
/// This is a convenience function following anti-duplication principles
pub async fn create_table_provider_for_multiple_urls(
    urls: Vec<String>,
    tx: &mut crate::transaction_guard::TransactionGuard<'_>,
) -> Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>, crate::error::TLogFSError> {
    use crate::file_table::{create_table_provider, TableProviderOptions};
    use tinyfs::NodeID;
    
    // Use dummy node IDs since we're providing explicit URLs
    let dummy_node_id = NodeID::root();
    let dummy_part_id = NodeID::root();
    
    let options = TableProviderOptions {
        additional_urls: urls,
        ..Default::default()
    };
    
    create_table_provider(dummy_node_id, dummy_part_id, tx, options).await
}
