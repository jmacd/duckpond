use crate::persistence::State;
use async_trait::async_trait;
use log::debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tinyfs::{
    AsyncReadSeek, Error as TinyFSError, File, FileID, Metadata, NodeID, NodeMetadata, PartID,
    persistence::PersistenceLayer,
};
use tokio::io::AsyncWrite;
use tokio::sync::RwLock;

/// TLogFS file with transaction-integrated state management
/// - Integrates write state with Delta Lake transaction lifecycle
/// - Single source of truth via persistence layer
/// - Proper separation of concerns
pub struct OpLogFile {
    /// Unique node identifier for this file
    id: FileID,

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
    #[must_use]
    pub fn new(id: FileID, state: State) -> Self {
        debug!("OpLogFile::new() - creating file with node_id: {id}");
        Self {
            id,
            state,
            transaction_state: Arc::new(RwLock::new(TransactionWriteState::Ready)),
        }
    }

    /// Get the node ID for this file
    #[must_use]
    pub fn node_id(&self) -> NodeID {
        self.id.node_id()
    }

    /// Get the part ID (parent directory node ID) for this file
    #[must_use]
    pub fn part_id(&self) -> PartID {
        self.id.part_id()
    }

    /// Create a file handle for TinyFS integration
    #[must_use]
    pub fn create_handle(oplog_file: OpLogFile) -> tinyfs::FileHandle {
        tinyfs::FileHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(oplog_file))))
    }
}

#[async_trait]
impl Metadata for OpLogFile {
    async fn metadata(&self) -> tinyfs::Result<NodeMetadata> {
        // For files, the partition is the parent directory (parent_node_id)
        self.state.metadata(self.id).await
    }
}

#[async_trait]
impl File for OpLogFile {
    async fn async_reader(&self) -> tinyfs::Result<Pin<Box<dyn AsyncReadSeek>>> {
        // Check transaction state
        let state = self.transaction_state.read().await;
        if let TransactionWriteState::WritingInTransaction = *state {
            return Err(tinyfs::Error::Other(
                "File is being written in active transaction".to_string(),
            ));
        }
        drop(state);

        debug!("OpLogFile::async_reader() - creating streaming reader via persistence layer");

        // Use streaming async reader instead of loading entire file into memory
        let reader = self
            .state
            .async_file_reader(self.id)
            .await
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
                return Err(tinyfs::Error::Other(
                    "File is already being written in this transaction".to_string(),
                ));
            }
            TransactionWriteState::Ready => {
                *state = TransactionWriteState::WritingInTransaction;
            }
        }
        drop(state);

        debug!("OpLogFile::async_writer()");

        // Get the current entry type from metadata to preserve it
        let metadata = self.state.metadata(self.id).await?;
        let entry_type = metadata.entry_type;

        // Create a simple buffering writer that will store content via persistence layer
        let persistence = self.state.clone();
        let file_id = self.id;
        let transaction_state = self.transaction_state.clone();

        Ok(Box::pin(OpLogFileWriter::new(
            persistence,
            file_id,
            transaction_state,
            entry_type,
        )))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_queryable(&self) -> Option<&dyn tinyfs::QueryableFile> {
        Some(self)
    }
}

/// Writer integrated with Delta Lake transactions
struct OpLogFileWriter {
    buffer: Vec<u8>,
    state: State,
    file_id: FileID,
    transaction_state: Arc<RwLock<TransactionWriteState>>,
    completed: bool,
    completion_future: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    entry_type: tinyfs::EntryType, // Store the original entry type
}

// OpLogFileWriter is Unpin because all its fields are Unpin
// (Pin<Box<T>> is Unpin even though the T inside may not be)
impl Unpin for OpLogFileWriter {}

impl OpLogFileWriter {
    fn new(
        state: State,
        file_id: FileID,
        transaction_state: Arc<RwLock<TransactionWriteState>>,
        entry_type: tinyfs::EntryType,
    ) -> Self {
        Self {
            buffer: Vec::new(),
            state,
            file_id,
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
            // 🚨 CRITICAL: Writer was dropped without calling shutdown()!
            // This means the file data was buffered but NEVER PERSISTED to Delta Lake.
            // The file metadata exists, but attempting to read will fail with
            // "No non-empty versions found for file"

            let bytes_lost = self.buffer.len();
            if bytes_lost > 0 {
                // PANIC on data loss - this is a programming error that MUST be fixed
                panic!(
                    "🚨 DATA LOSS: OpLogFileWriter dropped without shutdown()! \
                    {} bytes of data were buffered but will NOT be persisted to Delta Lake. \
                    You MUST call writer.shutdown().await? before dropping the writer. \
                    Pattern: writer.write_all(...).await?; writer.flush().await?; writer.shutdown().await?;",
                    bytes_lost
                );
            }

            // Even if no data was written, warn about improper shutdown
            log::warn!(
                "⚠️  OpLogFileWriter dropped without shutdown() - no data was written, \
                but this indicates improper AsyncWrite usage. \
                Always call writer.shutdown().await? even for empty files."
            );

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
        let this = self.get_mut();
        if this.completed {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Writer already completed",
            )));
        }
        this.buffer.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        let this = self.get_mut();
        if this.completed {
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Writer already completed",
            )))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let this = self.get_mut();

        if this.completed {
            return Poll::Ready(Ok(()));
        }

        // Create completion future if not already created
        if this.completion_future.is_none() {
            let content = std::mem::take(&mut this.buffer);
            let mut state = this.state.clone();
            let file_id = this.file_id;
            let transaction_state = this.transaction_state.clone();
            let entry_type = this.entry_type;

            let content_len = content.len();
            let entry_type_debug = format!("{:?}", entry_type);
            debug!(
                "OpLogFileWriter::poll_shutdown() - storing {content_len} bytes via persistence layer, entry_type: {entry_type_debug}"
            );

            debug!(
                "OpLogFileWriter::poll_shutdown() - about to use new FileWriter architecture via store_file_content_ref_transactional"
            );

            let future = Box::pin(async move {
                // Phase 4: Use new FileWriter architecture instead of old update methods
                // Get the OpLogPersistence to access transaction guard API
                let result = async {
                    // Use the new FileWriter pattern through transaction guard API
                    state.store_file_content_ref(
                        file_id,
                        crate::file_writer::ContentRef::Small(content.clone()),
                        match entry_type {
                            tinyfs::EntryType::FileSeriesPhysical | tinyfs::EntryType::FileSeriesDynamic => {
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
                                        Err(e) => {
                                            // FAIL-FAST: Temporal metadata extraction failure is a data integrity issue
                                            return Err(tinyfs::Error::Other(format!(
                                                "Failed to extract temporal metadata from FileSeries content: {}. \
                                                This indicates corrupted or incompatible data format.", e
                                            )));
                                        }
                                    }
                                }
                            }
                            tinyfs::EntryType::FileTablePhysical | tinyfs::EntryType::FileTableDynamic => {
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
                        if entry_type.is_series_file() {
                            debug!(
                                "OpLogFileWriter::poll_shutdown() - successfully stored FileSeries via new FileWriter architecture"
                            );
                        } else {
                            debug!(
                                "OpLogFileWriter::poll_shutdown() - successfully stored content via new FileWriter architecture"
                            );
                        }
                    }
                    Err(e) => {
                        let error_str = e.to_string();
                        debug!(
                            "OpLogFileWriter::poll_shutdown() - failed to store content via FileWriter: {error_str}"
                        );
                    }
                }

                // Reset transaction state
                let mut state = transaction_state.write().await;
                *state = TransactionWriteState::Ready;
            });
            this.completion_future = Some(future);
        }

        // Poll the completion future
        match this
            .completion_future
            .as_mut()
            .expect("@@@ unsafe")
            .as_mut()
            .poll(cx)
        {
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
impl tinyfs::QueryableFile for OpLogFile {
    /// Create TableProvider for OpLogFile by delegating to existing logic
    ///
    /// Follows anti-duplication: reuses existing create_listing_table_provider
    /// instead of duplicating DataFusion setup logic.
    async fn as_table_provider(
        &self,
        id: FileID,
        context: &tinyfs::ProviderContext,
    ) -> tinyfs::Result<Arc<dyn datafusion::catalog::TableProvider>> {
        log::debug!("DELEGATING OpLogFile to create_listing_table_provider: id={id}",);
        // Extract State from ProviderContext
        let state = context
            .persistence
            .as_any()
            .downcast_ref::<State>()
            .ok_or_else(|| TinyFSError::Other("Persistence is not a tlogfs State".to_string()))?;
        // Delegate to existing create_listing_table_provider - no duplication
        crate::file_table::create_listing_table_provider(id, state)
            .await
            .map_err(|e| tinyfs::Error::Other(e.to_string()))
    }
}
