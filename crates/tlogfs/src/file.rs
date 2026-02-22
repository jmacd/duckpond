// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::persistence::State;
use async_trait::async_trait;
use log::{debug, error};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tinyfs::{
    AsyncReadSeek, Error as TinyFSError, File, FileID, FileMetadataWriter, Metadata, NodeID,
    NodeMetadata, PartID, persistence::PersistenceLayer,
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

    async fn async_writer(&self) -> tinyfs::Result<Pin<Box<dyn FileMetadataWriter>>> {
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

        // Get store path for HybridWriter
        let store_path = self.state.store_path().await;

        // PRE-ALLOCATE version number NOW (not in shutdown)
        // This ensures correct ordering even if multiple writes happen concurrently
        let allocated_version = self
            .state
            .allocate_version_for_write(self.id)
            .await
            .map_err(|e| tinyfs::Error::Other(e.to_string()))?;

        debug!(
            "Pre-allocated version {allocated_version} for file {}",
            self.id
        );

        // Create streaming writer that will store content via persistence layer
        let persistence = self.state.clone();
        let file_id = self.id;
        let transaction_state = self.transaction_state.clone();

        Ok(Box::pin(OpLogFileWriter::new(
            persistence,
            file_id,
            transaction_state,
            entry_type,
            store_path,
            allocated_version,
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
/// Streams writes directly to HybridWriter for memory efficiency
pub struct OpLogFileWriter {
    storage: crate::large_files::HybridWriter,
    state: State,
    file_id: FileID,
    transaction_state: Arc<RwLock<TransactionWriteState>>,
    completed: bool,
    completion_future: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    entry_type: tinyfs::EntryType,
    precomputed_metadata: Option<crate::file_writer::FileMetadata>,
    /// Pre-allocated version number for this write (allocated at writer creation)
    allocated_version: i64,
}

// OpLogFileWriter is Unpin because all its fields are Unpin
impl Unpin for OpLogFileWriter {}

impl OpLogFileWriter {
    fn new(
        state: State,
        file_id: FileID,
        transaction_state: Arc<RwLock<TransactionWriteState>>,
        entry_type: tinyfs::EntryType,
        store_path: std::path::PathBuf,
        allocated_version: i64,
    ) -> Self {
        let options = state.large_file_options().clone();
        Self {
            storage: crate::large_files::HybridWriter::with_options(store_path, options),
            state,
            file_id,
            transaction_state,
            completed: false,
            completion_future: None,
            entry_type,
            precomputed_metadata: None,
            allocated_version,
        }
    }
}

#[async_trait]
impl FileMetadataWriter for OpLogFileWriter {
    fn set_temporal_metadata(&mut self, min: i64, max: i64, timestamp_column: String) {
        self.precomputed_metadata = Some(crate::file_writer::FileMetadata::Series {
            min_timestamp: min,
            max_timestamp: max,
            timestamp_column,
        });
    }

    async fn infer_temporal_bounds(&mut self) -> tinyfs::Result<(i64, i64, String)> {
        // First, flush the writer to ensure all bytes are written (but don't shutdown yet)
        use tokio::io::AsyncWriteExt;
        self.flush().await.map_err(|e| {
            tinyfs::Error::Other(format!("Failed to flush before inferring bounds: {}", e))
        })?;

        // Read back the bytes from the temp file in HybridWriter
        // This is efficient because parquet footer parsing only needs the end of the file
        let temp_path = self
            .storage
            .temp_file_path()
            .ok_or_else(|| tinyfs::Error::Other("No temp file for inferring bounds".to_string()))?
            .clone();

        let bytes = tokio::fs::read(&temp_path)
            .await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to read temp file: {}", e)))?;

        // Parse parquet footer to extract temporal bounds
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use tokio_util::bytes::Bytes;

        let bytes = Bytes::from(bytes);
        let reader_builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| tinyfs::Error::Other(format!("Failed to parse parquet: {}", e)))?;

        let parquet_metadata = reader_builder.metadata();
        let schema = reader_builder.schema();

        // Detect timestamp column
        let timestamp_column = crate::schema::detect_timestamp_column(schema)
            .map_err(|e| tinyfs::Error::Other(format!("No timestamp column found: {}", e)))?;

        // Extract temporal bounds
        let (min_time, max_time) =
            tinyfs::arrow::parquet::extract_temporal_bounds_from_parquet_metadata(
                parquet_metadata.as_ref(),
                schema,
                &timestamp_column,
            )?;

        // Set the metadata on ourselves
        self.set_temporal_metadata(min_time, max_time, timestamp_column.clone());

        // Now shutdown with the metadata set
        self.shutdown().await.map_err(|e| {
            tinyfs::Error::Other(format!(
                "Failed to finalize write after setting metadata: {}",
                e
            ))
        })?;

        Ok((min_time, max_time, timestamp_column))
    }
}

impl Drop for OpLogFileWriter {
    fn drop(&mut self) {
        if !self.completed {
            let total_written = self.storage.total_written();
            if total_written > 0 {
                // PANIC on data loss - this is a programming error that MUST be fixed
                panic!(
                    "[ALERT] DATA LOSS: OpLogFileWriter dropped without shutdown()! \
                    {} bytes of data were written but will NOT be persisted to Delta Lake. \
                    You MUST call writer.shutdown().await? before dropping the writer. \
                    Pattern: writer.write_all(...).await?; writer.flush().await?; writer.shutdown().await?;",
                    total_written
                );
            }

            // Even if no data was written, warn about improper shutdown
            log::warn!(
                "[WARN]  OpLogFileWriter dropped without shutdown() - no data was written, \
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
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let this = self.get_mut();
        if this.completed {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Writer already completed",
            )));
        }
        // Stream directly to HybridWriter (which writes to temp file)
        Pin::new(&mut this.storage).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        let this = self.get_mut();
        if this.completed {
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Writer already completed",
            )))
        } else {
            // Flush HybridWriter
            Pin::new(&mut this.storage).poll_flush(cx)
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
            // Take ownership of storage to finalize it
            let storage = std::mem::replace(
                &mut this.storage,
                crate::large_files::HybridWriter::with_options(
                    std::path::PathBuf::new(),
                    Default::default(),
                ),
            );
            let state = this.state.clone();
            let file_id = this.file_id;
            let transaction_state = this.transaction_state.clone();
            let entry_type = this.entry_type;
            let precomputed_metadata = this.precomputed_metadata.clone();
            let allocated_version = this.allocated_version;

            let future = Box::pin(async move {
                // Finalize HybridWriter to get content
                let result = async {
                    let hybrid_result = storage.finalize().await
                        .map_err(|e| tinyfs::Error::Other(format!("Failed to finalize storage: {}", e)))?;

                    let content = hybrid_result.content;
                    let content_len = hybrid_result.size;
                    let blake3 = hybrid_result.blake3;
                    let bao_state = hybrid_result.bao_state;

                    debug!(
                        "OpLogFileWriter::poll_shutdown() - finalized {} bytes, blake3={}, is_large={}, bao_total_size={}",
                        content_len,
                        blake3,
                        content.is_empty() && content_len > 0,
                        bao_state.total_size
                    );

                    // Determine ContentRef based on whether content is empty (large file external)
                    let content_ref = if content.is_empty() && content_len >= crate::large_files::LARGE_FILE_THRESHOLD {
                        // Large file - stored externally
                        crate::file_writer::ContentRef::Large(blake3, content_len as u64)
                    } else {
                        // Small file - content in memory
                        crate::file_writer::ContentRef::Small(content.clone())
                    };

                    // Compute bao_outboard automatically based on entry_type
                    let bao_outboard = match entry_type {
                        tinyfs::EntryType::FilePhysicalSeries => {
                            // For series files, compute incremental bao_outboard
                            // Get previous version's bao_outboard (if any)
                            let prev_version = if allocated_version > 1 {
                                allocated_version - 1
                            } else {
                                0
                            };

                            let prev_bao = if prev_version > 0 {
                                // Get bao_outboard from metadata instead of separate method
                                state.metadata(file_id).await.ok().and_then(|meta| meta.bao_outboard)
                            } else {
                                None
                            };

                            let series_outboard = if let Some(prev_bao_bytes) = prev_bao {
                                // Deserialize previous SeriesOutboard
                                let prev_outboard = utilities::bao_outboard::SeriesOutboard::from_bytes(&prev_bao_bytes)
                                    .map_err(|e| tinyfs::Error::Other(format!("Failed to deserialize previous bao_outboard: {}", e)))?;

                                // Determine if we can efficiently read pending bytes
                                // For small cumulative sizes, read all previous content to get pending bytes
                                // For large cumulative sizes, fall back to from_incremental_state (cumulative_blake3 will be wrong)
                                let can_read_pending = prev_outboard.cumulative_size < 10 * 1024 * 1024; // 10MB threshold

                                if can_read_pending && !content.is_empty() {
                                    // SMALL CUMULATIVE: Read previous content to compute proper cumulative hash
                                    let pending_len = (prev_outboard.cumulative_size % utilities::bao_outboard::BLOCK_SIZE as u64) as usize;
                                    let verified_pending = if pending_len > 0 {
                                        // Read all previous content and take the last pending_len bytes
                                        let mut reader = state.async_file_reader(file_id).await
                                            .map_err(|e| tinyfs::Error::Other(format!("Failed to read previous content: {}", e)))?;
                                        use tokio::io::AsyncReadExt;
                                        let mut all_content = Vec::with_capacity(prev_outboard.cumulative_size as usize);
                                        let _ = reader.read_to_end(&mut all_content).await
                                            .map_err(|e| tinyfs::Error::Other(format!("Failed to read previous content: {}", e)))?;
                                        // Take the last pending_len bytes
                                        let pending_start = all_content.len().saturating_sub(pending_len);
                                        all_content[pending_start..].to_vec()
                                    } else {
                                        Vec::new()
                                    };

                                    // Use append_version with proper cumulative hash
                                    utilities::bao_outboard::SeriesOutboard::append_version(
                                        &prev_outboard,
                                        &verified_pending,
                                        &content,
                                    )
                                } else {
                                    // LARGE FILE: Content is stored externally, use from_incremental_state
                                    // NOTE: The cumulative_blake3 will be incorrect for large file appends
                                    // because we can't efficiently resume without the pending bytes.
                                    // cumulative_size is still correct.
                                    utilities::bao_outboard::SeriesOutboard::from_incremental_state(
                                        &prev_outboard,
                                        &bao_state,
                                        content_len as u64,
                                    )
                                }
                            } else {
                                // First version - use bao_state from HybridWriter
                                // This correctly handles large files where content is empty but
                                // bao_state.total_size has the actual size
                                utilities::bao_outboard::SeriesOutboard::from_first_version_state(&bao_state, content_len as u64)
                            };

                            Some(series_outboard.to_bytes())
                        }
                        tinyfs::EntryType::FilePhysicalVersion => {
                            // For version files, compute standalone VersionOutboard
                            let version_outboard = utilities::bao_outboard::VersionOutboard::new(&content);
                            Some(version_outboard.to_bytes())
                        }
                        _ => {
                            // Other entry types don't use bao_outboard
                            None
                        }
                    };

                    // Extract metadata based on file type
                    let metadata = match entry_type {
                        tinyfs::EntryType::TablePhysicalSeries => {
                            // Series files should have precomputed metadata from the parquet writer
                            // Exception: empty writes (for extended attributes only) are allowed without metadata
                            if let Some(precomputed) = precomputed_metadata {
                                precomputed
                            } else if content.is_empty() && content_len == 0 {
                                // Empty version for setting extended attributes - no metadata needed
                                crate::file_writer::FileMetadata::Series {
                                    min_timestamp: 0,
                                    max_timestamp: 0,
                                    timestamp_column: String::new(),
                                }
                            } else {
                                return Err(tinyfs::Error::Other(
                                    "TablePhysicalSeries written without temporal metadata - caller must use FileMetadataWriter::set_temporal_metadata() or infer_temporal_bounds()".to_string()
                                ));
                            }
                        }
                        tinyfs::EntryType::TablePhysicalVersion => {
                            if let Some(precomputed) = precomputed_metadata {
                                precomputed
                            } else if content.is_empty() {
                                // Large file - use placeholder
                                crate::file_writer::FileMetadata::Table {
                                    schema: r#"{"type": "struct", "fields": []}"#.to_string(),
                                }
                            } else {
                                // Small file - extract schema
                                use std::io::Cursor;
                                let reader = Cursor::new(&content);
                                match crate::file_writer::TableProcessor::validate_schema(reader).await {
                                    Ok(metadata) => metadata,
                                    Err(_) => {
                                        crate::file_writer::FileMetadata::Table {
                                            schema: r#"{"type": "struct", "fields": []}"#.to_string(),
                                        }
                                    }
                                }
                            }
                        }
                        _ => crate::file_writer::FileMetadata::Data,
                    };

                    state.store_file_content_ref(file_id, content_ref, metadata, Some(allocated_version), bao_outboard).await
                        .map_err(|e| tinyfs::Error::Other(format!("Failed to store file: {}", e)))
                }.await;

                match result {
                    Ok(_) => {
                        if entry_type.is_series_file() {
                            debug!(
                                "OpLogFileWriter::poll_shutdown() - successfully stored FileSeries version {allocated_version}"
                            );
                        } else {
                            debug!(
                                "OpLogFileWriter::poll_shutdown() - successfully stored content version {allocated_version}"
                            );
                        }
                    }
                    Err(e) => {
                        let error_str = e.to_string();
                        error!(
                            "OpLogFileWriter::poll_shutdown() - WRITE FAILED, poisoning transaction: {error_str}"
                        );
                        // POISON THE TRANSACTION - this write failed, cannot commit
                        state.poison_transaction(format!(
                            "File write failed for {file_id} version {allocated_version}: {error_str}"
                        )).await;
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
    /// Create TableProvider for OpLogFile by delegating to provider crate
    ///
    /// Follows anti-duplication: uses provider::create_table_provider directly
    async fn as_table_provider(
        &self,
        id: FileID,
        context: &tinyfs::ProviderContext,
    ) -> tinyfs::Result<Arc<dyn datafusion::catalog::TableProvider>> {
        log::debug!("DELEGATING OpLogFile to provider::create_table_provider: id={id}");
        // Delegate to provider crate - no duplication
        provider::create_table_provider(id, context, provider::TableProviderOptions::default())
            .await
            .map_err(|e| tinyfs::Error::Other(e.to_string()))
    }
}
