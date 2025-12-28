// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::EntryType;
use crate::error;
use crate::file::{AsyncReadSeek, File, Handle};
use crate::metadata::{Metadata, NodeMetadata};
use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;
use tokio::sync::{Mutex, RwLock};

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
            timestamp: 0,                // TODO
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
            return Err(error::Error::Other(
                "File is currently being written".to_string(),
            ));
        }
        drop(state);

        let content = self.content.lock().await;
        // std::io::Cursor implements both AsyncRead and AsyncSeek
        Ok(Box::pin(std::io::Cursor::new(content.clone())))
    }

    async fn async_writer(&self) -> error::Result<Pin<Box<dyn crate::file::FileMetadataWriter>>> {
        // Acquire write lock
        let mut state = self.write_state.write().await;
        if *state == WriteState::Writing {
            return Err(error::Error::Other(
                "File is already being written".to_string(),
            ));
        }
        *state = WriteState::Writing;
        drop(state);

        Ok(Box::pin(MemoryFileWriter::new(
            self.content.clone(),
            self.write_state.clone(),
        )))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_queryable(&self) -> Option<&dyn crate::file::QueryableFile> {
        Some(self)
    }
}

#[async_trait]
impl crate::file::QueryableFile for MemoryFile {
    async fn as_table_provider(
        &self,
        id: crate::FileID,
        context: &crate::ProviderContext,
    ) -> error::Result<Arc<dyn datafusion::catalog::TableProvider>> {
        use datafusion::datasource::file_format::parquet::ParquetFormat;
        use datafusion::datasource::listing::ListingTable;
        use datafusion::datasource::listing::{
            ListingOptions, ListingTableConfig, ListingTableUrl,
        };

        // Use the same pattern as tlogfs: create a ListingTable with a tinyfs:// URL
        // The TinyFsObjectStore (registered in SessionContext) handles reading from MemoryPersistence

        // Build URL pattern for this file: tinyfs:///part/{part_id}/node/{node_id}/version/
        // This matches the TinyFsObjectStore path format expectations
        let url_pattern = format!(
            "tinyfs:///part/{}/node/{}/version/",
            id.part_id(),
            id.node_id()
        );

        let table_url = ListingTableUrl::parse(&url_pattern)
            .map_err(|e| error::Error::Other(format!("Failed to parse table URL: {}", e)))?;

        // Create ListingTable configuration with Parquet format
        let file_format = Arc::new(ParquetFormat::default());
        let listing_options = ListingOptions::new(file_format);
        let config = ListingTableConfig::new(table_url).with_listing_options(listing_options);

        // Infer schema from the SessionContext (which will use the registered ObjectStore)
        let ctx = &context.datafusion_session;
        let config_with_schema = config
            .infer_schema(&ctx.state())
            .await
            .map_err(|e| error::Error::Other(format!("Schema inference failed: {}", e)))?;

        // Create ListingTable
        let listing_table = ListingTable::try_new(config_with_schema)
            .map_err(|e| error::Error::Other(format!("ListingTable creation failed: {}", e)))?;

        // Get temporal bounds for filtering (if any)
        let (min_time, max_time) = context
            .persistence
            .get_temporal_bounds(id)
            .await?
            .unwrap_or((i64::MIN, i64::MAX));

        // Wrap in TemporalFilteredListingTable for consistent behavior with tlogfs
        // Note: Need to import TemporalFilteredListingTable from provider crate
        // For now, return ListingTable directly - temporal filtering can be added when needed
        if min_time != i64::MIN || max_time != i64::MAX {
            log::debug!(
                "MemoryFile temporal bounds [{}, {}] available but TemporalFilteredListingTable not yet integrated",
                min_time,
                max_time
            );
            // TODO: Wrap in provider::TemporalFilteredListingTable once provider is accessible from tinyfs
        }

        Ok(Arc::new(listing_table))
    }
}

impl MemoryFile {
    /// Create a new MemoryFile handle with the given content
    pub fn new_handle<T: AsRef<[u8]>>(content: T) -> Handle {
        let memory_file = MemoryFile {
            content: Arc::new(Mutex::new(content.as_ref().to_vec())),
            write_state: Arc::new(RwLock::new(WriteState::Ready)),
            entry_type: EntryType::FileDataPhysical, // Default for memory files
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

#[async_trait]
impl crate::file::FileMetadataWriter for MemoryFileWriter {
    fn set_temporal_metadata(&mut self, _min: i64, _max: i64, _timestamp_column: String) {
        // Memory files don't persist metadata - this is a no-op
        // In a real implementation, we'd store this in MemoryFile
    }

    async fn infer_temporal_bounds(&mut self) -> error::Result<(i64, i64, String)> {
        // For memory files, we don't support infer_temporal_bounds
        // This would require reading the bytes and parsing parquet
        Err(crate::Error::Other(
            "infer_temporal_bounds not supported for memory files".to_string(),
        ))
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
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        if self.completed {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Writer already completed",
            )));
        }
        self.buffer.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        if self.completed {
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Writer already completed",
            )))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
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

        // Poll the completion future. (Memory readers do not fail.)
        match self
            .completion_future
            .as_mut()
            .expect("infallible")
            .as_mut()
            .poll(cx)
        {
            Poll::Ready(()) => {
                self.completed = true;
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
