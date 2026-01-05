// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Streaming writer for chunked files - wrapper around utilities::chunked_files
//!
//! This module provides Delta Lake integration for the chunked file writer.
//! The actual chunking logic lives in utilities::chunked_files.

use crate::Result;
use deltalake::DeltaTable;
use deltalake::operations::write::WriteBuilder;
use log::debug;

/// Streaming writer that chunks files and writes to Delta Lake
///
/// This is a wrapper around utilities::chunked_files::ChunkedWriter that
/// adds Delta Lake transaction support.
pub struct ChunkedWriter<R> {
    inner: utilities::chunked_files::ChunkedWriter<R>,
}

impl<R: tokio::io::AsyncRead + Unpin> ChunkedWriter<R> {
    /// Create a new chunked writer
    #[must_use]
    pub fn new(pond_txn_id: i64, path: String, reader: R) -> Self {
        Self {
            inner: utilities::chunked_files::ChunkedWriter::new(pond_txn_id, path, reader),
        }
    }

    /// Set custom chunk size (for testing or optimization)
    #[must_use]
    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.inner = self.inner.with_chunk_size(chunk_size);
        self
    }

    /// Set bundle_id override (for metadata)
    #[must_use]
    pub fn with_bundle_id(mut self, bundle_id: String) -> Self {
        self.inner = self.inner.with_bundle_id(bundle_id);
        self
    }

    /// Write the file to Delta Lake in chunks with bounded memory
    ///
    /// # Returns
    /// The bundle_id (BLAKE3 root hash) of the written file
    pub async fn write_to_table(self, table: &mut DeltaTable) -> Result<String> {
        let result = self.inner.write_to_batches().await?;

        debug!(
            "Writing {} batches to Delta Lake for {}",
            result.batches.len(),
            result.bundle_id
        );

        let updated_table = WriteBuilder::new(
            table.log_store(),
            table.state.as_ref().map(|s| s.snapshot().clone()),
        )
        .with_input_batches(result.batches)
        .await?;

        *table = updated_table;
        debug!("Successfully wrote file {} to remote", result.bundle_id);

        Ok(result.bundle_id)
    }
}
