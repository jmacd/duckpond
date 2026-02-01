// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Streaming reader for chunked files - wrapper around utilities::chunked_files
//!
//! This module provides Delta Lake query integration for the chunked file reader.
//! The actual verification logic lives in utilities::chunked_files.

use crate::Result;
use crate::error::RemoteError;
use arrow_array::RecordBatch;
use datafusion::prelude::*;
use deltalake::DeltaTable;
use futures::TryStreamExt;
use log::debug;

/// Streaming reader for chunked files
pub struct ChunkedReader<'a> {
    table: &'a DeltaTable,
    bundle_id: String,
    path: String,
    pond_txn_id: i64,
}

impl<'a> ChunkedReader<'a> {
    /// Create a new chunked reader
    #[must_use]
    pub fn new(
        table: &'a DeltaTable,
        bundle_id: impl Into<String>,
        path: impl Into<String>,
        pond_txn_id: i64,
    ) -> Self {
        Self {
            table,
            bundle_id: bundle_id.into(),
            path: path.into(),
            pond_txn_id,
        }
    }

    /// Read the complete file, verifying checksums
    pub async fn read_to_writer<W: tokio::io::AsyncWrite + Unpin>(self, writer: W) -> Result<()> {
        debug!("Reading file {} from remote", self.bundle_id);

        // Query Delta Lake for chunks
        let ctx = SessionContext::new();
        ctx.register_table("remote_files", std::sync::Arc::new(self.table.clone()))?;

        let df = ctx
            .sql(&format!(
                "SELECT chunk_id, chunk_hash, chunk_outboard, chunk_data, total_size, root_hash 
                 FROM remote_files 
                 WHERE bundle_id = '{}' AND path = '{}' AND pond_txn_id = {} 
                 ORDER BY chunk_id",
                self.bundle_id, self.path, self.pond_txn_id
            ))
            .await?;

        let mut stream = df.execute_stream().await?;
        let mut batches: Vec<RecordBatch> = Vec::new();

        while let Some(batch) = stream.try_next().await? {
            batches.push(batch);
        }

        if batches.is_empty() {
            return Err(RemoteError::FileNotFound(self.bundle_id.clone()));
        }

        // Use utilities reader for verification
        let reader = utilities::chunked_files::ChunkedReader::new(batches)
            .with_expected_bundle_id(self.bundle_id.clone());

        reader
            .read_to_writer(writer)
            .await
            .map_err(|e| RemoteError::TableOperation(format!("Failed to read file: {}", e)))?;

        Ok(())
    }
}
