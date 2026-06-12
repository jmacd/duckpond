// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Shared test helpers for factory unit tests.
//!
//! These helpers are deduplicated from the individual factory test modules
//! (`sql_derived`, `temporal_reduce`, `timeseries_join`, `timeseries_pivot`),
//! which previously each carried byte-identical copies.

#![cfg(test)]

use crate::ProviderContext;
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use parquet::arrow::ArrowWriter;
use std::io::Cursor;
use std::sync::Arc;
use tinyfs::{EntryType, FS, FileID, MemoryPersistence};

/// Create a `crate::FactoryContext` from a `ProviderContext` for tests.
#[must_use]
pub fn test_context(context: &ProviderContext, file_id: FileID) -> crate::FactoryContext {
    crate::FactoryContext::new(context.clone(), file_id)
}

/// Create a fresh `ProviderContext` backed by `MemoryPersistence` with the
/// TinyFS object store registered on its DataFusion session.
#[must_use]
pub fn create_provider_context() -> ProviderContext {
    let persistence = MemoryPersistence::default();
    let session = Arc::new(SessionContext::new());
    let _ = crate::register_tinyfs_object_store(&session, persistence.clone())
        .expect("Failed to register TinyFS object store");
    ProviderContext::new(session, Arc::new(persistence))
}

/// Create a test environment: an `FS` and a `ProviderContext` that share the
/// SAME `MemoryPersistence` instance.
pub async fn create_test_environment() -> (FS, ProviderContext) {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone())
        .await
        .expect("Failed to create FS");
    let session = Arc::new(SessionContext::new());
    let _ = crate::register_tinyfs_object_store(&session, persistence.clone())
        .expect("Failed to register TinyFS object store");
    let provider_context = ProviderContext::new(session, Arc::new(persistence));
    (fs, provider_context)
}

/// Write a file with the given entry type into both FS and persistence.
///
/// This ensures the file exists in FS (so glob patterns can find it) and the
/// content is in persistence (so `TinyFsObjectStore` can read it).
pub async fn create_file_with_type(
    fs: &FS,
    path: &str,
    content: Vec<u8>,
    entry_type: EntryType,
) -> Result<FileID, Box<dyn std::error::Error>> {
    use tokio::io::AsyncWriteExt;
    let root = fs.root().await?;
    let mut file_writer = root.async_writer_path_with_type(path, entry_type).await?;
    file_writer.write_all(&content).await?;
    file_writer.flush().await?;
    file_writer.shutdown().await?;

    let node_path = root.get_node_path(path).await?;
    Ok(node_path.id())
}

/// Create a parquet file (raw bytes) in both FS and persistence.
pub async fn create_parquet_file(
    fs: &FS,
    path: &str,
    parquet_data: Vec<u8>,
    entry_type: EntryType,
) -> Result<FileID, Box<dyn std::error::Error>> {
    create_file_with_type(fs, path, parquet_data, entry_type).await
}

/// Create a text file (e.g. CSV) in both FS and persistence.
pub async fn create_text_file(
    fs: &FS,
    path: &str,
    content: Vec<u8>,
    entry_type: EntryType,
) -> Result<FileID, Box<dyn std::error::Error>> {
    create_file_with_type(fs, path, content, entry_type).await
}

/// Serialize a `RecordBatch` to parquet and store it via `create_parquet_file`.
pub async fn create_parquet_from_batch(
    fs: &FS,
    path: &str,
    batch: &RecordBatch,
    entry_type: EntryType,
) -> Result<FileID, Box<dyn std::error::Error>> {
    let mut parquet_buffer = Vec::new();
    {
        let cursor = Cursor::new(&mut parquet_buffer);
        let mut writer = ArrowWriter::try_new(cursor, batch.schema(), None)?;
        writer.write(batch)?;
        _ = writer.close()?;
    }
    create_parquet_file(fs, path, parquet_buffer, entry_type).await
}
