// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::FileID;
use crate::error;
use crate::file::{AsyncReadSeek, File, Handle};
use crate::metadata::{Metadata, NodeMetadata};
use async_trait::async_trait;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;

/// Represents a file backed by a host filesystem file.
///
/// Reads and writes go directly to the host file. No versioning,
/// no bao-tree integrity, no OpLog — just plain file I/O.
///
/// `host_path` is `None` when the node has been created by
/// `create_file_node` but not yet inserted into a directory (the path
/// is only known at `insert()` time). Any I/O on a path-less file
/// returns a clear error.
pub struct HostFile {
    /// Absolute host path, or None if the file hasn't been placed yet
    host_path: Option<PathBuf>,
    /// File identifier (deterministic, computed from path)
    id: FileID,
}

#[async_trait]
impl Metadata for HostFile {
    async fn metadata(&self) -> error::Result<NodeMetadata> {
        let path = self.require_path()?;
        let metadata = tokio::fs::metadata(path).await.map_err(|e| {
            error::Error::Other(format!("Failed to stat '{}': {}", path.display(), e))
        })?;

        let size = metadata.len();
        let timestamp = metadata
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);

        Ok(NodeMetadata {
            version: 1,
            size: Some(size),
            blake3: None,       // Not computed for host files
            bao_outboard: None, // Not computed for host files
            entry_type: self.id.entry_type(),
            timestamp,
        })
    }
}

#[async_trait]
impl File for HostFile {
    async fn async_reader(&self) -> error::Result<Pin<Box<dyn AsyncReadSeek>>> {
        let path = self.require_path()?;
        let file = tokio::fs::File::open(path).await.map_err(|e| {
            error::Error::Other(format!(
                "Failed to open '{}' for reading: {}",
                path.display(),
                e
            ))
        })?;

        Ok(Box::pin(file))
    }

    async fn async_writer(&self) -> error::Result<Pin<Box<dyn crate::file::FileMetadataWriter>>> {
        let path = self.require_path()?;
        let file = tokio::fs::File::create(path).await.map_err(|e| {
            error::Error::Other(format!(
                "Failed to open '{}' for writing: {}",
                path.display(),
                e
            ))
        })?;

        Ok(Box::pin(HostFileWriter { inner: file }))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl HostFile {
    /// Create a new HostFile backed by a known host path
    #[must_use]
    pub fn new(host_path: PathBuf, id: FileID) -> Self {
        Self {
            host_path: Some(host_path),
            id,
        }
    }

    /// Create a HostFile with no path yet — must be resolved via `set_path`
    /// before any I/O is attempted.
    #[must_use]
    pub fn new_pending(id: FileID) -> Self {
        Self {
            host_path: None,
            id,
        }
    }

    /// Create a new HostFile handle with a known path
    #[must_use]
    pub fn new_handle(host_path: PathBuf, id: FileID) -> Handle {
        Handle::new(Arc::new(tokio::sync::Mutex::new(Box::new(Self::new(
            host_path, id,
        )))))
    }

    /// Create a handle for a pending (path-less) HostFile
    #[must_use]
    pub fn new_pending_handle(id: FileID) -> Handle {
        Handle::new(Arc::new(tokio::sync::Mutex::new(Box::new(
            Self::new_pending(id),
        ))))
    }

    /// Set the host path. Called by `HostDirectory::insert()` once the
    /// file's location on the host filesystem is known.
    pub fn set_path(&mut self, path: PathBuf) {
        self.host_path = Some(path);
    }

    /// Get the host path, if set
    #[must_use]
    pub fn host_path(&self) -> Option<&PathBuf> {
        self.host_path.as_ref()
    }

    /// Require a path or return a clear error
    fn require_path(&self) -> error::Result<&PathBuf> {
        self.host_path.as_ref().ok_or_else(|| {
            error::Error::Other(format!(
                "HostFile {} has no path — node was created but not yet \
                 inserted into a directory",
                self.id
            ))
        })
    }
}

/// A writer that writes directly to a host filesystem file.
///
/// No versioning, no bao-tree, no transactions. Just forwards writes
/// to `tokio::fs::File`. `FileMetadataWriter` methods are no-ops.
struct HostFileWriter {
    inner: tokio::fs::File,
}

#[async_trait]
impl crate::file::FileMetadataWriter for HostFileWriter {
    fn set_temporal_metadata(&mut self, _min: i64, _max: i64, _timestamp_column: String) {
        // No-op: host files don't track temporal metadata
    }

    async fn infer_temporal_bounds(&mut self) -> error::Result<(i64, i64, String)> {
        Err(crate::Error::Other(
            "infer_temporal_bounds not supported for host files".to_string(),
        ))
    }
}

impl AsyncWrite for HostFileWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
