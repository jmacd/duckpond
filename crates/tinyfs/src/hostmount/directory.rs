// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::EntryType;
use crate::dir::{Directory, DirectoryEntry, Handle};
use crate::error::{Error, Result};
use crate::metadata::{Metadata, NodeMetadata};
use crate::node::{FileID, Node, NodeType, PartID};
use async_trait::async_trait;
use futures::stream::{self, Stream};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

/// A directory backed by a host filesystem directory.
///
/// Read operations (`get`, `entries`) map to `std::fs::read_dir`.
/// Write operations (`insert`) create files/directories on the host.
/// `remove` deletes host entries.
///
/// All children get deterministic `FileID`s via `FileID::from_content()`,
/// derived from their name and the parent's partition ID.
pub struct HostDirectory {
    /// Absolute host path this directory maps to, or None if pending placement
    host_path: Option<PathBuf>,
    /// The FileID of this directory (used as parent_part_id for children)
    file_id: FileID,
}

impl HostDirectory {
    /// Create a new HostDirectory mapping to a known host path
    #[must_use]
    pub fn new(host_path: PathBuf, file_id: FileID) -> Self {
        Self {
            host_path: Some(host_path),
            file_id,
        }
    }

    /// Create a HostDirectory with no path yet -- must be resolved via `set_path`
    /// before any I/O is attempted.
    #[must_use]
    pub fn new_pending(file_id: FileID) -> Self {
        Self {
            host_path: None,
            file_id,
        }
    }

    /// Create a Handle for this directory with a known path
    #[must_use]
    pub fn new_handle(host_path: PathBuf, file_id: FileID) -> Handle {
        Handle::new(Arc::new(tokio::sync::Mutex::new(Box::new(Self::new(
            host_path, file_id,
        )))))
    }

    /// Create a handle for a pending (path-less) HostDirectory
    #[must_use]
    pub fn new_pending_handle(file_id: FileID) -> Handle {
        Handle::new(Arc::new(tokio::sync::Mutex::new(Box::new(
            Self::new_pending(file_id),
        ))))
    }

    /// Set the host path. Called by `HostDirectory::insert()` once the
    /// directory's location on the host filesystem is known.
    pub fn set_path(&mut self, path: PathBuf) {
        self.host_path = Some(path);
    }

    /// Require a path or return a clear error
    fn require_path(&self) -> Result<&PathBuf> {
        self.host_path.as_ref().ok_or_else(|| {
            Error::Other(format!(
                "HostDirectory {} has no path -- node was created but not yet \
                 inserted into a directory",
                self.file_id
            ))
        })
    }

    /// Compute a deterministic FileID for a child entry
    fn child_file_id(&self, name: &str, entry_type: EntryType) -> FileID {
        let parent_part_id = PartID::from_node_id(self.file_id.node_id());
        FileID::from_content(parent_part_id, entry_type, name.as_bytes())
    }

    /// Determine the entry type for a host filesystem entry
    fn entry_type_for(metadata: &std::fs::Metadata) -> EntryType {
        if metadata.is_dir() {
            EntryType::DirectoryPhysical
        } else {
            // All host files default to data (FilePhysicalVersion).
            // URL scheme layering handles type interpretation.
            EntryType::FilePhysicalVersion
        }
    }

    /// Build a Node for a child host entry.
    /// Requires that `self.host_path` is `Some`.
    fn build_child_node(&self, name: &str, host_child_path: &Path) -> Result<Node> {
        let metadata = std::fs::metadata(host_child_path).map_err(|e| {
            Error::Other(format!(
                "Failed to stat '{}': {}",
                host_child_path.display(),
                e
            ))
        })?;

        let entry_type = Self::entry_type_for(&metadata);
        let child_id = self.child_file_id(name, entry_type);

        let node_type = if metadata.is_dir() {
            NodeType::Directory(HostDirectory::new_handle(
                host_child_path.to_path_buf(),
                child_id,
            ))
        } else {
            NodeType::File(super::HostFile::new_handle(
                host_child_path.to_path_buf(),
                child_id,
            ))
        };

        Ok(Node::new(child_id, node_type))
    }
}

#[async_trait]
impl Metadata for HostDirectory {
    async fn metadata(&self) -> Result<NodeMetadata> {
        let path = self.require_path()?;
        let metadata = std::fs::metadata(path).map_err(|e| {
            Error::Other(format!(
                "Failed to stat directory '{}': {}",
                path.display(),
                e
            ))
        })?;

        let timestamp = metadata
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);

        Ok(NodeMetadata {
            version: 1,
            size: None,
            blake3: None,
            bao_outboard: None,
            entry_type: EntryType::DirectoryPhysical,
            timestamp,
        })
    }
}

#[async_trait]
impl Directory for HostDirectory {
    async fn get(&self, name: &str) -> Result<Option<Node>> {
        let host_path = self.require_path()?;
        let child_path = host_path.join(name);
        if !child_path.exists() {
            return Ok(None);
        }

        // Ensure the child doesn't escape the host directory (basic path traversal guard)
        let canonical = child_path.canonicalize().map_err(|e| {
            Error::Other(format!(
                "Failed to canonicalize '{}': {}",
                child_path.display(),
                e
            ))
        })?;
        let canonical_parent = host_path.canonicalize().map_err(|e| {
            Error::Other(format!(
                "Failed to canonicalize parent '{}': {}",
                host_path.display(),
                e
            ))
        })?;
        if !canonical.starts_with(&canonical_parent) {
            return Err(Error::Other(format!(
                "Path '{}' escapes hostmount root",
                name
            )));
        }

        Ok(Some(self.build_child_node(name, &child_path)?))
    }

    async fn insert(&mut self, name: String, node: Node) -> Result<()> {
        let host_path = self.require_path()?.clone();
        let child_path = host_path.join(&name);

        if child_path.exists() {
            return Err(Error::already_exists(&name));
        }

        match &node.node_type {
            NodeType::Directory(handle) => {
                std::fs::create_dir(&child_path).map_err(|e| {
                    Error::Other(format!(
                        "Failed to create directory '{}': {}",
                        child_path.display(),
                        e
                    ))
                })?;
                // The node was created with host_path=None (pending).
                // Now we know the real path, so replace the inner object.
                // Since Handle wraps Arc<Mutex<...>>, this is visible to all clones.
                let arc = handle.get_inner();
                let mut inner = arc.lock().await;
                *inner = Box::new(HostDirectory::new(child_path, node.id));
            }
            NodeType::File(handle) => {
                // Create an empty file -- content will be written via the File handle
                let _ = std::fs::File::create(&child_path).map_err(|e| {
                    Error::Other(format!(
                        "Failed to create file '{}': {}",
                        child_path.display(),
                        e
                    ))
                })?;
                // The node was created with host_path=None (pending).
                // Now we know the real path, so replace the inner object.
                let arc = handle.get_file().await;
                let mut inner = arc.lock().await;
                *inner = Box::new(super::HostFile::new(child_path, node.id));
            }
            NodeType::Symlink(_) => {
                return Err(Error::Other(
                    "Symlinks not supported on hostmount".to_string(),
                ));
            }
        }

        Ok(())
    }

    async fn remove(&mut self, name: &str) -> Result<Option<Node>> {
        let host_path = self.require_path()?;
        let child_path = host_path.join(name);
        if !child_path.exists() {
            return Ok(None);
        }

        let node = self.build_child_node(name, &child_path)?;
        let metadata = std::fs::metadata(&child_path).map_err(|e| {
            Error::Other(format!("Failed to stat '{}': {}", child_path.display(), e))
        })?;

        if metadata.is_dir() {
            std::fs::remove_dir_all(&child_path).map_err(|e| {
                Error::Other(format!(
                    "Failed to remove directory '{}': {}",
                    child_path.display(),
                    e
                ))
            })?;
        } else {
            std::fs::remove_file(&child_path).map_err(|e| {
                Error::Other(format!(
                    "Failed to remove file '{}': {}",
                    child_path.display(),
                    e
                ))
            })?;
        }

        Ok(Some(node))
    }

    async fn entries(&self) -> Result<Pin<Box<dyn Stream<Item = Result<DirectoryEntry>> + Send>>> {
        let host_path = self.require_path()?;
        let read_dir = std::fs::read_dir(host_path).map_err(|e| {
            Error::Other(format!(
                "Failed to read directory '{}': {}",
                host_path.display(),
                e
            ))
        })?;

        let mut items = Vec::new();
        for entry_result in read_dir {
            let entry = entry_result
                .map_err(|e| Error::Other(format!("Failed to read directory entry: {}", e)))?;

            let name = entry.file_name().to_string_lossy().to_string();

            // Skip hidden files (dotfiles)
            if name.starts_with('.') {
                continue;
            }

            let metadata = entry
                .metadata()
                .map_err(|e| Error::Other(format!("Failed to stat '{}': {}", name, e)))?;

            let entry_type = Self::entry_type_for(&metadata);
            let child_id = self.child_file_id(&name, entry_type);

            let timestamp = metadata
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_micros() as i64)
                .unwrap_or(0);

            items.push(Ok(DirectoryEntry::new(
                name,
                child_id.node_id(),
                entry_type,
                timestamp,
            )));
        }

        // Sort by name for deterministic ordering
        items.sort_by(|a, b| {
            let a_name = a
                .as_ref()
                .map(|e| &e.name)
                .unwrap_or(&String::new())
                .clone();
            let b_name = b
                .as_ref()
                .map(|e| &e.name)
                .unwrap_or(&String::new())
                .clone();
            a_name.cmp(&b_name)
        });

        Ok(Box::pin(stream::iter(items)))
    }
}
