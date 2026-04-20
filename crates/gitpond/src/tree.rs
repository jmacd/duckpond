// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Dynamic directory and file implementations for git-ingest.
//!
//! Provides lazy, read-only views into a bare git repository:
//! - `GitRootDirectory` — top-level mount; resolves ref on each access
//! - `GitTreeDirectory` — subtree view keyed by immutable tree OID
//! - `GitBlobFile` — serves blob content on demand

use crate::git;
use async_trait::async_trait;
use futures::stream;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tinyfs::{
    DirHandle, Directory, EntryType, FileHandle, Metadata, Node, NodeMetadata, NodeType,
    SymlinkHandle,
};

/// Info needed to auto-clone a bare repo on first access (cross-pond import).
pub struct AutoCloneInfo {
    /// Git remote URL
    pub url: String,
    /// Pond root directory (for creating {pond}/git/)
    pub pond_path: PathBuf,
}

/// Top-level dynamic directory for a git-ingest mount point.
///
/// Holds the git ref name (not a resolved OID), so every access
/// re-resolves the ref to pick up fetched changes.
pub struct GitRootDirectory {
    repo_path: PathBuf,
    git_ref: String,
    prefix: Option<String>,
    file_id: tinyfs::FileID,
    /// If set, auto-clone the repo on first access when the bare repo
    /// doesn't exist (cross-pond import case).
    auto_clone: Option<AutoCloneInfo>,
}

impl GitRootDirectory {
    #[must_use]
    pub fn new(
        repo_path: PathBuf,
        git_ref: String,
        prefix: Option<String>,
        file_id: tinyfs::FileID,
        auto_clone: Option<AutoCloneInfo>,
    ) -> Self {
        Self {
            repo_path,
            git_ref,
            prefix,
            file_id,
            auto_clone,
        }
    }

    #[must_use]
    pub fn create_handle(self) -> DirHandle {
        DirHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }

    /// Ensure the bare repo exists, cloning on first access if needed.
    fn ensure_repo(&self) -> tinyfs::Result<()> {
        if self.repo_path.exists() {
            return Ok(());
        }
        let clone_info = self.auto_clone.as_ref().ok_or_else(|| {
            tinyfs::Error::Other(format!(
                "Bare repo not found at {} and no auto-clone info available. \
                 Run 'pond run <path> pull' to fetch.",
                self.repo_path.display()
            ))
        })?;

        log::info!(
            "git-ingest: bare repo not found, cloning from {} (ref: {})",
            clone_info.url,
            self.git_ref
        );
        let git_dir = clone_info.pond_path.join("git");
        std::fs::create_dir_all(&git_dir)
            .map_err(|e| tinyfs::Error::Other(format!("Failed to create git dir: {}", e)))?;
        let _ = git::fetch_and_resolve(&self.repo_path, &clone_info.url, &self.git_ref)?;
        Ok(())
    }

    /// Resolve the ref to a tree OID, applying prefix navigation.
    fn resolve_tree(&self) -> tinyfs::Result<String> {
        self.ensure_repo()?;
        let tree_oid = git::resolve_tree_at_ref(&self.repo_path, &self.git_ref)?;

        if let Some(ref prefix) = self.prefix {
            let repo = git::open_repo(&self.repo_path)?;
            git::navigate_to_prefix(&repo, &tree_oid, prefix)
        } else {
            Ok(tree_oid)
        }
    }
}

#[async_trait]
impl Directory for GitRootDirectory {
    async fn get(&self, name: &str) -> tinyfs::Result<Option<Node>> {
        let tree_oid = self.resolve_tree()?;
        let inner = GitTreeDirectory::new(self.repo_path.clone(), tree_oid, self.file_id);
        inner.get(name).await
    }

    async fn insert(&mut self, _name: String, _id: Node) -> tinyfs::Result<()> {
        Err(tinyfs::Error::Other(
            "Git directory is read-only".to_string(),
        ))
    }

    async fn remove(&mut self, _name: &str) -> tinyfs::Result<Option<Node>> {
        Err(tinyfs::Error::Other(
            "Git directory is read-only".to_string(),
        ))
    }

    async fn entries(
        &self,
    ) -> tinyfs::Result<
        std::pin::Pin<
            Box<dyn futures::Stream<Item = tinyfs::Result<tinyfs::DirectoryEntry>> + Send>,
        >,
    > {
        let tree_oid = self.resolve_tree()?;
        let inner = GitTreeDirectory::new(self.repo_path.clone(), tree_oid, self.file_id);
        inner.entries().await
    }
}

#[async_trait]
impl Metadata for GitRootDirectory {
    async fn metadata(&self) -> tinyfs::Result<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,
            size: None,
            blake3: None,
            bao_outboard: None,
            entry_type: EntryType::DirectoryDynamic,
            timestamp: 0,
        })
    }
}

/// A directory backed by an immutable git tree OID.
///
/// Children are created lazily on `get()` and cached for the
/// lifetime of this object.
pub struct GitTreeDirectory {
    repo_path: PathBuf,
    tree_oid: String,
    file_id: tinyfs::FileID,
    entry_cache: tokio::sync::RwLock<HashMap<String, Node>>,
}

impl GitTreeDirectory {
    fn new(repo_path: PathBuf, tree_oid: String, file_id: tinyfs::FileID) -> Self {
        Self {
            repo_path,
            tree_oid,
            file_id,
            entry_cache: tokio::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Build a deterministic child FileID from the parent identity and child info.
    fn child_file_id(&self, name: &str, oid: &str, entry_type: EntryType) -> tinyfs::FileID {
        let parent_part_id = tinyfs::PartID::from_node_id(self.file_id.node_id());
        let mut id_bytes = Vec::new();
        id_bytes.extend_from_slice(name.as_bytes());
        id_bytes.extend_from_slice(oid.as_bytes());
        tinyfs::FileID::from_content(
            parent_part_id,
            entry_type,
            &id_bytes,
            self.file_id.pond_id(),
        )
    }

    /// Create a Node for a single tree child entry.
    fn create_child_node(&self, child: &git::TreeChild) -> tinyfs::Result<Node> {
        match child.kind {
            git::EntryKind::Directory => {
                let entry_type = EntryType::DirectoryDynamic;
                let child_fid = self.child_file_id(&child.name, &child.oid, entry_type);
                let child_dir =
                    GitTreeDirectory::new(self.repo_path.clone(), child.oid.clone(), child_fid);
                let handle = DirHandle::new(Arc::new(tokio::sync::Mutex::new(
                    Box::new(child_dir) as Box<dyn Directory>
                )));
                Ok(Node::new(child_fid, NodeType::Directory(handle)))
            }
            git::EntryKind::File => {
                let entry_type = EntryType::FileDynamic;
                let child_fid = self.child_file_id(&child.name, &child.oid, entry_type);
                let child_file = GitBlobFile::new(self.repo_path.clone(), child.oid.clone());
                let handle = FileHandle::new(Arc::new(tokio::sync::Mutex::new(
                    Box::new(child_file) as Box<dyn tinyfs::File>,
                )));
                Ok(Node::new(child_fid, NodeType::File(handle)))
            }
            git::EntryKind::Symlink => {
                let target = git::read_symlink_target_from_repo(&self.repo_path, &child.oid)?;
                let entry_type = EntryType::Symlink;
                let child_fid = self.child_file_id(&child.name, &child.oid, entry_type);
                let symlink = GitSymlink::new(target);
                let handle = SymlinkHandle::new(Arc::new(tokio::sync::Mutex::new(
                    Box::new(symlink) as Box<dyn tinyfs::Symlink>,
                )));
                Ok(Node::new(child_fid, NodeType::Symlink(handle)))
            }
        }
    }
}

#[async_trait]
impl Directory for GitTreeDirectory {
    async fn get(&self, name: &str) -> tinyfs::Result<Option<Node>> {
        // Check cache
        {
            let cache = self.entry_cache.read().await;
            if let Some(node) = cache.get(name) {
                return Ok(Some(node.clone()));
            }
        }

        // Look up the child in the git tree
        let child = git::get_tree_child(&self.repo_path, &self.tree_oid, name)?;
        if let Some(child) = child {
            let node = self.create_child_node(&child)?;
            let mut cache = self.entry_cache.write().await;
            _ = cache.insert(name.to_string(), node.clone());
            Ok(Some(node))
        } else {
            Ok(None)
        }
    }

    async fn insert(&mut self, _name: String, _id: Node) -> tinyfs::Result<()> {
        Err(tinyfs::Error::Other(
            "Git directory is read-only".to_string(),
        ))
    }

    async fn remove(&mut self, _name: &str) -> tinyfs::Result<Option<Node>> {
        Err(tinyfs::Error::Other(
            "Git directory is read-only".to_string(),
        ))
    }

    async fn entries(
        &self,
    ) -> tinyfs::Result<
        std::pin::Pin<
            Box<dyn futures::Stream<Item = tinyfs::Result<tinyfs::DirectoryEntry>> + Send>,
        >,
    > {
        let children = git::list_tree_children(&self.repo_path, &self.tree_oid)?;
        let mut results = Vec::new();

        for child in &children {
            match self.create_child_node(child) {
                Ok(node) => {
                    // Cache the node
                    let mut cache = self.entry_cache.write().await;
                    _ = cache.insert(child.name.clone(), node.clone());

                    let dir_entry = tinyfs::DirectoryEntry::new(
                        child.name.clone(),
                        node.id().node_id(),
                        node.id().entry_type(),
                        0,
                    );
                    results.push(Ok(dir_entry));
                }
                Err(e) => {
                    results.push(Err(tinyfs::Error::Other(format!(
                        "Failed to create entry '{}': {}",
                        child.name, e
                    ))));
                }
            }
        }

        Ok(Box::pin(stream::iter(results)))
    }
}

#[async_trait]
impl Metadata for GitTreeDirectory {
    async fn metadata(&self) -> tinyfs::Result<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,
            size: None,
            blake3: None,
            bao_outboard: None,
            entry_type: EntryType::DirectoryDynamic,
            timestamp: 0,
        })
    }
}

/// A read-only file backed by a git blob OID.
pub struct GitBlobFile {
    repo_path: PathBuf,
    blob_oid: String,
}

impl GitBlobFile {
    fn new(repo_path: PathBuf, blob_oid: String) -> Self {
        Self {
            repo_path,
            blob_oid,
        }
    }
}

#[async_trait]
impl tinyfs::File for GitBlobFile {
    async fn async_reader(&self) -> tinyfs::Result<std::pin::Pin<Box<dyn tinyfs::AsyncReadSeek>>> {
        let data = git::read_blob(&self.repo_path, &self.blob_oid)?;
        let cursor = std::io::Cursor::new(data);
        Ok(Box::pin(cursor))
    }

    async fn async_writer(
        &self,
    ) -> tinyfs::Result<std::pin::Pin<Box<dyn tinyfs::FileMetadataWriter>>> {
        Err(tinyfs::Error::Other(
            "Git blob file is read-only".to_string(),
        ))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[async_trait]
impl Metadata for GitBlobFile {
    async fn metadata(&self) -> tinyfs::Result<NodeMetadata> {
        // Read blob to get size; gix objects are local, this is fast
        let data = git::read_blob(&self.repo_path, &self.blob_oid)?;
        Ok(NodeMetadata {
            version: 1,
            size: Some(data.len() as u64),
            blake3: None,
            bao_outboard: None,
            entry_type: EntryType::FileDynamic,
            timestamp: 0,
        })
    }
}

/// A read-only symlink backed by a git blob containing the target path.
struct GitSymlink {
    target: String,
}

impl GitSymlink {
    fn new(target: String) -> Self {
        Self { target }
    }
}

#[async_trait]
impl tinyfs::Symlink for GitSymlink {
    async fn readlink(&self) -> tinyfs::Result<PathBuf> {
        Ok(PathBuf::from(&self.target))
    }
}

#[async_trait]
impl Metadata for GitSymlink {
    async fn metadata(&self) -> tinyfs::Result<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,
            size: Some(self.target.len() as u64),
            blake3: None,
            bao_outboard: None,
            entry_type: EntryType::Symlink,
            timestamp: 0,
        })
    }
}
