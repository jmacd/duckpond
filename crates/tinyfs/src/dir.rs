use std::cell::Ref;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::io::{AsyncRead, AsyncWrite};

use async_trait::async_trait;
use futures::stream::{Stream, StreamExt};
use diagnostics::log_debug;

use crate::error::*;
use crate::metadata::Metadata;
use crate::node::*;

/// Represents a directory containing named entries.
#[async_trait]
pub trait Directory: Metadata + Send + Sync {
    async fn get(&self, name: &str) -> Result<Option<NodeRef>>;

    async fn insert(&mut self, name: String, id: NodeRef) -> Result<()>;

    async fn entries(&self) -> Result<Pin<Box<dyn Stream<Item = Result<(String, NodeRef)>> + Send>>>;
}

/// A handle for a refcounted directory.
#[derive(Clone)]
pub struct Handle(Arc<tokio::sync::Mutex<Box<dyn Directory>>>);

/// Async directory entry stream
pub struct DirEntryStream {
    entries: Vec<NodePath>,
    current: usize,
}

impl Stream for DirEntryStream {
    type Item = NodePath;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        if self.current < self.entries.len() {
            let entry = self.entries[self.current].clone();
            self.current += 1;
            std::task::Poll::Ready(Some(entry))
        } else {
            std::task::Poll::Ready(None)
        }
    }
}

/// Represents a Dir/File/Symlink handle with the active path.
#[derive(Clone)]
pub struct Pathed<T> {
    handle: T,
    path: PathBuf,
}

impl Handle {
    pub fn new(r: Arc<tokio::sync::Mutex<Box<dyn Directory>>>) -> Self {
        Self(r)
    }

    pub async fn get(&self, name: &str) -> Result<Option<NodeRef>> {
        let dir = self.0.lock().await;
        dir.get(name).await
    }

    pub async fn insert(&self, name: String, id: NodeRef) -> Result<()> {
        log_debug!("Handle::insert() - forwarding to Directory trait: {name}", name: name);
        let mut dir = self.0.lock().await;
        let type_name = std::any::type_name::<dyn Directory>();
        log_debug!("Handle::insert() - calling Directory::insert() on: {type_name}", type_name: type_name, name: name);
        let result = dir.insert(name, id).await;
        let result_str = result.as_ref().map(|_| "Ok").map_err(|e| format!("Err({})", e));
        let result_display = format!("{:?}", result_str);
        log_debug!("Handle::insert() - Directory::insert() completed with result: {result_display}", result_display: result_display);
        result
    }

    pub async fn entries(&self) -> Result<Pin<Box<dyn Stream<Item = Result<(String, NodeRef)>> + Send>>> {
        let dir = self.0.lock().await;
        dir.entries().await
    }

    /// Get metadata through the directory handle
    pub async fn metadata(&self) -> Result<crate::NodeMetadata> {
        let dir = self.0.lock().await;
        dir.metadata().await
    }

    /// Get metadata through the directory handle
    pub async fn metadata_u64(&self, name: &str) -> Result<Option<u64>> {
        let dir = self.0.lock().await;
        dir.metadata_u64(name).await
    }
}

impl<T> Pathed<T> {
    pub fn new<P: AsRef<Path>>(path: P, handle: T) -> Self {
        Self {
            handle,
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn path(&self) -> PathBuf {
        self.path.to_path_buf()
    }
}

impl Pathed<crate::file::Handle> {
    /// Get async reader with seek support for file content
    pub async fn async_reader(&self) -> Result<Pin<Box<dyn crate::file::AsyncReadSeek>>> {
        self.handle.async_reader().await
    }
    
    /// Get async writer for streaming file content
    pub async fn async_writer(&self) -> Result<Pin<Box<dyn AsyncWrite + Send>>> {
        self.handle.async_writer().await
    }

    /// Get metadata through the file handle
    pub async fn metadata(&self) -> Result<crate::NodeMetadata> {
        self.handle.metadata().await
    }
}

impl Pathed<Handle> {
    pub async fn get(&self, name: &str) -> Result<Option<NodePath>> {
        if let Some(nr) = self.handle.get(name).await? {
            Ok(Some(NodePath {
                node: nr,
                path: self.path.join(name),
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn insert(&self, name: String, id: NodeRef) -> Result<()> {
        self.handle.insert(name, id).await
    }

    pub async fn read_dir(&self) -> Result<DirEntryStream> {
        let mut entries = Vec::new();
        let mut stream = self.handle.entries().await?;
        
        use futures::StreamExt;
        while let Some(result) = stream.next().await {
            let (name, nref) = result?;
            entries.push(NodePath {
                node: nref,
                path: self.path.join(name),
            });
        }
        
        Ok(DirEntryStream {
            entries,
            current: 0,
        })
    }
}

impl Pathed<crate::symlink::Handle> {
    pub async fn readlink(&self) -> Result<PathBuf> {
        self.handle.readlink().await
    }
}
