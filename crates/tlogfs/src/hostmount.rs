// Hostmount dynamic directory factory for TLogFS
use std::sync::Arc;
use std::path::PathBuf;
use serde::{Serialize, Deserialize};
use tinyfs::{Directory, NodeRef, EntryType, Metadata, NodeMetadata};
use async_trait::async_trait;
use diagnostics;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HostmountConfig {
    pub directory: PathBuf,
}

pub struct HostmountDirectory {
    config: HostmountConfig,
}

impl HostmountDirectory {
    pub fn new(config: HostmountConfig) -> Self {
        let dir_str = format!("{}", config.directory.display());
        diagnostics::log_info!("HostmountDirectory::new - mounting host directory {dir}", dir: dir_str);
        Self { config }
    }
    
    /// Create a DirHandle from this hostmount directory
    pub fn create_handle(self) -> tinyfs::DirHandle {
        tinyfs::DirHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }
}

#[async_trait]
impl Directory for HostmountDirectory {
    async fn get(&self, name: &str) -> tinyfs::Result<Option<NodeRef>> {
        let path = self.config.directory.join(name);
        if path.exists() {
            let node_ref = if path.is_file() {
                tinyfs::NodeRef::new(Arc::new(tokio::sync::Mutex::new(tinyfs::Node {
                    id: tinyfs::NodeID::generate(),
                    node_type: tinyfs::NodeType::File(tinyfs::memory::MemoryFile::new_handle(vec![])),
                })))
            } else if path.is_dir() {
                tinyfs::NodeRef::new(Arc::new(tokio::sync::Mutex::new(tinyfs::Node {
                    id: tinyfs::NodeID::generate(),
                    node_type: tinyfs::NodeType::Directory(tinyfs::memory::MemoryDirectory::new_handle()),
                })))
            } else {
                return Ok(None);
            };
            Ok(Some(node_ref))
        } else {
            Ok(None)
        }
    }

    async fn insert(&mut self, _name: String, _id: NodeRef) -> tinyfs::Result<()> {
        diagnostics::log_info!("HostmountDirectory::insert - mutation not permitted");
        Err(tinyfs::Error::Other("hostmount directory is read-only".to_string()))
    }

    async fn entries(&self) -> tinyfs::Result<std::pin::Pin<Box<dyn futures::Stream<Item = tinyfs::Result<(String, NodeRef)>> + Send>>> {
        use futures::stream;
        use std::fs;
        let dir_path = self.config.directory.clone();
        let mut entries = vec![];
        match fs::read_dir(&dir_path) {
            Ok(read_dir) => {
                for entry in read_dir {
                    if let Ok(entry) = entry {
                        let file_name = entry.file_name().to_string_lossy().to_string();
                        let file_path = entry.path();
                        let node_ref = if file_path.is_file() {
                            tinyfs::NodeRef::new(Arc::new(tokio::sync::Mutex::new(tinyfs::Node {
                                id: tinyfs::NodeID::generate(),
                                node_type: tinyfs::NodeType::File(tinyfs::memory::MemoryFile::new_handle(vec![])),
                            })))
                        } else if file_path.is_dir() {
                            tinyfs::NodeRef::new(Arc::new(tokio::sync::Mutex::new(tinyfs::Node {
                                id: tinyfs::NodeID::generate(),
                                node_type: tinyfs::NodeType::Directory(tinyfs::memory::MemoryDirectory::new_handle()),
                            })))
                        } else {
                            continue;
                        };
                        entries.push(Ok((file_name, node_ref)));
                    }
                }
            }
            Err(e) => {
                return Err(tinyfs::Error::Other(format!("Failed to read host directory: {}", e)));
            }
        }
        let stream = stream::iter(entries);
        Ok(Box::pin(stream))
    }
}

#[async_trait]
impl Metadata for HostmountDirectory {
    async fn metadata(&self) -> tinyfs::Result<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,
            size: None,
            sha256: None,
            entry_type: EntryType::Directory,
            timestamp: 0,
        })
    }
}

// Factory function to create HostmountDirectory from config metadata
pub fn create_hostmount_directory(config_bytes: &[u8]) -> Result<Arc<dyn Directory + Send + Sync>, Box<dyn std::error::Error>> {
    let config: HostmountConfig = serde_yaml::from_slice(config_bytes)?;
    Ok(Arc::new(HostmountDirectory::new(config)))
}
