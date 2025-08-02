// Hostmount dynamic directory factory for TLogFS
use std::sync::Arc;
use std::path::PathBuf;
use std::pin::Pin;
use serde::{Serialize, Deserialize};
use tinyfs::{Directory, File, NodeRef, EntryType, Metadata, NodeMetadata, AsyncReadSeek};
use async_trait::async_trait;
use tokio::io::AsyncWrite;
use diagnostics;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HostmountConfig {
    pub directory: PathBuf,
}

pub struct HostmountDirectory {
    config: HostmountConfig,
}

pub struct HostmountFile {
    host_path: PathBuf,
}

impl HostmountFile {
    pub fn new(host_path: PathBuf) -> Self {
        Self { host_path }
    }
    
    /// Create a FileHandle from this hostmount file
    pub fn create_handle(self) -> tinyfs::FileHandle {
        tinyfs::FileHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }
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
                // Create HostmountFile for host files
                let hostmount_file = HostmountFile::new(path);
                tinyfs::NodeRef::new(Arc::new(tokio::sync::Mutex::new(tinyfs::Node {
                    id: tinyfs::NodeID::generate(),
                    node_type: tinyfs::NodeType::File(hostmount_file.create_handle()),
                })))
            } else if path.is_dir() {
                // Create nested HostmountDirectory for subdirectories
                let subdir_config = HostmountConfig { directory: path };
                let hostmount_subdir = HostmountDirectory::new(subdir_config);
                tinyfs::NodeRef::new(Arc::new(tokio::sync::Mutex::new(tinyfs::Node {
                    id: tinyfs::NodeID::generate(),
                    node_type: tinyfs::NodeType::Directory(hostmount_subdir.create_handle()),
                })))
            } else {
                // Skip symlinks and other special files
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
                            // Create HostmountFile for host files
                            let hostmount_file = HostmountFile::new(file_path);
                            tinyfs::NodeRef::new(Arc::new(tokio::sync::Mutex::new(tinyfs::Node {
                                id: tinyfs::NodeID::generate(),
                                node_type: tinyfs::NodeType::File(hostmount_file.create_handle()),
                            })))
                        } else if file_path.is_dir() {
                            // Create nested HostmountDirectory for subdirectories
                            let subdir_config = HostmountConfig { directory: file_path };
                            let hostmount_subdir = HostmountDirectory::new(subdir_config);
                            tinyfs::NodeRef::new(Arc::new(tokio::sync::Mutex::new(tinyfs::Node {
                                id: tinyfs::NodeID::generate(),
                                node_type: tinyfs::NodeType::Directory(hostmount_subdir.create_handle()),
                            })))
                        } else {
                            // Skip symlinks and other special files
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
impl File for HostmountFile {
    async fn async_reader(&self) -> tinyfs::Result<Pin<Box<dyn AsyncReadSeek>>> {
        let host_path_str = format!("{}", self.host_path.display());
        diagnostics::log_debug!("HostmountFile::async_reader() - reading host file {path}", path: host_path_str);
        
        // Read the entire file content into memory
        // For production use, we might want to implement streaming, but for MVP this works
        let content = tokio::fs::read(&self.host_path).await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to read host file {}: {}", self.host_path.display(), e)))?;
        
        let content_len = content.len();
        diagnostics::log_debug!("HostmountFile::async_reader() - loaded {content_len} bytes from host file", content_len: content_len);
        
        // std::io::Cursor implements both AsyncRead and AsyncSeek
        Ok(Box::pin(std::io::Cursor::new(content)))
    }
    
    async fn async_writer(&self) -> tinyfs::Result<Pin<Box<dyn AsyncWrite + Send>>> {
        diagnostics::log_info!("HostmountFile::async_writer - mutation not permitted");
        Err(tinyfs::Error::Other("hostmount file is read-only".to_string()))
    }
}

#[async_trait]
impl Metadata for HostmountFile {
    async fn metadata(&self) -> tinyfs::Result<NodeMetadata> {
        // Get file metadata from host filesystem
        let metadata = tokio::fs::metadata(&self.host_path).await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to get host file metadata {}: {}", self.host_path.display(), e)))?;
        
        Ok(NodeMetadata {
            version: 1,
            size: Some(metadata.len()),
            sha256: None, // We could compute this but it's expensive and optional
            entry_type: EntryType::FileData,
            timestamp: metadata.modified()
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
        })
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
