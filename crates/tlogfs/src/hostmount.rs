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
}

#[async_trait]
impl Directory for HostmountDirectory {
    async fn get(&self, name: &str) -> tinyfs::Result<Option<NodeRef>> {
        // Read-only: map host directory contents
        let path = self.config.directory.join(name);
        if path.exists() {
            // Only allow files and directories, no mutation
            // TODO: Implement mapping logic to NodeRef
            Ok(None)
        } else {
            Ok(None)
        }
    }

    async fn insert(&mut self, _name: String, _id: NodeRef) -> tinyfs::Result<()> {
        // Read-only: mutation not allowed
        diagnostics::log_info!("HostmountDirectory::insert - mutation not permitted");
        Err(tinyfs::Error::Other("hostmount directory is read-only".to_string()))
    }

    async fn entries(&self) -> tinyfs::Result<std::pin::Pin<Box<dyn futures::Stream<Item = tinyfs::Result<(String, NodeRef)>> + Send>>> {
        // TODO: List host directory contents as NodeRefs
        Err(tinyfs::Error::Other("not yet implemented".to_string()))
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
