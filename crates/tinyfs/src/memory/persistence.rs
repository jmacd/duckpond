use crate::error::{Error, Result};
use crate::node::{NodeID, FileID, NodeType};
use crate::persistence::{DirectoryOperation, FileVersionInfo, PersistenceLayer};
use crate::{EntryType, NodeMetadata};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Version information for a file in memory persistence
#[derive(Debug, Clone)]
struct MemoryFileVersion {
    version: u64,
    timestamp: i64,
    content: Vec<u8>,
    entry_type: EntryType,
    extended_metadata: Option<HashMap<String, String>>,
}

/// In-memory persistence layer for testing and derived file computation
/// This implements the PersistenceLayer trait using in-memory storage
pub struct MemoryPersistence {
    // Store multiple versions of each file: (node_id, part_id) -> Vec<MemoryFileVersion>
    file_versions: Arc<Mutex<HashMap<FileID, Vec<MemoryFileVersion>>>>,

    // Non-file nodes (directories, symlinks): (node_id, part_id) -> NodeType
    nodes: Arc<Mutex<HashMap<FileID, NodeType>>>,

    // parent_id -> {name -> child_id}    
    directories: Arc<Mutex<HashMap<FileID, HashMap<String, FileID>>>>,

    // Root directory state
    root_dir: Arc<Mutex<crate::dir::Handle>>,
}

impl Default for MemoryPersistence {
    fn default() -> Self {
        Self {
            file_versions: Arc::new(Mutex::new(HashMap::new())),
            nodes: Arc::new(Mutex::new(HashMap::new())),
            directories: Arc::new(Mutex::new(HashMap::new())),
            root_dir: Arc::new(Mutex::new(crate::memory::MemoryDirectory::new_handle())),
        }
    }
}

#[async_trait]
impl PersistenceLayer for MemoryPersistence {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn load_node(&self, id: FileID) -> Result<NodeType> {
        let nodes = self.nodes.lock().await;
        match nodes.get(&id) {
            Some(node_type) => Ok(node_type.clone()),
            None => Err(Error::IDNotFound(id))
        }
    }

    async fn store_node(
        &self,
        id: FileID,
        node_type: &NodeType,
    ) -> Result<()> {
        let mut nodes = self.nodes.lock().await;
        _ = nodes.insert(id, node_type.clone());
        Ok(())
    }

    async fn exists_node(&self, id: FileID) -> Result<bool> {
        let nodes = self.nodes.lock().await;
        Ok(nodes.contains_key(&id))
    }

    async fn load_directory_entries(
        &self,
        parent_id: FileID,
    ) -> Result<HashMap<String, crate::DirectoryEntry>> {
        let directories = self.directories.lock().await;
        
        // Convert from (NodeID, EntryType) to DirectoryEntry
        Ok(directories
            .get(&parent_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|(name, (child_node_id, entry_type))| {
                let dir_entry = crate::DirectoryEntry::new(
                    name.clone(),
                    child_node_id,
                    entry_type,
                    0, // Version not tracked in memory @@@
                );
                (name, dir_entry)
            })
            .collect())
    }

    async fn load_symlink_target(
        &self,
        id: FileID,
    ) -> Result<std::path::PathBuf> {
        let node_type = self.load_node(id).await?;
        match node_type {
            NodeType::Symlink(symlink_handle) => symlink_handle.readlink().await,
            _ => Err(Error::Other(
                "Expected symlink node type".to_string(),
            )),
        }
    }

    async fn store_symlink_target(
        &self,
        id: FileID,
        target: &std::path::Path,
    ) -> Result<()> {
        let symlink_handle = crate::memory::MemorySymlink::new_handle(target.to_path_buf());
        let node_type = NodeType::Symlink(symlink_handle);
        self.store_node(id, &node_type).await
    }

    async fn create_file_node(
        &self,
        id: FileID,
    ) -> Result<NodeType> {
	// @@@ DO NOT PASS CONTENT????
        let file_handle = crate::memory::MemoryFile::new_handle_with_entry_type([], id.entry_type());
        Ok(NodeType::File(file_handle))
    }

    async fn create_directory_node(&self, _id: FileID) -> Result<NodeType> {
        let dir_handle = crate::memory::MemoryDirectory::new_handle();
        Ok(NodeType::Directory(dir_handle))
    }

    async fn create_symlink_node(
        &self,
        id: FileID,
        target: &std::path::Path,
    ) -> Result<NodeType> {
        let symlink_handle = crate::memory::MemorySymlink::new_handle(target.to_path_buf());
        let node_type = NodeType::Symlink(symlink_handle.clone());
        self.store_node(id, &node_type).await?;
        Ok(node_type)
    }

    async fn query_directory_entry(
        &self,
        parent_id: FileID,
        entry_name: &str,
    ) -> Result<Option<(FileID, EntryType)>> {
        let directories = self.directories.lock().await;
        Ok(directories
            .get(&parent_id)
            .and_then(|entries| entries.get(entry_name))
            .cloned())
    }

    async fn metadata(&self, node_id: NodeID, part_id: NodeID) -> Result<NodeMetadata> {
        let nodes = self.nodes.lock().await;
        if let Some(node_type) = nodes.get(&(node_id, part_id)) {
            match node_type {
                NodeType::File(handle) => {
                    // Query the handle's metadata to get the entry type
                    handle.metadata().await
                }
                NodeType::Directory(_) => Ok(NodeMetadata {
                    version: 1,
                    size: None,
                    sha256: None,
                    entry_type: EntryType::DirectoryPhysical,
                    timestamp: 0,
                }),
                NodeType::Symlink(_) => Ok(NodeMetadata {
                    version: 1,
                    size: None,
                    sha256: None,
                    entry_type: EntryType::Symlink,
                    timestamp: 0,
                }),
            }
        } else if node_id == NodeID::root() {
            Ok(NodeMetadata {
                version: 1,
                size: None,
                sha256: None,
                entry_type: EntryType::DirectoryPhysical,
                timestamp: 0,
            })
        } else {
            Err(Error::NotFound(std::path::PathBuf::from(
                format!("Node {} not found", node_id),
            )))
        }
    }

    async fn metadata_u64(
        &self,
        _id: FileID,
        _name: &str,
    ) -> Result<Option<u64>> {
        Err(Error::Internal("unimplemented".into()))
    }

    async fn update_directory_entry(
        &self,
        parent_node_id: NodeID,
        entry_name: &str,
        operation: DirectoryOperation,
    ) -> Result<()> {
        let mut directories = self.directories.lock().await;
        let dir_entries = directories
            .entry(parent_node_id)
            .or_insert_with(HashMap::new);
        match operation {
            DirectoryOperation::InsertWithType(node_id, entry_type) => {
                _ = dir_entries.insert(entry_name.to_string(), (node_id, entry_type));
            }
            DirectoryOperation::DeleteWithType(_) => {
                _ = dir_entries.remove(entry_name);
            }
            DirectoryOperation::RenameWithType(new_name, node_id, entry_type) => {
                _ = dir_entries.remove(entry_name);
                _ = dir_entries.insert(new_name, (node_id, entry_type));
            }
        }
        Ok(())
    }

    async fn list_file_versions(
        &self,
        id: FileID,
    ) -> Result<Vec<FileVersionInfo>> {
        let file_versions = self.file_versions.lock().await;
        if let Some(versions) = file_versions.get(&id) {
            let version_infos = versions
                .iter()
                .map(|v| FileVersionInfo {
                    version: v.version,
                    timestamp: v.timestamp,
                    size: v.content.len() as u64,
                    sha256: None,
                    entry_type: v.entry_type,
                    extended_metadata: v.extended_metadata.clone(),
                })
                .collect();
            Ok(version_infos)
        } else {
            Ok(Vec::new())
        }
    }

    async fn read_file_version(
        &self,
        id: FileID,
        version: Option<u64>,
    ) -> Result<Vec<u8>> {
        let file_versions = self.file_versions.lock().await;
        if let Some(versions) = file_versions.get(&id) {
            match version {
                Some(v) => {
                    if let Some(file_version) = versions.iter().find(|fv| fv.version == v) {
                        Ok(file_version.content.clone())
                    } else {
                        Err(Error::NotFound(std::path::PathBuf::from(
                            format!("Version {} of file {} not found", v, id),
                        )))
                    }
                }
                None => {
                    if let Some(latest) = versions.last() {
                        Ok(latest.content.clone())
                    } else {
                        Err(Error::NotFound(std::path::PathBuf::from(
                            format!("No versions of file {} found", id),
                        )))
                    }
                }
            }
        } else {
            Err(Error::NotFound(std::path::PathBuf::from(
                format!("File {} not found", id),
            )))
        }
    }

    async fn set_extended_attributes(
        &self,
        id: FileID,
        attributes: HashMap<String, String>,
    ) -> Result<()> {
        let mut file_versions = self.file_versions.lock().await;
        if let Some(versions) = file_versions.get_mut(&id) {
            if let Some(latest_version) = versions.last_mut() {
                latest_version.extended_metadata = Some(attributes);
                Ok(())
            } else {
                Err(Error::NotFound(std::path::PathBuf::from(
                    format!("No versions of file {} found", id),
                )))
            }
        } else {
            Err(Error::NotFound(std::path::PathBuf::from(
                format!("File {} not found", id),
            )))
        }
    }

    async fn create_dynamic_node(
        &self,
        _id: FileID,
        _name: String,
	_entry_type: EntryType,
        _factory_type: &str,
        _config_content: Vec<u8>,
    ) -> Result<NodeType> {
        Err(Error::Other(
            "Dynamic nodes not supported in memory persistence".to_string(),
        ))
    }

    async fn get_dynamic_node_config(
        &self,
        _id: FileID,
    ) -> Result<Option<(String, Vec<u8>)>> {
        Err(Error::Internal("unimplemented".into()))
    }

    async fn update_dynamic_node_config(
        &self,
        _id: FileID,
        _factory_type: &str,
        _config_content: Vec<u8>,
    ) -> Result<()> {
        Err(Error::Other(
            "Dynamic node updates not supported in memory persistence".to_string(),
        ))
    }
}
