use crate::persistence::{PersistenceLayer, DirectoryOperation, FileVersionInfo};
use crate::node::{NodeID, NodeType};
use crate::error::Result;
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
    file_versions: Arc<Mutex<HashMap<(NodeID, NodeID), Vec<MemoryFileVersion>>>>,
    // Non-file nodes (directories, symlinks): (node_id, part_id) -> NodeType
    nodes: Arc<Mutex<HashMap<(NodeID, NodeID), NodeType>>>, 
    directories: Arc<Mutex<HashMap<NodeID, HashMap<String, (NodeID, EntryType)>>>>, // parent_id -> {name -> child_id}
    root_dir: Arc<Mutex<Option<crate::dir::Handle>>>, // Shared root directory state
    next_version: Arc<Mutex<u64>>, // Global version counter
}

impl MemoryPersistence {
    pub fn new() -> Self {
        Self {
            file_versions: Arc::new(Mutex::new(HashMap::new())),
            nodes: Arc::new(Mutex::new(HashMap::new())),
            directories: Arc::new(Mutex::new(HashMap::new())),
            root_dir: Arc::new(Mutex::new(None)), // Initially no root
            next_version: Arc::new(Mutex::new(1)), // Start version counter at 1
        }
    }
    
    /// Get the next version number for a file
    async fn get_next_version(&self) -> u64 {
        let mut counter = self.next_version.lock().await;
        let version = *counter;
        *counter += 1;
        version
    }
}

#[async_trait]
impl PersistenceLayer for MemoryPersistence {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn load_node(&self, node_id: NodeID, part_id: NodeID) -> Result<NodeType> {
        let nodes = self.nodes.lock().await;
        match nodes.get(&(node_id, part_id)) {
            Some(node_type) => Ok(node_type.clone()),
            None => {
                if node_id == NodeID::root() {
                    let mut root_guard = self.root_dir.lock().await;
                    if let Some(ref existing_root) = *root_guard {
                        Ok(NodeType::Directory(existing_root.clone()))
                    } else {
                        let new_root = crate::memory::MemoryDirectory::new_handle();
                        *root_guard = Some(new_root.clone());
                        Ok(NodeType::Directory(new_root))
                    }
                } else {
                    Err(crate::error::Error::NotFound(std::path::PathBuf::from(format!("Node {} not found", node_id))))
                }
            }
        }
    }

    async fn store_node(&self, node_id: NodeID, part_id: NodeID, node_type: &NodeType) -> Result<()> {
        let mut nodes = self.nodes.lock().await;
        nodes.insert((node_id, part_id), node_type.clone());
        Ok(())
    }

    async fn exists_node(&self, node_id: NodeID, part_id: NodeID) -> Result<bool> {
        let nodes = self.nodes.lock().await;
        Ok(nodes.contains_key(&(node_id, part_id)))
    }

    async fn load_directory_entries(&self, parent_node_id: NodeID) -> Result<HashMap<String, (NodeID, EntryType)>> {
        let directories = self.directories.lock().await;
        Ok(directories.get(&parent_node_id).cloned().unwrap_or_default())
    }

    async fn load_symlink_target(&self, node_id: NodeID, part_id: NodeID) -> Result<std::path::PathBuf> {
        let node_type = self.load_node(node_id, part_id).await?;
        match node_type {
            NodeType::Symlink(symlink_handle) => {
                symlink_handle.readlink().await
            }
            _ => Err(crate::error::Error::Other("Expected symlink node type".to_string()))
        }
    }

    async fn store_symlink_target(&self, node_id: NodeID, part_id: NodeID, target: &std::path::Path) -> Result<()> {
        let symlink_handle = crate::memory::MemorySymlink::new_handle(target.to_path_buf());
        let node_type = NodeType::Symlink(symlink_handle);
        self.store_node(node_id, part_id, &node_type).await
    }

    async fn create_file_node(&self, _node_id: NodeID, _part_id: NodeID, entry_type: EntryType) -> Result<NodeType> {
        let file_handle = crate::memory::MemoryFile::new_handle_with_entry_type(&[], entry_type);
        Ok(NodeType::File(file_handle))
    }

    async fn create_directory_node(&self, _node_id: NodeID, _part_id: NodeID) -> Result<NodeType> {
        let dir_handle = crate::memory::MemoryDirectory::new_handle();
        Ok(NodeType::Directory(dir_handle))
    }

    async fn create_symlink_node(&self, node_id: NodeID, part_id: NodeID, target: &std::path::Path) -> Result<NodeType> {
        let symlink_handle = crate::memory::MemorySymlink::new_handle(target.to_path_buf());
        let node_type = NodeType::Symlink(symlink_handle.clone());
        self.store_node(node_id, part_id, &node_type).await?;
        Ok(node_type)
    }

    
    async fn query_directory_entry(&self, parent_node_id: NodeID, entry_name: &str) -> Result<Option<(NodeID, EntryType)>> {
        let directories = self.directories.lock().await;
        Ok(directories.get(&parent_node_id)
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
                    entry_type: EntryType::Directory,
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
                entry_type: EntryType::Directory,
                timestamp: 0,
            })
        } else {
            Err(crate::error::Error::NotFound(std::path::PathBuf::from(format!("Node {} not found", node_id))))
        }
    }

    async fn metadata_u64(&self, _node_id: NodeID, _part_id: NodeID, _name: &str) -> Result<Option<u64>> {
        Ok(None)
    }

    async fn update_directory_entry(&self, parent_node_id: NodeID, entry_name: &str, operation: DirectoryOperation) -> Result<()> {
        let mut directories = self.directories.lock().await;
        let dir_entries = directories.entry(parent_node_id).or_insert_with(HashMap::new);
        match operation {
            DirectoryOperation::InsertWithType(node_id, entry_type) => {
                dir_entries.insert(entry_name.to_string(), (node_id, entry_type));
            }
            DirectoryOperation::DeleteWithType(_) => {
                dir_entries.remove(entry_name);
            }
            DirectoryOperation::RenameWithType(new_name, node_id, entry_type) => {
                dir_entries.remove(entry_name);
                dir_entries.insert(new_name, (node_id, entry_type));
            }
        }
        Ok(())
    }

    async fn list_file_versions(&self, node_id: NodeID, part_id: NodeID) -> Result<Vec<FileVersionInfo>> {
        let file_versions = self.file_versions.lock().await;
        if let Some(versions) = file_versions.get(&(node_id, part_id)) {
            let version_infos = versions.iter().map(|v| FileVersionInfo {
                version: v.version,
                timestamp: v.timestamp,
                size: v.content.len() as u64,
                sha256: None,
                entry_type: v.entry_type.clone(),
                extended_metadata: v.extended_metadata.clone(),
            }).collect();
            Ok(version_infos)
        } else {
            Ok(Vec::new())
        }
    }

    async fn read_file_version(&self, node_id: NodeID, part_id: NodeID, version: Option<u64>) -> Result<Vec<u8>> {
        let file_versions = self.file_versions.lock().await;
        if let Some(versions) = file_versions.get(&(node_id, part_id)) {
            match version {
                Some(v) => {
                    if let Some(file_version) = versions.iter().find(|fv| fv.version == v) {
                        Ok(file_version.content.clone())
                    } else {
                        Err(crate::error::Error::NotFound(
                            std::path::PathBuf::from(format!("Version {} of file {} not found", v, node_id))
                        ))
                    }
                }
                None => {
                    if let Some(latest) = versions.last() {
                        Ok(latest.content.clone())
                    } else {
                        Err(crate::error::Error::NotFound(
                            std::path::PathBuf::from(format!("No versions of file {} found", node_id))
                        ))
                    }
                }
            }
        } else {
            Err(crate::error::Error::NotFound(
                std::path::PathBuf::from(format!("File {} not found", node_id))
            ))
        }
    }

    async fn create_dynamic_directory_node(&self, _parent_node_id: NodeID, _name: String, _factory_type: &str, _config_content: Vec<u8>) -> Result<NodeID> {
        Err(crate::Error::Other("Dynamic nodes not supported in memory persistence".to_string()))
    }

    async fn create_dynamic_file_node(&self, _parent_node_id: NodeID, _name: String, _file_type: crate::EntryType, _factory_type: &str, _config_content: Vec<u8>) -> Result<NodeID> {
        Err(crate::Error::Other("Dynamic nodes not supported in memory persistence".to_string()))
    }

    async fn get_dynamic_node_config(&self, _node_id: NodeID, _part_id: NodeID) -> Result<Option<(String, Vec<u8>)>> {
        Ok(None)
    }
}
