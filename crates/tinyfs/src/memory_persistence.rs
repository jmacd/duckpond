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
    directories: Arc<Mutex<HashMap<NodeID, HashMap<String, NodeID>>>>, // parent_id -> {name -> child_id}
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
                // For root directory, return the shared root if it exists, create one if it doesn't
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
    
    async fn load_directory_entries(&self, parent_node_id: NodeID) -> Result<HashMap<String, NodeID>> {
        let directories = self.directories.lock().await;
        Ok(directories.get(&parent_node_id).cloned().unwrap_or_default())
    }
    
    async fn query_directory_entry_by_name(&self, parent_node_id: NodeID, entry_name: &str) -> Result<Option<NodeID>> {
        let directories = self.directories.lock().await;
        Ok(directories.get(&parent_node_id)
            .and_then(|entries| entries.get(entry_name))
            .cloned())
    }
    
    async fn load_file_content(&self, node_id: NodeID, part_id: NodeID) -> Result<Vec<u8>> {
        // Get the latest version of the file
        let file_versions = self.file_versions.lock().await;
        if let Some(versions) = file_versions.get(&(node_id, part_id)) {
            if let Some(latest) = versions.last() {
                return Ok(latest.content.clone());
            }
        }
        
        // Fallback: check if it's stored as a non-file node (legacy)
        let node_type = self.load_node(node_id, part_id).await?;
        match node_type {
            NodeType::File(file_handle) => {
                crate::async_helpers::buffer_helpers::read_file_to_vec(&file_handle).await
            }
            _ => Err(crate::error::Error::Other("Expected file node type".to_string()))
        }
    }
    
    async fn store_file_content(&self, node_id: NodeID, part_id: NodeID, content: &[u8]) -> Result<()> {
        self.store_file_content_with_type(node_id, part_id, content, EntryType::FileData).await
    }
    
    async fn store_file_content_with_type(&self, node_id: NodeID, part_id: NodeID, content: &[u8], entry_type: crate::EntryType) -> Result<()> {
        let version = self.get_next_version().await;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;
        
        let file_version = MemoryFileVersion {
            version,
            timestamp,
            content: content.to_vec(),
            entry_type,
            extended_metadata: None,
        };
        
        let mut file_versions = self.file_versions.lock().await;
        let versions = file_versions.entry((node_id, part_id)).or_insert_with(Vec::new);
        versions.push(file_version);
        
        // Also create a file handle for backward compatibility
        let file_handle = crate::memory::MemoryFile::new_handle(content);
        let node_type = NodeType::File(file_handle);
        
        let mut nodes = self.nodes.lock().await;
        nodes.insert((node_id, part_id), node_type);
        
        Ok(())
    }
    
    async fn load_symlink_target(&self, node_id: NodeID, part_id: NodeID) -> Result<std::path::PathBuf> {
        // Load the node and extract target directly
        let node_type = self.load_node(node_id, part_id).await?;
        match node_type {
            NodeType::Symlink(symlink_handle) => {
                symlink_handle.readlink().await
            }
            _ => Err(crate::error::Error::Other("Expected symlink node type".to_string()))
        }
    }
    
    async fn store_symlink_target(&self, node_id: NodeID, part_id: NodeID, target: &std::path::Path) -> Result<()> {
        // Create and store a memory symlink with the target
        let symlink_handle = crate::memory::MemorySymlink::new_handle(target.to_path_buf());
        let node_type = NodeType::Symlink(symlink_handle);
        self.store_node(node_id, part_id, &node_type).await
    }
    
    async fn create_file_node(&self, node_id: NodeID, part_id: NodeID, content: &[u8], _entry_type: EntryType) -> Result<NodeType> {
        // Create a memory file with the content
        // Note: EntryType is stored separately and not used in memory implementation
        let file_handle = crate::memory::MemoryFile::new_handle(content);
        let node_type = NodeType::File(file_handle.clone());
        
        // Store it to persistence
        self.store_node(node_id, part_id, &node_type).await?;
        
        Ok(node_type)
    }
    
    async fn create_file_node_memory_only(&self, _node_id: NodeID, _part_id: NodeID, _entry_type: EntryType) -> Result<NodeType> {
        // For memory persistence, just create a file handle without persisting
        // The entry_type is stored in the file metadata for later use
        let file_handle = crate::memory::MemoryFile::new_handle(&[]);
        Ok(NodeType::File(file_handle))
    }
    
    async fn create_directory_node(&self, _node_id: NodeID, _parent_node_id: NodeID) -> Result<NodeType> {
        // Create a memory directory
        let dir_handle = crate::memory::MemoryDirectory::new_handle();
        Ok(NodeType::Directory(dir_handle))
    }
    
    async fn create_symlink_node(&self, node_id: NodeID, part_id: NodeID, target: &std::path::Path) -> Result<NodeType> {
        // Create a memory symlink with the target
        let symlink_handle = crate::memory::MemorySymlink::new_handle(target.to_path_buf());
        let node_type = NodeType::Symlink(symlink_handle.clone());
        
        // Store it to persistence
        self.store_node(node_id, part_id, &node_type).await?;
        
        Ok(node_type)
    }
    
    async fn begin_transaction(&self) -> Result<()> {
        // Memory persistence doesn't need explicit transactions
        Ok(())
    }
    
    async fn commit(&self) -> Result<()> {
        // Memory persistence doesn't need commit
        Ok(())
    }
    
    async fn rollback(&self) -> Result<()> {
        // Memory persistence doesn't support rollback for now
        Ok(())
    }
    
    async fn metadata(&self, node_id: NodeID, part_id: NodeID) -> Result<NodeMetadata> {
        // For memory persistence, we need to check the node type and return appropriate metadata
        let nodes = self.nodes.lock().await;
        if let Some(node_type) = nodes.get(&(node_id, part_id)) {
            match node_type {
                NodeType::File(_) => Ok(NodeMetadata {
                    version: 1, // Memory files don't track versions
                    size: Some(0), // Would need actual file size - placeholder for now
                    sha256: Some("memory-file-placeholder".to_string()),
                    entry_type: EntryType::FileData, // Default for memory files
		    timestamp: 0, // TODO
                }),
                NodeType::Directory(_) => Ok(NodeMetadata {
                    version: 1,
                    size: None, // Directories don't have sizes
                    sha256: None, // Directories don't have checksums
                    entry_type: EntryType::Directory,
           		    timestamp: 0, // TODO
     }),
                NodeType::Symlink(_) => Ok(NodeMetadata {
                    version: 1,
                    size: None, // Symlinks don't have sizes
                    sha256: None, // Symlinks don't have checksums
                    entry_type: EntryType::Symlink,
		    timestamp: 0, // TODO
                }),
            }
        } else if node_id == NodeID::root() {
            // Special case for root directory. TODO: why is this special?
            Ok(NodeMetadata {
                version: 1,
                size: None,
                sha256: None,
                entry_type: EntryType::Directory,
		timestamp: 0, // TODO

            })
        } else {
            Err(crate::error::Error::NotFound(std::path::PathBuf::from(format!("Node {} not found", node_id))))
        }
    }
    
    async fn metadata_u64(&self, _node_id: NodeID, _part_id: NodeID, _name: &str) -> Result<Option<u64>> {
        // Memory persistence doesn't store metadata
        // For testing purposes, return None for all metadata queries
        Ok(None)
    }
    
    async fn has_pending_operations(&self) -> Result<bool> {
        // Memory persistence doesn't have pending operations concept
        Ok(false)
    }
    
    async fn query_directory_entry_with_type_by_name(&self, parent_node_id: NodeID, entry_name: &str) -> Result<Option<(NodeID, crate::EntryType)>> {
        // Memory persistence is for testing only - return "file:data" as default node type since tests don't track this
        if let Some(child_node_id) = self.query_directory_entry_by_name(parent_node_id, entry_name).await? {
            Ok(Some((child_node_id, crate::EntryType::FileData)))
        } else {
            Ok(None)
        }
    }
    
    async fn load_directory_entries_with_types(&self, parent_node_id: NodeID) -> Result<HashMap<String, (NodeID, crate::EntryType)>> {
        // Memory persistence is for testing only - return "file:data" as default node type for all entries
        let entries = self.load_directory_entries(parent_node_id).await?;
        let entries_with_types = entries.into_iter()
            .map(|(name, node_id)| (name, (node_id, crate::EntryType::FileData)))
            .collect();
        Ok(entries_with_types)
    }
    
    async fn update_directory_entry_with_type(&self, parent_node_id: NodeID, entry_name: &str, operation: DirectoryOperation, _node_type: &crate::EntryType) -> Result<()> {
        // For memory persistence, ignore the node type but handle operations directly
        let mut directories = self.directories.lock().await;
        let dir_entries = directories.entry(parent_node_id).or_insert_with(HashMap::new);
        
        match operation {
            DirectoryOperation::InsertWithType(node_id, _node_type) => {
                // For memory persistence, ignore the node type but store the operation
                dir_entries.insert(entry_name.to_string(), node_id);
            }
            DirectoryOperation::DeleteWithType(_node_type) => {
                dir_entries.remove(entry_name);
            }
            DirectoryOperation::RenameWithType(new_name, node_id, _node_type) => {
                // Remove old entry and add new one
                dir_entries.remove(entry_name);
                dir_entries.insert(new_name, node_id);
            }
        }
        
        Ok(())
    }
    
    async fn current_transaction_id(&self) -> Result<Option<i64>> {
        // Memory persistence doesn't have real transactions, so always return None
        Ok(None)
    }
    
    // Versioning operations implementation
    async fn list_file_versions(&self, node_id: NodeID, part_id: NodeID) -> Result<Vec<FileVersionInfo>> {
        let file_versions = self.file_versions.lock().await;
        if let Some(versions) = file_versions.get(&(node_id, part_id)) {
            let version_infos = versions.iter().map(|v| FileVersionInfo {
                version: v.version,
                timestamp: v.timestamp,
                size: v.content.len() as u64,
                sha256: None, // Memory persistence doesn't compute SHA256
                entry_type: v.entry_type.clone(),
                extended_metadata: v.extended_metadata.clone(),
            }).collect();
            Ok(version_infos)
        } else {
            // Return empty list if no versions found
            Ok(Vec::new())
        }
    }
    
    async fn read_file_version(&self, node_id: NodeID, part_id: NodeID, version: Option<u64>) -> Result<Vec<u8>> {
        let file_versions = self.file_versions.lock().await;
        if let Some(versions) = file_versions.get(&(node_id, part_id)) {
            match version {
                Some(v) => {
                    // Find specific version
                    if let Some(file_version) = versions.iter().find(|fv| fv.version == v) {
                        Ok(file_version.content.clone())
                    } else {
                        Err(crate::error::Error::NotFound(
                            std::path::PathBuf::from(format!("Version {} of file {} not found", v, node_id))
                        ))
                    }
                }
                None => {
                    // Return latest version
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
    
    async fn is_versioned_file(&self, node_id: NodeID, part_id: NodeID) -> Result<bool> {
        let file_versions = self.file_versions.lock().await;
        if let Some(versions) = file_versions.get(&(node_id, part_id)) {
            // A file is considered versioned if it has more than one version
            Ok(versions.len() > 1)
        } else {
            Ok(false)
        }
    }
    
    // Dynamic node factory methods - not supported in memory persistence
    async fn create_dynamic_directory_node(&self, _parent_node_id: NodeID, _name: String, _factory_type: &str, _config_content: Vec<u8>) -> Result<NodeID> {
        Err(crate::Error::Other("Dynamic nodes not supported in memory persistence".to_string()))
    }
    
    async fn create_dynamic_file_node(&self, _parent_node_id: NodeID, _name: String, _file_type: crate::EntryType, _factory_type: &str, _config_content: Vec<u8>) -> Result<NodeID> {
        Err(crate::Error::Other("Dynamic nodes not supported in memory persistence".to_string()))
    }
    
    async fn get_dynamic_node_config(&self, _node_id: NodeID, _part_id: NodeID) -> Result<Option<(String, Vec<u8>)>> {
        Ok(None) // Memory persistence doesn't support dynamic nodes
    }
}
