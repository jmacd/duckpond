use crate::persistence::{PersistenceLayer, DirectoryOperation};
use crate::node::{NodeID, NodeType};
use crate::error::Result;
use crate::EntryType;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// In-memory persistence layer for testing and derived file computation
/// This implements the PersistenceLayer trait using in-memory storage
pub struct MemoryPersistence {
    nodes: Arc<Mutex<HashMap<(NodeID, NodeID), NodeType>>>, // (node_id, part_id) -> NodeType
    directories: Arc<Mutex<HashMap<NodeID, HashMap<String, NodeID>>>>, // parent_id -> {name -> child_id}
    root_dir: Arc<Mutex<Option<crate::dir::Handle>>>, // Shared root directory state
}

impl MemoryPersistence {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(Mutex::new(HashMap::new())),
            directories: Arc::new(Mutex::new(HashMap::new())),
            root_dir: Arc::new(Mutex::new(None)), // Initially no root
        }
    }
}

#[async_trait]
impl PersistenceLayer for MemoryPersistence {
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
        // Load the node and extract content directly
        let node_type = self.load_node(node_id, part_id).await?;
        match node_type {
            NodeType::File(file_handle) => {
                crate::async_helpers::buffer_helpers::read_file_to_vec(&file_handle).await
            }
            _ => Err(crate::error::Error::Other("Expected file node type".to_string()))
        }
    }
    
    async fn store_file_content(&self, node_id: NodeID, part_id: NodeID, content: &[u8]) -> Result<()> {
        // Create and store a memory file with the content
        let file_handle = crate::memory::MemoryFile::new_handle(content);
        let node_type = NodeType::File(file_handle);
        self.store_node(node_id, part_id, &node_type).await
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
}
