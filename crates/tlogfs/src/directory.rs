// Clean architecture implementation of Directory for OpLog persistence
use super::TLogFSError;
use tinyfs::{DirHandle, Directory, Metadata, NodeMetadata, NodeRef, NodeID, persistence::{PersistenceLayer, DirectoryOperation}};
use std::sync::Arc;
use crate::persistence::State;
use async_trait::async_trait;
use tokio::sync::Mutex;
use futures::Stream;
use std::pin::Pin;

/// Clean architecture directory implementation
/// - NO local state (all data comes from persistence layer)
/// - Simple delegation to persistence layer
/// - Proper separation of concerns
pub struct OpLogDirectory {
    /// Unique node identifier for this directory
    node_id: String,
    
    /// Parent directory node ID (for partition lookup)
    parent_node_id: String,
    
    /// Reference to persistence layer (single source of truth)
    state: State,
}

impl OpLogDirectory {
    /// Create new directory instance with persistence layer dependency injection
    pub fn new(
        node_id: String,
        parent_node_id: String,
        state: State,
    ) -> Self {
        let node_id_bound = &node_id;
        let parent_node_id_bound = &parent_node_id;
        diagnostics::debug!("OpLogDirectory::new() - creating directory with node_id: {node_id}, parent: {parent_node_id}", 
                                node_id: node_id_bound, parent_node_id: parent_node_id_bound);
        
        Self {
            node_id,
            parent_node_id,
            state,
        }
    }
    
    /// Create a DirHandle from this directory
    pub fn create_handle(self) -> DirHandle {
        DirHandle::new(Arc::new(Mutex::new(Box::new(self))))
    }
    
    /// Convert hex node_id string to NodeID
    fn parse_node_id(&self) -> Result<NodeID, TLogFSError> {
        NodeID::from_hex_string(&self.node_id)
            .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Invalid node ID: {}", e))))
    }
    
    /// Convert hex parent_node_id string to NodeID
    fn parse_parent_node_id(&self) -> Result<NodeID, TLogFSError> {
        NodeID::from_hex_string(&self.parent_node_id)
            .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Invalid parent node ID: {}", e))))
    }
}

#[async_trait]
impl Metadata for OpLogDirectory {
    async fn metadata(&self) -> tinyfs::Result<NodeMetadata> {
        let node_id = self.parse_node_id()
            .map_err(|e| tinyfs::Error::Other(e.to_string()))?;
        let parent_node_id = self.parse_parent_node_id()
            .map_err(|e| tinyfs::Error::Other(e.to_string()))?;
        // For directories, the partition is the parent directory (just like files)
        self.state.metadata(node_id, parent_node_id).await
    }
}

#[async_trait]
impl Directory for OpLogDirectory {
    async fn get(&self, name: &str) -> tinyfs::Result<Option<NodeRef>> {
        let name_bound = name;
        diagnostics::debug!("OpLogDirectory::get('{name}') - querying single entry via optimized persistence layer", name: name_bound);
        
        // Get current directory node ID
        let node_id = self.parse_node_id()
            .map_err(|e| tinyfs::Error::Other(e.to_string()))?;
        
        // Use enhanced query that returns node type
        if let Some(child_node_id) = self.state.query_directory_entry(node_id, name).await? {
            // Load the child node using deterministic partition selection
	    // @@@ HERE compute entry type Put in tinyfs.@@@

            // Load node from correct partition
            let child_node_type = self.state.load_node(child_node_id, part_id).await?;
	    
            let part_id = match entry_type {
                tinyfs::EntryType::Directory => {
                    // Check if this is a dynamic directory
                    // Dynamic directories are stored in parent's partition, static directories in their own
                    let child_node_id_str = child_node_id.to_hex_string();
                    match self.persistence.get_dynamic_node_config(child_node_id, node_id).await {
                        Ok(Some(_)) => {
                            // Dynamic directory - use parent's partition
                            diagnostics::debug!("Directory::get - loading dynamic directory {child_node_id_str} from parent partition", child_node_id_str: child_node_id_str);
                            node_id
                        }
                        _ => {
                            // Static directory - use own partition
                            diagnostics::debug!("Directory::get - loading static directory {child_node_id_str} from own partition", child_node_id_str: child_node_id_str);
                            child_node_id
                        }
                    }
                }
                _ => node_id, // Files and symlinks use parent's partition
            };
            
            // Create Node and wrap in NodeRef
            let node = tinyfs::Node {
                id: child_node_id,
                node_type: child_node_type,
            };
            let node_ref = NodeRef::new(Arc::new(tokio::sync::Mutex::new(node)));
            
            let name_bound = name;
            let child_node_id_bound = format!("{:?}", child_node_id);
            diagnostics::debug!("OpLogDirectory::get('{name}') - found child with node_id: {child_node_id}", 
                                    name: name_bound, child_node_id: child_node_id_bound);
            Ok(Some(node_ref))
        } else {
            let name_bound = name;
            diagnostics::debug!("OpLogDirectory::get('{name}') - not found", name: name_bound);
            Ok(None)
        }
    }
    
    async fn insert(&mut self, name: String, node_ref: NodeRef) -> tinyfs::Result<()> {
        let name_bound = &name;
        diagnostics::debug!("OpLogDirectory::insert('{name}') - delegating to persistence layer", name: name_bound);
        
        // Get current directory node ID
        let node_id = self.parse_node_id()
            .map_err(|e| tinyfs::Error::Other(e.to_string()))?;
        
        // Get child node ID from NodeRef
        let child_node_id = node_ref.id().await;
        
        // Get child node type by accessing the node directly
        let child_node_type = {
            let node = node_ref.lock().await;
            node.node_type.clone()
        };
        
        // Store the child node first (if not already stored)
        // For static directories, they should use their own node_id as part_id (create their own partition)
        // For dynamic directories, they should use the parent's node_id as part_id (no separate partition)
        // For files and symlinks, they should use the parent's node_id as part_id
        let part_id = match &child_node_type {
            tinyfs::NodeType::Directory(_) => {
                // Check if this is a dynamic directory by looking for factory configuration
                // Dynamic directories should not create their own partitions
                let child_node_id_str = child_node_id.to_hex_string();
                match self.state.get_dynamic_node_config(child_node_id, node_id).await {
                    Ok(Some(_)) => {
                        // Dynamic directory - use parent's partition
                        diagnostics::debug!("Directory::insert - detected dynamic directory {child_node_id_str}, using parent partition", child_node_id_str: child_node_id_str);
                        node_id
                    }
                    _ => {
                        // Static directory - create own partition
                        diagnostics::debug!("Directory::insert - detected static directory {child_node_id_str}, creating own partition", child_node_id_str: child_node_id_str);
                        child_node_id
                    }
                }
            }
            _ => node_id, // Files and symlinks use parent's partition
        };
        
        let already_exists = self.persistence.exists_node(child_node_id, part_id).await?;
        if !already_exists {
            self.persistence.store_node(child_node_id, part_id, &child_node_type).await?;
        }
        
        // Determine node type for directory entry by using the actual entry type stored in NodeType
        let entry_type = match &child_node_type {
            tinyfs::NodeType::File(handle) => {
                // Query the file handle's metadata to get the entry type
                handle.metadata().await
                    .map_err(|e| tinyfs::Error::Other(format!("Failed to get file metadata: {}", e)))?
                    .entry_type
            },
            tinyfs::NodeType::Directory(_) => tinyfs::EntryType::Directory,
            tinyfs::NodeType::Symlink(_) => tinyfs::EntryType::Symlink,
        };
        
        self.persistence.update_directory_entry_with_type(
            node_id,
            &name,
            DirectoryOperation::InsertWithType(child_node_id, entry_type.clone()),
        ).await?;
        
        let name_bound = &name;
        let node_type_bound = entry_type.as_str();
        diagnostics::debug!("OpLogDirectory::insert('{name}') - completed via persistence layer with node_type: {node_type}", name: name_bound, node_type: node_type_bound);
        Ok(())
    }
    
    async fn entries(&self) -> tinyfs::Result<Pin<Box<dyn Stream<Item = tinyfs::Result<(String, NodeRef)>> + Send>>> {
        diagnostics::debug!("OpLogDirectory::entries() - querying via persistence layer");
        
        // Get current directory node ID
        let node_id = self.parse_node_id()
            .map_err(|e| tinyfs::Error::Other(e.to_string()))?;
        
        // Load directory entries with types from persistence layer
        let entries_with_types = self.persistence.load_directory_entries_with_types(node_id).await?;
        
        let entry_count = entries_with_types.len();
        diagnostics::debug!("OpLogDirectory::entries() - found {entry_count} entries", entry_count: entry_count);
        
        // Convert to stream of NodeRef instances
        let mut entry_results = Vec::new();
        
        for (name, (child_node_id, entry_type)) in entries_with_types {
            // Load each child node using deterministic partition selection
            let part_id = match entry_type {
                tinyfs::EntryType::Directory => {
                    // Check if this is a dynamic directory
                    // Dynamic directories are stored in parent's partition, static directories in their own
                    let child_node_id_str = child_node_id.to_hex_string();
                    match self.persistence.get_dynamic_node_config(child_node_id, node_id).await {
                        Ok(Some(_)) => {
                            // Dynamic directory - use parent's partition
                            diagnostics::debug!("Directory::entries - loading dynamic directory {child_node_id_str} from parent partition", child_node_id_str: child_node_id_str);
                            node_id
                        }
                        _ => {
                            // Static directory - use own partition
                            diagnostics::debug!("Directory::entries - loading static directory {child_node_id_str} from own partition", child_node_id_str: child_node_id_str);
                            child_node_id
                        }
                    }
                }
                _ => node_id, // Files and symlinks use parent's partition
            };
            
            // Load node from correct partition
            match self.persistence.load_node(child_node_id, part_id).await {
                Ok(child_node_type) => {
                    // Create Node and wrap in NodeRef
                    let node = tinyfs::Node {
                        id: child_node_id,
                        node_type: child_node_type,
                    };
                    let node_ref = NodeRef::new(Arc::new(tokio::sync::Mutex::new(node)));
                    entry_results.push(Ok((name, node_ref)));
                }
                Err(e) => {
                    let child_node_hex = child_node_id.to_hex_string();
                    let error_msg = format!("{}", e);
                    diagnostics::debug!("  Warning: Failed to load child node {child_node_hex}: {error_msg}", 
                                           child_node_hex: child_node_hex, error_msg: error_msg);
                    entry_results.push(Err(e));
                }
            }
        }
        
        // Create stream from results
        let stream = futures::stream::iter(entry_results);
        Ok(Box::pin(stream))
    }
}
