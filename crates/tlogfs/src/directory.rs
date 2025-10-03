// Clean architecture implementation of Directory for OpLog persistence
use crate::persistence::State;
use async_trait::async_trait;
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;
use log::debug;
use tinyfs::{
    DirHandle, Directory, Metadata, NodeID, NodeMetadata, NodeRef,
    persistence::{DirectoryOperation, PersistenceLayer},
};
use tokio::sync::Mutex;

/// Clean architecture directory implementation
/// - NO local state (all data comes from persistence layer)
/// - Simple delegation to persistence layer
/// - Proper separation of concerns
pub struct OpLogDirectory {
    /// Unique node identifier for this directory
    node_id: NodeID,

    /// Parent directory node ID (for partition lookup)
    /// For dynamic dirs, this will != node_id.
    part_id: NodeID,

    /// Reference to persistence layer (single source of truth)
    state: State,
}

impl OpLogDirectory {
    /// Create new directory instance with persistence layer dependency injection
    pub fn new(node_id: NodeID, part_id: NodeID, state: State) -> Self {
        debug!("directory node {node_id} part {part_id}");

        Self {
            node_id,
            part_id,
            state,
        }
    }

    /// Create a DirHandle from this directory
    pub fn create_handle(self) -> DirHandle {
        DirHandle::new(Arc::new(Mutex::new(Box::new(self))))
    }
}

#[async_trait]
impl Metadata for OpLogDirectory {
    async fn metadata(&self) -> tinyfs::Result<NodeMetadata> {
        self.state.metadata(self.node_id, self.part_id).await
    }
}

#[async_trait]
impl Directory for OpLogDirectory {
    async fn get(&self, name: &str) -> tinyfs::Result<Option<NodeRef>> {
        let name_bound = name;
        debug!("get {name_bound} via persistence layer");

        // Get current directory node ID
        // let node_id = self
        //     .parse_node_id()
        //     .map_err(|e| tinyfs::Error::Other(e.to_string()))?;

        // Use enhanced query that returns node type
        if let Some((child_node_id, entry_type)) =
            self.state.query_directory_entry(self.node_id, name).await?
        {
            let part_id = match entry_type {
                tinyfs::EntryType::Directory => {

		    // @@@ This is dumb
                    match self
                        .state
                        .get_dynamic_node_config(child_node_id, self.node_id)
                        .await
                    {
                        Ok(Some(_)) => self.node_id,
                        _ => child_node_id,
                    }
                }
                _ => self.node_id, // Files and symlinks use parent's partition
            };
            // Load node from correct partition
            let child_node_type = self.state.load_node(child_node_id, part_id).await?;

            // Create Node and wrap in NodeRef
            let node = tinyfs::Node {
                id: child_node_id,
                node_type: child_node_type,
            };
            let node_ref = NodeRef::new(Arc::new(tokio::sync::Mutex::new(node)));

            let name_bound = name;
            let child_node_id_bound = format!("{:?}", child_node_id);
            debug!("get '{name_bound}' found child with node_id: {child_node_id_bound}");
            Ok(Some(node_ref))
        } else {
            let name_bound = name;
            debug!("get '{name_bound}' not found");
            Ok(None)
        }
    }

    async fn insert(&mut self, name: String, node_ref: NodeRef) -> tinyfs::Result<()> {
        let name_bound = &name;
        debug!("OpLogDirectory::insert('{name_bound}') - delegating to persistence layer");

        // // Get current directory node ID
        // let node_id = self
        //     .parse_node_id()
        //     .map_err(|e| tinyfs::Error::Other(e.to_string()))?;

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
                match self
                    .state
                    .get_dynamic_node_config(child_node_id, self.node_id)
                    .await
                {
		    // @@@ This is a pattern repeating
                    Ok(Some(_)) => {
                        // Dynamic directory - use parent's partition
                        debug!("Directory::insert - detected dynamic directory {child_node_id_str}, using parent partition");
                        self.node_id
                    }
                    _ => {
                        // Static directory - create own partition
                        debug!("Directory::insert - detected static directory {child_node_id_str}, creating own partition");
                        child_node_id
                    }
                }
            }
            _ => self.node_id, // Files and symlinks use parent's partition
        };

        let already_exists = self.state.exists_node(child_node_id, part_id).await?;
        if !already_exists {
            self.state
                .store_node(child_node_id, part_id, &child_node_type)
                .await?;
        }

        // Determine node type for directory entry by using the actual entry type stored in NodeType
        let entry_type = match &child_node_type {
            tinyfs::NodeType::File(handle) => {
                // Query the file handle's metadata to get the entry type
                handle
                    .metadata()
                    .await
                    .map_err(|e| {
                        tinyfs::Error::Other(format!("Failed to get file metadata: {}", e))
                    })?
                    .entry_type
            }
            tinyfs::NodeType::Directory(_) => tinyfs::EntryType::Directory,
            tinyfs::NodeType::Symlink(_) => tinyfs::EntryType::Symlink,
        };

        self.state
            .update_directory_entry(
                self.node_id,
                &name,
                DirectoryOperation::InsertWithType(child_node_id, entry_type.clone()),
            )
            .await?;

        let name_bound = &name;
        let node_type_bound = entry_type.as_str();
        debug!("OpLogDirectory::insert('{name_bound}') - completed via persistence layer with node_type: {node_type_bound}");
        Ok(())
    }

    async fn entries(
        &self,
    ) -> tinyfs::Result<Pin<Box<dyn Stream<Item = tinyfs::Result<(String, NodeRef)>> + Send>>> {
        debug!("OpLogDirectory::entries() - querying via persistence layer");

        // // Get current directory node ID
        // let node_id = self
        //     .parse_node_id()
        //     .map_err(|e| tinyfs::Error::Other(e.to_string()))?;

        // Load directory entries with types from persistence layer
        let entries_with_types = self.state.load_directory_entries(self.node_id).await?;

        let entry_count = entries_with_types.len();
        debug!("OpLogDirectory::entries() - found {entry_count} entries");

        // Convert to stream of NodeRef instances
        let mut entry_results = Vec::new();

        for (name, (child_node_id, entry_type)) in entries_with_types {
            // Load each child node using deterministic partition selection
            let part_id = match entry_type {
                tinyfs::EntryType::Directory => {
		    // @@@ AGAIN
                    // Check if this is a dynamic directory
                    // Dynamic directories are stored in parent's partition, static directories in their own
                    let child_node_id_str = child_node_id.to_hex_string();
                    match self
                        .state
                        .get_dynamic_node_config(child_node_id, self.node_id)
                        .await
                    {
                        Ok(Some(_)) => {
                            // Dynamic directory - use parent's partition
                            debug!("Directory::entries - loading dynamic directory {child_node_id_str} from parent partition");
                            self.node_id
                        }
                        _ => {
                            // Static directory - use own partition
                            debug!("Directory::entries - loading static directory {child_node_id_str} from own partition");
                            child_node_id
                        }
                    }
                }
                _ => self.node_id, // Files and symlinks use parent's partition
            };

            // Load node from correct partition
            match self.state.load_node(child_node_id, part_id).await {
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
                    debug!("  Warning: Failed to load child node {child_node_hex}: {error_msg}");
                    
                    // Create a clearer error for data integrity issues
                    let integrity_error = tinyfs::Error::NotFound(
                        std::path::PathBuf::from(format!(
                            "DATA INTEGRITY ISSUE: Directory '{}' contains reference to nonexistent node {}. \
                            This indicates metadata corruption where directory entries point to deleted or missing nodes. \
                            Original error: {}", 
                            name, child_node_hex, error_msg
                        ))
                    );
                    entry_results.push(Err(integrity_error));
                }
            }
        }

        // Create stream from results
        let stream = futures::stream::iter(entry_results);
        Ok(Box::pin(stream))
    }
}
