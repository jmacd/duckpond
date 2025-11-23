// Clean architecture implementation of Directory for OpLog persistence
use crate::persistence::State;
use async_trait::async_trait;
use futures::Stream;
use log::debug;
use std::pin::Pin;
use std::sync::Arc;
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
    /// Unique node identifier for this directory, same partition ID.
    node_id: NodeID,

    /// Reference to persistence layer (single source of truth)
    state: State,
}

impl OpLogDirectory {
    /// Create new directory instance with persistence layer dependency injection
    #[must_use]
    pub fn new(node_id: NodeID, state: State) -> Self {
        debug!("directory node/part {node_id}");

        Self { node_id, state }
    }

    /// Create a DirHandle from this directory
    #[must_use]
    pub fn create_handle(self) -> DirHandle {
        DirHandle::new(Arc::new(Mutex::new(Box::new(self))))
    }

    /// Determine the correct partition ID for a child based on its comprehensive EntryType
    ///
    /// CRITICAL: EntryType is now comprehensive - it includes both the access method
    /// (directory, file, symlink) AND whether the node is physical or dynamic.
    /// This allows partition determination WITHOUT querying OplogEntry.factory.
    ///
    /// Determine partition ID for a child entry.
    /// Delegates to EntryType::partition_id() for consistent logic across codebase.
    fn get_child_partition_id(
        &self,
        child_node_id: NodeID,
        entry_type: &tinyfs::EntryType,
    ) -> tinyfs::Result<NodeID> {
        let part_id = entry_type.partition_id(child_node_id, self.node_id);
        
        if matches!(entry_type, tinyfs::EntryType::DirectoryPhysical) {
            debug!(
                "Physical directory {}, creating own partition",
                child_node_id
            );
        } else {
            debug!(
                "Node {} (type: {}), using parent partition",
                child_node_id,
                entry_type.as_str()
            );
        }
        
        Ok(part_id)
    }
}

#[async_trait]
impl Metadata for OpLogDirectory {
    async fn metadata(&self) -> tinyfs::Result<NodeMetadata> {
        self.state.metadata(self.node_id, self.node_id).await
    }
}

#[async_trait]
impl Directory for OpLogDirectory {
    async fn get(&self, name: &str) -> tinyfs::Result<Option<NodeRef>> {
        let name_bound = name;
        debug!("get {name_bound} via persistence layer");

        // Use enhanced query that returns node type
        if let Some((child_node_id, entry_type)) =
            self.state.query_directory_entry(self.node_id, name).await?
        {
            let part_id = self.get_child_partition_id(child_node_id, &entry_type)?;
            // Load node from correct partition
            let child_node_type = self.state.load_node(child_node_id, part_id).await?;

            // Create Node and wrap in NodeRef
            let node = tinyfs::Node {
                id: child_node_id,
                node_type: child_node_type,
            };
            let node_ref = NodeRef::new(Arc::new(Mutex::new(node)));

            let name_bound = name;
            let child_node_id_bound = format!("{}", child_node_id);
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
        // Determine comprehensive entry type including physical/dynamic distinction
        let entry_type = match &child_node_type {
            tinyfs::NodeType::File(handle) => {
                // Query the file handle's metadata to get the comprehensive entry type
                // (includes physical/dynamic distinction)
                handle
                    .metadata()
                    .await
                    .map_err(|e| {
                        tinyfs::Error::Other(format!("Failed to get file metadata: {}", e))
                    })?
                    .entry_type
            }
            tinyfs::NodeType::Directory(_) => {
                // For directories being inserted via insert(), they are physical directories
                // Dynamic directories are created through create_dynamic_directory path
                // and don't go through OpLogDirectory::insert()
                tinyfs::EntryType::DirectoryPhysical
            }
            tinyfs::NodeType::Symlink(_) => tinyfs::EntryType::Symlink,
        };

        let part_id = self.get_child_partition_id(child_node_id, &entry_type)?;

        let already_exists = self.state.exists_node(child_node_id, part_id).await?;
        if !already_exists {
            // For directories: Track as created but don't store yet
            // This avoids creating empty v0 when the directory will be populated in the same transaction
            // flush_directory_operations() will create the entry with content if the directory gets populated
            // Truly empty leaf directories will be handled at commit time
            match &entry_type {
                tinyfs::EntryType::DirectoryPhysical | tinyfs::EntryType::DirectoryDynamic => {
                    debug!(
                        "OpLogDirectory::insert - directory {} tracked as created (deferred storage)",
                        child_node_id
                    );
                    self.state.track_created_directory(child_node_id).await;
                }
                _ => {
                    // Files and symlinks store immediately
                    self.state
                        .store_node(child_node_id, part_id, &child_node_type)
                        .await?;
                }
            }
        }

        self.state
            .update_directory_entry(
                self.node_id,
                &name,
                DirectoryOperation::InsertWithType(child_node_id, entry_type),
            )
            .await?;

        let name_bound = &name;
        let node_type_bound = entry_type.as_str();
        debug!(
            "OpLogDirectory::insert('{name_bound}') - completed via persistence layer with node_type: {node_type_bound}"
        );
        Ok(())
    }

    async fn entries(
        &self,
    ) -> tinyfs::Result<Pin<Box<dyn Stream<Item = tinyfs::Result<tinyfs::DirectoryEntry>> + Send>>> {
        debug!("OpLogDirectory::entries() - querying via persistence layer");

        // Load directory entries with types from persistence layer
        let entries_map = self.state.load_directory_entries(self.node_id).await?;

        let entry_count = entries_map.len();
        debug!("OpLogDirectory::entries() - found {entry_count} entries");

        // Convert from HashMap<String, DirectoryEntry> to stream of tinyfs::DirectoryEntry
        let mut entry_results = Vec::new();

        for (_name, dir_entry) in entries_map {
            // dir_entry is already tinyfs::DirectoryEntry (schema.rs has type alias now)
            entry_results.push(Ok(dir_entry));
        }

        // Create stream from results
        let stream = futures::stream::iter(entry_results);
        Ok(Box::pin(stream))
    }
}
