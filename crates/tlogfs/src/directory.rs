use crate::persistence::State;
use async_trait::async_trait;
use futures::Stream;
use log::debug;
use std::pin::Pin;
use std::sync::Arc;
use tinyfs::{
    DirHandle, Directory, Metadata, NodeMetadata, Node, FileID,
    persistence::PersistenceLayer,
};
use tokio::sync::Mutex;

pub struct OpLogDirectory {
    /// Unique node identifier for this directory
    /// @@@ Note PartID == NodeID here, could store PartID
    id: FileID,

    /// Reference to persistence layer (single source of truth)
    state: State,
}

impl OpLogDirectory {
    /// Create new directory instance with persistence layer dependency injection
    #[must_use]
    pub fn new(id: FileID, state: State) -> Self {
        debug!("directory part {id}");

        Self { id, state }
    }

    /// Create a DirHandle from this directory
    #[must_use]
    pub fn create_handle(self) -> DirHandle {
        DirHandle::new(Arc::new(Mutex::new(Box::new(self))))
    }

    // /// Determine the correct partition ID for a child based on its comprehensive EntryType
    // fn get_child_partition_id(
    //     &self,
    //     child_node_id: NodeID,
    //     entry_type: &tinyfs::EntryType,
    // ) -> tinyfs::Result<NodeID> {
    //     let part_id = entry_type.partition_id(child_node_id, self.node_id);
        
    //     if matches!(entry_type, tinyfs::EntryType::DirectoryPhysical) {
    //         debug!(
    //             "Physical directory {}, creating own partition",
    //             child_node_id
    //         );
    //     } else {
    //         debug!(
    //             "Node {} (type: {}), using parent partition",
    //             child_node_id,
    //             entry_type.as_str()
    //         );
    //     }
        
    //     Ok(part_id)
    // }
}

#[async_trait]
impl Metadata for OpLogDirectory {
    async fn metadata(&self) -> tinyfs::Result<NodeMetadata> {
        self.state.metadata(self.id).await
    }
}

#[async_trait]
impl Directory for OpLogDirectory {
    async fn get(&self, name: &str) -> tinyfs::Result<Option<Node>> {
        debug!("OpLogDirectory::get('{name}')");
        
        // Query for the directory's latest OplogEntry which contains child entries
        let records = self.state.query_records(self.id).await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to query directory records: {}", e)))?;
        
        let Some(record) = records.first() else {
            debug!("OpLogDirectory::get: directory not found in oplog");
            return Ok(None);
        };
        
        // Decode directory entries from Arrow IPC
        let Some(content_bytes) = &record.content else {
            debug!("OpLogDirectory::get: directory has no entries (content=None)");
            return Ok(None);
        };
        
        let entries: Vec<tinyfs::DirectoryEntry> = crate::schema::decode_directory_entries(content_bytes)
            .map_err(|e| tinyfs::Error::Other(format!("Failed to decode directory entries: {}", e)))?;
        
        debug!("OpLogDirectory::get: found {} entries in directory", entries.len());
        for entry in &entries {
            debug!("  - entry name='{}' child_node_id={}", entry.name, entry.child_node_id);
        }
        
        // Find the requested entry
        for entry in entries {
            if entry.name == name {
                debug!("OpLogDirectory::get: found entry '{name}'!");
                // Construct FileID based on entry type:
                // - Physical directories: part_id == node_id (self-partitioned)
                // - Everything else: part_id == parent's part_id
                let child_file_id = if entry.entry_type == tinyfs::EntryType::DirectoryPhysical {
                    FileID::from_physical_dir_node_id(entry.child_node_id)
                } else {
                    FileID::new_from_ids(self.id.part_id(), entry.child_node_id)
                };
                
                // Load the child node
                let child_node = self.state.load_node(child_file_id).await?;
                return Ok(Some(child_node));
            }
        }
        
        debug!("OpLogDirectory::get: entry '{name}' not found");
        Ok(None)
    }

    async fn insert(&mut self, name: String, node: Node) -> tinyfs::Result<()> {
        debug!("OpLogDirectory::insert('{name}', {:?})", node.id());
        
        // Get current directory entries
        let mut entries: Vec<tinyfs::DirectoryEntry> = {
            let records = self.state.query_records(self.id).await
                .map_err(|e| tinyfs::Error::Other(format!("Failed to query directory: {}", e)))?;
            
            if let Some(record) = records.first() {
                if let Some(content_bytes) = &record.content {
                    crate::schema::decode_directory_entries(content_bytes)
                        .map_err(|e| tinyfs::Error::Other(format!("Failed to decode entries: {}", e)))?
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            }
        };
        
        // Add or update the entry
        let node_id = node.id();
        let new_entry = tinyfs::DirectoryEntry::new(
            name.clone(),
            node_id.node_id(),
            node_id.entry_type(),
            1, // version_last_modified - will be set properly by persistence layer
        );
        
        // Remove existing entry with same name if present
        entries.retain(|e| e.name != name);
        entries.push(new_entry);
        
        // Encode entries back to Arrow IPC
        let content_bytes = crate::schema::encode_directory_entries(&entries)
            .map_err(|e| tinyfs::Error::Other(format!("Failed to encode entries: {}", e)))?;
        
        // Store updated directory content
        self.state.update_directory_content(self.id, content_bytes).await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to update directory: {}", e)))?;
        
        Ok(())
    }

    async fn entries(
        &self,
    ) -> tinyfs::Result<Pin<Box<dyn Stream<Item = tinyfs::Result<tinyfs::DirectoryEntry>> + Send>>> {
        debug!("OpLogDirectory::entries() - directory operations not yet fully implemented with FileID");
        // TODO: Implement directory operations with FileID refactoring
        let _ = (&self.id, &self.state);
        let stream = futures::stream::iter(vec![]);
        Ok(Box::pin(stream))
    }
}
