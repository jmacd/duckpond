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
        debug!("OpLogDirectory::get('{name}') - directory operations not yet fully implemented with FileID");
        // TODO: Implement directory operations with FileID refactoring
        // Need to query oplog for child nodes and reconstruct them
        let _ = (name, &self.id, &self.state);
        Ok(None)
    }

    async fn insert(&mut self, name: String, node: Node) -> tinyfs::Result<()> {
        debug!("OpLogDirectory::insert('{name}', {:?}) - directory operations not yet fully implemented with FileID", node.id());
        // TODO: Implement directory operations with FileID refactoring  
        let _ = (name, node, &self.id, &self.state);
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
