use crate::DirectoryEntry;
use crate::error::{Error, Result};
use crate::memory::MemoryDirectory;
use crate::node::{FileID, Node, NodeType};
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
pub struct MemoryPersistence(Arc<Mutex<State>>);

pub struct State {
    // Store multiple versions of each file: (node_id, part_id) -> Vec<MemoryFileVersion>
    file_versions: HashMap<FileID, Vec<MemoryFileVersion>>,

    // Non-file nodes (directories, symlinks): (node_id, part_id) -> Node
    nodes: HashMap<FileID, Node>,

    // parent_id -> {name -> child_id}
    directories: HashMap<FileID, HashMap<String, DirectoryEntry>>,
}

impl Default for State {
    fn default() -> Self {
        let root_dir = Node::new(
            FileID::root(),
            NodeType::Directory(MemoryDirectory::new_handle()),
        );
        Self {
            file_versions: HashMap::new(),
            nodes: HashMap::from([(root_dir.id, root_dir)]),
            directories: HashMap::new(),
            //root_dir,
        }
    }
}

impl Default for MemoryPersistence {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(State::default())))
    }
}

#[async_trait]
impl PersistenceLayer for MemoryPersistence {
    /// Downcast support for accessing concrete implementation methods
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    // Node operations
    async fn load_node(&self, id: FileID) -> Result<Node> {
        self.0.lock().await.load_node(id).await
    }

    async fn store_node(&self, id: FileID, node_type: &Node) -> Result<()> {
        self.0.lock().await.store_node(id, node_type).await
    }

    async fn exists_node(&self, id: FileID) -> Result<bool> {
        self.0.lock().await.exists_node(id).await
    }

    // Symlink operations
    async fn load_symlink_target(&self, id: FileID) -> Result<std::path::PathBuf> {
        self.0.lock().await.load_symlink_target(id).await
    }

    // Factory methods for creating nodes directly with persistence
    async fn create_file_node(&self, id: FileID) -> Result<Node> {
        self.0.lock().await.create_file_node(id).await
    }

    async fn create_directory_node(&self, id: FileID) -> Result<Node> {
        self.0.lock().await.create_directory_node(id).await
    }

    async fn create_symlink_node(&self, id: FileID, target: &std::path::Path) -> Result<Node> {
        self.0.lock().await.create_symlink_node(id, target).await
    }

    async fn create_dynamic_node(
        &self,
        id: FileID,
        name: String,
        entry_type: EntryType,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<Node> {
        self.0
            .lock()
            .await
            .create_dynamic_node(id, name, entry_type, factory_type, config_content)
            .await
    }

    async fn get_dynamic_node_config(&self, id: FileID) -> Result<Option<(String, Vec<u8>)>> {
        self.0.lock().await.get_dynamic_node_config(id).await
    }

    async fn update_dynamic_node_config(
        &self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<()> {
        self.0
            .lock()
            .await
            .update_dynamic_node_config(id, factory_type, config_content)
            .await
    }

    // Directory operations with versioning
    /// Load all directory entries with full metadata (without loading nodes)
    async fn load_directory_entries(&self, id: FileID) -> Result<HashMap<String, DirectoryEntry>> {
        self.0.lock().await.load_directory_entries(id).await
    }

    async fn batch_load_nodes(&self, requests: Vec<FileID>) -> Result<HashMap<FileID, Node>> {
        self.0.lock().await.batch_load_nodes(requests).await
    }

    async fn query_directory_entry(
        &self,
        id: FileID,
        entry_name: &str,
    ) -> Result<Option<DirectoryEntry>> {
        self.0
            .lock()
            .await
            .query_directory_entry(id, entry_name)
            .await
    }

    async fn update_directory_entry(
        &self,
        id: FileID,
        entry_name: &str,
        operation: DirectoryOperation,
    ) -> Result<()> {
        self.0
            .lock()
            .await
            .update_directory_entry(id, entry_name, operation)
            .await
    }

    async fn metadata(&self, id: FileID) -> Result<NodeMetadata> {
        self.0.lock().await.metadata(id).await
    }

    async fn list_file_versions(&self, id: FileID) -> Result<Vec<FileVersionInfo>> {
        self.0.lock().await.list_file_versions(id).await
    }

    async fn read_file_version(&self, id: FileID, version: Option<u64>) -> Result<Vec<u8>> {
        self.0.lock().await.read_file_version(id, version).await
    }

    async fn set_extended_attributes(
        &self,
        id: FileID,
        attributes: HashMap<String, String>,
    ) -> Result<()> {
        self.0
            .lock()
            .await
            .set_extended_attributes(id, attributes)
            .await
    }
}

#[async_trait]
impl PersistenceLayer for State {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn load_node(&self, id: FileID) -> Result<Node> {
        match self.nodes.get(&id) {
            Some(node) => Ok(node.node_type.clone()),
            None => Err(Error::IDNotFound(id)),
        }
    }

    async fn store_node(&self, id: FileID, node_type: &Node) -> Result<()> {
        _ = self.nodes.insert(id, node_type.clone());
        Ok(())
    }

    async fn exists_node(&self, id: FileID) -> Result<bool> {
        Ok(self.nodes.contains_key(&id))
    }

    async fn load_directory_entries(
        &self,
        parent_id: FileID,
    ) -> Result<HashMap<String, DirectoryEntry>> {
        Ok(self
            .directories
            .get(&parent_id)
            .cloned()
            .ok_or_else(|| Error::IDNotFound(parent_id))?
            .into_iter()
            .collect())
    }

    async fn load_symlink_target(&self, id: FileID) -> Result<std::path::PathBuf> {
        let node_type = self.load_node(id).await?;
        match node_type {
            Node::Symlink(symlink_handle) => symlink_handle.readlink().await,
            _ => Err(Error::Other("Expected symlink node type".to_string())),
        }
    }

    async fn create_file_node(&self, id: FileID) -> Result<Node> {
        // @@@ DO NOT PASS CONTENT????
        let file_handle =
            crate::memory::MemoryFile::new_handle_with_entry_type([], id.entry_type());
        Ok(Node::File(file_handle))
    }

    async fn create_directory_node(&self, _id: FileID) -> Result<Node> {
        let dir_handle = MemoryDirectory::new_handle();
        Ok(Node::Directory(dir_handle))
    }

    async fn create_symlink_node(&self, id: FileID, target: &std::path::Path) -> Result<Node> {
        let symlink_handle = crate::memory::MemorySymlink::new_handle(target.to_path_buf());
        let node_type = Node::Symlink(symlink_handle.clone());
        self.store_node(id, &node_type).await?;
        Ok(node_type)
    }

    async fn batch_load_nodes(&self, _requests: Vec<FileID>) -> Result<HashMap<FileID, Node>> {
        Err(Error::internal("not implemented"))
    }

    async fn query_directory_entry(
        &self,
        parent_id: FileID,
        entry_name: &str,
    ) -> Result<Option<DirectoryEntry>> {
        Ok(self
            .directories
            .get(&parent_id)
            .and_then(|entries| entries.get(entry_name))
            .cloned())
    }

    async fn metadata(&self, id: FileID) -> Result<NodeMetadata> {
        let node_type = self.nodes.get(&id).ok_or_else(|| {
            Error::NotFound(std::path::PathBuf::from(format!("Node {id:?} not found",)))
        })?;

        match node_type {
            Node::File(handle) => {
                // Query the handle's metadata to get the entry type
                handle.metadata().await
            }
            _ => Err(Error::Other("Non-file metadata unimplemented".to_string())),
        }
    }

    async fn update_directory_entry(
        &self,
        id: FileID,
        entry_name: &str,
        operation: DirectoryOperation,
    ) -> Result<()> {
        let version = 0; // @@@ unused
        let name = entry_name.to_string();
        let dir_entries = self.directories.entry(id).or_insert_with(HashMap::new);
        match operation {
            DirectoryOperation::InsertWithType(node_id, entry_type) => {
                _ = dir_entries.insert(
                    name.clone(),
                    DirectoryEntry::new(name, node_id, entry_type, version),
                );
            }
            DirectoryOperation::DeleteWithType(_) => {
                _ = dir_entries.remove(entry_name);
            }
            DirectoryOperation::RenameWithType(new_name, node_id, entry_type) => {
                _ = dir_entries.remove(entry_name);
                _ = dir_entries.insert(
                    new_name.clone(),
                    DirectoryEntry::new(new_name, node_id, entry_type, version),
                );
            }
        }
        Ok(())
    }

    async fn list_file_versions(&self, id: FileID) -> Result<Vec<FileVersionInfo>> {
        if let Some(versions) = self.file_versions.get(&id) {
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

    async fn read_file_version(&self, id: FileID, version: Option<u64>) -> Result<Vec<u8>> {
        if let Some(versions) = self.file_versions.get(&id) {
            match version {
                Some(v) => {
                    if let Some(file_version) = versions.iter().find(|fv| fv.version == v) {
                        Ok(file_version.content.clone())
                    } else {
                        Err(Error::NotFound(std::path::PathBuf::from(format!(
                            "Version {v} of file {id:?} not found"
                        ))))
                    }
                }
                None => {
                    if let Some(latest) = versions.last() {
                        Ok(latest.content.clone())
                    } else {
                        Err(Error::NotFound(std::path::PathBuf::from(format!(
                            "No versions of file {id:?} found",
                        ))))
                    }
                }
            }
        } else {
            Err(Error::NotFound(std::path::PathBuf::from(format!(
                "File {id:?} not found",
            ))))
        }
    }

    async fn set_extended_attributes(
        &self,
        id: FileID,
        attributes: HashMap<String, String>,
    ) -> Result<()> {
        if let Some(versions) = self.file_versions.get_mut(&id) {
            if let Some(latest_version) = versions.last_mut() {
                latest_version.extended_metadata = Some(attributes);
                Ok(())
            } else {
                Err(Error::NotFound(std::path::PathBuf::from(format!(
                    "No versions of file {id:?} found",
                ))))
            }
        } else {
            Err(Error::NotFound(std::path::PathBuf::from(format!(
                "File {id:?} not found",
            ))))
        }
    }

    async fn create_dynamic_node(
        &self,
        _id: FileID,
        _name: String,
        _entry_type: EntryType,
        _factory_type: &str,
        _config_content: Vec<u8>,
    ) -> Result<Node> {
        Err(Error::Other(
            "Dynamic nodes not supported in memory persistence".to_string(),
        ))
    }

    async fn get_dynamic_node_config(&self, _id: FileID) -> Result<Option<(String, Vec<u8>)>> {
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
