use crate::error::{Error, Result};
use crate::memory::MemoryDirectory;
use crate::node::{FileID, Node, NodeType};
use crate::persistence::{FileVersionInfo, PersistenceLayer};
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
#[derive(Clone)]
pub struct MemoryPersistence(Arc<Mutex<State>>);

pub struct State {
    // Store multiple versions of each file: (node_id, part_id) -> Vec<MemoryFileVersion>
    // Also used for dynamic nodes (FileDataDynamic, DirectoryDynamic, FileExecutable) - config is the content
    file_versions: HashMap<FileID, Vec<MemoryFileVersion>>,

    // Non-file nodes (directories, symlinks): (node_id, part_id) -> Node
    nodes: HashMap<FileID, Node>,
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

    async fn store_node(&self, node: &Node) -> Result<()> {
        self.0.lock().await.store_node(node).await
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
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<Node> {
        self.0
            .lock()
            .await
            .create_dynamic_node(id, factory_type, config_content)
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

    async fn metadata(&self, id: FileID) -> Result<NodeMetadata> {
        self.0.lock().await.metadata(id).await
    }

    async fn list_file_versions(&self, id: FileID) -> Result<Vec<FileVersionInfo>> {
        self.0.lock().await.list_file_versions(id).await
    }

    async fn read_file_version(&self, id: FileID, version: u64) -> Result<Vec<u8>> {
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

impl State {
    async fn load_node(&self, id: FileID) -> Result<Node> {
        match self.nodes.get(&id) {
            Some(node) => Ok(node.clone()),
            None => Err(Error::IDNotFound(id)),
        }
    }

    async fn store_node(&mut self, node: &Node) -> Result<()> {
        _ = self.nodes.insert(node.id, node.clone());
        Ok(())
    }

    async fn create_file_node(&self, id: FileID) -> Result<Node> {
        // @@@ shouldn't pass content
        let file_handle =
            crate::memory::MemoryFile::new_handle_with_entry_type([], id.entry_type());
        Ok(Node::new(id, NodeType::File(file_handle)))
    }

    async fn create_directory_node(&self, id: FileID) -> Result<Node> {
        let dir_handle = MemoryDirectory::new_handle();
        Ok(Node::new(id, NodeType::Directory(dir_handle)))
    }

    async fn create_symlink_node(&mut self, id: FileID, target: &std::path::Path) -> Result<Node> {
        let symlink_handle = crate::memory::MemorySymlink::new_handle(target.to_path_buf());
        let node = Node::new(id, NodeType::Symlink(symlink_handle.clone()));
        self.store_node(&node).await?;
        Ok(node)
    }

    async fn metadata(&self, id: FileID) -> Result<NodeMetadata> {
        let node = self.nodes.get(&id).ok_or_else(|| {
            Error::NotFound(std::path::PathBuf::from(format!("Node {id} not found",)))
        })?;

        match &node.node_type {
            NodeType::File(handle) => handle.metadata().await,
            _ => Err(Error::Other("Non-file metadata unimplemented".to_string())),
        }
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

    async fn read_file_version(&self, id: FileID, version: u64) -> Result<Vec<u8>> {
        if let Some(versions) = self.file_versions.get(&id) {
            if let Some(file_version) = versions.iter().find(|fv| fv.version == version) {
                Ok(file_version.content.clone())
            } else {
                Err(Error::NotFound(std::path::PathBuf::from(format!(
                    "Version {version} of file {id} not found"
                ))))
            }
            // None => {
            //     if let Some(latest) = versions.last() {
            //         Ok(latest.content.clone())
            //     } else {
            //         Err(Error::NotFound(std::path::PathBuf::from(format!(
            //             "No versions of file {id} found",
            //         ))))
            //     }
            // }
        } else {
            Err(Error::NotFound(std::path::PathBuf::from(format!(
                "File {id} not found",
            ))))
        }
    }

    async fn set_extended_attributes(
        &mut self,
        id: FileID,
        attributes: HashMap<String, String>,
    ) -> Result<()> {
        if let Some(versions) = self.file_versions.get_mut(&id) {
            if let Some(latest_version) = versions.last_mut() {
                latest_version.extended_metadata = Some(attributes);
                Ok(())
            } else {
                Err(Error::NotFound(std::path::PathBuf::from(format!(
                    "No versions of file {id} found",
                ))))
            }
        } else {
            Err(Error::NotFound(std::path::PathBuf::from(format!(
                "File {id} not found",
            ))))
        }
    }

    async fn create_dynamic_node(
        &mut self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<Node> {
        // Dynamic nodes are stored like files with config as content
        // Factory type goes in extended_metadata["factory"]
        let mut extended_metadata = HashMap::new();
        _ = extended_metadata.insert("factory".to_string(), factory_type.to_string());

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;

        let version = MemoryFileVersion {
            version: 1,
            timestamp,
            content: config_content,
            entry_type: id.entry_type(),
            extended_metadata: Some(extended_metadata),
        };

        self.file_versions
            .entry(id)
            .or_insert_with(Vec::new)
            .push(version);

        // Create a dummy node - actual factory instantiation happens on read
        Ok(Node::new(
            id,
            NodeType::File(super::MemoryFile::new_handle(vec![])),
        ))
    }

    async fn get_dynamic_node_config(&self, id: FileID) -> Result<Option<(String, Vec<u8>)>> {
        if let Some(versions) = self.file_versions.get(&id) {
            if let Some(latest) = versions.last() {
                if let Some(ref metadata) = latest.extended_metadata {
                    if let Some(factory_type) = metadata.get("factory") {
                        return Ok(Some((factory_type.clone(), latest.content.clone())));
                    }
                }
            }
        }
        Ok(None)
    }

    async fn update_dynamic_node_config(
        &mut self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<()> {
        let mut extended_metadata = HashMap::new();
        _ = extended_metadata.insert("factory".to_string(), factory_type.to_string());

        let new_version = if let Some(versions) = self.file_versions.get(&id) {
            let next_version = versions.last().map(|v| v.version + 1).unwrap_or(1);
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as i64;

            MemoryFileVersion {
                version: next_version,
                timestamp,
                content: config_content,
                entry_type: id.entry_type(),
                extended_metadata: Some(extended_metadata),
            }
        } else {
            return Err(Error::Other(format!("Dynamic node not found: {}", id)));
        };

        self.file_versions
            .get_mut(&id)
            .ok_or_else(|| Error::Other(format!("Dynamic node not found: {}", id)))?
            .push(new_version);

        Ok(())
    }
}
