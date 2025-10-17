//! Dynamic directory factory for TLogFS
//!
//! This factory creates configurable directories where each entry is dynamically generated
//! using other factories. It allows building complex directory structures by composing
//! multiple dynamic nodes within a single directory configuration.
//!
//! # Configuration Format
//!
//! The configuration defines a list of directory entries, each with:
//! - `name`: The entry name in the directory
//! - `factory`: The factory type to use for creating the entry
//! - `config`: The configuration for the specific factory
//!
//! # Example Configuration
//! ```yaml
//! entries:
//!   - name: "field_station"
//!     factory: "sql-derived-series"
//!     config:
//!       patterns:
//!         - "/measurements/surface/*"
//!         - "/measurements/bottom/*"
//!       query: "SELECT * FROM source ORDER BY timestamp"
//!   
//!   - name: "host_logs"
//!     factory: "hostmount"
//!     config:
//!       directory: "/var/log/application"
//!       
//!   - name: "csv_data"
//!     factory: "csvdir"
//!     config:
//!       source: "/raw/data/*.csv"
//! ```
//!
//! # Architecture
//!
//! The DynamicDirDirectory acts as a composition root that:
//! 1. Parses its own configuration to get the list of entries
//! 2. For each entry, delegates to the appropriate factory to create the node
//! 3. Presents all created nodes as a unified directory interface
//! 4. Handles factory lookup and validation through the FactoryRegistry
//!
//! This enables building complex directory structures that combine multiple data sources
//! and processing types in a single configuration file.

use crate::factory::{FactoryContext, FactoryRegistry};
use crate::register_dynamic_factory;
use async_trait::async_trait;
use futures::stream;
use log::{debug, error};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tinyfs::{
    DirHandle, Directory, EntryType, Metadata, NodeMetadata, NodeRef, Result as TinyFSResult,
};

/// Configuration for a single directory entry
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DynamicDirEntry {
    /// Name of the entry in the directory
    pub name: String,
    /// Factory type to use for creating this entry
    pub factory: String,
    /// Configuration for the specific factory (as raw Value for flexibility)
    pub config: Value,
}

/// Configuration for the dynamic directory
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DynamicDirConfig {
    /// List of directory entries to create
    pub entries: Vec<DynamicDirEntry>,
}

/// Dynamic directory that creates entries using configured factories
pub struct DynamicDirDirectory {
    config: DynamicDirConfig,
    context: FactoryContext,
    /// Cache of created nodes to avoid recreating them on each access
    entry_cache: tokio::sync::RwLock<HashMap<String, NodeRef>>,
}

impl DynamicDirDirectory {
    pub fn new(config: DynamicDirConfig, context: FactoryContext) -> Self {
        let entries_count = config.entries.len();
        debug!("DynamicDirDirectory::new - creating directory with {entries_count} entries");

        // Log each entry for debugging
        for entry in &config.entries {
            debug!(
                "DynamicDirDirectory::new - entry '{}' using factory '{}'",
                entry.name, entry.factory
            );
        }

        Self {
            config,
            context,
            entry_cache: tokio::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Create a DirHandle from this dynamic directory
    pub fn create_handle(self) -> DirHandle {
        tinyfs::DirHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }

    /// Create a node for a specific entry using its configured factory
    async fn create_entry_node(&self, entry: &DynamicDirEntry) -> TinyFSResult<NodeRef> {
        debug!(
            "DynamicDirDirectory::create_entry_node - creating entry '{}' with factory '{}'",
            entry.name, entry.factory
        );

        // Convert the configuration to JSON bytes for factory validation
        let config_bytes = serde_json::to_vec(&entry.config).map_err(|e| {
            tinyfs::Error::Other(format!(
                "Failed to serialize config for entry '{}': {}",
                entry.name, e
            ))
        })?;

        // Try to create as a directory first, then as a file
        let node_type = if let Ok(dir_handle) = FactoryRegistry::create_directory(
            &entry.factory,
            &config_bytes,
            self.context.clone(),
        ) {
            debug!(
                "DynamicDirDirectory::create_entry_node - created directory for entry '{}'",
                entry.name
            );
            tinyfs::NodeType::Directory(dir_handle)
        } else if let Ok(file_handle) =
            FactoryRegistry::create_file(&entry.factory, &config_bytes, self.context.clone()).await
        {
            debug!(
                "DynamicDirDirectory::create_entry_node - created file for entry '{}'",
                entry.name
            );
            tinyfs::NodeType::File(file_handle)
        } else {
            let error_msg = format!(
                "Factory '{}' for entry '{}' does not support directories or files",
                entry.factory, entry.name
            );
            error!("DynamicDirDirectory::create_entry_node - {error_msg}");
            return Err(tinyfs::Error::Other(error_msg));
        };

        // Deterministically generate NodeID for entry node based on entry name, factory, and config
        // Pass concatenated bytes directly to NodeID::from_content to avoid double-hashing
        let mut id_bytes = Vec::new();
        id_bytes.extend_from_slice(entry.name.as_bytes());
        id_bytes.extend_from_slice(entry.factory.as_bytes());
        id_bytes.extend_from_slice(&config_bytes);
        let node_id = tinyfs::NodeID::from_content(&id_bytes);

        let node_ref = tinyfs::NodeRef::new(Arc::new(tokio::sync::Mutex::new(tinyfs::Node {
            id: node_id,
            node_type,
        })));

        Ok(node_ref)
    }

    /// Get or create a cached entry node
    async fn get_entry_node(&self, entry_name: &str) -> TinyFSResult<Option<NodeRef>> {
        // Check cache first
        {
            let cache = self.entry_cache.read().await;
            if let Some(node_ref) = cache.get(entry_name) {
                debug!(
                    "DynamicDirDirectory::get_entry_node - returning cached entry '{entry_name}'"
                );
                return Ok(Some(node_ref.clone()));
            }
        }

        // Find the entry configuration
        let entry = self.config.entries.iter().find(|e| e.name == entry_name);

        if let Some(entry) = entry {
            // Create the node
            let node_ref = self.create_entry_node(entry).await?;

            // Cache it
            {
                let mut cache = self.entry_cache.write().await;
                cache.insert(entry_name.to_string(), node_ref.clone());
            }

            Ok(Some(node_ref))
        } else {
            debug!(
                "DynamicDirDirectory::get_entry_node - entry '{entry_name}' not found in configuration"
            );
            Ok(None)
        }
    }
}

#[async_trait]
impl Directory for DynamicDirDirectory {
    async fn get(&self, name: &str) -> tinyfs::Result<Option<NodeRef>> {
        debug!("DynamicDirDirectory::get - looking for entry '{name}'");
        self.get_entry_node(name).await
    }

    async fn insert(&mut self, _name: String, _id: NodeRef) -> tinyfs::Result<()> {
        debug!("DynamicDirDirectory::insert - mutation not permitted on dynamic directory");
        Err(tinyfs::Error::Other(
            "Dynamic directory is read-only".to_string(),
        ))
    }

    async fn entries(
        &self,
    ) -> tinyfs::Result<
        std::pin::Pin<Box<dyn futures::Stream<Item = tinyfs::Result<(String, NodeRef)>> + Send>>,
    > {
        let entries_count = self.config.entries.len();
        debug!("DynamicDirDirectory::entries - listing {entries_count} configured entries");

        let mut results = Vec::new();

        for entry in &self.config.entries {
            match self.get_entry_node(&entry.name).await {
                Ok(Some(node_ref)) => {
                    debug!(
                        "DynamicDirDirectory::entries - successfully created entry '{}'",
                        entry.name
                    );
                    results.push(Ok((entry.name.clone(), node_ref)));
                }
                Ok(None) => {
                    // This shouldn't happen since we control the configuration
                    let error_msg = format!("Entry '{}' not found in configuration", entry.name);
                    error!("DynamicDirDirectory::entries - {error_msg}");
                    results.push(Err(tinyfs::Error::Other(error_msg)));
                }
                Err(e) => {
                    let error_msg = format!("Failed to create entry '{}': {}", entry.name, e);
                    error!("DynamicDirDirectory::entries - {error_msg}");
                    results.push(Err(tinyfs::Error::Other(error_msg)));
                }
            }
        }

        let results_count = results.len();
        debug!("DynamicDirDirectory::entries - returning {results_count} entries");
        Ok(Box::pin(stream::iter(results)))
    }
}

#[async_trait]
impl Metadata for DynamicDirDirectory {
    async fn metadata(&self) -> tinyfs::Result<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,
            size: None,
            sha256: None,
            entry_type: EntryType::DirectoryDynamic,
            timestamp: 0,
        })
    }
}

// Factory functions for the linkme registration system
fn create_dynamic_dir_handle(
    config: Value,
    context: FactoryContext,
) -> TinyFSResult<DirHandle> {
    let config: DynamicDirConfig = serde_json::from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid dynamic directory config: {}", e)))?;

    // Instrument: log parent_node_id, entry names, and config hash
    let parent_node_id = context.parent_node_id;
    let entry_names: Vec<_> = config.entries.iter().map(|e| e.name.clone()).collect();
    let mut hasher = DefaultHasher::new();
    config.hash(&mut hasher);
    let config_hash = hasher.finish();
    debug!(
        "[INSTRUMENT] create_dynamic_dir_handle: parent_node_id={:?}, entry_names={:?}, config_hash={:x}",
        parent_node_id, entry_names, config_hash
    );

    // Create cache key using config hash as entry name to ensure uniqueness per configuration
    let cache_entry_name = format!("dynamic_dir_{:x}", config_hash);

    // Create cache key synchronously - we'll use a placeholder part_id since we can't await here

    let cache_key = crate::persistence::DynamicNodeKey::new(parent_node_id, cache_entry_name);

    debug!("[INSTRUMENT] cache_key: {:?}", cache_key);

    // Check if we have a cached directory for this configuration
    if let Some(cached_node_type) = context.state.get_dynamic_node_cache(&cache_key) {
        if let tinyfs::NodeType::Directory(cached_dir_handle) = cached_node_type {
            debug!(
                "[INSTRUMENT] returning cached dynamic directory for config_hash={:x}",
                config_hash
            );
            return Ok(cached_dir_handle);
        }
    }

    // Create new instance
    let dynamic_dir = DynamicDirDirectory::new(config, context.clone());
    let dir_handle = dynamic_dir.create_handle();

    // Cache the directory handle for future access within this transaction
    context
        .state
        .set_dynamic_node_cache(cache_key, tinyfs::NodeType::Directory(dir_handle.clone()));
    debug!(
        "[INSTRUMENT] cached new dynamic directory for config_hash={:x}",
        config_hash
    );

    Ok(dir_handle)
}

fn validate_dynamic_dir_config(config: &[u8]) -> TinyFSResult<Value> {
    // Parse as YAML first (user format)
    let mut yaml_config: DynamicDirConfig = serde_yaml::from_slice(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid YAML config: {}", e)))?;

    // Validate that entries list is not empty
    if yaml_config.entries.is_empty() {
        return Err(tinyfs::Error::Other(
            "Entries list cannot be empty".to_string(),
        ));
    }

    // Validate each entry
    for (i, entry) in yaml_config.entries.iter_mut().enumerate() {
        // Check that name is not empty
        if entry.name.trim().is_empty() {
            return Err(tinyfs::Error::Other(format!("Entry {} has empty name", i)));
        }

        // Check that factory name is not empty
        if entry.factory.trim().is_empty() {
            return Err(tinyfs::Error::Other(format!(
                "Entry '{}' has empty factory name",
                entry.name
            )));
        }

        // Verify that the factory exists
        if FactoryRegistry::get_factory(&entry.factory).is_none() {
            return Err(tinyfs::Error::Other(format!(
                "Unknown factory '{}' for entry '{}'",
                entry.factory, entry.name
            )));
        }

        // Validate the entry's configuration with its factory
        // Convert config to JSON bytes for validation
        let config_bytes = serde_json::to_vec(&entry.config).map_err(|e| {
            tinyfs::Error::Other(format!(
                "Failed to serialize config for entry '{}': {}",
                entry.name, e
            ))
        })?;

        // Validate with the specific factory and get processed config
        let processed_config = FactoryRegistry::validate_config(&entry.factory, &config_bytes)
            .map_err(|e| {
                tinyfs::Error::Other(format!(
                    "Invalid config for entry '{}' using factory '{}': {}: {:?}",
                    entry.name,
                    entry.factory,
                    e,
                    String::from_utf8_lossy(&config_bytes),
                ))
            })?;

        // Update the entry with the processed config
        entry.config = processed_config;
    }

    // Check for duplicate entry names
    let mut names = std::collections::HashSet::new();
    for entry in &yaml_config.entries {
        if !names.insert(&entry.name) {
            return Err(tinyfs::Error::Other(format!(
                "Duplicate entry name: '{}'",
                entry.name
            )));
        }
    }

    // Convert to JSON Value for internal processing
    let json_value = serde_json::to_value(yaml_config)
        .map_err(|e| tinyfs::Error::Other(format!("Failed to convert config to JSON: {}", e)))?;

    Ok(json_value)
}

// Register the dynamic directory factory
register_dynamic_factory!(
    name: "dynamic-dir",
    description: "Create configurable directories where each entry is dynamically generated using other factories",
    directory: create_dynamic_dir_handle,
    validate: validate_dynamic_dir_config
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::OpLogPersistence;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_dynamic_dir_config_validation() {
        // Test valid configuration
        let valid_config = r#"
entries:
  - name: "test_entry"
    factory: "template"
    config:
      in_pattern: "/base/*.tmpl"
      out_pattern: "$0.txt"
      template: "Test content"
"#;

        let result = validate_dynamic_dir_config(valid_config.as_bytes());
        assert!(result.is_ok());

        // Test empty entries list
        let empty_entries_config = r#"
entries: []
"#;

        let result = validate_dynamic_dir_config(empty_entries_config.as_bytes());
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Entries list cannot be empty")
        );

        // Test duplicate entry names
        let duplicate_names_config = r#"
entries:
  - name: "duplicate"
    factory: "template"
    config:
      in_pattern: "/base/*.tmpl"
      out_pattern: "$0.txt"
      template: "Test content"
  - name: "duplicate"
    factory: "template"
    config:
      in_pattern: "/other/*.tmpl"
      out_pattern: "$0.html"
      template: "Other content"
"#;

        let result = validate_dynamic_dir_config(duplicate_names_config.as_bytes());
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Duplicate entry name")
        );

        // Test empty entry name
        let empty_name_config = r#"
entries:
  - name: ""
    factory: "template"
    config:
      in_pattern: "/base/*.tmpl"
      out_pattern: "$0.txt"
      template: "Test content"
"#;

        let result = validate_dynamic_dir_config(empty_name_config.as_bytes());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty name"));

        // Test empty factory name
        let empty_factory_config = r#"
entries:
  - name: "test"
    factory: ""
    config:
      in_pattern: "/base/*.tmpl"
      out_pattern: "$0.txt"
      template: "Test content"
"#;

        let result = validate_dynamic_dir_config(empty_factory_config.as_bytes());
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("empty factory name")
        );

        // Test unknown factory
        let unknown_factory_config = r#"
entries:
  - name: "test"
    factory: "nonexistent_factory"
    config:
      some_config: "value"
"#;

        let result = validate_dynamic_dir_config(unknown_factory_config.as_bytes());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unknown factory"));
    }

    #[tokio::test]
    async fn test_dynamic_dir_creation() {
        let temp_dir = TempDir::new().unwrap();

        let mut persistence = OpLogPersistence::create(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();
        let tx_guard = persistence.begin(1).await.unwrap();
        let state = tx_guard.state().unwrap();
        use tinyfs::NodeID;
        let context = FactoryContext::new(state, NodeID::root());

        // Create a valid configuration using template factory
        let config = DynamicDirConfig {
            entries: vec![DynamicDirEntry {
                name: "template_dir".to_string(),
                factory: "template".to_string(),
                config: serde_json::json!({
                    "in_pattern": "/base/*.tmpl",
                    "out_pattern": "$0.txt",
                    "template": "Test template content"
                }),
            }],
        };

        // Create the dynamic directory
        let dynamic_dir = DynamicDirDirectory::new(config, context);

        // Test that we can get the configured entry
        let result = dynamic_dir.get("template_dir").await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());

        // Test that we get None for non-existent entries
        let result = dynamic_dir.get("nonexistent").await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_dynamic_dir_entries_listing() {
        use futures::StreamExt;

        let temp_dir = TempDir::new().unwrap();

        let mut persistence = OpLogPersistence::create(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();
        let tx_guard = persistence.begin(1).await.unwrap();
        let state = tx_guard.state().unwrap();
        use tinyfs::NodeID;
        let context = FactoryContext::new(state, NodeID::root());

        // Create configuration with multiple entries using template factory
        let config = DynamicDirConfig {
            entries: vec![
                DynamicDirEntry {
                    name: "template1".to_string(),
                    factory: "template".to_string(),
                    config: serde_json::json!({
                        "in_pattern": "/base/*.tmpl",
                        "out_pattern": "$0.txt",
                        "template": "Template 1 content"
                    }),
                },
                DynamicDirEntry {
                    name: "template2".to_string(),
                    factory: "template".to_string(),
                    config: serde_json::json!({
                        "in_pattern": "/other/*.tmpl",
                        "out_pattern": "$0.html",
                        "template": "Template 2 content"
                    }),
                },
            ],
        };

        let dynamic_dir = DynamicDirDirectory::new(config, context);

        // Test entries listing
        let entries_stream = dynamic_dir.entries().await.unwrap();
        let entries: Vec<_> = entries_stream.collect().await;

        assert_eq!(entries.len(), 2);

        let names: std::collections::HashSet<String> = entries
            .iter()
            .filter_map(|result| result.as_ref().ok())
            .map(|(name, _)| name.clone())
            .collect();

        assert!(names.contains("template1"));
        assert!(names.contains("template2"));
    }
}
