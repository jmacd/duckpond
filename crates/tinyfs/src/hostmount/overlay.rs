// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Overlay directory -- merges host filesystem entries with factory-mounted nodes
//!
//! An `OverlayDirectory` wraps a `HostDirectory` and adds factory-backed children
//! at specific mount points. When `get(name)` is called:
//!
//! 1. Check overlay entries first (factory-defined children take precedence)
//! 2. Fall back to the underlying `HostDirectory` for real files
//!
//! This is a union-mount where overlay entries shadow host files of the same name.

use crate::dir::{Directory, DirectoryEntry, Handle};
use crate::error::{Error, Result};
use crate::metadata::{Metadata, NodeMetadata};
use crate::node::Node;
use crate::{EntryType, NodeType};
use async_trait::async_trait;
use futures::stream::{self, Stream, StreamExt};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use super::HostDirectory;

/// A directory that overlays factory-mounted nodes on top of a HostDirectory.
///
/// Overlay entries shadow host filesystem entries of the same name.
/// Writes always go to the underlying host directory. Removing an overlay
/// mount is an error.
pub struct OverlayDirectory {
    /// The underlying host directory
    host_dir: HostDirectory,
    /// Factory-mounted children: name -> Node
    overlays: HashMap<String, Node>,
}

impl OverlayDirectory {
    /// Create a new overlay directory wrapping the given host directory
    /// with the specified overlay entries.
    #[must_use]
    pub fn new(host_dir: HostDirectory, overlays: HashMap<String, Node>) -> Self {
        Self {
            host_dir,
            overlays,
        }
    }

    /// Create a Handle for this overlay directory.
    #[must_use]
    pub fn create_handle(self) -> Handle {
        Handle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }
}

#[async_trait]
impl Metadata for OverlayDirectory {
    async fn metadata(&self) -> Result<NodeMetadata> {
        // Delegate to the underlying host directory
        self.host_dir.metadata().await
    }
}

#[async_trait]
impl Directory for OverlayDirectory {
    async fn get(&self, name: &str) -> Result<Option<Node>> {
        // Overlay entries take precedence over host entries
        if let Some(overlay_node) = self.overlays.get(name) {
            log::debug!(
                "OverlayDirectory::get - returning overlay entry '{}'",
                name
            );
            return Ok(Some(overlay_node.clone()));
        }

        // Fall back to host directory
        self.host_dir.get(name).await
    }

    async fn insert(&mut self, name: String, node: Node) -> Result<()> {
        // Cannot insert over an overlay mount
        if self.overlays.contains_key(&name) {
            return Err(Error::Other(format!(
                "Cannot insert '{}': path is an overlay mount",
                name
            )));
        }

        // Delegate to host directory for real writes
        self.host_dir.insert(name, node).await
    }

    async fn remove(&mut self, name: &str) -> Result<Option<Node>> {
        // Cannot remove an overlay mount
        if self.overlays.contains_key(name) {
            return Err(Error::Other(format!(
                "Cannot remove '{}': path is an overlay mount",
                name
            )));
        }

        // Delegate to host directory for real removals
        self.host_dir.remove(name).await
    }

    async fn entries(&self) -> Result<Pin<Box<dyn Stream<Item = Result<DirectoryEntry>> + Send>>> {
        // Get host entries
        let host_stream = self.host_dir.entries().await?;
        let host_entries: Vec<Result<DirectoryEntry>> = host_stream.collect().await;

        // Collect overlay entry names for shadowing check
        let overlay_names: std::collections::HashSet<&String> =
            self.overlays.keys().collect();

        // Filter out host entries that are shadowed by overlays
        let mut merged: Vec<Result<DirectoryEntry>> = host_entries
            .into_iter()
            .filter(|entry| {
                entry
                    .as_ref()
                    .map(|e| !overlay_names.contains(&e.name))
                    .unwrap_or(true) // Keep errors
            })
            .collect();

        // Add overlay entries
        for (name, node) in &self.overlays {
            let entry_type = match &node.node_type {
                NodeType::Directory(_) => EntryType::DirectoryDynamic,
                NodeType::File(_) => EntryType::TableDynamic,
                NodeType::Symlink(_) => EntryType::Symlink,
            };

            merged.push(Ok(DirectoryEntry::new(
                name.clone(),
                node.id.node_id(),
                entry_type,
                0, // No meaningful timestamp for overlay entries
            )));
        }

        // Sort by name for deterministic ordering
        merged.sort_by(|a, b| {
            let a_name = a
                .as_ref()
                .map(|e| &e.name)
                .unwrap_or(&String::new())
                .clone();
            let b_name = b
                .as_ref()
                .map(|e| &e.name)
                .unwrap_or(&String::new())
                .clone();
            a_name.cmp(&b_name)
        });

        Ok(Box::pin(stream::iter(merged)))
    }
}

/// Specification for a single overlay mount point.
///
/// Parsed from `--hostmount <mount_path>=host+<factory>:///<config_path>`.
#[derive(Debug, Clone)]
pub struct MountSpec {
    /// Where in the TinyFS namespace the factory node appears (e.g., "/reduced")
    pub mount_path: String,
    /// The factory type name (e.g., "dynamic-dir")
    pub factory_name: String,
    /// Path to the config file, relative to the hostmount root
    pub config_path: String,
}

impl MountSpec {
    /// Parse a mount specification from `--hostmount` flag value.
    ///
    /// Format: `<mount_path>=host+<factory>:///<config_path>`
    ///
    /// Examples:
    ///   `/reduced=host+dyndir:///reduce.yaml`
    ///   `/reduced=host+dynamic-dir:///reduce.yaml`
    pub fn parse(spec: &str) -> std::result::Result<Self, String> {
        // Split on '=' to get mount_path and URL
        let parts: Vec<&str> = spec.splitn(2, '=').collect();
        if parts.len() != 2 {
            return Err(format!(
                "Invalid mount spec '{}': expected <mount_path>=host+<factory>:///<config_path>",
                spec
            ));
        }

        let mount_path = parts[0].to_string();
        let url_part = parts[1];

        // Validate mount path starts with /
        if !mount_path.starts_with('/') {
            return Err(format!(
                "Invalid mount path '{}': must start with '/'",
                mount_path
            ));
        }

        // Parse the URL part: host+<factory>:///<config_path>
        // Strip the "host+" prefix
        let url_without_host = if let Some(stripped) = url_part.strip_prefix("host+") {
            stripped
        } else {
            return Err(format!(
                "Invalid mount URL '{}': must start with 'host+'",
                url_part
            ));
        };

        // Split on ":///" to get factory name and config path
        let url_parts: Vec<&str> = url_without_host.splitn(2, ":///").collect();
        if url_parts.len() != 2 {
            return Err(format!(
                "Invalid mount URL '{}': expected host+<factory>:///<config_path>",
                url_part
            ));
        }

        let factory_name = url_parts[0].to_string();
        let config_path = url_parts[1].to_string();

        if factory_name.is_empty() {
            return Err(format!(
                "Invalid mount URL '{}': factory name cannot be empty",
                url_part
            ));
        }

        if config_path.is_empty() {
            return Err(format!(
                "Invalid mount URL '{}': config path cannot be empty",
                url_part
            ));
        }

        Ok(Self {
            mount_path,
            factory_name,
            config_path,
        })
    }

    /// Get the first path component of the mount path (the root-level name).
    ///
    /// For `/reduced` this returns `"reduced"`.
    /// For `/data/live` this returns `"data"`.
    #[must_use]
    pub fn root_component(&self) -> &str {
        let trimmed = self.mount_path.trim_start_matches('/');
        trimmed.split('/').next().unwrap_or(trimmed)
    }

    /// Get the remaining path components after the root (for nested mounts).
    ///
    /// For `/reduced` this returns `None`.
    /// For `/data/live` this returns `Some("live")`.
    #[must_use]
    pub fn remaining_path(&self) -> Option<&str> {
        let trimmed = self.mount_path.trim_start_matches('/');
        let first_slash = trimmed.find('/');
        first_slash.map(|idx| &trimmed[idx + 1..])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_mount_spec_simple() {
        let spec = MountSpec::parse("/reduced=host+dyndir:///reduce.yaml").unwrap();
        assert_eq!(spec.mount_path, "/reduced");
        assert_eq!(spec.factory_name, "dyndir");
        assert_eq!(spec.config_path, "reduce.yaml");
        assert_eq!(spec.root_component(), "reduced");
        assert!(spec.remaining_path().is_none());
    }

    #[test]
    fn test_parse_mount_spec_nested() {
        let spec = MountSpec::parse("/data/live=host+dynamic-dir:///live.yaml").unwrap();
        assert_eq!(spec.mount_path, "/data/live");
        assert_eq!(spec.factory_name, "dynamic-dir");
        assert_eq!(spec.config_path, "live.yaml");
        assert_eq!(spec.root_component(), "data");
        assert_eq!(spec.remaining_path(), Some("live"));
    }

    #[test]
    fn test_parse_mount_spec_no_equals() {
        let result = MountSpec::parse("/reduced");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_mount_spec_no_host_prefix() {
        let result = MountSpec::parse("/reduced=dyndir:///reduce.yaml");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_mount_spec_no_leading_slash() {
        let result = MountSpec::parse("reduced=host+dyndir:///reduce.yaml");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_mount_spec_empty_factory() {
        let result = MountSpec::parse("/reduced=host+:///reduce.yaml");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_mount_spec_empty_config() {
        let result = MountSpec::parse("/reduced=host+dyndir:///");
        assert!(result.is_err());
    }
}
