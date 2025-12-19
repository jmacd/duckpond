// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::EntryType;
use crate::error::Result;
use crate::node::{FileID, Node};
use crate::transaction_guard::TransactionState;
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// Information about a specific version of a file
#[derive(Debug, Clone)]
pub struct FileVersionInfo {
    /// Version number (monotonically increasing)
    pub version: u64,
    /// Timestamp when this version was created (Unix microseconds)
    pub timestamp: i64,
    /// Size of the file content in bytes
    pub size: u64,
    /// SHA256 hash of the content (for integrity checking)
    pub sha256: Option<String>,
    /// Entry type for this version
    pub entry_type: EntryType,
    /// Extended metadata for this version
    pub extended_metadata: Option<HashMap<String, String>>,
}

/// Pure persistence layer - no caching, no NodeRef management
#[async_trait]
pub trait PersistenceLayer: Send + Sync {
    /// Downcast support for accessing concrete implementation methods
    fn as_any(&self) -> &dyn std::any::Any;

    /// Get the transaction state for this persistence layer
    ///
    /// This allows transaction guards to be created from the persistence layer
    fn transaction_state(&self) -> Arc<TransactionState>;

    async fn load_node(&self, file_id: FileID) -> Result<Node>;

    async fn store_node(&self, node: &Node) -> Result<()>;

    async fn create_file_node(&self, file_id: FileID) -> Result<Node>;

    async fn create_directory_node(&self, id: FileID) -> Result<Node>;

    async fn create_symlink_node(&self, id: FileID, target: &Path) -> Result<Node>;

    async fn create_dynamic_node(
        &self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<Node>;

    async fn get_dynamic_node_config(&self, id: FileID) -> Result<Option<(String, Vec<u8>)>>; // (factory_type, config)

    async fn update_dynamic_node_config(
        &self,
        id: FileID,
        factory_type: &str,
        config_content: Vec<u8>,
    ) -> Result<()>;

    /// Get consolidated metadata for a node
    /// Requires both node_id and part_id for efficient querying
    async fn metadata(&self, id: FileID) -> Result<crate::NodeMetadata>;

    /// List all versions of a file, returning metadata for each version
    /// Returns versions in chronological order (oldest to newest)
    async fn list_file_versions(&self, id: FileID) -> Result<Vec<FileVersionInfo>>;

    /// Read content of a specific version of a file
    /// If version is None, reads the latest version
    async fn read_file_version(&self, id: FileID, version: u64) -> Result<Vec<u8>>;

    /// Set extended attributes on an existing node
    /// This should modify the pending version of the node in the current transaction
    async fn set_extended_attributes(
        &self,
        id: FileID,
        attributes: HashMap<String, String>,
    ) -> Result<()>;

    /// Get temporal bounds for a FileSeries node
    ///
    /// Returns the time range (min_time, max_time) that this FileSeries covers,
    /// used for data quality filtering at the Parquet reader level.
    ///
    /// This is similar to node metadata but optimized for performance:
    /// - tlogfs stores temporal bounds in dedicated OplogEntry columns (not in attributes map)
    /// - This allows efficient queries without deserializing JSON metadata
    /// - Common use case: filter out garbage data outside valid time range
    ///
    /// Returns:
    /// - Ok(Some((min, max))) if the node is a FileSeries with temporal bounds
    /// - Ok(None) if the node has no temporal bounds or is not a FileSeries
    /// - Err(_) if the query fails
    async fn get_temporal_bounds(&self, id: FileID) -> Result<Option<(i64, i64)>>;
}
