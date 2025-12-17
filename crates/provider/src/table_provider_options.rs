//! Table Provider Configuration Options
//!
//! This module provides configuration types for creating DataFusion TableProviders
//! in a flexible, provider-agnostic way.

use crate::VersionSelection;
use tinyfs::FileID;

/// Configuration options for table provider creation
///
/// Follows anti-duplication principles: single configurable function instead of multiple variants.
/// Used by both tlogfs and in-memory testing implementations.
#[derive(Default, Clone)]
pub struct TableProviderOptions {
    /// Version selection strategy (LatestVersion, AllVersions, or SpecificVersion)
    pub version_selection: VersionSelection,

    /// Multiple file URLs/paths to combine into a single table
    /// If empty, will use the node_id/part_id pattern (existing behavior)
    pub additional_urls: Vec<String>,
}

/// Cache key for TableProvider instances
///
/// Used to avoid recreating TableProviders for the same file/version combination.
/// Enables efficient schema inference caching across queries.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableProviderKey {
    /// The file being accessed
    pub file_id: FileID,

    /// Version selection strategy
    pub version_selection: VersionSelection,
}

impl TableProviderKey {
    /// Create a new cache key
    #[must_use]
    pub fn new(file_id: FileID, version_selection: VersionSelection) -> Self {
        Self {
            file_id,
            version_selection,
        }
    }

    /// Convert to string for HashMap-based caching
    ///
    /// Format: "file_id:node_id:part_id:version_selection"
    /// Example: "12345:node123:part456:latest"
    #[must_use]
    pub fn to_cache_string(&self) -> String {
        format!(
            "{}:{}",
            self.file_id,
            self.version_selection.to_cache_string()
        )
    }
}
