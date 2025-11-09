use std::str::FromStr;

/// Node type identifiers for directory entries and persistence
///
/// This enum provides type-safe alternatives to string literals
/// for identifying node types in directory entries and persistence layers.
/// Files are distinguished by their format for different access patterns.
///
/// CRITICAL: This enum is now COMPREHENSIVE - it includes both the access method
/// (directory, file, symlink) AND whether the node is physical (real TLogFS) or
/// dynamic (factory-based). This eliminates the need to query OplogEntry.factory
/// to determine partition assignment.
///
/// Partition Rules:
/// - DirectoryPhysical: uses child_node_id as part_id (own partition)
/// - DirectoryDynamic: uses parent_node_id as part_id (parent's partition)
/// - All Files (physical/dynamic): use parent_node_id as part_id (parent's partition)
/// - Symlinks: use parent_node_id as part_id (parent's partition, always physical)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryType {
    /// Physical directory - real TLogFS directory that creates its own partition
    #[serde(rename = "dir:physical")]
    DirectoryPhysical,

    /// Dynamic directory - factory-based directory that uses parent's partition
    #[serde(rename = "dir:dynamic")]
    DirectoryDynamic,

    /// Symbolic link entry (always physical, no dynamic symlinks)
    Symlink,

    /// Physical data file - arbitrary byte content, accessed via Read/Write traits
    #[serde(rename = "file:data:physical")]
    FileDataPhysical,

    /// Dynamic data file - factory-generated data file
    #[serde(rename = "file:data:dynamic")]
    FileDataDynamic,

    /// Physical table file - single-version table stored as Parquet
    #[serde(rename = "file:table:physical")]
    FileTablePhysical,

    /// Dynamic table file - factory-generated table
    #[serde(rename = "file:table:dynamic")]
    FileTableDynamic,

    /// Physical series file - multi-version table series, supports time-travel queries
    #[serde(rename = "file:series:physical")]
    FileSeriesPhysical,

    /// Dynamic series file - factory-generated time series
    #[serde(rename = "file:series:dynamic")]
    FileSeriesDynamic,
}

impl EntryType {
    /// Check if this entry is a file (any format, physical or dynamic)
    #[must_use]
    pub fn is_file(&self) -> bool {
        matches!(
            self,
            EntryType::FileDataPhysical
                | EntryType::FileDataDynamic
                | EntryType::FileTablePhysical
                | EntryType::FileTableDynamic
                | EntryType::FileSeriesPhysical
                | EntryType::FileSeriesDynamic
        )
    }

    /// Check if this entry is a directory (physical or dynamic)
    #[must_use]
    pub fn is_directory(&self) -> bool {
        matches!(
            self,
            EntryType::DirectoryPhysical | EntryType::DirectoryDynamic
        )
    }

    /// Check if this entry is dynamic (factory-based)
    #[must_use]
    pub fn is_dynamic(&self) -> bool {
        matches!(
            self,
            EntryType::DirectoryDynamic
                | EntryType::FileDataDynamic
                | EntryType::FileTableDynamic
                | EntryType::FileSeriesDynamic
        )
    }

    /// Check if this entry is physical (real TLogFS node)
    #[must_use]
    pub fn is_physical(&self) -> bool {
        !self.is_dynamic() && *self != EntryType::Symlink
    }

    /// Check if this entry is a data file (physical or dynamic)
    #[must_use]
    pub fn is_data_file(&self) -> bool {
        matches!(
            self,
            EntryType::FileDataPhysical | EntryType::FileDataDynamic
        )
    }

    /// Check if this entry is a table file (physical or dynamic)
    #[must_use]
    pub fn is_table_file(&self) -> bool {
        matches!(
            self,
            EntryType::FileTablePhysical | EntryType::FileTableDynamic
        )
    }

    /// Check if this entry is a series file (physical or dynamic)
    #[must_use]
    pub fn is_series_file(&self) -> bool {
        matches!(
            self,
            EntryType::FileSeriesPhysical | EntryType::FileSeriesDynamic
        )
    }

    /// Check if this entry requires Parquet storage (table or series, physical or dynamic)
    #[must_use]
    pub fn is_parquet_file(&self) -> bool {
        matches!(
            self,
            EntryType::FileTablePhysical
                | EntryType::FileTableDynamic
                | EntryType::FileSeriesPhysical
                | EntryType::FileSeriesDynamic
        )
    }

    /// Get the base file format (ignoring physical/dynamic distinction)
    #[must_use]
    pub fn base_format(&self) -> &'static str {
        match self {
            EntryType::DirectoryPhysical | EntryType::DirectoryDynamic => "directory",
            EntryType::Symlink => "symlink",
            EntryType::FileDataPhysical | EntryType::FileDataDynamic => "file:data",
            EntryType::FileTablePhysical | EntryType::FileTableDynamic => "file:table",
            EntryType::FileSeriesPhysical | EntryType::FileSeriesDynamic => "file:series",
        }
    }

    /// Convert EntryType to string for serialization/storage
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            EntryType::DirectoryPhysical => "dir:physical",
            EntryType::DirectoryDynamic => "dir:dynamic",
            EntryType::Symlink => "symlink",
            EntryType::FileDataPhysical => "file:data:physical",
            EntryType::FileDataDynamic => "file:data:dynamic",
            EntryType::FileTablePhysical => "file:table:physical",
            EntryType::FileTableDynamic => "file:table:dynamic",
            EntryType::FileSeriesPhysical => "file:series:physical",
            EntryType::FileSeriesDynamic => "file:series:dynamic",
        }
    }

    /// Convert from NodeType to EntryType for directory entries
    ///
    /// Query the handle's metadata to determine the actual EntryType
    pub async fn from_node_type(node_type: &crate::NodeType) -> crate::error::Result<Self> {
        match node_type {
            crate::NodeType::File(handle) => {
                let metadata = handle.metadata().await?;
                Ok(metadata.entry_type)
            }
            crate::NodeType::Directory(_) => {
                // For directories, we need additional context to determine if they're dynamic
                // This method is insufficient - use from_node_type_with_factory instead
                // Default to physical for compatibility
                Ok(EntryType::DirectoryPhysical)
            }
            crate::NodeType::Symlink(_) => Ok(EntryType::Symlink),
        }
    }
}

impl FromStr for EntryType {
    type Err = String;

    /// Parse EntryType from string for deserialization
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            // New comprehensive format
            "dir:physical" => Ok(EntryType::DirectoryPhysical),
            "dir:dynamic" => Ok(EntryType::DirectoryDynamic),
            "symlink" => Ok(EntryType::Symlink),
            "file:data:physical" => Ok(EntryType::FileDataPhysical),
            "file:data:dynamic" => Ok(EntryType::FileDataDynamic),
            "file:table:physical" => Ok(EntryType::FileTablePhysical),
            "file:table:dynamic" => Ok(EntryType::FileTableDynamic),
            "file:series:physical" => Ok(EntryType::FileSeriesPhysical),
            "file:series:dynamic" => Ok(EntryType::FileSeriesDynamic),

            other => Err(format!("Unknown entry type: {}", other)),
        }
    }

}

impl std::fmt::Display for EntryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_type_string_conversion() {
        assert_eq!(EntryType::DirectoryPhysical.as_str(), "dir:physical");
        assert_eq!(EntryType::DirectoryDynamic.as_str(), "dir:dynamic");
        assert_eq!(EntryType::Symlink.as_str(), "symlink");
        assert_eq!(EntryType::FileDataPhysical.as_str(), "file:data:physical");
        assert_eq!(EntryType::FileDataDynamic.as_str(), "file:data:dynamic");
        assert_eq!(EntryType::FileTablePhysical.as_str(), "file:table:physical");
        assert_eq!(EntryType::FileTableDynamic.as_str(), "file:table:dynamic");
        assert_eq!(
            EntryType::FileSeriesPhysical.as_str(),
            "file:series:physical"
        );
        assert_eq!(EntryType::FileSeriesDynamic.as_str(), "file:series:dynamic");
    }

    #[test]
    fn test_entry_type_parsing() {
        // New comprehensive format
        assert_eq!(
            EntryType::from_str("dir:physical").unwrap(),
            EntryType::DirectoryPhysical
        );
        assert_eq!(
            EntryType::from_str("dir:dynamic").unwrap(),
            EntryType::DirectoryDynamic
        );
        assert_eq!(
            EntryType::from_str("file:data:physical").unwrap(),
            EntryType::FileDataPhysical
        );
        assert_eq!(
            EntryType::from_str("file:data:dynamic").unwrap(),
            EntryType::FileDataDynamic
        );
        assert_eq!(
            EntryType::from_str("file:table:physical").unwrap(),
            EntryType::FileTablePhysical
        );
        assert_eq!(
            EntryType::from_str("file:table:dynamic").unwrap(),
            EntryType::FileTableDynamic
        );
        assert_eq!(
            EntryType::from_str("file:series:physical").unwrap(),
            EntryType::FileSeriesPhysical
        );
        assert_eq!(
            EntryType::from_str("file:series:dynamic").unwrap(),
            EntryType::FileSeriesDynamic
        );
        assert_eq!(EntryType::from_str("symlink").unwrap(), EntryType::Symlink);

        assert!(EntryType::from_str("invalid").is_err());

        // Legacy formats should now fail
        assert!(EntryType::from_str("directory").is_err());
        assert!(EntryType::from_str("file:data").is_err());
        assert!(EntryType::from_str("file:table").is_err());
        assert!(EntryType::from_str("file:series").is_err());
    }

    #[test]
    fn test_file_type_queries() {
        // Test is_file()
        assert!(EntryType::FileDataPhysical.is_file());
        assert!(EntryType::FileDataDynamic.is_file());
        assert!(EntryType::FileTablePhysical.is_file());
        assert!(EntryType::FileTableDynamic.is_file());
        assert!(EntryType::FileSeriesPhysical.is_file());
        assert!(EntryType::FileSeriesDynamic.is_file());
        assert!(!EntryType::DirectoryPhysical.is_file());
        assert!(!EntryType::DirectoryDynamic.is_file());
        assert!(!EntryType::Symlink.is_file());

        // Test is_directory()
        assert!(EntryType::DirectoryPhysical.is_directory());
        assert!(EntryType::DirectoryDynamic.is_directory());
        assert!(!EntryType::FileDataPhysical.is_directory());
        assert!(!EntryType::Symlink.is_directory());

        // Test is_dynamic()
        assert!(EntryType::DirectoryDynamic.is_dynamic());
        assert!(EntryType::FileDataDynamic.is_dynamic());
        assert!(EntryType::FileTableDynamic.is_dynamic());
        assert!(EntryType::FileSeriesDynamic.is_dynamic());
        assert!(!EntryType::DirectoryPhysical.is_dynamic());
        assert!(!EntryType::FileDataPhysical.is_dynamic());
        assert!(!EntryType::Symlink.is_dynamic());

        // Test is_physical()
        assert!(EntryType::DirectoryPhysical.is_physical());
        assert!(EntryType::FileDataPhysical.is_physical());
        assert!(EntryType::FileTablePhysical.is_physical());
        assert!(EntryType::FileSeriesPhysical.is_physical());
        assert!(!EntryType::DirectoryDynamic.is_physical());
        assert!(!EntryType::FileDataDynamic.is_physical());
        assert!(!EntryType::Symlink.is_physical()); // Symlinks are neither (special case)

        // Test is_data_file()
        assert!(EntryType::FileDataPhysical.is_data_file());
        assert!(EntryType::FileDataDynamic.is_data_file());
        assert!(!EntryType::FileTablePhysical.is_data_file());
        assert!(!EntryType::FileTableDynamic.is_data_file());

        // Test is_table_file()
        assert!(EntryType::FileTablePhysical.is_table_file());
        assert!(EntryType::FileTableDynamic.is_table_file());
        assert!(!EntryType::FileDataPhysical.is_table_file());
        assert!(!EntryType::FileDataDynamic.is_table_file());

        // Test is_series_file()
        assert!(EntryType::FileSeriesPhysical.is_series_file());
        assert!(EntryType::FileSeriesDynamic.is_series_file());
        assert!(!EntryType::FileDataPhysical.is_series_file());
        assert!(!EntryType::FileDataDynamic.is_series_file());

        // Test is_parquet_file()
        assert!(EntryType::FileTablePhysical.is_parquet_file());
        assert!(EntryType::FileTableDynamic.is_parquet_file());
        assert!(EntryType::FileSeriesPhysical.is_parquet_file());
        assert!(EntryType::FileSeriesDynamic.is_parquet_file());
        assert!(!EntryType::FileDataPhysical.is_parquet_file());
        assert!(!EntryType::FileDataDynamic.is_parquet_file());
    }

    #[test]
    fn test_display_trait() {
        assert_eq!(
            format!("{}", EntryType::FileDataPhysical),
            "file:data:physical"
        );
        assert_eq!(
            format!("{}", EntryType::FileDataDynamic),
            "file:data:dynamic"
        );
        assert_eq!(
            format!("{}", EntryType::FileTablePhysical),
            "file:table:physical"
        );
        assert_eq!(
            format!("{}", EntryType::FileTableDynamic),
            "file:table:dynamic"
        );
        assert_eq!(
            format!("{}", EntryType::FileSeriesPhysical),
            "file:series:physical"
        );
        assert_eq!(
            format!("{}", EntryType::FileSeriesDynamic),
            "file:series:dynamic"
        );
        assert_eq!(format!("{}", EntryType::DirectoryPhysical), "dir:physical");
        assert_eq!(format!("{}", EntryType::DirectoryDynamic), "dir:dynamic");
        assert_eq!(format!("{}", EntryType::Symlink), "symlink");
    }

    #[test]
    fn test_serde_serialization() {
        // Test that serde serialization works as expected
        let data_file_json = serde_json::to_string(&EntryType::FileDataPhysical).unwrap();
        assert_eq!(data_file_json, "\"file:data:physical\"");

        let table_file_json = serde_json::to_string(&EntryType::FileTableDynamic).unwrap();
        assert_eq!(table_file_json, "\"file:table:dynamic\"");

        let series_file_json = serde_json::to_string(&EntryType::FileSeriesPhysical).unwrap();
        assert_eq!(series_file_json, "\"file:series:physical\"");

        let dir_phys_json = serde_json::to_string(&EntryType::DirectoryPhysical).unwrap();
        assert_eq!(dir_phys_json, "\"dir:physical\"");

        let dir_dyn_json = serde_json::to_string(&EntryType::DirectoryDynamic).unwrap();
        assert_eq!(dir_dyn_json, "\"dir:dynamic\"");

        let symlink_json = serde_json::to_string(&EntryType::Symlink).unwrap();
        assert_eq!(symlink_json, "\"symlink\"");

        // Test deserialization
        let data_file_parsed: EntryType = serde_json::from_str("\"file:data:physical\"").unwrap();
        assert_eq!(data_file_parsed, EntryType::FileDataPhysical);

        let table_file_parsed: EntryType = serde_json::from_str("\"file:table:dynamic\"").unwrap();
        assert_eq!(table_file_parsed, EntryType::FileTableDynamic);

        let series_file_parsed: EntryType =
            serde_json::from_str("\"file:series:physical\"").unwrap();
        assert_eq!(series_file_parsed, EntryType::FileSeriesPhysical);

        let dir_parsed: EntryType = serde_json::from_str("\"dir:physical\"").unwrap();
        assert_eq!(dir_parsed, EntryType::DirectoryPhysical);

        let symlink_parsed: EntryType = serde_json::from_str("\"symlink\"").unwrap();
        assert_eq!(symlink_parsed, EntryType::Symlink);
    }

    #[tokio::test]
    async fn test_from_node_type() {
        // Create memory files with different entry types in their metadata
        let file_handle = crate::memory::MemoryFile::new_handle_with_entry_type(
            vec![],
            EntryType::FileSeriesPhysical,
        );
        let file_node = crate::NodeType::File(file_handle);
        assert_eq!(
            EntryType::from_node_type(&file_node).await.unwrap(),
            EntryType::FileSeriesPhysical
        );

        // Test with FileData
        let file_handle2 = crate::memory::MemoryFile::new_handle_with_entry_type(
            vec![],
            EntryType::FileDataDynamic,
        );
        let file_node2 = crate::NodeType::File(file_handle2);
        assert_eq!(
            EntryType::from_node_type(&file_node2).await.unwrap(),
            EntryType::FileDataDynamic
        );
    }

    #[test]
    fn test_base_format() {
        assert_eq!(EntryType::DirectoryPhysical.base_format(), "directory");
        assert_eq!(EntryType::DirectoryDynamic.base_format(), "directory");
        assert_eq!(EntryType::FileDataPhysical.base_format(), "file:data");
        assert_eq!(EntryType::FileDataDynamic.base_format(), "file:data");
        assert_eq!(EntryType::FileTablePhysical.base_format(), "file:table");
        assert_eq!(EntryType::FileTableDynamic.base_format(), "file:table");
        assert_eq!(EntryType::FileSeriesPhysical.base_format(), "file:series");
        assert_eq!(EntryType::FileSeriesDynamic.base_format(), "file:series");
        assert_eq!(EntryType::Symlink.base_format(), "symlink");
    }
}
