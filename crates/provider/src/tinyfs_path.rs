//! TinyFS path handling utilities for ObjectStore integration
//!
//! Provides centralized path building for TinyFS files in ObjectStore contexts,
//! following the partition → node → version hierarchy.

use tinyfs::FileID;

/// Centralized TinyFS path handling following partition → node → version hierarchy
pub struct TinyFsPathBuilder;

impl TinyFsPathBuilder {
    /// Create path for all versions: "part/{part_id}/node/{node_id}/version/"
    #[must_use]
    pub fn all_versions(file_id: &FileID) -> String {
        format!("part/{}/node/{}/version/", file_id.part_id(), file_id.node_id())
    }

    /// Create path for specific version: "part/{part_id}/node/{node_id}/version/{version}.parquet"
    #[must_use]
    pub fn specific_version(file_id: &FileID, version: u64) -> String {
        format!(
            "part/{}/node/{}/version/{}.parquet",
            file_id.part_id(),
            file_id.node_id(),
            version
        )
    }

    /// Create tinyfs:// URL for all versions
    #[must_use]
    pub fn url_all_versions(file_id: &FileID) -> String {
        format!("tinyfs:///{}", Self::all_versions(file_id))
    }

    /// Create tinyfs:// URL for specific version  
    #[must_use]
    pub fn url_specific_version(file_id: &FileID, version: u64) -> String {
        format!("tinyfs:///{}", Self::specific_version(file_id, version))
    }

    /// Create path for directory entries: "directory/{node_id}"
    #[must_use]
    pub fn directory(file_id: &FileID) -> String {
        format!("directory/{}", file_id.node_id())
    }

    /// Create tinyfs:// URL for directory entries
    #[must_use]
    pub fn url_directory(file_id: &FileID) -> String {
        format!("tinyfs:///{}", Self::directory(file_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_building() {
        // Use root FileID for testing
        let file_id = FileID::root();

        // Test all_versions path
        let path = TinyFsPathBuilder::all_versions(&file_id);
        assert!(path.starts_with("part/"));
        assert!(path.contains("/node/"));
        assert!(path.ends_with("/version/"));

        // Test specific_version path
        let path = TinyFsPathBuilder::specific_version(&file_id, 42);
        assert!(path.starts_with("part/"));
        assert!(path.contains("/node/"));
        assert!(path.ends_with("/version/42.parquet"));

        // Test URL formats
        let url = TinyFsPathBuilder::url_all_versions(&file_id);
        assert!(url.starts_with("tinyfs:///part/"));

        let url = TinyFsPathBuilder::url_specific_version(&file_id, 42);
        assert!(url.starts_with("tinyfs:///part/"));
        assert!(url.ends_with("/version/42.parquet"));

        // Test directory path
        let path = TinyFsPathBuilder::directory(&file_id);
        assert_eq!(path, format!("directory/{}", file_id.node_id()));

        let url = TinyFsPathBuilder::url_directory(&file_id);
        assert_eq!(url, format!("tinyfs:///directory/{}", file_id.node_id()));
    }
}
