//! Version selection for ListingTable queries
//!
//! Determines which file versions to include when querying data.

use log::debug;
use tinyfs::FileID;

/// Version selection for ListingTable
#[derive(Clone, Debug, Hash, PartialEq, Eq, Default)]
pub enum VersionSelection {
    /// All versions (replaces SeriesTable)
    #[default]
    AllVersions,
    /// Latest version only (replaces TinyFsTableProvider)
    LatestVersion,
    /// Specific version (replaces NodeVersionTable)
    SpecificVersion(u64),
}

impl VersionSelection {
    /// Centralized debug logging for version selection
    /// Eliminates duplicate debug logging patterns throughout the codebase
    pub fn log_debug(&self, node_id: &tinyfs::NodeID) {
        match self {
            VersionSelection::AllVersions => {
                debug!("Version selection: ALL versions for node {node_id}");
            }
            VersionSelection::LatestVersion => {
                debug!("Version selection: LATEST version for node {node_id}");
            }
            VersionSelection::SpecificVersion(version) => {
                debug!("Version selection: SPECIFIC version {version} for node {node_id}");
            }
        }
    }

    /// Generate URL pattern for this version selection
    /// Eliminates duplicate URL pattern generation throughout the codebase
    #[must_use]
    pub fn to_url_pattern(&self, file_id: &FileID) -> String {
        match self {
            VersionSelection::AllVersions | VersionSelection::LatestVersion => {
                crate::TinyFsPathBuilder::url_all_versions(file_id)
            }
            VersionSelection::SpecificVersion(version) => {
                crate::TinyFsPathBuilder::url_specific_version(file_id, *version)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_selection_url_patterns() {
        let file_id = FileID::root();

        let all = VersionSelection::AllVersions;
        let url = all.to_url_pattern(&file_id);
        assert!(url.starts_with("tinyfs:///part/"));
        assert!(url.contains("/version/"));

        let latest = VersionSelection::LatestVersion;
        let url = latest.to_url_pattern(&file_id);
        assert!(url.starts_with("tinyfs:///part/"));

        let specific = VersionSelection::SpecificVersion(42);
        let url = specific.to_url_pattern(&file_id);
        assert!(url.starts_with("tinyfs:///part/"));
        assert!(url.ends_with("/version/42.parquet"));
    }
}
