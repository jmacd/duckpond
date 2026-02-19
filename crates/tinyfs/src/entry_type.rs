// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// Node type identifiers for directory entries and persistence
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[repr(u8)]
pub enum EntryType {
    /// Physical directory - real TLogFS directory that creates its own partition
    #[serde(rename = "dir:physical")]
    DirectoryPhysical = 1,

    /// Dynamic directory - factory-based directory that uses parent's partition
    #[serde(rename = "dir:dynamic")]
    DirectoryDynamic = 2,

    /// Symbolic link entry (always physical, no dynamic symlinks)
    Symlink = 3,

    /// Physical data file - arbitrary byte content, accessed via Read/Write traits
    #[serde(rename = "file:physical:version")]
    FilePhysicalVersion = 4,

    /// Dynamic data file - factory-generated data file
    #[serde(rename = "file:dynamic")]
    FileDynamic = 5,

    /// Physical table file - single-version table stored as Parquet
    #[serde(rename = "table:physical:version")]
    TablePhysicalVersion = 6,

    /// Physical data series - multi-version byte data, versions concatenated on read (oldest first)
    #[serde(rename = "file:physical:series")]
    FilePhysicalSeries = 7,

    /// Physical series file - m    FilePhysicalSeries -> ChainedReader (version concatenation) -> csv:// URL -> CsvProvider -> RecordBatch stream -> MemTable -> DataFusion SQLulti-version table series, supports time-travel queries
    #[serde(rename = "table:physical:series")]
    TablePhysicalSeries = 8,

    /// Dynamic series file - factory-generated time series
    #[serde(rename = "table:dynamic")]
    TableDynamic = 9,
}

impl EntryType {
    /// Check if this entry is a file (any format, physical or dynamic)
    #[must_use]
    pub fn is_file(&self) -> bool {
        matches!(
            self,
            EntryType::FilePhysicalVersion
                | EntryType::FilePhysicalSeries
                | EntryType::FileDynamic
                | EntryType::TablePhysicalVersion
                | EntryType::TablePhysicalSeries
                | EntryType::TableDynamic
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
            EntryType::DirectoryDynamic | EntryType::FileDynamic | EntryType::TableDynamic
        )
    }

    /// Check if this entry is physical (real TLogFS node)
    #[must_use]
    pub fn is_physical(&self) -> bool {
        !self.is_dynamic() && *self != EntryType::Symlink
    }

    /// Check if this entry is a data file (raw bytes, physical or dynamic)
    #[must_use]
    pub fn is_data_file(&self) -> bool {
        matches!(
            self,
            EntryType::FilePhysicalVersion | EntryType::FilePhysicalSeries | EntryType::FileDynamic
        )
    }

    /// Check if this entry is a table file (physical only - dynamic files use series)
    #[must_use]
    pub fn is_table_file(&self) -> bool {
        matches!(self, EntryType::TablePhysicalVersion)
    }

    /// Check if this entry is a series file (physical or dynamic)
    #[must_use]
    pub fn is_series_file(&self) -> bool {
        matches!(
            self,
            EntryType::TablePhysicalSeries | EntryType::TableDynamic
        )
    }

    /// Check if this entry requires Parquet storage (table or series, physical or dynamic)
    #[must_use]
    pub fn is_parquet_file(&self) -> bool {
        matches!(
            self,
            EntryType::TablePhysicalVersion
                | EntryType::TablePhysicalSeries
                | EntryType::TableDynamic
        )
    }

    /// Get the base file format (ignoring physical/dynamic distinction)
    #[must_use]
    pub fn base_format(&self) -> &'static str {
        match self {
            EntryType::DirectoryPhysical | EntryType::DirectoryDynamic => "directory",
            EntryType::Symlink => "symlink",
            EntryType::FilePhysicalVersion
            | EntryType::FilePhysicalSeries
            | EntryType::FileDynamic => "file:data",
            EntryType::TablePhysicalVersion => "file:table",
            EntryType::TablePhysicalSeries | EntryType::TableDynamic => "file:series",
        }
    }

    /// Convert EntryType to string for serialization/storage
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            EntryType::DirectoryPhysical => "dir:physical",
            EntryType::DirectoryDynamic => "dir:dynamic",
            EntryType::Symlink => "symlink",
            EntryType::FilePhysicalVersion => "file:physical:version",
            EntryType::FilePhysicalSeries => "file:physical:series",
            EntryType::FileDynamic => "file:dynamic",
            EntryType::TablePhysicalVersion => "table:physical:version",
            EntryType::TablePhysicalSeries => "table:physical:series",
            EntryType::TableDynamic => "table:dynamic",
        }
    }
}

impl TryFrom<u8> for EntryType {
    type Error = String;

    fn try_from(v: u8) -> Result<Self, String> {
        match v {
            1 => Ok(EntryType::DirectoryPhysical),
            2 => Ok(EntryType::DirectoryDynamic),
            3 => Ok(EntryType::Symlink),
            4 => Ok(EntryType::FilePhysicalVersion),
            5 => Ok(EntryType::FileDynamic),
            6 => Ok(EntryType::TablePhysicalVersion),
            7 => Ok(EntryType::FilePhysicalSeries),
            8 => Ok(EntryType::TablePhysicalSeries),
            9 => Ok(EntryType::TableDynamic),
            _ => Err(format!("Unknown EntryType: {}", v)),
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
            "file:physical:version" => Ok(EntryType::FilePhysicalVersion),
            "file:physical:series" => Ok(EntryType::FilePhysicalSeries),
            "file:dynamic" => Ok(EntryType::FileDynamic),
            "table:physical:version" => Ok(EntryType::TablePhysicalVersion),
            // "file:table:dynamic" no longer exists - use table:dynamic instead
            "table:physical:series" => Ok(EntryType::TablePhysicalSeries),
            "table:dynamic" => Ok(EntryType::TableDynamic),

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
        assert_eq!(
            EntryType::FilePhysicalVersion.as_str(),
            "file:physical:version"
        );
        assert_eq!(
            EntryType::FilePhysicalSeries.as_str(),
            "file:physical:series"
        );
        assert_eq!(EntryType::FileDynamic.as_str(), "file:dynamic");
        assert_eq!(
            EntryType::TablePhysicalVersion.as_str(),
            "table:physical:version"
        );
        assert_eq!(
            EntryType::TablePhysicalSeries.as_str(),
            "table:physical:series"
        );
        assert_eq!(EntryType::TableDynamic.as_str(), "table:dynamic");
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
            EntryType::from_str("file:physical:version").unwrap(),
            EntryType::FilePhysicalVersion
        );
        assert_eq!(
            EntryType::from_str("file:physical:series").unwrap(),
            EntryType::FilePhysicalSeries
        );
        assert_eq!(
            EntryType::from_str("file:dynamic").unwrap(),
            EntryType::FileDynamic
        );
        assert_eq!(
            EntryType::from_str("table:physical:version").unwrap(),
            EntryType::TablePhysicalVersion
        );
        // file:table:dynamic no longer exists - should fail
        assert!(EntryType::from_str("file:table:dynamic").is_err());
        assert_eq!(
            EntryType::from_str("table:physical:series").unwrap(),
            EntryType::TablePhysicalSeries
        );
        assert_eq!(
            EntryType::from_str("table:dynamic").unwrap(),
            EntryType::TableDynamic
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
        assert!(EntryType::FilePhysicalVersion.is_file());
        assert!(EntryType::FilePhysicalSeries.is_file());
        assert!(EntryType::FileDynamic.is_file());
        assert!(EntryType::TablePhysicalVersion.is_file());
        assert!(EntryType::TablePhysicalSeries.is_file());
        assert!(EntryType::TableDynamic.is_file());
        assert!(!EntryType::DirectoryPhysical.is_file());
        assert!(!EntryType::DirectoryDynamic.is_file());
        assert!(!EntryType::Symlink.is_file());

        // Test is_directory()
        assert!(EntryType::DirectoryPhysical.is_directory());
        assert!(EntryType::DirectoryDynamic.is_directory());
        assert!(!EntryType::FilePhysicalVersion.is_directory());
        assert!(!EntryType::Symlink.is_directory());

        // Test is_dynamic()
        assert!(EntryType::DirectoryDynamic.is_dynamic());
        assert!(EntryType::FileDynamic.is_dynamic());
        assert!(EntryType::TableDynamic.is_dynamic());
        assert!(!EntryType::DirectoryPhysical.is_dynamic());
        assert!(!EntryType::FilePhysicalVersion.is_dynamic());
        assert!(!EntryType::Symlink.is_dynamic());

        // Test is_physical()
        assert!(EntryType::DirectoryPhysical.is_physical());
        assert!(EntryType::FilePhysicalVersion.is_physical());
        assert!(EntryType::FilePhysicalSeries.is_physical());
        assert!(EntryType::TablePhysicalVersion.is_physical());
        assert!(EntryType::TablePhysicalSeries.is_physical());
        assert!(!EntryType::DirectoryDynamic.is_physical());
        assert!(!EntryType::FileDynamic.is_physical());
        assert!(!EntryType::Symlink.is_physical()); // Symlinks are neither (special case)

        // Test is_data_file()
        assert!(EntryType::FilePhysicalVersion.is_data_file());
        assert!(EntryType::FilePhysicalSeries.is_data_file());
        assert!(EntryType::FileDynamic.is_data_file());
        assert!(!EntryType::TablePhysicalVersion.is_data_file());
        assert!(!EntryType::TableDynamic.is_data_file());

        // Test is_table_file() - only physical tables exist
        assert!(EntryType::TablePhysicalVersion.is_table_file());
        assert!(!EntryType::TableDynamic.is_table_file());
        assert!(!EntryType::FilePhysicalVersion.is_table_file());

        // Test is_series_file()
        assert!(EntryType::TablePhysicalSeries.is_series_file());
        assert!(EntryType::TableDynamic.is_series_file());
        assert!(!EntryType::FilePhysicalVersion.is_series_file());
        assert!(!EntryType::FileDynamic.is_series_file());

        // Test is_parquet_file()
        assert!(EntryType::TablePhysicalVersion.is_parquet_file());
        assert!(EntryType::TablePhysicalSeries.is_parquet_file());
        assert!(EntryType::TableDynamic.is_parquet_file());
        assert!(!EntryType::FilePhysicalVersion.is_parquet_file());
        assert!(!EntryType::FilePhysicalSeries.is_parquet_file());
        assert!(!EntryType::FileDynamic.is_parquet_file());
    }

    #[test]
    fn test_display_trait() {
        assert_eq!(
            format!("{}", EntryType::FilePhysicalVersion),
            "file:physical:version"
        );
        assert_eq!(
            format!("{}", EntryType::FilePhysicalSeries),
            "file:physical:series"
        );
        assert_eq!(format!("{}", EntryType::FileDynamic), "file:dynamic");
        assert_eq!(
            format!("{}", EntryType::TablePhysicalVersion),
            "table:physical:version"
        );
        assert_eq!(
            format!("{}", EntryType::TablePhysicalSeries),
            "table:physical:series"
        );
        assert_eq!(format!("{}", EntryType::TableDynamic), "table:dynamic");
        assert_eq!(format!("{}", EntryType::DirectoryPhysical), "dir:physical");
        assert_eq!(format!("{}", EntryType::DirectoryDynamic), "dir:dynamic");
        assert_eq!(format!("{}", EntryType::Symlink), "symlink");
    }

    #[test]
    fn test_serde_serialization() {
        // Test that serde serialization works as expected
        let data_file_json = serde_json::to_string(&EntryType::FilePhysicalVersion).unwrap();
        assert_eq!(data_file_json, "\"file:physical:version\"");

        let table_file_json = serde_json::to_string(&EntryType::TablePhysicalVersion).unwrap();
        assert_eq!(table_file_json, "\"table:physical:version\"");

        let series_file_json = serde_json::to_string(&EntryType::TablePhysicalSeries).unwrap();
        assert_eq!(series_file_json, "\"table:physical:series\"");

        let dir_phys_json = serde_json::to_string(&EntryType::DirectoryPhysical).unwrap();
        assert_eq!(dir_phys_json, "\"dir:physical\"");

        let dir_dyn_json = serde_json::to_string(&EntryType::DirectoryDynamic).unwrap();
        assert_eq!(dir_dyn_json, "\"dir:dynamic\"");

        let symlink_json = serde_json::to_string(&EntryType::Symlink).unwrap();
        assert_eq!(symlink_json, "\"symlink\"");

        // Test deserialization
        let data_file_parsed: EntryType =
            serde_json::from_str("\"file:physical:version\"").unwrap();
        assert_eq!(data_file_parsed, EntryType::FilePhysicalVersion);

        let table_file_parsed: EntryType =
            serde_json::from_str("\"table:physical:version\"").unwrap();
        assert_eq!(table_file_parsed, EntryType::TablePhysicalVersion);

        let series_file_parsed: EntryType =
            serde_json::from_str("\"table:physical:series\"").unwrap();
        assert_eq!(series_file_parsed, EntryType::TablePhysicalSeries);

        let dir_parsed: EntryType = serde_json::from_str("\"dir:physical\"").unwrap();
        assert_eq!(dir_parsed, EntryType::DirectoryPhysical);

        let symlink_parsed: EntryType = serde_json::from_str("\"symlink\"").unwrap();
        assert_eq!(symlink_parsed, EntryType::Symlink);
    }

    #[test]
    fn test_base_format() {
        assert_eq!(EntryType::DirectoryPhysical.base_format(), "directory");
        assert_eq!(EntryType::DirectoryDynamic.base_format(), "directory");
        assert_eq!(EntryType::FilePhysicalVersion.base_format(), "file:data");
        assert_eq!(EntryType::FilePhysicalSeries.base_format(), "file:data");
        assert_eq!(EntryType::FileDynamic.base_format(), "file:data");
        assert_eq!(EntryType::TablePhysicalVersion.base_format(), "file:table");
        assert_eq!(EntryType::TablePhysicalSeries.base_format(), "file:series");
        assert_eq!(EntryType::TableDynamic.base_format(), "file:series");
        assert_eq!(EntryType::Symlink.base_format(), "symlink");
    }
}
