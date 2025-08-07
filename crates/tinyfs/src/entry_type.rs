/// Node type identifiers for directory entries and persistence
/// 
/// This enum provides type-safe alternatives to string literals
/// for identifying node types in directory entries and persistence layers.
/// Files are distinguished by their format for different access patterns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryType {
    /// Directory entry  
    Directory,
    /// Symbolic link entry
    Symlink,
    /// Data file - arbitrary byte content, accessed via Read/Write traits
    #[serde(rename = "file:data")]
    FileData,
    /// Table file - single-version table stored as Parquet, accessed via DataFusion
    #[serde(rename = "file:table")]
    FileTable,
    /// Series file - multi-version table series stored as Parquet, supports time-travel queries
    #[serde(rename = "file:series")]
    FileSeries,
}

impl EntryType {
    /// Check if this entry is a file (any format)
    pub fn is_file(&self) -> bool {
        matches!(self, EntryType::FileData | EntryType::FileTable | EntryType::FileSeries)
    }
    
    /// Check if this entry is a data file
    pub fn is_data_file(&self) -> bool {
        matches!(self, EntryType::FileData)
    }
    
    /// Check if this entry is a table file
    pub fn is_table_file(&self) -> bool {
        matches!(self, EntryType::FileTable)
    }
    
    /// Check if this entry is a series file
    pub fn is_series_file(&self) -> bool {
        matches!(self, EntryType::FileSeries)
    }
    
    /// Check if this entry requires Parquet storage (table or series)
    pub fn is_parquet_file(&self) -> bool {
        matches!(self, EntryType::FileTable | EntryType::FileSeries)
    }
    
    /// Convert EntryType to string for serialization/storage
    pub fn as_str(&self) -> &'static str {
        match self {
            EntryType::Directory => "directory", 
            EntryType::Symlink => "symlink",
            EntryType::FileData => "file:data",
            EntryType::FileTable => "file:table",
            EntryType::FileSeries => "file:series",
        }
    }
    
    /// Parse EntryType from string for deserialization
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "directory" => Ok(EntryType::Directory),
            "symlink" => Ok(EntryType::Symlink),
            "file:data" => Ok(EntryType::FileData),
            "file:table" => Ok(EntryType::FileTable),
            "file:series" => Ok(EntryType::FileSeries),
            other => Err(format!("Unknown entry type: {}", other)),
        }
    }
    
    /// Convert from NodeType to EntryType for directory entries
    /// 
    /// Extract EntryType from NodeType
    pub fn from_node_type(node_type: &crate::NodeType) -> Self {
        match node_type {
            crate::NodeType::File(_, entry_type) => entry_type.clone(),
            crate::NodeType::Directory(_) => EntryType::Directory,
            crate::NodeType::Symlink(_) => EntryType::Symlink,
        }
    }
}

impl std::fmt::Display for EntryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for EntryType {
    type Err = String;
    
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_type_string_conversion() {
        assert_eq!(EntryType::Directory.as_str(), "directory");
        assert_eq!(EntryType::Symlink.as_str(), "symlink");
        assert_eq!(EntryType::FileData.as_str(), "file:data");
        assert_eq!(EntryType::FileTable.as_str(), "file:table");
        assert_eq!(EntryType::FileSeries.as_str(), "file:series");
    }

    #[test]
    fn test_entry_type_parsing() {
        assert_eq!(EntryType::from_str("file:data").unwrap(), EntryType::FileData);
        assert_eq!(EntryType::from_str("file:table").unwrap(), EntryType::FileTable);
        assert_eq!(EntryType::from_str("file:series").unwrap(), EntryType::FileSeries);
        assert_eq!(EntryType::from_str("directory").unwrap(), EntryType::Directory);
        assert_eq!(EntryType::from_str("symlink").unwrap(), EntryType::Symlink);
        assert!(EntryType::from_str("invalid").is_err());
    }

    #[test]
    fn test_file_type_queries() {
        assert!(EntryType::FileData.is_file());
        assert!(EntryType::FileTable.is_file());
        assert!(EntryType::FileSeries.is_file());
        assert!(!EntryType::Directory.is_file());
        assert!(!EntryType::Symlink.is_file());
        
        assert!(EntryType::FileData.is_data_file());
        assert!(!EntryType::FileTable.is_data_file());
        
        assert!(EntryType::FileTable.is_table_file());
        assert!(!EntryType::FileData.is_table_file());
        
        assert!(EntryType::FileSeries.is_series_file());
        assert!(!EntryType::FileData.is_series_file());
        
        assert!(EntryType::FileTable.is_parquet_file());
        assert!(EntryType::FileSeries.is_parquet_file());
        assert!(!EntryType::FileData.is_parquet_file());
    }

    #[test]
    fn test_display_trait() {
        assert_eq!(format!("{}", EntryType::FileData), "file:data");
        assert_eq!(format!("{}", EntryType::FileTable), "file:table");
        assert_eq!(format!("{}", EntryType::FileSeries), "file:series");
        assert_eq!(format!("{}", EntryType::Directory), "directory");
        assert_eq!(format!("{}", EntryType::Symlink), "symlink");
    }

    #[test]
    fn test_serde_serialization() {
        // Test that serde serialization works as expected
        let data_file_json = serde_json::to_string(&EntryType::FileData).unwrap();
        assert_eq!(data_file_json, "\"file:data\"");
        
        let table_file_json = serde_json::to_string(&EntryType::FileTable).unwrap();
        assert_eq!(table_file_json, "\"file:table\"");
        
        let series_file_json = serde_json::to_string(&EntryType::FileSeries).unwrap();
        assert_eq!(series_file_json, "\"file:series\"");
        
        let dir_json = serde_json::to_string(&EntryType::Directory).unwrap();
        assert_eq!(dir_json, "\"directory\"");
        
        let symlink_json = serde_json::to_string(&EntryType::Symlink).unwrap();
        assert_eq!(symlink_json, "\"symlink\"");
        
        // Test deserialization
        let data_file_parsed: EntryType = serde_json::from_str("\"file:data\"").unwrap();
        assert_eq!(data_file_parsed, EntryType::FileData);
        
        let table_file_parsed: EntryType = serde_json::from_str("\"file:table\"").unwrap();
        assert_eq!(table_file_parsed, EntryType::FileTable);
        
        let series_file_parsed: EntryType = serde_json::from_str("\"file:series\"").unwrap();
        assert_eq!(series_file_parsed, EntryType::FileSeries);
        
        let dir_parsed: EntryType = serde_json::from_str("\"directory\"").unwrap();
        assert_eq!(dir_parsed, EntryType::Directory);
        
        let symlink_parsed: EntryType = serde_json::from_str("\"symlink\"").unwrap();
        assert_eq!(symlink_parsed, EntryType::Symlink);
    }

    #[test]
    fn test_from_node_type() {
        // Test that EntryType is correctly extracted from NodeType
        let file_handle = crate::memory::MemoryFile::new_handle(vec![]);
        let file_node = crate::NodeType::File(file_handle, EntryType::FileSeries);
        assert_eq!(EntryType::from_node_type(&file_node), EntryType::FileSeries);
        
        // Test with FileData
        let file_handle2 = crate::memory::MemoryFile::new_handle(vec![]);
        let file_node2 = crate::NodeType::File(file_handle2, EntryType::FileData);
        assert_eq!(EntryType::from_node_type(&file_node2), EntryType::FileData);
    }
}
