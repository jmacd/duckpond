/// Node type identifiers for directory entries and persistence
/// 
/// This enum provides type-safe alternatives to string literals
/// for identifying node types in directory entries and persistence layers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryType {
    /// Regular file entry
    File,
    /// Directory entry  
    Directory,
    /// Symbolic link entry
    Symlink,
}

impl EntryType {
    /// Convert EntryType to string for serialization/storage
    pub fn as_str(&self) -> &'static str {
        match self {
            EntryType::File => "file",
            EntryType::Directory => "directory", 
            EntryType::Symlink => "symlink",
        }
    }
    
    /// Parse EntryType from string for deserialization
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "file" => Ok(EntryType::File),
            "directory" => Ok(EntryType::Directory),
            "symlink" => Ok(EntryType::Symlink),
            other => Err(format!("Unknown entry type: {}", other)),
        }
    }
    
    /// Convert from NodeType to EntryType for directory entries
    pub fn from_node_type(node_type: &crate::NodeType) -> Self {
        match node_type {
            crate::NodeType::File(_) => EntryType::File,
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
        assert_eq!(EntryType::File.as_str(), "file");
        assert_eq!(EntryType::Directory.as_str(), "directory");
        assert_eq!(EntryType::Symlink.as_str(), "symlink");
    }

    #[test]
    fn test_entry_type_parsing() {
        assert_eq!(EntryType::from_str("file").unwrap(), EntryType::File);
        assert_eq!(EntryType::from_str("directory").unwrap(), EntryType::Directory);
        assert_eq!(EntryType::from_str("symlink").unwrap(), EntryType::Symlink);
        assert!(EntryType::from_str("invalid").is_err());
    }

    #[test]
    fn test_display_trait() {
        assert_eq!(format!("{}", EntryType::File), "file");
        assert_eq!(format!("{}", EntryType::Directory), "directory");
        assert_eq!(format!("{}", EntryType::Symlink), "symlink");
    }

    #[test]
    fn test_serde_serialization() {
        // Test that serde serialization works as expected
        let file_json = serde_json::to_string(&EntryType::File).unwrap();
        assert_eq!(file_json, "\"file\"");
        
        let dir_json = serde_json::to_string(&EntryType::Directory).unwrap();
        assert_eq!(dir_json, "\"directory\"");
        
        let symlink_json = serde_json::to_string(&EntryType::Symlink).unwrap();
        assert_eq!(symlink_json, "\"symlink\"");
        
        // Test deserialization
        let file_parsed: EntryType = serde_json::from_str("\"file\"").unwrap();
        assert_eq!(file_parsed, EntryType::File);
        
        let dir_parsed: EntryType = serde_json::from_str("\"directory\"").unwrap();
        assert_eq!(dir_parsed, EntryType::Directory);
        
        let symlink_parsed: EntryType = serde_json::from_str("\"symlink\"").unwrap();
        assert_eq!(symlink_parsed, EntryType::Symlink);
    }
}
