use std::path::PathBuf;
use std::env;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono;

/// Get the pond path from POND environment variable or an override
pub fn get_pond_path() -> Result<PathBuf> {
    get_pond_path_with_override(None)
}

/// Get the pond path with an optional override, falling back to POND environment variable
pub fn get_pond_path_with_override(override_path: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = override_path {
        return Ok(path.join("store"));
    }
    
    let pond_base = env::var("POND")
        .map_err(|_| anyhow!("POND environment variable not set"))
        .map(PathBuf::from)?;
    Ok(pond_base.join("store"))
}

/// Core function to format a u64 ID value with friendly hex formatting
/// Shows exactly 4, 8, 12, or 16 hex digits based on the magnitude of the ID
/// 0000-FFFF -> 4 digits, 00010000-FFFFFFFF -> 8 digits, etc.
pub fn format_id_value(id_value: u64) -> String {
    if id_value <= 0xFFFF {
        // 0-65535: show as exactly 4 hex digits
        format!("{:04X}", id_value)
    } else if id_value <= 0xFFFFFFFF {
        // 65536-4294967295: show as exactly 8 hex digits
        format!("{:08X}", id_value)
    } else if id_value <= 0xFFFFFFFFFFFF {
        // Show as exactly 12 hex digits
        format!("{:012X}", id_value)
    } else {
        // Show as exactly 16 hex digits
        format!("{:016X}", id_value)
    }
}

/// Helper function to format node IDs in a friendly way
/// Shows exactly 4, 8, 12, or 16 hex digits based on the magnitude of the ID
/// 0000-FFFF -> 4 digits, 00010000-FFFFFFFF -> 8 digits, etc.
pub fn format_node_id(node_id: &str) -> String {
    // Parse the node_id as a u64 to determine its magnitude
    let id_value = u64::from_str_radix(node_id, 16).unwrap_or(0);
    format_id_value(id_value)
}

/// Helper function to format file sizes
pub fn format_file_size(size: usize) -> String {
    if size >= 1024 * 1024 {
        format!("{:.1}MB", size as f64 / (1024.0 * 1024.0))
    } else if size >= 1024 {
        format!("{:.1}KB", size as f64 / 1024.0)
    } else {
        format!("{}B", size)
    }
}

/// Helper function to truncate strings
pub fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}

// /// Helper function to parse OplogEntry from IPC bytes
// pub fn parse_oplog_entry_content(content: &[u8]) -> Result<tinylogfs::OplogEntry> {
//     use arrow::ipc::reader::StreamReader;
    
//     let cursor = std::io::Cursor::new(content);
//     let reader = StreamReader::try_new(cursor, None)?;
    
//     let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>()?;
//     if let Some(batch) = batches.first() {
//         let entries: Vec<tinylogfs::OplogEntry> = serde_arrow::from_record_batch(batch)?;
//         entries.into_iter().next()
//             .ok_or_else(|| anyhow!("No OplogEntry found in batch"))
//     } else {
//         Err(anyhow!("No batches found in OplogEntry IPC stream"))
//     }
// }

/// Helper function to parse directory content
pub fn parse_directory_content(content: &[u8]) -> Result<Vec<tinylogfs::VersionedDirectoryEntry>> {
    if content.is_empty() {
        return Ok(Vec::new());
    }
    
    use arrow::ipc::reader::StreamReader;
    
    let cursor = std::io::Cursor::new(content);
    let reader = StreamReader::try_new(cursor, None)?;
    
    let mut all_entries = Vec::new();
    for batch_result in reader {
        let batch = batch_result?;
        let entries: Vec<tinylogfs::VersionedDirectoryEntry> = serde_arrow::from_record_batch(&batch)?;
        all_entries.extend(entries);
    }
    
    Ok(all_entries)
}

/// Helper struct to store file information for DuckPond-specific output
#[derive(Debug)]
pub struct FileInfo {
    pub path: String,
    pub node_type: String,
    pub size: usize,
    pub timestamp: Option<i64>,
    pub symlink_target: Option<String>,
    pub node_id: Option<String>,
    pub version: Option<i64>,
}

impl FileInfo {
    /// Format in DuckPond-specific style showing meaningful metadata
    pub fn format_duckpond_style(&self) -> String {
        let type_symbol = match self.node_type.as_str() {
            "directory" => "📁",
            "symlink" => "🔗",
            _ => "📄",
        };

        let size_str = if self.node_type == "directory" {
            "-".to_string()
        } else {
            format_file_size(self.size)
        };

        let time_str = if let Some(timestamp_us) = self.timestamp {
            let dt = chrono::DateTime::from_timestamp(
                timestamp_us / 1_000_000, 
                ((timestamp_us % 1_000_000) * 1000) as u32
            ).unwrap_or_else(|| chrono::Utc::now());
            dt.format("%Y-%m-%d %H:%M:%S").to_string()
        } else {
            "unknown".to_string()
        };

        let node_id_str = if let Some(node_id) = &self.node_id {
            format_node_id(node_id)
        } else {
            "unknown".to_string()
        };

        let version_str = if let Some(version) = self.version {
            format!("v{}", version)
        } else {
            "v?".to_string()
        };

        let symlink_part = if let Some(target) = &self.symlink_target {
            format!(" -> {}", target)
        } else {
            String::new()
        };

        format!("{} {:>8} {:>8} {} {} {}{}\n",
                type_symbol, size_str, node_id_str, version_str, time_str, self.path, symlink_part)
    }
}

/// Visitor implementation to collect file information
pub struct FileInfoVisitor {
    show_all: bool,
}

impl FileInfoVisitor {
    pub fn new(show_all: bool) -> Self {
        Self {
            show_all,
        }
    }
}

#[async_trait]
impl tinyfs::Visitor<FileInfo> for FileInfoVisitor {
    async fn visit(&mut self, node: tinyfs::NodePath, _captured: &[String]) -> tinyfs::Result<FileInfo> {
        let node_ref = node.borrow().await;
        let path = node.path().to_string_lossy().to_string();
        
        // Skip hidden files unless --all is specified
        let basename = node.basename();
        if !self.show_all && basename.starts_with('.') && basename != "." && basename != ".." {
            return Err(tinyfs::Error::Other("Hidden file skipped".to_string()));
        }

        // Extract metadata that we can access from the node
        let node_id = node.id().await.to_hex_string();

        match node_ref.node_type() {
            tinyfs::NodeType::File(file_handle) => {
                let content = file_handle.content().await.unwrap_or_default();
                Ok(FileInfo {
                    path,
                    node_type: "file".to_string(),
                    size: content.len(),
                    timestamp: None, // TODO: Extract from oplog metadata via persistence layer
                    symlink_target: None,
                    node_id: Some(node_id),
                    version: None, // TODO: Extract from oplog metadata via persistence layer
                })
            }
            tinyfs::NodeType::Directory(_) => {
                Ok(FileInfo {
                    path,
                    node_type: "directory".to_string(),
                    size: 0,
                    timestamp: None, // TODO: Extract from oplog metadata via persistence layer
                    symlink_target: None,
                    node_id: Some(node_id),
                    version: None, // TODO: Extract from oplog metadata via persistence layer
                })
            }
            tinyfs::NodeType::Symlink(symlink_handle) => {
                let target = symlink_handle.readlink().await.unwrap_or_default();
                Ok(FileInfo {
                    path,
                    node_type: "symlink".to_string(),
                    size: 0,
                    timestamp: None, // TODO: Extract from oplog metadata via persistence layer
                    symlink_target: Some(target.to_string_lossy().to_string()),
                    node_id: Some(node_id),
                    version: None, // TODO: Extract from oplog metadata via persistence layer
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_id_value() {
        // Test range 0000-FFFF (4 hex digits max)
        assert_eq!(format_id_value(0), "0000");
        assert_eq!(format_id_value(1), "0001");
        assert_eq!(format_id_value(10), "000A");
        assert_eq!(format_id_value(0xFFFF), "FFFF");
        
        // Test range 00010000-FFFFFFFF (8 hex digits)
        assert_eq!(format_id_value(0x10000), "00010000");
        assert_eq!(format_id_value(0xFFFFFFFF), "FFFFFFFF");
        assert_eq!(format_id_value(0x12345678), "12345678");
        
        // Test range 000100000000-FFFFFFFFFFFF (12 hex digits)
        assert_eq!(format_id_value(0x100000000000), "100000000000");
        assert_eq!(format_id_value(0xFFFFFFFFFFFF), "FFFFFFFFFFFF");
        assert_eq!(format_id_value(0x123456789ABC), "123456789ABC");
        
        // Test range 0001000000000000-FFFFFFFFFFFFFFFF (16 hex digits)
        assert_eq!(format_id_value(0x1000000000000000), "1000000000000000");
        assert_eq!(format_id_value(0xFFFFFFFFFFFFFFFF), "FFFFFFFFFFFFFFFF");
        assert_eq!(format_id_value(0x123456789ABCDEF0), "123456789ABCDEF0");
    }

    #[test]
    fn test_format_node_id() {
        // Test range 0000-FFFF (4 hex digits max)
        assert_eq!(format_node_id("0000000000000000"), "0000");
        assert_eq!(format_node_id("0000000000000001"), "0001");
        assert_eq!(format_node_id("000000000000000A"), "000A");
        assert_eq!(format_node_id("000000000000FFFF"), "FFFF");
        
        // Test range 00010000-FFFFFFFF (8 hex digits)
        assert_eq!(format_node_id("0000000000010000"), "00010000");
        assert_eq!(format_node_id("00000000FFFFFFFF"), "FFFFFFFF");
        assert_eq!(format_node_id("0000000012345678"), "12345678");
        
        // Test range 000100000000-FFFFFFFFFFFF (12 hex digits)
        assert_eq!(format_node_id("0000100000000000"), "100000000000");
        assert_eq!(format_node_id("0000FFFFFFFFFFFF"), "FFFFFFFFFFFF");
        assert_eq!(format_node_id("0000123456789ABC"), "123456789ABC");
        
        // Test range 0001000000000000-FFFFFFFFFFFFFFFF (16 hex digits)
        assert_eq!(format_node_id("1000000000000000"), "1000000000000000");
        assert_eq!(format_node_id("FFFFFFFFFFFFFFFF"), "FFFFFFFFFFFFFFFF");
        assert_eq!(format_node_id("123456789ABCDEF0"), "123456789ABCDEF0");
    }

    #[tokio::test]
    async fn test_file_info_visitor_integration() {
        use tinyfs::memory::new_fs;
        
        // Create a test filesystem with some files
        let fs = new_fs().await;
        let root = fs.root().await.unwrap();
        
        // Create test files
        root.create_file_path("/file1.txt", b"content1").await.unwrap();
        root.create_file_path("/file2.txt", b"content2").await.unwrap();
        root.create_dir_path("/subdir").await.unwrap();
        root.create_file_path("/subdir/file3.txt", b"content3").await.unwrap();
        
        // Test the FileInfoVisitor with the /** pattern
        let mut visitor = FileInfoVisitor::new(false);
        let results = root.visit_with_visitor("/**", &mut visitor).await.unwrap();
        
        // Verify we got results from the return value (not visitor.results)
        assert_eq!(results.len(), 4); // file1.txt, file2.txt, subdir, file3.txt
        
        // Verify the results contain the expected files
        let paths: Vec<String> = results.iter().map(|info| info.path.clone()).collect();
        assert!(paths.contains(&"/file1.txt".to_string()));
        assert!(paths.contains(&"/file2.txt".to_string()));
        assert!(paths.contains(&"/subdir".to_string()));
        assert!(paths.contains(&"/subdir/file3.txt".to_string()));
        
        // Test with a more specific pattern
        let mut visitor2 = FileInfoVisitor::new(false);
        let results2 = root.visit_with_visitor("/**/*.txt", &mut visitor2).await.unwrap();
        
        // Should find all .txt files
        assert_eq!(results2.len(), 3); // file1.txt, file2.txt, file3.txt
        let txt_paths: Vec<String> = results2.iter().map(|info| info.path.clone()).collect();
        assert!(txt_paths.contains(&"/file1.txt".to_string()));
        assert!(txt_paths.contains(&"/file2.txt".to_string()));
        assert!(txt_paths.contains(&"/subdir/file3.txt".to_string()));
    }
}
