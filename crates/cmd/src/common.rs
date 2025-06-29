use std::env;
use std::path::PathBuf;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono;

/// Get the pond path from environment variable
pub fn get_pond_path() -> Result<PathBuf> {
    match env::var("POND") {
        Ok(val) => Ok(PathBuf::from(val).join("store")),
        Err(_) => Err(anyhow!("POND environment variable not set")),
    }
}

/// Helper function to format node IDs 
pub fn format_node_id(node_id: &str) -> String {
    if node_id.len() >= 8 {
        format!("{}..{}", &node_id[..4], &node_id[node_id.len()-4..])
    } else {
        node_id.to_string()
    }
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

/// Helper function to parse OplogEntry from IPC bytes
pub fn parse_oplog_entry_content(content: &[u8]) -> Result<tinylogfs::OplogEntry> {
    use arrow::ipc::reader::StreamReader;
    
    let cursor = std::io::Cursor::new(content);
    let reader = StreamReader::try_new(cursor, None)?;
    
    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>()?;
    if let Some(batch) = batches.first() {
        let entries: Vec<tinylogfs::OplogEntry> = serde_arrow::from_record_batch(batch)?;
        entries.into_iter().next()
            .ok_or_else(|| anyhow!("No OplogEntry found in batch"))
    } else {
        Err(anyhow!("No batches found in OplogEntry IPC stream"))
    }
}

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

/// Helper struct to store file information for ls -l output
#[derive(Debug)]
pub struct FileInfo {
    pub path: String,
    pub node_type: String,
    pub size: usize,
    pub timestamp: Option<i64>,
    pub symlink_target: Option<String>,
}

impl FileInfo {
    pub fn format_ls_style(&self) -> String {
        let type_char = match self.node_type.as_str() {
            "directory" => "d",
            "symlink" => "l",
            _ => "-",
        };

        let size_str = if self.node_type == "directory" {
            format!("{:>8}", "-")
        } else {
            format!("{:>8}", format_file_size(self.size))
        };

        let time_str = if let Some(timestamp_us) = self.timestamp {
            let dt = chrono::DateTime::from_timestamp(
                timestamp_us / 1_000_000, 
                ((timestamp_us % 1_000_000) * 1000) as u32
            ).unwrap_or_else(|| chrono::Utc::now());
            dt.format("%b %d %H:%M").to_string()
        } else {
            "           ".to_string()
        };

        let symlink_part = if let Some(target) = &self.symlink_target {
            format!(" -> {}", target)
        } else {
            String::new()
        };

        format!("{}rwxr-xr-x 1 user group {} {} {}{}\n",
                type_char, size_str, time_str, self.path, symlink_part)
    }
}

/// Visitor implementation to collect file information
pub struct FileInfoVisitor {
    pub results: Vec<FileInfo>,
    show_all: bool,
}

impl FileInfoVisitor {
    pub fn new(show_all: bool) -> Self {
        Self {
            results: Vec::new(),
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

        match node_ref.node_type() {
            tinyfs::NodeType::File(file_handle) => {
                let content = file_handle.content().await.unwrap_or_default();
                Ok(FileInfo {
                    path,
                    node_type: "file".to_string(),
                    size: content.len(),
                    timestamp: None, // TODO: Extract from oplog metadata
                    symlink_target: None,
                })
            }
            tinyfs::NodeType::Directory(_) => {
                Ok(FileInfo {
                    path,
                    node_type: "directory".to_string(),
                    size: 0,
                    timestamp: None, // TODO: Extract from oplog metadata
                    symlink_target: None,
                })
            }
            tinyfs::NodeType::Symlink(symlink_handle) => {
                let target = symlink_handle.readlink().await.unwrap_or_default();
                Ok(FileInfo {
                    path,
                    node_type: "symlink".to_string(),
                    size: 0,
                    timestamp: None, // TODO: Extract from oplog metadata
                    symlink_target: Some(target.to_string_lossy().to_string()),
                })
            }
        }
    }
}
