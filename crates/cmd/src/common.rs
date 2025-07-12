use std::path::PathBuf;
use std::env;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono;
use clap::ValueEnum;
use tinyfs::EntryType;

/// Which filesystem to access in the steward-managed pond
#[derive(Clone, Debug, ValueEnum)]
pub enum FilesystemChoice {
    /// Primary data filesystem (default)
    Data,
    /// Control filesystem for transaction metadata
    Control,
}

/// Context needed to create and operate on a Ship
/// 
/// This captures the pond location and command metadata together,
/// representing everything needed to initialize a Ship with proper transaction tracking.
#[derive(Debug, Clone)]
pub struct ShipContext {
    /// Optional pond path override (None means use POND env var)
    pub pond_path: Option<PathBuf>,
    /// Original command line arguments for transaction metadata
    pub original_args: Vec<String>,
}

impl ShipContext {
    /// Create a new ShipContext from CLI parsing
    pub fn new(pond_path: Option<PathBuf>, original_args: Vec<String>) -> Self {
        Self {
            pond_path,
            original_args,
        }
    }

    /// Resolve the actual pond path using the override or environment variable
    pub fn resolve_pond_path(&self) -> Result<PathBuf> {
        get_pond_path_with_override(self.pond_path.clone())
    }

    /// Create a Ship for an existing pond (read-only operations)
    pub async fn create_ship(&self) -> Result<steward::Ship> {
        let pond_path = self.resolve_pond_path()?;
        steward::Ship::open_existing_pond(&pond_path).await
            .map_err(|e| anyhow!("Failed to initialize ship: {}", e))
    }

    /// Create a Ship for an existing pond with transaction started (write operations)
    pub async fn create_ship_with_transaction(&self) -> Result<steward::Ship> {
        let mut ship = self.create_ship().await?;
        ship.begin_transaction_with_args(self.original_args.clone()).await
            .map_err(|e| anyhow!("Failed to begin transaction: {}", e))?;
        Ok(ship)
    }

    /// Initialize a new pond (for init command only)
    pub async fn initialize_new_pond(&self) -> Result<steward::Ship> {
        let pond_path = self.resolve_pond_path()?;
        steward::Ship::initialize_new_pond(&pond_path, self.original_args.clone()).await
            .map_err(|e| anyhow!("Failed to initialize pond: {}", e))
    }
}

/// Get the pond path with an optional override, falling back to POND environment variable
pub fn get_pond_path_with_override(override_path: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = override_path {
        return Ok(path);
    }
    
    let pond_base = env::var("POND")
        .map_err(|_| anyhow!("POND environment variable not set"))
        .map(PathBuf::from)?;
    Ok(pond_base)
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
/// For UUID7 strings, shows last 8 hex characters (random part, git-style)
/// For legacy hex strings, shows exactly 4, 8, 12, or 16 hex digits based on magnitude
pub fn format_node_id(node_id: &str) -> String {
    // Check if this looks like a UUID7 (contains hyphens)
    if node_id.contains('-') {
        // UUID7 format - take last 8 hex characters (random part)
        // This avoids timestamp collisions when UUIDs are generated rapidly
        let hex_only: String = node_id.chars().filter(|c| c.is_ascii_hexdigit()).collect();
        let len = hex_only.len();
        if len >= 8 {
            hex_only[len-8..].to_string()
        } else {
            hex_only
        }
    } else {
        // Legacy format - parse as hex and format based on magnitude
        let id_value = u64::from_str_radix(node_id, 16).unwrap_or(0);
        format_id_value(id_value)
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

/// Helper function to parse directory content
pub fn parse_directory_content(content: &[u8]) -> Result<Vec<tlogfs::VersionedDirectoryEntry>> {
    if content.is_empty() {
        return Ok(Vec::new());
    }
    
    use arrow::ipc::reader::StreamReader;
    
    let cursor = std::io::Cursor::new(content);
    let reader = StreamReader::try_new(cursor, None)?;
    
    let mut all_entries = Vec::new();
    for batch_result in reader {
        let batch = batch_result?;
        let entries: Vec<tlogfs::VersionedDirectoryEntry> = serde_arrow::from_record_batch(&batch)?;
        all_entries.extend(entries);
    }
    
    Ok(all_entries)
}

/// Helper struct to store file information for DuckPond-specific output
#[derive(Debug)]
pub struct FileInfo {
    pub path: String,
    pub node_type: EntryType,
    pub size: usize,
    pub timestamp: Option<i64>,
    pub symlink_target: Option<String>,
    pub node_id: Option<String>,
    pub version: Option<i64>,
}

impl FileInfo {
    /// Format in DuckPond-specific style showing meaningful metadata
    pub fn format_duckpond_style(&self) -> String {
        let type_symbol = match self.node_type {
            EntryType::Directory => "📁",
            EntryType::Symlink => "🔗",
            EntryType::FileData => "📄",
            EntryType::FileTable => "📊",
            EntryType::FileSeries => "📈",
        };

        let size_str = if self.node_type == EntryType::Directory {
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
                
                // Get metadata from the file handle
                let timestamp = file_handle.metadata_u64("timestamp").await
                    .unwrap_or(None)
                    .map(|t| t as i64);
                
                let version = file_handle.metadata_u64("version").await
                    .unwrap_or(None)
                    .map(|v| v as i64);
                
                Ok(FileInfo {
                    path,
                    node_type: EntryType::FileData,
                    size: content.len(),
                    timestamp,
                    symlink_target: None,
                    node_id: Some(node_id),
                    version,
                })
            }
            tinyfs::NodeType::Directory(dir_handle) => {
                // Get metadata from the directory handle
                let timestamp = dir_handle.metadata_u64("timestamp").await
                    .unwrap_or(None)
                    .map(|t| t as i64);
                
                let version = dir_handle.metadata_u64("version").await
                    .unwrap_or(None)
                    .map(|v| v as i64);
                
                Ok(FileInfo {
                    path,
                    node_type: EntryType::Directory,
                    size: 0,
                    timestamp,
                    symlink_target: None,
                    node_id: Some(node_id),
                    version,
                })
            }
            tinyfs::NodeType::Symlink(symlink_handle) => {
                let target = symlink_handle.readlink().await.unwrap_or_default();
                
                // Get metadata from the symlink handle
                let timestamp = symlink_handle.metadata_u64("timestamp").await
                    .unwrap_or(None)
                    .map(|t| t as i64);
                
                let version = symlink_handle.metadata_u64("version").await
                    .unwrap_or(None)
                    .map(|v| v as i64);
                
                Ok(FileInfo {
                    path,
                    node_type: EntryType::Symlink,
                    size: 0,
                    timestamp,
                    symlink_target: Some(target.to_string_lossy().to_string()),
                    node_id: Some(node_id),
                    version,
                })
            }
        }
    }
}

/// Create a steward Ship instance for the given pond path
/// This is the new way to access the filesystem that replaces direct tlogfs usage
pub async fn create_ship(pond_path: Option<PathBuf>) -> Result<steward::Ship> {
    let pond_path = get_pond_path_with_override(pond_path)?;
    steward::Ship::open_existing_pond(&pond_path).await
        .map_err(|e| anyhow!("Failed to initialize ship: {}", e))
}



/// Create a ship specifically for read-only operations
/// This provides a clear intent for read-only commands
pub async fn create_ship_for_reading(pond_path: Option<PathBuf>) -> Result<steward::Ship> {
    // For read-only operations, we use the same ship creation but with clear intent
    create_ship(pond_path).await
}

/// Helper for read-only file operations via steward
/// This provides a consistent pattern for reading files across commands
pub async fn read_file_via_steward(
    path: &str, 
    filesystem: FilesystemChoice, 
    pond_path: Option<PathBuf>
) -> Result<Vec<u8>> {
    let ship = create_ship_for_reading(pond_path).await?;
    
    let fs = match filesystem {
        FilesystemChoice::Data => ship.data_fs(),
        FilesystemChoice::Control => ship.control_fs(),
    };
    
    let root = fs.root().await?;
    root.read_file_path(path).await
        .map_err(|e| anyhow!("Failed to read file '{}' from {} filesystem: {}", 
            path, 
            match filesystem {
                FilesystemChoice::Data => "data",
                FilesystemChoice::Control => "control",
            },
            e))
}

/// Helper for read-only listing operations via steward
/// This provides a consistent pattern for listing files across commands
pub async fn list_files_via_steward<T>(
    pattern: &str, 
    visitor: &mut T,
    filesystem: FilesystemChoice, 
    pond_path: Option<PathBuf>
) -> Result<Vec<FileInfo>>
where 
    T: tinyfs::Visitor<FileInfo>
{
    let ship = create_ship_for_reading(pond_path).await?;
    
    let fs = match filesystem {
        FilesystemChoice::Data => ship.data_fs(),
        FilesystemChoice::Control => ship.control_fs(),
    };
    
    let root = fs.root().await?;
    root.visit_with_visitor(pattern, visitor).await
        .map_err(|e| anyhow!("Failed to list files matching '{}' from {} filesystem: {}", 
            pattern, 
            match filesystem {
                FilesystemChoice::Data => "data",
                FilesystemChoice::Control => "control",
            },
            e))
}
