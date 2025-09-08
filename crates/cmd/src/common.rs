use std::path::PathBuf;
use std::env;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono;
use clap::ValueEnum;
use tinyfs::EntryType;
use diagnostics::*;
use tempfile::{TempDir, tempdir};

/// Which filesystem to access in the steward-managed pond
#[derive(Clone, Debug, ValueEnum, PartialEq)]
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
    pub async fn open_pond(&self) -> Result<steward::Ship> {
        let pond_path = self.resolve_pond_path()?;
        steward::Ship::open_pond(&pond_path).await
            .map_err(|e| anyhow!("Failed to initialize ship: {}", e))
    }

    /// Initialize a new pond (for init command only)
    pub async fn create_pond(&self) -> Result<steward::Ship> {
        let pond_path = self.resolve_pond_path()?;
        steward::Ship::create_pond(&pond_path).await
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
/// For hex strings, shows appropriate number of digits based on magnitude
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
        // Hex format - parse as hex and format based on magnitude
        let id_value = u64::from_str_radix(node_id, 16).unwrap_or(0);
        format_id_value(id_value)
    }
}

/// Helper function to format file sizes
pub fn format_file_size(size: u64) -> String {
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
    pub node_id: String,
    pub metadata: tinyfs::NodeMetadata,
    pub symlink_target: Option<String>,
}

impl FileInfo {
    /// Format in DuckPond-specific style showing meaningful metadata
    pub fn format_duckpond_style(&self) -> String {
        let type_symbol = match self.metadata.entry_type {
            EntryType::Directory => "📁",
            EntryType::Symlink => "🔗",
            EntryType::FileData => "📄",
            EntryType::FileTable => "📊",
            EntryType::FileSeries => "📈",
        };

        let size_str = if self.metadata.entry_type == EntryType::Directory {
            "-".to_string()
        } else {
            format_file_size(self.metadata.size.unwrap_or(0))
        };

	let timestamp_us = self.metadata.timestamp;
        let dt = chrono::DateTime::from_timestamp(
            timestamp_us / 1_000_000, 
            ((timestamp_us % 1_000_000) * 1000) as u32
        ).unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());

	let time_str =  dt.format("%Y-%m-%d %H:%M:%S").to_string();

        let node_id_str = format_node_id(&self.node_id);

        let version_str = format!("v{}", self.metadata.version);

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
                // Get consolidated metadata from the file handle
                let metadata = file_handle.metadata().await?;
                let entry_type_str = metadata.entry_type.as_str();
                let size_val = metadata.size.unwrap_or(0);

                let final_type_str = metadata.entry_type.as_str();
                let version = metadata.version;
                debug!("FileInfoVisitor: Successfully got metadata - entry_type={entry_type_str}, version={version}, size={size_val}");
                
                debug!("FileInfoVisitor: Final FileInfo will have node_type={final_type_str}");
            
                Ok(FileInfo {
                    path,
		    metadata,
                    symlink_target: None,
                    node_id,
                })
            }
            tinyfs::NodeType::Directory(dir_handle) => {
                let metadata = dir_handle.metadata().await?;
                Ok(FileInfo {
                    path,
		    metadata,
                    symlink_target: None,
                    node_id,
                })
            }
            tinyfs::NodeType::Symlink(symlink_handle) => {
                let target = symlink_handle.readlink().await.unwrap_or_default();
                let metadata = symlink_handle.metadata().await?;
                
                Ok(FileInfo {
                    path,
		    metadata,
                    symlink_target: Some(target.to_string_lossy().to_string()),
                    node_id,
                })
            }
        }
    }
}

// Ship context utilities to eliminate duplication in test setup patterns

/// Test environment that encapsulates ship setup patterns
/// Eliminates duplication in ship creation across test files
#[cfg(test)]
pub struct TestShipEnvironment {
    pub temp_dir: TempDir,
    pub ship_context: ShipContext,
}

#[cfg(test)]
impl TestShipEnvironment {
    /// Create a new test environment with temporary pond
    /// This eliminates the repetitive pattern across many test files:
    /// - Create temp dir
    /// - Initialize pond
    /// - Create ship context
    pub async fn new() -> Result<Self> {
        let temp_dir = tempdir()?;
        let pond_path = temp_dir.path().join("test_pond");
        
        // Initialize a new pond for testing
        let ship_context = ShipContext::new(Some(pond_path), vec!["test".to_string()]);
        let _ship = ship_context.create_pond().await?;

        Ok(Self {
            temp_dir,
            ship_context,
        })
    }

    /// Create a ship for read operations
    pub async fn create_ship(&self) -> Result<steward::Ship> {
        self.ship_context.open_pond().await
    }

    /// Execute a closure with a ship using scoped transactions
    /// This replaces the old create_ship_with_transaction pattern
    pub async fn with_scoped_transaction<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(steward::Ship) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let ship = self.create_ship().await?;
        f(ship).await
    }

    /// Get the pond path for the test environment
    pub fn pond_path(&self) -> PathBuf {
        self.temp_dir.path().join("test_pond")
    }
}

/// Common test patterns for ship operations
#[cfg(test)]
pub struct ShipTestUtils;

#[cfg(test)]
impl ShipTestUtils {
    /// Create a test CSV file in a temp directory
    pub fn create_test_csv(content: &str) -> Result<(TempDir, PathBuf)> {
        let temp_dir = tempdir()?;
        let csv_path = temp_dir.path().join("test.csv");
        std::fs::write(&csv_path, content)?;
        Ok((temp_dir, csv_path))
    }

    /// Standard CSV content for testing
    pub fn sample_csv_content() -> &'static str {
        "timestamp,sensor_id,temperature,humidity\n\
         1609459200000,sensor1,23.5,45.2\n\
         1609459260000,sensor1,24.1,46.8\n\
         1609459320000,sensor2,22.8,44.1\n\
         1609459380000,sensor2,25.2,48.9"
    }

    /// Standard CSV content with different data patterns
    pub fn sample_csv_content_variant() -> &'static str {
        "timestamp,device_id,value\n\
         1609459200000,device1,100.0\n\
         1609459260000,device1,101.5\n\
         1609459320000,device2,99.2\n\
         1609459380000,device2,102.8"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ship_environment_setup() -> Result<()> {
        let env = TestShipEnvironment::new().await?;
        
        // Test basic ship creation
        let _ship = env.create_ship().await?;
        // Basic validation that ship was created successfully
        // (Ship creation itself validates the pond setup)
        
        Ok(())
    }

    #[tokio::test]
    async fn test_with_transaction_pattern() -> Result<()> {
        let env = TestShipEnvironment::new().await?;
        
        let result = env.with_scoped_transaction(|_ship| async move {
            // Test the transaction pattern
            // This would normally do some file operations
            Ok("transaction completed".to_string())
        }).await?;
        
        assert_eq!(result, "transaction completed");
        Ok(())
    }

    #[test]
    fn test_csv_utilities() -> Result<()> {
        let (_temp_dir, csv_path) = ShipTestUtils::create_test_csv(ShipTestUtils::sample_csv_content())?;
        
        assert!(csv_path.exists());
        let content = std::fs::read_to_string(&csv_path)?;
        assert!(content.contains("timestamp,sensor_id"));
        
        Ok(())
    }
}
