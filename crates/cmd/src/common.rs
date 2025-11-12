use std::collections::HashMap;
use std::env;
use std::path::Path;
use std::path::PathBuf;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use log::debug;
use tinyfs::EntryType;

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
    /// Template variables from CLI (-v key=value flags)
    pub template_variables: HashMap<String, String>,
}

impl ShipContext {
    /// Create a new ShipContext from CLI parsing
    #[must_use]
    pub fn new<P: AsRef<Path>>(pond_path: Option<P>, original_args: Vec<String>) -> Self {
        Self {
            pond_path: pond_path.map(|p| p.as_ref().to_path_buf()),
            original_args,
            template_variables: HashMap::new(),
        }
    }

    /// Create a new ShipContext with template variables from CLI parsing
    #[must_use]
    pub fn with_variables(
        pond_path: Option<PathBuf>,
        original_args: Vec<String>,
        template_variables: HashMap<String, String>,
    ) -> Self {
        Self {
            pond_path,
            original_args,
            template_variables,
        }
    }

    /// Resolve the actual pond path using the override or environment variable
    pub fn resolve_pond_path(&self) -> Result<PathBuf> {
        get_pond_path_with_override(self.pond_path.clone())
    }

    /// Create a Ship for an existing pond (read-only operations)
    pub async fn open_pond(&self) -> Result<steward::Ship> {
        let pond_path = self.resolve_pond_path()?;
        steward::Ship::open_pond(&pond_path)
            .await
            .map_err(|e| anyhow!("Failed to initialize ship: {}", e))
    }

    /// Initialize a new pond (for init command only)
    pub async fn create_pond(&self) -> Result<steward::Ship> {
        let pond_path = self.resolve_pond_path()?;
        steward::Ship::create_pond(&pond_path)
            .await
            .map_err(|e| anyhow!("Failed to initialize pond: {}", e))
    }

    /// Create pond infrastructure for restoration from bundles
    ///
    /// Unlike `create_pond()`, this creates the pond structure WITHOUT recording
    /// the initial transaction #1. The first bundle will create txn_seq=1 with
    /// the original command metadata from the source pond.
    ///
    /// REQUIRED: preserve_metadata must be provided with the source pond's identity.
    /// Restoration means cloning another pond, not creating a new one.
    pub async fn create_pond_for_restoration(
        &self,
        preserve_metadata: steward::PondMetadata,
    ) -> Result<steward::Ship> {
        let pond_path = self.resolve_pond_path()?;
        steward::Ship::create_pond_for_restoration(&pond_path, preserve_metadata)
            .await
            .map_err(|e| anyhow!("Failed to create pond for restoration: {}", e))
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

/// Number of hex characters to show when shortening UUID7 values
/// This matches the rightmost block (12 characters) of a UUID
const UUID_SHORT_LENGTH: usize = 12;

/// Helper function to format node IDs in a friendly way with square brackets
/// For UUID7 strings, shows last 12 hex characters (rightmost block)
/// For hex strings, shows appropriate number of digits based on magnitude
/// Always wraps the result in square brackets for consistency
#[must_use]
pub fn format_node_id(node_id: &str) -> String {
    // @@@ YUCK Use Uuid, fail, do not fallback
    // Check if this looks like a UUID7 (contains hyphens)
    let id_str = if node_id.contains('-') {
        // UUID7 format - take last UUID_SHORT_LENGTH hex characters (rightmost block)
        // This avoids timestamp collisions when UUIDs are generated rapidly
        let hex_only: String = node_id.chars().filter(|c| c.is_ascii_hexdigit()).collect();
        let len = hex_only.len();
        if len >= UUID_SHORT_LENGTH {
            hex_only[len - UUID_SHORT_LENGTH..].to_string()
        } else {
            hex_only
        }
    } else {
        // Hex format - parse as hex and format based on magnitude
        panic!("unhandled code path");
    };

    format!("[{}]", id_str)
}

/// Helper function to format file sizes
#[must_use]
pub fn format_file_size(size: u64) -> String {
    if size >= 1024 * 1024 {
        format!("{:.1}MB", size as f64 / (1024.0 * 1024.0))
    } else if size >= 1024 {
        format!("{:.1}KB", size as f64 / 1024.0)
    } else {
        format!("{}B", size)
    }
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
    #[must_use]
    pub fn format_duckpond_style(&self) -> String {
        let type_symbol = match self.metadata.entry_type {
            EntryType::DirectoryPhysical | EntryType::DirectoryDynamic => "📁",
            EntryType::Symlink => "🔗",
            EntryType::FileDataPhysical | EntryType::FileDataDynamic => "📄",
            EntryType::FileTablePhysical | EntryType::FileTableDynamic => "📊",
            EntryType::FileSeriesPhysical | EntryType::FileSeriesDynamic => "📈",
        };

        let size_str = if self.metadata.entry_type.is_directory() {
            "-".to_string()
        } else {
            format_file_size(self.metadata.size.unwrap_or(0))
        };

        let timestamp_us = self.metadata.timestamp;
        let dt = chrono::DateTime::from_timestamp(
            timestamp_us / 1_000_000,
            ((timestamp_us % 1_000_000) * 1000) as u32,
        )
        .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).expect("ok"));

        let time_str = dt.format("%Y-%m-%d %H:%M:%S").to_string();

        let node_id_str = format_node_id(&self.node_id);

        let version_str = format!("v{}", self.metadata.version);

        let symlink_part = if let Some(target) = &self.symlink_target {
            format!(" -> {}", target)
        } else {
            String::new()
        };

        format!(
            "{} {:>8} {:>8} {} {} {}{}\n",
            type_symbol, size_str, node_id_str, version_str, time_str, self.path, symlink_part
        )
    }
}

/// Visitor implementation to collect file information
pub struct FileInfoVisitor {
    show_all: bool,
}

impl FileInfoVisitor {
    #[must_use]
    pub fn new(show_all: bool) -> Self {
        Self { show_all }
    }
}

#[async_trait]
impl tinyfs::Visitor<FileInfo> for FileInfoVisitor {
    async fn visit(
        &mut self,
        node: tinyfs::NodePath,
        _captured: &[String],
    ) -> tinyfs::Result<FileInfo> {
        let node_ref = node.borrow().await;
        let path = node.path().to_string_lossy().to_string();

        // Skip hidden files unless --all is specified
        let basename = node.basename();
        if !self.show_all && basename.starts_with('.') && basename != "." && basename != ".." {
            return Err(tinyfs::Error::Other("Hidden file skipped".to_string()));
        }

        // Extract metadata that we can access from the node
        let node_id = node.id().await.to_string();

        match node_ref.node_type() {
            tinyfs::NodeType::File(file_handle) => {
                // Get consolidated metadata from the file handle
                let metadata = file_handle.metadata().await?;
                let entry_type_str = metadata.entry_type.as_str();
                let size_val = metadata.size.unwrap_or(0);

                let final_type_str = metadata.entry_type.as_str();
                let version = metadata.version;
                debug!(
                    "FileInfoVisitor: Successfully got metadata - entry_type={entry_type_str}, version={version}, size={size_val}"
                );

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
