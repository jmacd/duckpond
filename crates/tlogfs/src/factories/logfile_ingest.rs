// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Logfile Ingestion Factory
//!
//! This factory ingests rotating log files from a host directory into the pond.
//! It tracks files with bao-tree blake3 digests for efficient change detection
//! and supports both archived (immutable) and active (append-only) files.

use crate::bao_outboard::{SeriesOutboard, compute_outboard, verify_prefix};
use crate::TLogFSError;
use datafusion::arrow::array::{Array, BinaryArray, Int64Array, StringArray};
use log::{debug, info, warn};
use provider::{register_executable_factory, ExecutionContext, FactoryContext};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tinyfs::{FileID, Result as TinyFSResult};

/// Configuration for the logfile ingestion factory
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogfileIngestConfig {
    /// Human-readable name for this source
    pub name: String,

    /// Glob pattern for archived (immutable) log files
    /// Example: "/var/log/casparwater-*.json"
    pub archived_pattern: String,

    /// Glob pattern for the active (append-only) log file
    /// Example: "/var/log/casparwater.json"
    pub active_pattern: String,

    /// Destination path within the pond (relative to pond root)
    /// Example: "logs/casparwater"
    pub pond_path: String,

    /// Verification mode for append-only files
    /// - "trust": Only compute new outboard from new bytes (fast)
    /// - "strict": Re-hash prefix and verify against stored outboard (slower, catches corruption)
    #[serde(default = "default_verification_mode")]
    pub verification_mode: String,
}

fn default_verification_mode() -> String {
    "trust".to_string()
}

impl LogfileIngestConfig {
    /// Validate the configuration
    pub fn validate(&self) -> TinyFSResult<()> {
        if self.name.is_empty() {
            return Err(tinyfs::Error::Other("name cannot be empty".to_string()));
        }

        if self.archived_pattern.is_empty() {
            return Err(tinyfs::Error::Other(
                "archived_pattern cannot be empty".to_string(),
            ));
        }

        if self.active_pattern.is_empty() {
            return Err(tinyfs::Error::Other(
                "active_pattern cannot be empty".to_string(),
            ));
        }

        if self.pond_path.is_empty() {
            return Err(tinyfs::Error::Other(
                "pond_path cannot be empty".to_string(),
            ));
        }

        if self.verification_mode != "trust" && self.verification_mode != "strict" {
            return Err(tinyfs::Error::Other(format!(
                "verification_mode must be 'trust' or 'strict', got '{}'",
                self.verification_mode
            )));
        }

        Ok(())
    }
}

/// State of a host file for tracking changes
#[allow(dead_code)] // Fields will be used when persistence layer is wired up
#[derive(Debug, Clone)]
struct HostFileState {
    /// Full path to the host file
    path: PathBuf,
    /// File size in bytes
    size: u64,
    /// Blake3 hash of the content
    blake3: Option<String>,
    /// Bao-tree outboard data (for FilePhysicalSeries)
    bao_outboard: Option<Vec<u8>>,
    /// Whether this is the active (append-only) file
    is_active: bool,
}

/// State of a pond file for comparison
#[allow(dead_code)] // Fields will be used when persistence layer is wired up
#[derive(Debug, Clone)]
struct PondFileState {
    /// Node ID in the pond
    node_id: FileID,
    /// Latest version number
    version: i64,
    /// File size in bytes
    size: u64,
    /// Blake3 hash of the content
    blake3: Option<String>,
    /// Bao-tree outboard data
    bao_outboard: Option<Vec<u8>>,
    /// Cumulative size (for FilePhysicalSeries)
    cumulative_size: u64,
}

/// Initialize factory (called once per dynamic node creation)
async fn initialize(
    _config: Value,
    _context: FactoryContext,
) -> Result<(), TLogFSError> {
    // No initialization needed for executable factory
    Ok(())
}

/// Execute the log ingestion process
async fn execute(
    config: Value,
    context: FactoryContext,
    ctx: ExecutionContext,
) -> Result<(), TLogFSError> {
    let config: LogfileIngestConfig = serde_json::from_value(config)
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Invalid config: {}", e))))?;
    
    info!(
        "Starting logfile ingestion for '{}' (mode: {:?})",
        config.name, ctx.mode()
    );

    // Step 1: Enumerate host files
    let host_files = enumerate_host_files(&config)
        .await
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string())))?;
    info!(
        "Found {} host files ({} archived, {} active)",
        host_files.len(),
        host_files.iter().filter(|f| !f.is_active).count(),
        host_files.iter().filter(|f| f.is_active).count()
    );

    // Step 2: Read pond state
    let pond_files = read_pond_state(&context, &config.pond_path)
        .await
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string())))?;
    info!("Found {} files in pond", pond_files.len());

    // Step 3: Detect changes and ingest
    let is_strict = config.verification_mode == "strict";
    
    for host_file in &host_files {
        let filename = host_file
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| {
                TLogFSError::TinyFS(tinyfs::Error::Other("Invalid filename".to_string()))
            })?;

        let pond_file = pond_files.get(filename);

        if host_file.is_active {
            // Active file: detect appends
            process_active_file(&context, &config, host_file, pond_file, is_strict)
                .await
                .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string())))?;
        } else {
            // Archived file: detect new or changed
            process_archived_file(&context, &config, host_file, pond_file)
                .await
                .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string())))?;
        }
    }

    info!("Logfile ingestion completed successfully");
    Ok(())
}

/// Parse a glob pattern into (base_dir, relative_pattern)
/// 
/// Splits at the first path component containing a wildcard.
/// E.g., "/var/log/*.log" -> ("/var/log", "*.log")
///       "/data/**/logs/*.txt" -> ("/data", "**/logs/*.txt")
fn parse_glob_pattern(pattern: &str) -> (PathBuf, String) {
    let path = Path::new(pattern);
    let mut base_components = Vec::new();
    let mut pattern_components = Vec::new();
    let mut found_wildcard = false;

    for component in path.components() {
        let s = component.as_os_str().to_string_lossy();
        if found_wildcard {
            pattern_components.push(s.to_string());
        } else if s.contains('*') || s.contains('?') {
            found_wildcard = true;
            pattern_components.push(s.to_string());
        } else {
            base_components.push(component);
        }
    }

    let base_dir: PathBuf = base_components.iter().collect();
    let relative_pattern = pattern_components.join("/");

    (base_dir, relative_pattern)
}

/// Enumerate files matching the configured patterns
async fn enumerate_host_files(config: &LogfileIngestConfig) -> std::io::Result<Vec<HostFileState>> {
    let mut files = Vec::new();

    // Match archived files
    let (base_dir, pattern) = parse_glob_pattern(&config.archived_pattern);
    let matches = tinyfs::glob::collect_host_matches(&pattern, &base_dir)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?;

    for (path, _captures) in matches {
        if let Ok(metadata) = std::fs::metadata(&path)
            && metadata.is_file()
        {
            files.push(HostFileState {
                path,
                size: metadata.len(),
                blake3: None,
                bao_outboard: None,
                is_active: false,
            });
        }
    }

    // Match active file
    let (base_dir, pattern) = parse_glob_pattern(&config.active_pattern);
    let matches = tinyfs::glob::collect_host_matches(&pattern, &base_dir)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?;

    for (path, _captures) in matches {
        if let Ok(metadata) = std::fs::metadata(&path)
            && metadata.is_file()
        {
            files.push(HostFileState {
                path,
                size: metadata.len(),
                blake3: None,
                bao_outboard: None,
                is_active: true,
            });
        }
    }

    Ok(files)
}

/// Read pond state for existing mirrored files
///
/// Queries the delta_table to find all files in the pond directory,
/// extracting their version, size, blake3 hash, and bao_outboard data.
async fn read_pond_state(
    context: &FactoryContext,
    pond_path: &str,
) -> std::io::Result<HashMap<String, PondFileState>> {
    let mut pond_files = HashMap::new();

    debug!("Reading pond state from: {}", pond_path);
    
    // Extract State from context to get DataFusion SessionContext
    let state = crate::extract_state(context)
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    
    let session_ctx = state.session_context().await
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    
    // Create tinyfs to navigate to pond_path and list directory entries
    let fs = tinyfs::FS::new(state.clone()).await
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let root = fs.root().await
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    
    // Navigate to the pond directory
    let pond_dir = match root.open_dir_path(pond_path).await {
        Ok(wd) => wd,
        Err(tinyfs::Error::NotFound(_)) => {
            // Directory doesn't exist yet - return empty state
            debug!("Pond directory '{}' doesn't exist yet", pond_path);
            return Ok(pond_files);
        }
        Err(e) => {
            return Err(std::io::Error::other(e.to_string()));
        }
    };
    
    // Get directory node_id which becomes part_id for children
    let dir_node_id = pond_dir.node_path().id();
    let part_id = tinyfs::PartID::from_hex_string(&dir_node_id.to_string())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    
    // List directory entries to get filename → node_id mapping
    use futures::StreamExt;
    let mut entries_stream = pond_dir.entries().await
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    
    let mut filename_to_node: HashMap<String, tinyfs::NodeID> = HashMap::new();
    while let Some(entry_result) = entries_stream.next().await {
        let entry = entry_result.map_err(|e| std::io::Error::other(e.to_string()))?;
        // Only include file entries (not directories)
        if entry.entry_type.is_file() {
            let _ = filename_to_node.insert(entry.name.clone(), entry.child_node_id);
        }
    }
    
    if filename_to_node.is_empty() {
        debug!("Pond directory '{}' is empty", pond_path);
        return Ok(pond_files);
    }
    
    // Build reverse map: node_id → filename
    let node_to_filename: HashMap<String, String> = filename_to_node
        .iter()
        .map(|(name, node_id)| (node_id.to_string(), name.clone()))
        .collect();
    
    // Query delta_table for files in this directory (part_id = dir_node_id)
    // Get the latest version of each file
    let sql = format!(
        "SELECT node_id, version, size, blake3, bao_outboard \
         FROM delta_table \
         WHERE part_id = '{}' \
           AND file_type IN ('file:physical', 'file:series:physical') \
         ORDER BY node_id, version DESC",
        dir_node_id
    );
    
    debug!("Querying pond state: {}", sql);
    
    let df = session_ctx.sql(&sql).await
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    
    let batches: Vec<_> = df.collect().await
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    
    // Get the part_id for constructing FileIDs
    let part_id = tinyfs::PartID::from_hex_string(&dir_node_id.to_string())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    
    // Process results, keeping only the latest version per node_id
    let mut seen_nodes = std::collections::HashSet::new();
    
    for batch in &batches {
        let node_id_col = batch.column_by_name("node_id")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "Missing node_id column"))?;
        
        let version_col = batch.column_by_name("version")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "Missing version column"))?;
        
        let size_col = batch.column_by_name("size")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
        
        let blake3_col = batch.column_by_name("blake3")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        
        let bao_col = batch.column_by_name("bao_outboard")
            .and_then(|c| c.as_any().downcast_ref::<BinaryArray>());
        
        for row in 0..batch.num_rows() {
            let node_id_str = node_id_col.value(row);
            
            // Skip if we've already seen this node (we ordered DESC, so first is latest)
            if seen_nodes.contains(node_id_str) {
                continue;
            }
            let _ = seen_nodes.insert(node_id_str.to_string());
            
            let version = version_col.value(row);
            let size = size_col.map(|c| c.value(row) as u64).unwrap_or(0);
            let blake3 = blake3_col.and_then(|c| {
                if c.is_null(row) { None } else { Some(c.value(row).to_string()) }
            });
            let bao_outboard = bao_col.and_then(|c| {
                if c.is_null(row) { None } else { Some(c.value(row).to_vec()) }
            });
            
            // Construct FileID from part_id and node_id
            let node_id = tinyfs::NodeID::from_hex_string(node_id_str)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            let file_id = FileID::new_from_ids(part_id, node_id);
            
            // Look up filename from directory entries
            let filename = match node_to_filename.get(node_id_str) {
                Some(name) => name.clone(),
                None => {
                    // Node exists in delta_table but not in directory - orphaned, skip it
                    debug!("Skipping orphaned node_id {} (not in directory)", node_id_str);
                    continue;
                }
            };
            
            let _ = pond_files.insert(filename, PondFileState {
                node_id: file_id,
                version,
                size,
                blake3,
                bao_outboard: bao_outboard.clone(),
                cumulative_size: size, // For series, this would be sum of all versions
            });
        }
    }
    
    debug!("Found {} files in pond", pond_files.len());
    Ok(pond_files)
}

/// Process an active (append-only) file
async fn process_active_file(
    context: &FactoryContext,
    config: &LogfileIngestConfig,
    host_file: &HostFileState,
    pond_file: Option<&PondFileState>,
    is_strict: bool,
) -> std::io::Result<()> {
    let filename = host_file
        .path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid filename"))?;

    match pond_file {
        None => {
            // New file: ingest completely
            info!("New active file detected: {}", filename);
            ingest_new_file(context, config, host_file).await?;
        }
        Some(pond_state) => {
            // Existing file: detect append
            if host_file.size > pond_state.cumulative_size {
                let new_bytes = host_file.size - pond_state.cumulative_size;
                info!(
                    "Active file {} grew by {} bytes (was {}, now {})",
                    filename, new_bytes, pond_state.cumulative_size, host_file.size
                );
                
                ingest_append(context, config, host_file, pond_state, is_strict).await?;
            } else if host_file.size < pond_state.cumulative_size {
                warn!(
                    "Active file {} SHRUNK from {} to {} bytes - unexpected!",
                    filename, pond_state.cumulative_size, host_file.size
                );
            } else {
                debug!("Active file {} unchanged ({} bytes)", filename, host_file.size);
            }
        }
    }

    Ok(())
}

/// Process an archived (immutable) file
async fn process_archived_file(
    context: &FactoryContext,
    config: &LogfileIngestConfig,
    host_file: &HostFileState,
    pond_file: Option<&PondFileState>,
) -> std::io::Result<()> {
    let filename = host_file
        .path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid filename"))?;

    match pond_file {
        None => {
            // New archived file
            info!("New archived file detected: {}", filename);
            ingest_new_file(context, config, host_file).await?;
        }
        Some(pond_state) => {
            // Verify archived file hasn't changed (should be immutable)
            let host_content = std::fs::read(&host_file.path)?;
            let host_hash = blake3::hash(&host_content);
            
            if Some(host_hash.to_hex().to_string()) != pond_state.blake3 {
                warn!(
                    "Archived file {} CHANGED - this violates immutability assumption!",
                    filename
                );
                // Re-ingest the changed file
                ingest_new_file(context, config, host_file).await?;
            } else {
                debug!("Archived file {} unchanged", filename);
            }
        }
    }

    Ok(())
}

/// Ingest a new file (first version)
async fn ingest_new_file(
    context: &FactoryContext,
    config: &LogfileIngestConfig,
    host_file: &HostFileState,
) -> std::io::Result<()> {
    let content = std::fs::read(&host_file.path)?;
    let filename = host_file
        .path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid filename"))?;

    let blake3_hash = blake3::hash(&content);
    
    // Compute bao-tree outboard for verified streaming
    let _ = compute_outboard(&content);
    
    let pond_dest = format!("{}/{}", config.pond_path, filename);
    
    info!(
        "Ingesting new file: {} -> {} ({} bytes, blake3={})",
        host_file.path.display(),
        pond_dest,
        content.len(),
        &blake3_hash.to_hex()[..16]
    );

    // Create SeriesOutboard for tracking cumulative content
    let series_outboard = SeriesOutboard::first_version_inline(&content);
    let bao_bytes = series_outboard.to_bytes();
    
    debug!(
        "Computed bao_outboard: {} bytes (version_outboard={}, cumulative_outboard={})",
        bao_bytes.len(),
        series_outboard.version_outboard.len(),
        series_outboard.cumulative_outboard.len()
    );

    // Extract State from context to access tinyfs
    let state = crate::extract_state(context)
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    
    // Create filesystem from state
    let fs = tinyfs::FS::new(state.clone()).await
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let root = fs.root().await
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    
    // Ensure the pond directory exists
    let _ = root.create_dir_path(&config.pond_path).await
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    
    // Write file with bao_outboard using the low-level writer API
    use tokio::io::AsyncWriteExt;
    let mut writer = root.async_writer_path(&pond_dest).await
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    
    // Set bao_outboard before writing content
    writer.set_bao_outboard(bao_bytes);
    
    // Write content and finalize
    writer.write_all(&content).await?;
    writer.shutdown().await?;
    
    info!(
        "Wrote file to pond with bao_outboard: {}",
        pond_dest
    );

    Ok(())
}

/// Ingest an append to an existing active file
async fn ingest_append(
    context: &FactoryContext,
    config: &LogfileIngestConfig,
    host_file: &HostFileState,
    pond_state: &PondFileState,
    is_strict: bool,
) -> std::io::Result<()> {
    let filename = host_file
        .path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid filename"))?;
    
    let pond_dest = format!("{}/{}", config.pond_path, filename);

    // Extract State from context to access tinyfs (once for all operations)
    let state = crate::extract_state(context)
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let fs = tinyfs::FS::new(state.clone()).await
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let root = fs.root().await
        .map_err(|e| std::io::Error::other(e.to_string()))?;

    // Read only the new bytes from host file
    let mut file = std::fs::File::open(&host_file.path)?;
    use std::io::{Read, Seek, SeekFrom};
    let _ = file.seek(SeekFrom::Start(pond_state.cumulative_size))?;
    
    let mut new_content = Vec::new();
    let _ = file.read_to_end(&mut new_content)?;

    info!(
        "Ingesting append to {}: {} new bytes (total will be {})",
        filename,
        new_content.len(),
        pond_state.cumulative_size + new_content.len() as u64
    );

    // Deserialize previous outboard if available
    let prev_series = if let Some(prev_outboard) = &pond_state.bao_outboard {
        Some(
            SeriesOutboard::from_bytes(prev_outboard)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
        )
    } else {
        None
    };

    if is_strict {
        // Verify prefix hasn't changed
        if let Some(ref prev) = prev_series {
            // Read the prefix from host file to verify
            let mut prefix_file = std::fs::File::open(&host_file.path)?;
            let mut prefix_content = vec![0u8; pond_state.cumulative_size as usize];
            prefix_file.read_exact(&mut prefix_content)?;
            
            verify_prefix(&prefix_content, &prev.cumulative_outboard, pond_state.cumulative_size)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            
            info!("Prefix verification passed for {}", filename);
        }
    }

    // Compute new SeriesOutboard incrementally using append_to_outboard
    let new_series_outboard = if let Some(ref prev) = prev_series {
        // Read pending bytes from the pond (last cumulative_size % BLOCK_SIZE bytes)
        use crate::bao_outboard::BLOCK_SIZE;
        let pending_size = (pond_state.cumulative_size % BLOCK_SIZE as u64) as usize;
        
        let mut pending_bytes = vec![0u8; pending_size];
        
        if pending_size > 0 {
            use tokio::io::{AsyncReadExt, AsyncSeekExt};
            let mut reader = root.async_reader_path(&pond_dest).await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            let seek_pos = pond_state.cumulative_size - pending_size as u64;
            let _ = reader.seek(SeekFrom::Start(seek_pos)).await?;
            let _ = reader.read_exact(&mut pending_bytes).await?;
        }
        
        // Compute incrementally using only new bytes
        let (_, new_cumulative_outboard, _) = crate::bao_outboard::append_to_outboard(
            &prev.cumulative_outboard,
            pond_state.cumulative_size,
            &pending_bytes,
            &new_content,
        ).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        
        SeriesOutboard {
            version_outboard: Vec::new(), // Inline content doesn't need this
            cumulative_outboard: new_cumulative_outboard,
            version_size: new_content.len() as u64,
            cumulative_size: pond_state.cumulative_size + new_content.len() as u64,
        }
    } else {
        // No previous outboard - compute from full content as fallback
        let full_content = std::fs::read(&host_file.path)?;
        SeriesOutboard::first_version_inline(&full_content)
    };
    
    let bao_bytes = new_series_outboard.to_bytes();
    
    debug!(
        "Computed new bao_outboard for append: {} bytes",
        bao_bytes.len()
    );

    // Write new version with updated bao_outboard
    use tokio::io::AsyncWriteExt;
    let mut writer = root.async_writer_path(&pond_dest).await
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    
    // Set bao_outboard before writing content
    writer.set_bao_outboard(bao_bytes);
    
    // Write only the new content (as a new version in the FilePhysicalSeries)
    // The ChainedReader will concatenate all versions when reading
    writer.write_all(&new_content).await?;
    writer.shutdown().await?;
    
    info!(
        "Wrote append to pond with bao_outboard: {} version {}",
        pond_dest,
        pond_state.version + 1
    );

    Ok(())
}

/// Validate configuration
fn validate_config(config: &[u8]) -> TinyFSResult<Value> {
    let config: LogfileIngestConfig = serde_json::from_slice(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid config JSON: {}", e)))?;

    config.validate()?;

    serde_json::to_value(&config)
        .map_err(|e| tinyfs::Error::Other(format!("Failed to serialize config: {}", e)))
}

// Register the factory
register_executable_factory!(
    name: "logfile-ingest",
    description: "Ingest rotating log files from host directory with bao-tree verification",
    validate: validate_config,
    initialize: initialize,
    execute: execute
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_config() {
        let config = LogfileIngestConfig {
            name: "test".to_string(),
            archived_pattern: "/var/log/test-*.json".to_string(),
            active_pattern: "/var/log/test.json".to_string(),
            pond_path: "logs/test".to_string(),
            verification_mode: "trust".to_string(),
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_config_empty_name() {
        let config = LogfileIngestConfig {
            name: "".to_string(),
            archived_pattern: "/var/log/test-*.json".to_string(),
            active_pattern: "/var/log/test.json".to_string(),
            pond_path: "logs/test".to_string(),
            verification_mode: "trust".to_string(),
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_config_invalid_verification_mode() {
        let config = LogfileIngestConfig {
            name: "test".to_string(),
            archived_pattern: "/var/log/test-*.json".to_string(),
            active_pattern: "/var/log/test.json".to_string(),
            pond_path: "logs/test".to_string(),
            verification_mode: "invalid".to_string(),
        };

        assert!(config.validate().is_err());
    }
}
