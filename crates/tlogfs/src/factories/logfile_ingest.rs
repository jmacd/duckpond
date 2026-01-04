// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Logfile Ingestion Factory
//!
//! This factory ingests rotating log files from a host directory into the pond.
//! It tracks files with bao-tree blake3 digests for efficient change detection
//! and supports both archived (immutable) and active (append-only) files.

use crate::TLogFSError;
use datafusion::arrow::array::{Array, BinaryArray, Int64Array, StringArray};
use log::{debug, info, warn};
use provider::{ExecutionContext, FactoryContext, register_executable_factory};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use tinyfs::{FileID, Result as TinyFSResult};
use utilities::bao_outboard::{SeriesOutboard, verify_prefix};

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

        Ok(())
    }
}

/// State of a host file for tracking changes
#[derive(Debug, Clone)]
struct HostFileState {
    /// Full path to the host file
    path: PathBuf,
    /// File size in bytes
    size: u64,
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
    /// Blake3 hash of the content (always present for pond files)
    blake3: String,
    /// Bao-tree outboard data (always present for pond files)
    bao_outboard: Vec<u8>,
    /// Cumulative size (for FilePhysicalSeries)
    cumulative_size: u64,
}

/// Initialize factory (called once per dynamic node creation)
async fn initialize(_config: Value, _context: FactoryContext) -> Result<(), TLogFSError> {
    // No initialization needed for executable factory
    Ok(())
}

/// Execute the log ingestion process
#[cfg_attr(test, allow(dead_code))] // Allow in tests even if not directly called
pub async fn execute(
    config: Value,
    context: FactoryContext,
    ctx: ExecutionContext,
) -> Result<(), TLogFSError> {
    let config: LogfileIngestConfig = serde_json::from_value(config)
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Invalid config: {}", e))))?;

    info!(
        "Starting logfile ingestion for '{}' (mode: {:?})",
        config.name,
        ctx.mode()
    );

    // Step 1: Enumerate host files
    let host_files = enumerate_host_files(&config).await?;
    info!(
        "Found {} host files ({} archived, {} active)",
        host_files.len(),
        host_files.iter().filter(|f| !f.is_active).count(),
        host_files.iter().filter(|f| f.is_active).count()
    );

    // Step 2: Read pond state
    let pond_files = read_pond_state(&context, &config.pond_path).await?;
    info!("Found {} files in pond", pond_files.len());

    // Step 3: Detect changes and ingest
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
            process_active_file(&context, &config, host_file, pond_file).await?;
        } else {
            // Archived file: detect new or changed
            process_archived_file(&context, &config, host_file, pond_file).await?;
        }
    }

    info!("Logfile ingestion completed successfully");
    Ok(())
}

/// Enumerate files matching the configured patterns
async fn enumerate_host_files(
    config: &LogfileIngestConfig,
) -> Result<Vec<HostFileState>, TLogFSError> {
    let mut files = Vec::new();

    // Match archived files - absolute patterns handled automatically
    let matches = utilities::glob::collect_host_matches(&config.archived_pattern, ".")
        .await
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string())))?;

    for (path, _captures) in matches {
        if let Ok(metadata) = std::fs::metadata(&path)
            && metadata.is_file()
        {
            files.push(HostFileState {
                path,
                size: metadata.len(),
                is_active: false,
            });
        }
    }

    // Match active file - absolute patterns handled automatically
    let matches = utilities::glob::collect_host_matches(&config.active_pattern, ".")
        .await
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string())))?;

    for (path, _captures) in matches {
        if let Ok(metadata) = std::fs::metadata(&path)
            && metadata.is_file()
        {
            files.push(HostFileState {
                path,
                size: metadata.len(),
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
) -> Result<HashMap<String, PondFileState>, TLogFSError> {
    let mut pond_files = HashMap::new();

    debug!("Reading pond state from: {}", pond_path);

    // Extract State from context to get DataFusion SessionContext
    let state = crate::extract_state(context)?;

    let session_ctx = state
        .session_context()
        .await
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string())))?;

    // Create tinyfs to navigate to pond_path and list directory entries
    let fs = tinyfs::FS::new(state.clone())
        .await
        .map_err(TLogFSError::TinyFS)?;
    let root = fs.root().await.map_err(TLogFSError::TinyFS)?;

    // Navigate to the pond directory
    let pond_dir = match root.open_dir_path(pond_path).await {
        Ok(wd) => wd,
        Err(tinyfs::Error::NotFound(_)) => {
            // Directory doesn't exist yet - return empty state
            debug!("Pond directory '{}' doesn't exist yet", pond_path);
            return Ok(pond_files);
        }
        Err(e) => {
            return Err(TLogFSError::TinyFS(e));
        }
    };

    // Get directory node_id which becomes part_id for children
    let dir_file_id = pond_dir.node_path().id();
    let part_id = tinyfs::PartID::from_node_id(dir_file_id.node_id());

    // List directory entries to get filename → FileID mapping
    use futures::StreamExt;
    let mut entries_stream = pond_dir.entries().await.map_err(TLogFSError::TinyFS)?;

    let mut filename_to_file_id: HashMap<String, FileID> = HashMap::new();
    while let Some(entry_result) = entries_stream.next().await {
        let entry = entry_result.map_err(TLogFSError::TinyFS)?;
        // Only include file entries (not directories)
        if entry.entry_type.is_file() {
            // Construct FileID from parent's part_id and child's node_id
            let file_id = FileID::new_from_ids(part_id, entry.child_node_id);
            let _ = filename_to_file_id.insert(entry.name.clone(), file_id);
        }
    }

    if filename_to_file_id.is_empty() {
        debug!("Pond directory '{}' is empty", pond_path);
        return Ok(pond_files);
    }

    // Build reverse map: node_id_string → filename (for looking up from delta_table results)
    let node_to_filename: HashMap<String, String> = filename_to_file_id
        .iter()
        .map(|(name, file_id)| (file_id.node_id().to_string(), name.clone()))
        .collect();

    // Query delta_table for files in this directory (part_id = dir_node_id)
    // Get the latest version of each file
    let sql = format!(
        "SELECT node_id, version, size, blake3, bao_outboard \
         FROM delta_table \
         WHERE part_id = '{}' \
           AND file_type IN ('file:physical', 'file:series:physical') \
         ORDER BY node_id, version DESC",
        dir_file_id.node_id() // Use the directory's node_id as the part_id for children
    );

    debug!("Querying pond state: {}", sql);

    let df = session_ctx
        .sql(&sql)
        .await
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string())))?;

    let batches: Vec<_> = df
        .collect()
        .await
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string())))?;

    // Process results, keeping only the latest version per node_id
    let mut seen_nodes = std::collections::HashSet::new();

    for batch in &batches {
        let node_id_col = batch
            .column_by_name("node_id")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                TLogFSError::TinyFS(tinyfs::Error::Other("Missing node_id column".to_string()))
            })?;

        let version_col = batch
            .column_by_name("version")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| {
                TLogFSError::TinyFS(tinyfs::Error::Other("Missing version column".to_string()))
            })?;

        let size_col = batch
            .column_by_name("size")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>());

        let blake3_col = batch
            .column_by_name("blake3")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());

        let bao_col = batch
            .column_by_name("bao_outboard")
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

            // Require blake3 and bao_outboard - fail fast if missing
            let blake3 = match blake3_col {
                Some(col) if !col.is_null(row) => col.value(row).to_string(),
                _ => {
                    return Err(TLogFSError::TinyFS(tinyfs::Error::Other(format!(
                        "Pond file {} missing required blake3 hash",
                        node_id_str
                    ))));
                }
            };

            let bao_outboard = match bao_col {
                Some(col) if !col.is_null(row) => col.value(row).to_vec(),
                _ => {
                    return Err(TLogFSError::TinyFS(tinyfs::Error::Other(format!(
                        "Pond file {} missing required bao_outboard",
                        node_id_str
                    ))));
                }
            };

            // Look up filename and FileID from directory entries
            let filename = match node_to_filename.get(node_id_str) {
                Some(name) => name.clone(),
                None => {
                    // Node exists in delta_table but not in directory - orphaned, skip it
                    debug!(
                        "Skipping orphaned node_id {} (not in directory)",
                        node_id_str
                    );
                    continue;
                }
            };

            // Get the FileID we already constructed from directory entries
            let file_id = *filename_to_file_id
                .get(&filename)
                .expect("filename must exist in map since we got it from node_to_filename");

            let _ = pond_files.insert(
                filename,
                PondFileState {
                    node_id: file_id,
                    version,
                    size,
                    blake3,
                    bao_outboard,
                    cumulative_size: size, // For series, this would be sum of all versions
                },
            );
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
) -> Result<(), TLogFSError> {
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

                ingest_append(context, config, host_file, pond_state).await?;
            } else if host_file.size < pond_state.cumulative_size {
                warn!(
                    "Active file {} SHRUNK from {} to {} bytes - unexpected!",
                    filename, pond_state.cumulative_size, host_file.size
                );
            } else {
                debug!(
                    "Active file {} unchanged ({} bytes)",
                    filename, host_file.size
                );
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
) -> Result<(), TLogFSError> {
    let filename = host_file
        .path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| TLogFSError::TinyFS(tinyfs::Error::Other("Invalid filename".to_string())))?;

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

            if host_hash.to_hex().to_string() != pond_state.blake3 {
                let host_hash_str = host_hash.to_hex().to_string();
                return Err(TLogFSError::TinyFS(tinyfs::Error::Other(format!(
                    "Archived file {} CHANGED - violates immutability assumption! \
                         Expected blake3={}, got blake3={}, size={} bytes",
                    filename, pond_state.blake3, host_hash_str, host_file.size
                ))));
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
) -> Result<(), TLogFSError> {
    let content = std::fs::read(&host_file.path)
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string())))?;
    let filename = host_file
        .path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| TLogFSError::TinyFS(tinyfs::Error::Other("Invalid filename".to_string())))?;

    let blake3_hash = blake3::hash(&content);

    let pond_dest = format!("{}/{}", config.pond_path, filename);

    info!(
        "Ingesting new file: {} -> {} ({} bytes, blake3={})",
        host_file.path.display(),
        pond_dest,
        content.len(),
        &blake3_hash.to_hex()[..16]
    );

    // Extract State from context to access tinyfs
    let state = crate::extract_state(context)?;

    // Create filesystem from state
    let fs = tinyfs::FS::new(state.clone())
        .await
        .map_err(TLogFSError::TinyFS)?;
    let root = fs.root().await.map_err(TLogFSError::TinyFS)?;

    // Ensure the pond directory exists (create all parent directories as needed)
    let _ = root
        .create_dir_all(&config.pond_path)
        .await
        .map_err(TLogFSError::TinyFS)?;

    // Write file - tinyfs will compute bao_outboard automatically
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path(&pond_dest)
        .await
        .map_err(TLogFSError::TinyFS)?;

    // Write content and finalize
    writer
        .write_all(&content)
        .await
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string())))?;
    writer
        .shutdown()
        .await
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string())))?;

    info!("Wrote file to pond: {}", pond_dest);

    Ok(())
}

/// Ingest an append to an existing active file
async fn ingest_append(
    context: &FactoryContext,
    config: &LogfileIngestConfig,
    host_file: &HostFileState,
    pond_state: &PondFileState,
) -> Result<(), TLogFSError> {
    let filename = host_file
        .path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| TLogFSError::TinyFS(tinyfs::Error::Other("Invalid filename".to_string())))?;

    let pond_dest = format!("{}/{}", config.pond_path, filename);

    // Extract State from context to access tinyfs (once for all operations)
    let state = crate::extract_state(context)?;
    let fs = tinyfs::FS::new(state.clone())
        .await
        .map_err(TLogFSError::TinyFS)?;
    let root = fs.root().await.map_err(TLogFSError::TinyFS)?;

    // Read only the new bytes from host file
    let mut file = std::fs::File::open(&host_file.path)
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string())))?;
    use std::io::{Read, Seek, SeekFrom};
    let _ = file
        .seek(SeekFrom::Start(pond_state.cumulative_size))
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string())))?;

    let mut new_content = Vec::new();
    let _ = file
        .read_to_end(&mut new_content)
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string())))?;

    info!(
        "Ingesting append to {}: {} new bytes (total will be {})",
        filename,
        new_content.len(),
        pond_state.cumulative_size + new_content.len() as u64
    );

    // Deserialize previous outboard - required for append verification
    let prev_series = SeriesOutboard::from_bytes(&pond_state.bao_outboard)
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string())))?;

    // Always verify prefix hasn't changed before appending
    {
        // Read the prefix from host file to verify
        let mut prefix_file = std::fs::File::open(&host_file.path)
            .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string())))?;
        let mut prefix_content = vec![0u8; pond_state.cumulative_size as usize];
        prefix_file
            .read_exact(&mut prefix_content)
            .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string())))?;

        verify_prefix(
            &prefix_content,
            &prev_series.cumulative_outboard,
            pond_state.cumulative_size,
        )
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string())))?;

        info!("Prefix verification passed for {}", filename);
    }

    // Write new version - tinyfs will compute bao_outboard automatically
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path(&pond_dest)
        .await
        .map_err(TLogFSError::TinyFS)?;

    // Write only the new content (as a new version in the FilePhysicalSeries)
    // The ChainedReader will concatenate all versions when reading
    writer
        .write_all(&new_content)
        .await
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string())))?;
    writer
        .shutdown()
        .await
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(e.to_string())))?;

    info!(
        "Wrote append to pond: {} version {}",
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
        };

        assert!(config.validate().is_err());
    }
}

#[cfg(test)]
mod test {
    //! Integration tests for logfile snapshotting and incremental verification
    //!
    //! This test simulates a growing logfile and verifies that:
    //! 1. Initial snapshots capture the full file correctly
    //! 2. Appends are detected and ingested incrementally
    //! 3. Incremental checksum verification (bao-tree) works correctly
    //! 4. Content is correctly copied into the pond

    use super::LogfileIngestConfig;
    use provider::{ExecutionContext, FactoryContext};
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tokio::fs;

    /// Test fixture for simulating a growing logfile
    struct LogfileSimulator {
        _temp_dir: TempDir,
        log_path: PathBuf,
        content: Vec<u8>,
    }

    impl LogfileSimulator {
        /// Create a new simulator with an empty logfile
        async fn new() -> std::io::Result<Self> {
            let temp_dir = TempDir::new()?;
            let log_path = temp_dir.path().join("app.log");

            // Create empty file
            fs::write(&log_path, b"").await?;

            Ok(Self {
                _temp_dir: temp_dir,
                log_path,
                content: Vec::new(),
            })
        }

        /// Append a line to the logfile
        async fn append_line(&mut self, line: &str) -> std::io::Result<()> {
            let mut line_bytes = line.as_bytes().to_vec();
            line_bytes.push(b'\n');

            self.content.extend_from_slice(&line_bytes);

            // Append to file
            use tokio::io::AsyncWriteExt;
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(&self.log_path)
                .await?;
            file.write_all(&line_bytes).await?;
            file.flush().await?;

            Ok(())
        }

        /// Get the current size of the logfile
        fn size(&self) -> u64 {
            self.content.len() as u64
        }

        /// Get the full content for verification
        fn content(&self) -> &[u8] {
            &self.content
        }

        /// Get the path to the logfile
        fn path(&self) -> &PathBuf {
            &self.log_path
        }

        /// Compute blake3 hash of current content
        fn blake3(&self) -> String {
            blake3::hash(&self.content).to_hex().to_string()
        }
    }

    /// Test fixture for pond state
    struct PondSimulator {
        _temp_dir: TempDir,
        store_path: String,
        /// FileID of the config node (created in new())
        config_file_id: tinyfs::FileID,
    }

    impl PondSimulator {
        /// Create a new pond with a config file for logfile-ingest factory
        async fn new(config: &LogfileIngestConfig) -> std::io::Result<Self> {
            let temp_dir = TempDir::new()?;
            let store_path = temp_dir
                .path()
                .join("pond.db")
                .to_string_lossy()
                .to_string();

            // Initialize pond persistence using tlogfs OpLogPersistence
            let mut persistence = crate::persistence::OpLogPersistence::create_test(&store_path)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            // Create config file in the pond and associate with factory
            let config_file_id = {
                let tx = persistence
                    .begin_test()
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

                let state = tx
                    .state()
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

                let fs = tinyfs::FS::new(state)
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                let root = fs
                    .root()
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

                // Create /etc/system.d/ directory structure
                _ = root
                    .create_dir_path("/etc")
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                _ = root
                    .create_dir_path("/etc/system.d")
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

                // Serialize config to YAML
                let config_yaml = serde_yaml::to_string(&config)
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

                // Create config file with factory association
                let config_node = root
                    .create_dynamic_path(
                        "/etc/system.d/logfile-ingest.yaml",
                        tinyfs::EntryType::FileDynamic,
                        "logfile-ingest",
                        config_yaml.as_bytes().to_vec(),
                    )
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

                let file_id = config_node.id();

                tx.commit_test()
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

                file_id
            };

            Ok(Self {
                _temp_dir: temp_dir,
                store_path,
                config_file_id,
            })
        }

        /// Get store path
        fn store_path(&self) -> &str {
            &self.store_path
        }

        /// Execute the logfile-ingest factory within a transaction
        /// This properly opens a transaction, creates the context, executes, and commits
        async fn execute_factory(&self, config: &LogfileIngestConfig) -> std::io::Result<()> {
            let mut persistence = crate::persistence::OpLogPersistence::open(self.store_path())
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let tx = persistence
                .begin_test()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            // Get State from transaction - it implements PersistenceLayer
            let state = tx
                .state()
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            // Create ProviderContext from State
            let provider_ctx = state.as_provider_context();

            // Create FactoryContext with real config FileID
            let factory_ctx = FactoryContext::new(provider_ctx, self.config_file_id);

            // Execute the factory
            let exec_ctx = ExecutionContext::pond_readwriter(vec![]);
            crate::factories::logfile_ingest::execute(
                serde_json::to_value(config).map_err(|e| std::io::Error::other(e.to_string()))?,
                factory_ctx,
                exec_ctx,
            )
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

            // Commit the transaction to persist changes
            tx.commit_test()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            Ok(())
        }

        /// Verify a file exists in the pond with expected properties
        async fn verify_file(
            &self,
            pond_path: &str,
            filename: &str,
            expected_size: u64,
            expected_blake3: &str,
            expected_content: &[u8],
        ) -> std::io::Result<()> {
            let mut persistence = crate::persistence::OpLogPersistence::open(self.store_path())
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let tx = persistence
                .begin_test()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let root = tx
                .root()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let file_path = format!("{}/{}", pond_path, filename);
            let file_node = root
                .get_node_path(&file_path)
                .await
                .map_err(|e| std::io::Error::other(format!("File not found: {}", e)))?;
            let file = file_node
                .as_file()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            // Read content
            use tokio::io::AsyncReadExt;
            let mut content = Vec::new();
            let mut reader = file
                .async_reader()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            let _ = reader.read_to_end(&mut content).await?;

            tx.commit_test()
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            // Verify size
            assert_eq!(
                content.len() as u64,
                expected_size,
                "File size mismatch: expected {}, got {}",
                expected_size,
                content.len()
            );

            // Verify blake3
            let actual_blake3 = blake3::hash(&content).to_hex().to_string();
            assert_eq!(actual_blake3, expected_blake3, "Blake3 hash mismatch");

            // Verify byte-by-byte content
            assert_eq!(
                content.as_slice(),
                expected_content,
                "Content mismatch: bytes differ"
            );

            Ok(())
        }
    }

    #[tokio::test]
    async fn test_single_logfile_growth_and_snapshots() {
        // Setup: Create a simulated logfile first
        let mut logfile = LogfileSimulator::new().await.unwrap();

        // Configuration for logfile ingestion (needs logfile path)
        let config = LogfileIngestConfig {
            name: "test_app".to_string(),
            active_pattern: logfile.path().to_string_lossy().to_string(),
            archived_pattern: "".to_string(), // No archived files for this test
            pond_path: "logs/test_app".to_string(),
        };

        // Create pond with the config (creates real config file with FileID)
        let pond = PondSimulator::new(&config).await.unwrap();

        // Step 1: Append initial content to logfile
        logfile
            .append_line("2025-01-03 00:00:00 INFO Starting application")
            .await
            .unwrap();
        logfile
            .append_line("2025-01-03 00:00:01 INFO Initialized database")
            .await
            .unwrap();
        logfile
            .append_line("2025-01-03 00:00:02 INFO Listening on port 8080")
            .await
            .unwrap();

        let size_v1 = logfile.size();
        let blake3_v1 = logfile.blake3();

        println!("Logfile v1: {} bytes, blake3={}", size_v1, &blake3_v1[..16]);

        // Step 2: First ingestion - should capture entire file as v1
        pond.execute_factory(&config).await.unwrap();

        // Verify: File should exist in pond with correct content
        pond.verify_file(
            "logs/test_app",
            "app.log",
            size_v1,
            &blake3_v1,
            logfile.content(),
        )
        .await
        .unwrap();

        println!("✓ Initial snapshot successful");

        // Step 3: Append more content to logfile
        logfile
            .append_line("2025-01-03 00:01:00 INFO Processed 100 requests")
            .await
            .unwrap();
        logfile
            .append_line("2025-01-03 00:02:00 INFO Processed 200 requests")
            .await
            .unwrap();
        logfile
            .append_line("2025-01-03 00:03:00 WARN Connection timeout for client 127.0.0.1")
            .await
            .unwrap();

        let size_v2 = logfile.size();
        let blake3_v2 = logfile.blake3();
        let appended_bytes = size_v2 - size_v1;

        println!(
            "Logfile v2: {} bytes (+{} appended), blake3={}",
            size_v2,
            appended_bytes,
            &blake3_v2[..16]
        );

        // Step 4: Second ingestion - should detect append and create v2
        pond.execute_factory(&config).await.unwrap();

        // Verify: File should have updated content
        pond.verify_file(
            "logs/test_app",
            "app.log",
            size_v2,
            &blake3_v2,
            logfile.content(),
        )
        .await
        .unwrap();

        println!("✓ Incremental append successful");

        // Step 5: Append even more content
        logfile
            .append_line("2025-01-03 00:04:00 INFO Processed 300 requests")
            .await
            .unwrap();
        logfile
            .append_line("2025-01-03 00:05:00 ERROR Database connection failed")
            .await
            .unwrap();
        logfile
            .append_line("2025-01-03 00:05:01 INFO Reconnected to database")
            .await
            .unwrap();
        logfile
            .append_line("2025-01-03 00:06:00 INFO Processed 400 requests")
            .await
            .unwrap();

        let size_v3 = logfile.size();
        let blake3_v3 = logfile.blake3();
        let appended_bytes_v3 = size_v3 - size_v2;

        println!(
            "Logfile v3: {} bytes (+{} appended), blake3={}",
            size_v3,
            appended_bytes_v3,
            &blake3_v3[..16]
        );

        // Step 6: Third ingestion - should detect second append and create v3
        pond.execute_factory(&config).await.unwrap();

        // Verify: File should have latest content
        pond.verify_file(
            "logs/test_app",
            "app.log",
            size_v3,
            &blake3_v3,
            logfile.content(),
        )
        .await
        .unwrap();

        println!("✓ Second incremental append successful");
        println!("✓ All snapshots and verifications passed!");
    }

    #[tokio::test]
    async fn test_logfile_no_change_detection() {
        // Test that running ingestion multiple times without file changes is idempotent
        let mut logfile = LogfileSimulator::new().await.unwrap();

        let config = LogfileIngestConfig {
            name: "test_app".to_string(),
            active_pattern: logfile.path().to_string_lossy().to_string(),
            archived_pattern: "".to_string(),
            pond_path: "logs/test_app".to_string(),
        };

        // Create pond with config (gets real FileID)
        let pond = PondSimulator::new(&config).await.unwrap();

        // Append some content
        logfile
            .append_line("2025-01-03 00:00:00 INFO Test line 1")
            .await
            .unwrap();
        logfile
            .append_line("2025-01-03 00:00:01 INFO Test line 2")
            .await
            .unwrap();

        let size = logfile.size();
        let blake3 = logfile.blake3();

        // First ingestion
        pond.execute_factory(&config).await.unwrap();

        // Second ingestion - no changes
        pond.execute_factory(&config).await.unwrap();

        // Third ingestion - still no changes
        pond.execute_factory(&config).await.unwrap();

        // Verify: File should still have same content
        pond.verify_file("logs/test_app", "app.log", size, &blake3, logfile.content())
            .await
            .unwrap();

        println!("✓ Idempotency test passed - multiple ingestions without changes work correctly");
    }
}
