// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Logfile Ingestion Factory (Persistence-Agnostic)
//!
//! This factory ingests rotating log files from a host directory into the pond.
//! It tracks files with bao-tree blake3 digests for efficient change detection
//! and supports both archived (immutable) and active (append-only) files.
//!
//! This implementation is persistence-agnostic - it works with both:
//! - `MemoryPersistence` for fast testing
//! - `OpLogPersistence` (tlogfs) for production

use crate::{ExecutionContext, FactoryContext, register_executable_factory};
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use tinyfs::{EntryType, FileID, Result as TinyFSResult};
use utilities::bao_outboard::IncrementalHashState;

/// Configuration for the logfile ingestion factory
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogfileIngestConfig {
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
    version: u64,
    /// File size in bytes
    size: u64,
    /// Blake3 hash of the content (bao-tree root, computed by tinyfs)
    blake3: String,
    /// Cumulative size (for FilePhysicalSeries)
    cumulative_size: u64,
}

/// Initialize factory (called once per dynamic node creation)
async fn initialize(_config: Value, _context: FactoryContext) -> Result<(), tinyfs::Error> {
    // No initialization needed for executable factory
    Ok(())
}

/// Execute the log ingestion process
#[cfg_attr(test, allow(dead_code))] // Allow in tests even if not directly called
pub async fn execute(
    config: Value,
    context: FactoryContext,
    ctx: ExecutionContext,
) -> Result<(), tinyfs::Error> {
    let config: LogfileIngestConfig = serde_json::from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid config: {}", e)))?;

    info!(
        "Starting logfile ingestion for (mode: {:?})",
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

    // Step 2: Read pond state (persistence-agnostic)
    let pond_files = read_pond_state(&context, &config.pond_path).await?;
    info!("Found {} files in pond", pond_files.len());

    // Step 3: Detect rotation - must happen BEFORE processing individual files
    // Rotation is detected when:
    // - Active file shrunk (size < pond's cumulative_size)
    // - Active file content doesn't match pond's prefix (rotation to same/larger size file)
    // - A new archived file exists that matches pond's tracked content
    let active_host_file = host_files.iter().find(|f| f.is_active);
    if let Some(host_active) = active_host_file {
        let active_filename = host_active
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");

        if let Some(pond_active) = pond_files.get(active_filename) {
            // Check if rotation might have occurred:
            // 1. File shrunk (classic case)
            // 2. File same size or larger, but content doesn't match prefix
            let might_be_rotated = if host_active.size < pond_active.cumulative_size {
                info!(
                    "Active file {} shrunk from {} to {} bytes - checking for rotation",
                    active_filename, pond_active.cumulative_size, host_active.size
                );
                true
            } else if pond_active.cumulative_size > 0 {
                // Content mismatch detection: compute blake3 of prefix and compare
                // Read prefix from host file (first cumulative_size bytes)
                let prefix_len = pond_active.cumulative_size as usize;
                if host_active.size >= pond_active.cumulative_size {
                    let mut prefix_file = std::fs::File::open(&host_active.path)
                        .map_err(|e| tinyfs::Error::Other(e.to_string()))?;
                    let mut prefix_content = vec![0u8; prefix_len];
                    use std::io::Read;
                    prefix_file.read_exact(&mut prefix_content)
                        .map_err(|e| tinyfs::Error::Other(e.to_string()))?;
                    
                    // Compute blake3 of prefix using same method as tinyfs (bao-tree root)
                    let mut hasher = IncrementalHashState::new();
                    hasher.ingest(&prefix_content);
                    let prefix_blake3 = hasher.root_hash().to_hex().to_string();
                    
                    // Compare to pond's stored blake3
                    if prefix_blake3 == pond_active.blake3 {
                        debug!("Active file {} prefix matches - no rotation", active_filename);
                        false
                    } else {
                        info!(
                            "Active file {} content mismatch (prefix blake3 {} != pond blake3 {}) - checking for rotation",
                            active_filename, &prefix_blake3[..16], &pond_active.blake3[..16]
                        );
                        true
                    }
                } else {
                    // File smaller than what we tracked - definitely rotated
                    true
                }
            } else {
                false
            };

            if might_be_rotated {
                // Find new archived files (not in pond) that might match
                let new_archived: Vec<_> = host_files
                    .iter()
                    .filter(|f| !f.is_active)
                    .filter(|f| {
                        let filename = f.path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                        !pond_files.contains_key(filename)
                    })
                    .collect();

                if !new_archived.is_empty() {
                    // Try to find a match using prefix verification
                    if let Some(matched_archived) =
                        find_rotated_file(&new_archived, pond_active).await?
                    {
                        let archived_filename = matched_archived
                            .path
                            .file_name()
                            .and_then(|n| n.to_str())
                            .ok_or_else(|| {
                                tinyfs::Error::Other("Invalid archived filename".to_string())
                            })?;

                        info!(
                            "Rotation detected: {} -> {} (matched content)",
                            active_filename, archived_filename
                        );

                        // FIRST: Append any missed bytes to the ACTIVE pond file
                        // (before renaming, so TinyFS computes correct cumulative checksums)
                        if matched_archived.size > pond_active.cumulative_size {
                            let missed_bytes = matched_archived.size - pond_active.cumulative_size;
                            info!(
                                "Appending {} missed bytes to active file {} before rename (grew from {} to {} bytes)",
                                missed_bytes, active_filename, pond_active.cumulative_size, matched_archived.size
                            );
                            
                            // Read the full archived file content, append only the new portion
                            let content = std::fs::read(&matched_archived.path)
                                .map_err(|e| tinyfs::Error::Other(e.to_string()))?;
                            let new_data = &content[pond_active.cumulative_size as usize..];
                            
                            // Append to the ACTIVE pond file (TinyFS handles checksums)
                            append_to_active_pond_file(&context, &config, active_filename, new_data).await?;
                        }

                        // THEN: Rename the (now complete) active pond file to archived name
                        rename_pond_file(&context, &config, active_filename, archived_filename)
                            .await?;

                        info!(
                            "Renamed pond file: {} -> {}",
                            active_filename, archived_filename
                        );
                    } else {
                        warn!(
                            "Active file {} changed but no matching archived file found",
                            active_filename
                        );
                    }
                }
            }
        }
    }

    // Re-read pond state after rotation handling to get fresh metadata from TinyFS
    // (TinyFS has computed new blake3 after any appends)
    let pond_files = read_pond_state(&context, &config.pond_path).await?;

    // Step 4: Process all files (with updated pond state after any rotation handling)
    for host_file in &host_files {
        let filename = host_file
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| tinyfs::Error::Other("Invalid filename".to_string()))?;

        let pond_file = pond_files.get(filename);

        if host_file.is_active {
            // Active file: detect appends or ingest new after rotation
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
) -> Result<Vec<HostFileState>, tinyfs::Error> {
    let mut files = Vec::new();

    // Match archived files - absolute patterns handled automatically
    let matches = utilities::glob::collect_host_matches(&config.archived_pattern, ".")
        .await
        .map_err(|e| tinyfs::Error::Other(e.to_string()))?;

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
        .map_err(|e| tinyfs::Error::Other(e.to_string()))?;

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
/// Uses the persistence-agnostic `metadata()` method to read file state.
/// Works with both MemoryPersistence and OpLogPersistence.
async fn read_pond_state(
    context: &FactoryContext,
    pond_path: &str,
) -> Result<HashMap<String, PondFileState>, tinyfs::Error> {
    let mut pond_files = HashMap::new();

    debug!("Reading pond state from: {}", pond_path);

    // Get filesystem from ProviderContext (persistence-agnostic)
    let fs = context.context.filesystem();
    let root = fs.root().await?;

    // Navigate to the pond directory
    let pond_dir = match root.open_dir_path(pond_path).await {
        Ok(wd) => wd,
        Err(tinyfs::Error::NotFound(_)) => {
            // Directory doesn't exist yet - return empty state
            debug!("Pond directory '{}' doesn't exist yet", pond_path);
            return Ok(pond_files);
        }
        Err(e) => {
            return Err(e);
        }
    };

    // Get directory node_id which becomes part_id for children
    let dir_file_id = pond_dir.node_path().id();
    let part_id = tinyfs::PartID::from_node_id(dir_file_id.node_id());

    // List directory entries and get metadata for each file
    use futures::StreamExt;
    let mut entries_stream = pond_dir.entries().await?;

    let persistence = context.context.persistence.clone();

    while let Some(entry_result) = entries_stream.next().await {
        let entry = entry_result?;
        
        // Only include file entries (not directories)
        if !entry.entry_type.is_file() {
            continue;
        }

        // Construct FileID from parent's part_id and child's node_id
        let file_id = FileID::new_from_ids(part_id, entry.child_node_id);
        let filename = entry.name.clone();

        // Get metadata from persistence layer (works with any backend)
        let metadata = persistence.metadata(file_id).await?;

        // Require blake3 - fail fast if missing
        let blake3 = metadata.blake3.ok_or_else(|| {
            tinyfs::Error::Other(format!(
                "Pond file {} missing required blake3 hash",
                filename
            ))
        })?;

        // For FilePhysicalSeries, extract cumulative_size from the bao_outboard
        // The metadata.size is just the latest version's size, not cumulative
        // NOTE: We use SeriesOutboard ONLY to get cumulative_size, not for verification
        let cumulative_size = if let Some(bao_outboard) = &metadata.bao_outboard {
            match utilities::bao_outboard::SeriesOutboard::from_bytes(bao_outboard) {
                Ok(series) => series.cumulative_size,
                Err(_) => metadata.size.unwrap_or(0),
            }
        } else {
            metadata.size.unwrap_or(0)
        };

        let size = metadata.size.unwrap_or(0);

        let _ = pond_files.insert(
            filename,
            PondFileState {
                node_id: file_id,
                version: metadata.version,
                size,
                blake3,
                cumulative_size,
            },
        );
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
) -> Result<(), tinyfs::Error> {
    let filename = host_file
        .path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| tinyfs::Error::Other("Invalid filename".to_string()))?;

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
) -> Result<(), tinyfs::Error> {
    let filename = host_file
        .path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| tinyfs::Error::Other("Invalid filename".to_string()))?;

    match pond_file {
        None => {
            // New archived file
            info!("New archived file detected: {}", filename);
            ingest_new_file(context, config, host_file).await?;
        }
        Some(pond_state) => {
            // Verify archived file hasn't changed (should be immutable)
            // Use bao-tree root hash (IncrementalHashState), not simple blake3::hash
            // because metadata.blake3 stores the cumulative bao-tree root
            let host_content = std::fs::read(&host_file.path)
                .map_err(|e| tinyfs::Error::Other(e.to_string()))?;
            
            let mut state = IncrementalHashState::new();
            state.ingest(&host_content);
            let host_hash = state.root_hash();

            if host_hash.to_hex().to_string() != pond_state.blake3 {
                let host_hash_str = host_hash.to_hex().to_string();
                return Err(tinyfs::Error::Other(format!(
                    "Archived file {} CHANGED - violates immutability assumption! \
                     Expected blake3={}, got blake3={}, size={} bytes",
                    filename, pond_state.blake3, host_hash_str, host_file.size
                )));
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
) -> Result<(), tinyfs::Error> {
    let content = std::fs::read(&host_file.path)
        .map_err(|e| tinyfs::Error::Other(e.to_string()))?;
    let filename = host_file
        .path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| tinyfs::Error::Other("Invalid filename".to_string()))?;

    let blake3_hash = blake3::hash(&content);

    let pond_dest = format!("{}/{}", config.pond_path, filename);

    info!(
        "Ingesting new file: {} -> {} ({} bytes, blake3={})",
        host_file.path.display(),
        pond_dest,
        content.len(),
        &blake3_hash.to_hex()[..16]
    );

    // Get filesystem from ProviderContext (persistence-agnostic)
    let fs = context.context.filesystem();
    let root = fs.root().await?;

    // Ensure the pond directory exists (create all parent directories as needed)
    let _ = root.create_dir_all(&config.pond_path).await?;

    // Write file as FilePhysicalSeries - enables cumulative bao_outboard for append detection
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type(&pond_dest, EntryType::FilePhysicalSeries)
        .await?;

    // Write content and finalize
    writer
        .write_all(&content)
        .await
        .map_err(|e| tinyfs::Error::Other(e.to_string()))?;
    writer
        .shutdown()
        .await
        .map_err(|e| tinyfs::Error::Other(e.to_string()))?;

    info!("Wrote file to pond: {}", pond_dest);

    Ok(())
}

/// Ingest an append to an existing active file
async fn ingest_append(
    context: &FactoryContext,
    config: &LogfileIngestConfig,
    host_file: &HostFileState,
    pond_state: &PondFileState,
) -> Result<(), tinyfs::Error> {
    let filename = host_file
        .path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| tinyfs::Error::Other("Invalid filename".to_string()))?;

    let pond_dest = format!("{}/{}", config.pond_path, filename);

    // Get filesystem from ProviderContext (persistence-agnostic)
    let fs = context.context.filesystem();
    let root = fs.root().await?;

    // Read only the new bytes from host file
    let mut file = std::fs::File::open(&host_file.path)
        .map_err(|e| tinyfs::Error::Other(e.to_string()))?;
    use std::io::{Read, Seek, SeekFrom};
    let _ = file
        .seek(SeekFrom::Start(pond_state.cumulative_size))
        .map_err(|e| tinyfs::Error::Other(e.to_string()))?;

    let mut new_content = Vec::new();
    let _ = file
        .read_to_end(&mut new_content)
        .map_err(|e| tinyfs::Error::Other(e.to_string()))?;

    info!(
        "Ingesting append to {}: {} new bytes (total will be {})",
        filename,
        new_content.len(),
        pond_state.cumulative_size + new_content.len() as u64
    );

    // Verify prefix hasn't changed before appending (using simple blake3 comparison)
    {
        // Read the prefix from host file
        let mut prefix_file = std::fs::File::open(&host_file.path)
            .map_err(|e| tinyfs::Error::Other(e.to_string()))?;
        let mut prefix_content = vec![0u8; pond_state.cumulative_size as usize];
        prefix_file
            .read_exact(&mut prefix_content)
            .map_err(|e| tinyfs::Error::Other(e.to_string()))?;

        // Compute blake3 of prefix (using same method as TinyFS - bao-tree root)
        let mut hasher = IncrementalHashState::new();
        hasher.ingest(&prefix_content);
        let prefix_blake3 = hasher.root_hash().to_hex().to_string();

        // Compare to pond's stored blake3
        if prefix_blake3 != pond_state.blake3 {
            return Err(tinyfs::Error::Other(format!(
                "Prefix verification failed for {}: expected blake3={}, got blake3={}",
                filename, pond_state.blake3, prefix_blake3
            )));
        }

        info!("Prefix verification passed for {}", filename);
    }

    // Write new version as FilePhysicalSeries
    // TinyFS automatically maintains cumulative blake3 and bao_outboard
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type(&pond_dest, EntryType::FilePhysicalSeries)
        .await?;

    // Write only the new content (as a new version in the FilePhysicalSeries)
    // The ChainedReader will concatenate all versions when reading
    writer
        .write_all(&new_content)
        .await
        .map_err(|e| tinyfs::Error::Other(e.to_string()))?;
    writer
        .shutdown()
        .await
        .map_err(|e| tinyfs::Error::Other(e.to_string()))?;

    info!(
        "Wrote append to pond: {} version {}",
        pond_dest,
        pond_state.version + 1
    );

    Ok(())
}

/// Find which archived file matches the pond's tracked content (for rotation detection)
/// Uses simple blake3 comparison: if archived file's prefix matches pond's blake3, it's the rotated file
async fn find_rotated_file<'a>(
    archived_files: &[&'a HostFileState],
    pond_state: &PondFileState,
) -> Result<Option<&'a HostFileState>, tinyfs::Error> {
    let tracked_size = pond_state.cumulative_size as usize;
    
    for host_file in archived_files {
        // File must be at least as large as what we tracked
        if host_file.size < pond_state.cumulative_size {
            continue;
        }
        
        // Read the prefix (first tracked_size bytes)
        let content = std::fs::read(&host_file.path)
            .map_err(|e| tinyfs::Error::Other(e.to_string()))?;
        let prefix = &content[..tracked_size];
        
        // Compute blake3 of prefix using same method as tinyfs (bao-tree root)
        let mut hasher = IncrementalHashState::new();
        hasher.ingest(prefix);
        let prefix_blake3 = hasher.root_hash().to_hex().to_string();
        
        // If prefix matches pond's blake3, this is the rotated file
        if prefix_blake3 == pond_state.blake3 {
            info!(
                "Found rotated file {} matching pond blake3 {}...",
                host_file.path.display(), &pond_state.blake3[..16]
            );
            return Ok(Some(host_file));
        }
    }
    
    Ok(None)
}

/// Rename a file in the pond (preserving version history)
async fn rename_pond_file(
    context: &FactoryContext,
    config: &LogfileIngestConfig,
    old_name: &str,
    new_name: &str,
) -> Result<(), tinyfs::Error> {
    let fs = context.context.filesystem();
    let root = fs.root().await?;

    let old_path = format!("{}/{}", config.pond_path, old_name);
    let new_path = format!("{}/{}", config.pond_path, new_name);

    // Get the directory and rename the entry
    let dir = root.open_dir_path(&config.pond_path).await?;
    dir.rename_entry(old_name, new_name).await?;

    info!("Renamed pond file: {} -> {}", old_path, new_path);

    Ok(())
}

/// Append missed bytes to an active pond file before rename
/// (TinyFS handles cumulative checksum computation via FilePhysicalSeries)
async fn append_to_active_pond_file(
    context: &FactoryContext,
    config: &LogfileIngestConfig,
    filename: &str,
    new_data: &[u8],
) -> Result<(), tinyfs::Error> {
    let pond_dest = format!("{}/{}", config.pond_path, filename);

    // Get filesystem from ProviderContext
    let fs = context.context.filesystem();
    let root = fs.root().await?;

    info!(
        "Appending {} missed bytes to {} before rename",
        new_data.len(),
        filename
    );

    // Write new version as FilePhysicalSeries
    // TinyFS automatically computes cumulative blake3 and bao_outboard
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type(&pond_dest, EntryType::FilePhysicalSeries)
        .await?;

    writer
        .write_all(new_data)
        .await
        .map_err(|e| tinyfs::Error::Other(e.to_string()))?;
    writer
        .shutdown()
        .await
        .map_err(|e| tinyfs::Error::Other(e.to_string()))?;

    info!("Appended missed bytes to pond file: {}", pond_dest);

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
