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
use log::{debug, info, warn};
use provider::{register_executable_factory, ExecutionContext, FactoryContext};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
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

/// Enumerate files matching the configured patterns
fn enumerate_host_files(config: &LogfileIngestConfig) -> std::io::Result<Vec<HostFileState>> {
    let mut files = Vec::new();

    // Match archived files
    for entry in glob::glob(&config.archived_pattern)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?
    {
        let path = entry.map_err(std::io::Error::other)?;
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
    for entry in glob::glob(&config.active_pattern)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?
    {
        let path = entry.map_err(std::io::Error::other)?;
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
async fn read_pond_state(
    _context: &FactoryContext,
    pond_path: &str,
) -> std::io::Result<HashMap<String, PondFileState>> {
    let pond_files = HashMap::new();

    // Query the pond directory for existing files
    // This is a simplified implementation - in production, we'd query the OplogEntry table
    debug!("Reading pond state from: {}", pond_path);
    
    // TODO: Use context to query OplogEntry table via DataFusion
    // For now, return empty state (all files will appear as new)
    
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
    _context: &FactoryContext,
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
    
    // Compute bao-tree outboard (verification happens via SeriesOutboard)
    let _ = compute_outboard(&content);
    
    let pond_dest = format!("{}/{}", config.pond_path, filename);
    
    info!(
        "Ingesting new file: {} -> {} ({} bytes, blake3={})",
        host_file.path.display(),
        pond_dest,
        content.len(),
        &blake3_hash.to_hex()[..16]
    );

    // Create SeriesOutboard for inline content
    let series_outboard = if host_file.is_active {
        SeriesOutboard::first_version_inline(&content)
    } else {
        // Archived files use VersionOutboard
        SeriesOutboard::first_version_inline(&content)
    };

    let bao_bytes = series_outboard.to_bytes();
    
    // TODO: Use context to write to pond via persistence layer
    // For now, just log what we would do
    debug!(
        "Would write OplogEntry: path={}, size={}, blake3={}, bao_outboard_len={}",
        pond_dest,
        content.len(),
        blake3_hash.to_hex(),
        bao_bytes.len()
    );

    Ok(())
}

/// Ingest an append to an existing active file
async fn ingest_append(
    _context: &FactoryContext,
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

    // Read only the new bytes
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

    if is_strict {
        // Verify prefix hasn't changed
        if let Some(prev_outboard) = &pond_state.bao_outboard {
            // Read the prefix to verify
            let mut prefix_file = std::fs::File::open(&host_file.path)?;
            let mut prefix_content = vec![0u8; pond_state.cumulative_size as usize];
            prefix_file.read_exact(&mut prefix_content)?;
            
            // Deserialize previous outboard
            let prev_series = SeriesOutboard::from_bytes(prev_outboard)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            
            verify_prefix(&prefix_content, &prev_series.cumulative_outboard, pond_state.cumulative_size)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            
            info!("Prefix verification passed for {}", filename);
        }
    }

    // Compute new SeriesOutboard for the appended content
    // TODO: Use append_to_outboard for incremental computation
    let full_content = std::fs::read(&host_file.path)?;
    let new_series_outboard = SeriesOutboard::first_version_inline(&full_content);
    let bao_bytes = new_series_outboard.to_bytes();

    let pond_dest = format!("{}/{}", config.pond_path, filename);
    
    debug!(
        "Would write OplogEntry version: path={}, version={}, new_bytes={}, bao_outboard_len={}",
        pond_dest,
        pond_state.version + 1,
        new_content.len(),
        bao_bytes.len()
    );

    // TODO: Use context to write to pond via persistence layer

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
