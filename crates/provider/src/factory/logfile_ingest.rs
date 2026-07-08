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

use crate::{ExecutionContext, ExecutionMode, FactoryContext, register_executable_factory};
use clap::{Parser, Subcommand};
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use tinyfs::ResultExt;
use tinyfs::{EntryType, FileID, Result as TinyFSResult};
use utilities::bao_outboard::IncrementalHashState;

/// Logfile ingest factory subcommands
#[derive(Debug, Parser)]
struct LogfileCommand {
    #[command(subcommand)]
    command: Option<LogfileSubcommand>,
}

#[derive(Debug, Subcommand)]
enum LogfileSubcommand {
    /// Print blake3 checksums in b3sum format
    ///
    /// Outputs checksums compatible with `b3sum --check`.
    /// Use: pond run /config b3sum > checksums.txt
    /// Then: cd /host_dir && b3sum --check checksums.txt
    B3sum,

    /// Sync files from host to pond (automatic mode trigger)
    ///
    /// This is invoked automatically when the factory mode is 'push'.
    /// Same as running with no subcommand.
    Push,

    /// Pull mode (no-op for logfile-ingest)
    ///
    /// Logfile-ingest only ingests files from host to pond.
    /// Pull mode is accepted for compatibility but does nothing.
    Pull,
}

/// Parse command-line arguments into LogfileCommand
fn parse_command(ctx: ExecutionContext) -> Result<LogfileCommand, tinyfs::Error> {
    // Build args list with fake program name for clap
    let args_with_prog_name: Vec<String> = std::iter::once("factory".to_string())
        .chain(ctx.args().iter().cloned())
        .collect();

    LogfileCommand::try_parse_from(args_with_prog_name).map_err(|e| {
        // Print Clap's helpful error message
        // Error will be propagated up
        tinyfs::Error::Other(format!("Command parse error: {}", e))
    })
}

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
    /// Stored bao-tree frontier (rightmost path of complete subtrees) for the
    /// tracked prefix. `Some` for FilePhysicalSeries with a parseable
    /// bao_outboard; `None` for legacy entries without one. When present it lets
    /// prefix verification resume from the committed hashes and read only the
    /// trailing partial block instead of the whole prefix.
    frontier: Option<Vec<(u32, [u8; 32], u64)>>,
}

/// Verify that the host file's tracked prefix still matches the pond's committed
/// cumulative hash, using the stored bao-tree frontier so we read at most one
/// `BLOCK_SIZE` trailing block instead of re-hashing the entire prefix.
///
/// Returns `(matches, host_root_hex)`. `matches` is true when the prefix is
/// intact (a normal append) and false when it changed (a rotation). The host
/// root hash is returned so callers can include it in diagnostics.
///
/// When no frontier is stored (a legacy entry without a bao_outboard) this falls
/// back to the historical full-prefix read so behavior is unchanged for that
/// degraded case. This is a precondition fallback, not a verification fallback:
/// a genuine hash mismatch is still surfaced to the caller as `matches == false`.
fn verify_prefix_matches(
    host_path: &std::path::Path,
    pond_state: &PondFileState,
) -> Result<(bool, String), tinyfs::Error> {
    use std::io::{Read, Seek, SeekFrom};
    use utilities::bao_outboard::{BLOCK_SIZE, IncrementalHashState};

    let cumulative_size = pond_state.cumulative_size;
    if cumulative_size == 0 {
        // Nothing tracked yet; any host content is a fresh prefix.
        return Ok((true, String::new()));
    }

    let host_root = match &pond_state.frontier {
        Some(frontier) => {
            let block = BLOCK_SIZE as u64;
            let pending_start = (cumulative_size / block) * block;
            let pending_len = (cumulative_size % block) as usize;

            let mut file = std::fs::File::open(host_path).map_other()?;
            let _ = file.seek(SeekFrom::Start(pending_start)).map_other()?;
            let mut verified_pending = vec![0u8; pending_len];
            file.read_exact(&mut verified_pending).map_other()?;

            let state = IncrementalHashState::resume(frontier, cumulative_size, &verified_pending)
                .map_other()?;
            state.root_hash().to_hex().to_string()
        }
        None => {
            let mut file = std::fs::File::open(host_path).map_other()?;
            let mut prefix_content = vec![0u8; cumulative_size as usize];
            file.read_exact(&mut prefix_content).map_other()?;
            let mut hasher = IncrementalHashState::new();
            hasher.ingest(&prefix_content);
            hasher.root_hash().to_hex().to_string()
        }
    };

    let matches = host_root == pond_state.blake3;
    Ok((matches, host_root))
}

/// Summary of ingestion activity for logging
#[derive(Debug, Default)]
struct IngestionStats {
    /// New files ingested (count, total bytes)
    new_files: (usize, u64),
    /// Files with appends (count, bytes appended)
    appended: (usize, u64),
    /// Files unchanged (count, total bytes)
    unchanged: (usize, u64),
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
    let config: LogfileIngestConfig =
        serde_json::from_value(config.clone()).map_other_context("Invalid config")?;

    // Parse command (default to sync if no subcommand)
    let cmd = parse_command(ctx)?;

    match cmd.command {
        Some(LogfileSubcommand::B3sum) => {
            return execute_b3sum(&context, &config).await;
        }
        Some(LogfileSubcommand::Pull) => {
            // Pull mode doesn't make sense for logfile-ingest - we only push from host to pond
            info!("logfile-ingest: 'pull' mode is a no-op (files only flow from host to pond)");
            return Ok(());
        }
        Some(LogfileSubcommand::Push) | None => {
            // Push mode or default: sync operation
        }
    }

    info!(
        "Starting logfile ingestion for (mode: {:?})",
        ExecutionMode::PondReadWriter
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
        let active_filename = match host_active.path.file_name().and_then(|n| n.to_str()) {
            Some(name) => name,
            None => {
                log::warn!(
                    "Skipping active file with non-UTF8 name: {:?}",
                    host_active.path
                );
                return Ok(());
            }
        };

        if let Some(pond_active) = pond_files.get(active_filename) {
            // Check if rotation might have occurred:
            // 1. File shrunk (classic case) -> definitely rotated
            // 2. File same size but content differs -> rotated to same-size file (rare)
            // 3. File grew -> normal append, NO prefix check needed here (verified in ingest_append)
            let might_be_rotated = if host_active.size < pond_active.cumulative_size {
                info!(
                    "Active file {} shrunk from {} to {} bytes - checking for rotation",
                    active_filename, pond_active.cumulative_size, host_active.size
                );
                true
            } else if host_active.size == pond_active.cumulative_size
                && pond_active.cumulative_size > 0
            {
                // Same size: could be unchanged OR rotated to a same-size file
                // Must check content to distinguish
                let mut prefix_file = std::fs::File::open(&host_active.path).map_other()?;
                let mut prefix_content = vec![0u8; pond_active.cumulative_size as usize];
                use std::io::Read;
                prefix_file.read_exact(&mut prefix_content).map_other()?;

                let mut hasher = IncrementalHashState::new();
                hasher.ingest(&prefix_content);
                let host_blake3 = hasher.root_hash().to_hex().to_string();

                if host_blake3 == pond_active.blake3 {
                    debug!(
                        "Active file {} unchanged ({} bytes)",
                        active_filename, host_active.size
                    );
                    false
                } else {
                    debug!(
                        "Active file {} same size ({} bytes) but hash differs: host={}, pond={}",
                        active_filename,
                        host_active.size,
                        &host_blake3[..16],
                        &pond_active.blake3[..16]
                    );
                    info!(
                        "Active file {} same size ({} bytes) but content differs - checking for rotation",
                        active_filename, host_active.size
                    );
                    true
                }
            } else {
                // host_active.size > pond_active.cumulative_size
                // Usually a normal append, but could also be a rotation where
                // the new file already grew past the old tracked size.
                // Verify the tracked prefix via the stored frontier (reads only
                // the trailing partial block, not the whole prefix).
                if pond_active.cumulative_size > 0 {
                    let (prefix_matches, host_blake3) =
                        verify_prefix_matches(&host_active.path, pond_active)?;

                    if prefix_matches {
                        debug!(
                            "Active file {} grew from {} to {} bytes - prefix matches, normal append",
                            active_filename, pond_active.cumulative_size, host_active.size
                        );
                        false
                    } else {
                        info!(
                            "Active file {} grew from {} to {} bytes but prefix changed (host root {}) - checking for rotation",
                            active_filename,
                            pond_active.cumulative_size,
                            host_active.size,
                            &host_blake3[..host_blake3.len().min(16)]
                        );
                        true
                    }
                } else {
                    debug!(
                        "Active file {} grew from 0 to {} bytes - first content",
                        active_filename, host_active.size
                    );
                    false
                }
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
                                missed_bytes,
                                active_filename,
                                pond_active.cumulative_size,
                                matched_archived.size
                            );

                            // Read the full archived file content, append only the new portion
                            let content = std::fs::read(&matched_archived.path).map_other()?;
                            let new_data = &content[pond_active.cumulative_size as usize..];

                            // Append to the ACTIVE pond file (TinyFS handles checksums)
                            append_to_active_pond_file(
                                &context,
                                &config,
                                active_filename,
                                new_data,
                            )
                            .await?;
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
    let mut stats = IngestionStats::default();
    for host_file in &host_files {
        let filename = host_file
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| tinyfs::Error::Other("Invalid filename".to_string()))?;

        let pond_file = pond_files.get(filename);

        if host_file.is_active {
            // Active file: detect appends or ingest new after rotation
            process_active_file(&context, &config, host_file, pond_file, &mut stats).await?;
        } else {
            // Archived file: detect new or changed
            process_archived_file(&context, &config, host_file, pond_file, &mut stats).await?;
        }
    }

    // Log summary at INFO level
    if stats.new_files.0 > 0 || stats.appended.0 > 0 {
        info!(
            "Logfile ingestion complete: {} new ({} bytes), {} appended (+{} bytes), {} unchanged",
            stats.new_files.0,
            stats.new_files.1,
            stats.appended.0,
            stats.appended.1,
            stats.unchanged.0
        );
    } else if stats.unchanged.0 > 0 {
        info!(
            "Logfile ingestion complete: no changes ({} files, {} bytes total)",
            stats.unchanged.0, stats.unchanged.1
        );
    } else {
        info!("Logfile ingestion complete: no files to process");
    }
    Ok(())
}

/// Execute the b3sum command - print checksums in b3sum format
///
/// Outputs blake3 checksums for all files in the pond ingest directory
/// in a format compatible with `b3sum --check`:
///
/// ```text
/// <64-char-hex-hash>  <filename>
/// ```
///
/// This allows verification on the host:
/// ```bash
/// pond run /config b3sum > checksums.txt
/// cd /host_dir && b3sum --check checksums.txt
/// ```
async fn execute_b3sum(
    context: &FactoryContext,
    config: &LogfileIngestConfig,
) -> Result<(), tinyfs::Error> {
    debug!("Executing b3sum for pond path: {}", config.pond_path);

    // Read all files from the pond directory
    let pond_files = read_pond_state(context, &config.pond_path).await?;

    if pond_files.is_empty() {
        // No files to checksum - silent success
        return Ok(());
    }

    // Sort filenames for deterministic output
    let mut filenames: Vec<_> = pond_files.keys().collect();
    filenames.sort();

    // Print in b3sum format: "<hash>  <filename>"
    // Note: b3sum uses two spaces between hash and filename
    #[allow(clippy::print_stdout)]
    for filename in filenames {
        if let Some(pond_state) = pond_files.get(filename) {
            // The blake3 hash is stored as a 64-character hex string
            println!("{}  {}", pond_state.blake3, filename);
        }
    }

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
        .map_other()?;

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
        .map_other()?;

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
        let file_id = FileID::new_from_ids(part_id, entry.child_node_id, dir_file_id.pond_id());
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

        // For FilePhysicalSeries, extract cumulative_size and the bao-tree
        // frontier from the bao_outboard. The frontier lets prefix verification
        // resume from committed hashes instead of re-reading the whole prefix.
        // metadata.size is just the latest version's size, not cumulative.
        let (cumulative_size, frontier) = if let Some(bao_outboard) = &metadata.bao_outboard {
            match utilities::bao_outboard::SeriesOutboard::from_bytes(bao_outboard) {
                Ok(series) => {
                    debug!(
                        "File {} has bao_outboard with cumulative_size={}",
                        filename, series.cumulative_size
                    );
                    (series.cumulative_size, Some(series.incremental.frontier))
                }
                Err(e) => {
                    warn!(
                        "File {} has bao_outboard but failed to parse: {:?}, falling back to size={}",
                        filename,
                        e,
                        metadata.size.unwrap_or(0)
                    );
                    (metadata.size.unwrap_or(0), None)
                }
            }
        } else {
            warn!(
                "File {} has NO bao_outboard, falling back to size={:?}",
                filename, metadata.size
            );
            (metadata.size.unwrap_or(0), None)
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
                frontier,
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
    stats: &mut IngestionStats,
) -> Result<(), tinyfs::Error> {
    let filename = host_file
        .path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| tinyfs::Error::Other("Invalid filename".to_string()))?;

    match pond_file {
        None => {
            // New file: ingest completely
            info!(
                "Ingesting new file: {} ({} bytes)",
                filename, host_file.size
            );
            ingest_new_file(context, config, host_file).await?;
            stats.new_files.0 += 1;
            stats.new_files.1 += host_file.size;
        }
        Some(pond_state) => {
            // Existing file: detect append
            if host_file.size > pond_state.cumulative_size {
                let new_bytes = host_file.size - pond_state.cumulative_size;
                info!(
                    "Appending to {}: +{} bytes ({} -> {} bytes)",
                    filename, new_bytes, pond_state.cumulative_size, host_file.size
                );

                ingest_append(context, config, host_file, pond_state).await?;
                stats.appended.0 += 1;
                stats.appended.1 += new_bytes;
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
                stats.unchanged.0 += 1;
                stats.unchanged.1 += host_file.size;
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
    stats: &mut IngestionStats,
) -> Result<(), tinyfs::Error> {
    let filename = host_file
        .path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| tinyfs::Error::Other("Invalid filename".to_string()))?;

    match pond_file {
        None => {
            // New archived file
            info!(
                "Ingesting new archived file: {} ({} bytes)",
                filename, host_file.size
            );
            ingest_new_file(context, config, host_file).await?;
            stats.new_files.0 += 1;
            stats.new_files.1 += host_file.size;
        }
        Some(pond_state) => {
            // Verify archived file hasn't changed (should be immutable)
            // Use bao-tree root hash (IncrementalHashState), not simple blake3::hash
            // because metadata.blake3 stores the cumulative bao-tree root
            let host_content = std::fs::read(&host_file.path).map_other()?;

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
                stats.unchanged.0 += 1;
                stats.unchanged.1 += host_file.size;
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
    let content = std::fs::read(&host_file.path).map_other()?;
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
    writer.write_all(&content).await.map_other()?;
    writer.shutdown().await.map_other()?;

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

    // IMPORTANT: Use the snapshot size from host_file.size (captured at start of run)
    // Do NOT re-read file size - the file may have grown since we started.
    // We'll catch any new bytes on the next run.
    let snapshot_size = host_file.size;
    let bytes_to_read = (snapshot_size - pond_state.cumulative_size) as usize;

    // Read only the new bytes from host file (up to snapshot, not current size)
    let mut file = std::fs::File::open(&host_file.path).map_other()?;
    use std::io::{Read, Seek, SeekFrom};
    let _ = file
        .seek(SeekFrom::Start(pond_state.cumulative_size))
        .map_other()?;

    // Read exactly the bytes we expect (not read_to_end which could get more)
    let mut new_content = vec![0u8; bytes_to_read];
    file.read_exact(&mut new_content).map_other()?;

    info!(
        "Ingesting append to {}: {} new bytes (total will be {})",
        filename,
        new_content.len(),
        snapshot_size
    );

    // Verify prefix hasn't changed before appending.
    // This guards against the file being rotated between when we checked size
    // and now (TOCTOU). Uses the stored frontier so it reads only the trailing
    // partial block, not the whole prefix.
    {
        let (prefix_matches, host_blake3) = verify_prefix_matches(&host_file.path, pond_state)?;
        if !prefix_matches {
            return Err(tinyfs::Error::Other(format!(
                "Prefix verification failed for {}: expected blake3={}, got blake3={}. \
                 File may have been rotated during ingestion.",
                filename, pond_state.blake3, host_blake3
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
    writer.write_all(&new_content).await.map_other()?;
    writer.shutdown().await.map_other()?;

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
        let content = std::fs::read(&host_file.path).map_other()?;
        let prefix = &content[..tracked_size];

        // Compute blake3 of prefix using same method as tinyfs (bao-tree root)
        let mut hasher = IncrementalHashState::new();
        hasher.ingest(prefix);
        let prefix_blake3 = hasher.root_hash().to_hex().to_string();

        // If prefix matches pond's blake3, this is the rotated file
        if prefix_blake3 == pond_state.blake3 {
            info!(
                "Found rotated file {} matching pond blake3 {}...",
                host_file.path.display(),
                &pond_state.blake3[..16]
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

    writer.write_all(new_data).await.map_other()?;
    writer.shutdown().await.map_other()?;

    info!("Appended missed bytes to pond file: {}", pond_dest);

    Ok(())
}

/// Validate configuration
fn validate_config(config: &[u8]) -> TinyFSResult<Value> {
    let config: LogfileIngestConfig =
        serde_yaml::from_slice(config).map_other_context("Invalid config YAML")?;

    config.validate()?;

    serde_json::to_value(&config).map_other_context("Failed to serialize config")
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
            archived_pattern: "/var/log/test-*.json".to_string(),
            active_pattern: "/var/log/test.json".to_string(),
            pond_path: "logs/test".to_string(),
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_config_empty_pond_path() {
        let config = LogfileIngestConfig {
            archived_pattern: "/var/log/test-*.json".to_string(),
            active_pattern: "/var/log/test.json".to_string(),
            pond_path: "".to_string(),
        };

        assert!(config.validate().is_err());
    }

    use std::io::Write;
    use utilities::bao_outboard::BLOCK_SIZE;

    /// Build a `PondFileState` whose blake3/frontier commit to `content`,
    /// mirroring how a FilePhysicalSeries stores its cumulative bao-tree state.
    fn pond_state_for(content: &[u8], with_frontier: bool) -> PondFileState {
        let mut hasher = IncrementalHashState::new();
        hasher.ingest(content);
        PondFileState {
            node_id: FileID::root(),
            version: 1,
            size: content.len() as u64,
            blake3: hasher.root_hash().to_hex().to_string(),
            cumulative_size: content.len() as u64,
            frontier: with_frontier.then(|| hasher.to_frontier()),
        }
    }

    fn write_host(bytes: &[u8]) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(bytes).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn verify_prefix_matches_multiblock_parity_and_rotation() {
        // > 1 block so the stored frontier is non-empty (the hot path).
        let prefix: Vec<u8> = (0..(BLOCK_SIZE * 3 + 123))
            .map(|i| (i % 251) as u8)
            .collect();
        let state = pond_state_for(&prefix, true);
        assert!(!state.frontier.as_ref().unwrap().is_empty());

        // Intact prefix -> matches, host root equals the committed root.
        let host = write_host(&prefix);
        let (matches, host_root) = verify_prefix_matches(host.path(), &state).unwrap();
        assert!(matches);
        assert_eq!(host_root, state.blake3);

        // Same-size prefix with a mutated trailing block -> rotation detected.
        let mut mutated = prefix.clone();
        *mutated.last_mut().unwrap() ^= 0xFF;
        let host = write_host(&mutated);
        let (matches, _) = verify_prefix_matches(host.path(), &state).unwrap();
        assert!(!matches);
    }

    #[test]
    fn verify_prefix_matches_ignores_appended_tail() {
        // A grown file: same tracked prefix plus new bytes. Only the prefix is
        // verified, so this is a normal append.
        let prefix: Vec<u8> = (0..(BLOCK_SIZE * 2)).map(|i| (i % 97) as u8).collect();
        let state = pond_state_for(&prefix, true);

        let mut grown = prefix.clone();
        grown.extend_from_slice(b"newly appended log line\n");
        let host = write_host(&grown);

        let (matches, host_root) = verify_prefix_matches(host.path(), &state).unwrap();
        assert!(matches);
        assert_eq!(host_root, state.blake3);
    }

    #[test]
    fn verify_prefix_matches_sub_block_empty_frontier() {
        // < 1 block: frontier is empty, resume covers the whole (tiny) prefix.
        let prefix = b"a few log lines\nnot even one block\n".to_vec();
        let state = pond_state_for(&prefix, true);
        assert!(state.frontier.as_ref().unwrap().is_empty());

        let host = write_host(&prefix);
        assert!(verify_prefix_matches(host.path(), &state).unwrap().0);

        let mut mutated = prefix.clone();
        mutated[0] ^= 0xFF;
        let host = write_host(&mutated);
        assert!(!verify_prefix_matches(host.path(), &state).unwrap().0);
    }

    #[test]
    fn verify_prefix_matches_no_frontier_full_read_fallback() {
        // Legacy entry without a stored frontier -> full-prefix read fallback,
        // which must still verify correctly.
        let prefix: Vec<u8> = (0..(BLOCK_SIZE + 500)).map(|i| (i % 211) as u8).collect();
        let state = pond_state_for(&prefix, false);
        assert!(state.frontier.is_none());

        let host = write_host(&prefix);
        assert!(verify_prefix_matches(host.path(), &state).unwrap().0);

        let mut mutated = prefix.clone();
        mutated[BLOCK_SIZE / 2] ^= 0x01;
        let host = write_host(&mutated);
        assert!(!verify_prefix_matches(host.path(), &state).unwrap().0);
    }
}
