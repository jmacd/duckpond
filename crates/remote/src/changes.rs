// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Change detection for Delta Lake transaction logs
//!
//! This module provides functionality to detect file changes (additions and removals)
//! from Delta Lake transaction logs at specific versions.

use crate::error::RemoteError;
use deltalake::DeltaTable;
use futures::TryStreamExt;

/// Represents a file change detected from the Delta Lake transaction log
#[derive(Debug, Clone)]
pub struct FileChange {
    /// Path to the Parquet file in the Delta table
    pub parquet_path: String,
    /// Size of the file in bytes
    pub size: i64,
    /// Part ID extracted from the Parquet path (if parseable)
    pub part_id: Option<u64>,
}

/// A set of changes (additions and removals) detected from a Delta version
#[derive(Debug, Clone)]
pub struct Changeset {
    /// Files that were added in this version
    pub added: Vec<FileChange>,
    /// Files that were removed in this version
    pub removed: Vec<FileChange>,
}

impl Changeset {
    /// Create an empty changeset
    pub fn new() -> Self {
        Self {
            added: Vec::new(),
            removed: Vec::new(),
        }
    }

    /// Total bytes of added files
    pub fn total_bytes_added(&self) -> i64 {
        self.added.iter().map(|f| f.size).sum()
    }

    /// Total bytes of removed files
    pub fn total_bytes_removed(&self) -> i64 {
        self.removed.iter().map(|f| f.size).sum()
    }
}

impl Default for Changeset {
    fn default() -> Self {
        Self::new()
    }
}

/// Detect changes from the Delta Lake transaction log at a specific version
///
/// This loads the table state at the given version and returns all active files
/// (Add actions) at that point in time.
///
/// # Arguments
/// * `table` - The Delta table to query
/// * `version` - The Delta table version to inspect
///
/// # Returns
/// A Changeset containing all added files in that version (cumulative state)
pub async fn detect_changes_from_delta_log(
    table: &DeltaTable,
    version: i64,
) -> Result<Changeset, RemoteError> {
    log::debug!("Detecting changes for Delta table version {}", version);

    // Load the table at the specific version
    let mut table_at_version = table.clone();
    table_at_version
        .load_version(version)
        .await
        .map_err(|e| RemoteError::VersionNotFound(version, e.to_string()))?;

    // Get the state snapshot
    let state = table_at_version
        .snapshot()
        .map_err(|e| RemoteError::DeltaTableError(format!("Failed to get snapshot: {}", e)))?;

    let mut changeset = Changeset::new();

    // Get all active files from the snapshot (Add actions after applying all Remove actions)
    let log_store = table_at_version.log_store();
    let mut file_stream = state.snapshot().files(log_store.as_ref(), None);
    let mut files = Vec::new();

    while let Some(file_view) = file_stream
        .try_next()
        .await
        .map_err(|e| RemoteError::DeltaTableError(format!("Failed to read file: {}", e)))?
    {
        files.push(file_view);
    }

    for file_view in files {
        let file_change = FileChange {
            parquet_path: file_view.path().to_string(),
            size: file_view.size(),
            part_id: extract_part_id_from_parquet_path(&file_view.path()),
        };
        changeset.added.push(file_change);
    }

    log::debug!(
        "Detected {} active files at version {} ({} bytes total)",
        changeset.added.len(),
        version,
        changeset.total_bytes_added()
    );

    Ok(changeset)
}

/// Extract part_id from a Parquet path
///
/// Parquet paths follow the pattern:
/// `part-{timestamp}-{uuid-part1}-{uuid-part2}-{uuid-part3}-{uuid-part4}-{uuid-part5}-c{chunk}.parquet`
///
/// The part_id is encoded in the UUID portion. However, this is a best-effort extraction
/// and may fail for non-standard paths.
fn extract_part_id_from_parquet_path(path: &str) -> Option<u64> {
    // Extract filename from path
    let filename = path.split('/').next_back()?;

    // Remove .parquet extension
    let stem = filename.strip_suffix(".parquet")?;

    // Split by dashes
    let parts: Vec<&str> = stem.split('-').collect();

    // We expect at least 7 parts: "part", timestamp, and 5 UUID segments
    if parts.len() < 7 {
        return None;
    }

    // Try to parse the timestamp part as part_id (parts[1])
    // This is a heuristic - in reality, the part_id may be encoded differently
    parts[1].parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_part_id_from_standard_path() {
        let path = "part-12345678-90ab-cdef-1234-567890abcdef-c000.parquet";
        let part_id = extract_part_id_from_parquet_path(path);
        assert_eq!(part_id, Some(12345678));
    }

    #[test]
    fn test_extract_part_id_from_path_with_directory() {
        let path = "data/subdir/part-98765432-10ab-cdef-1234-567890abcdef-c000.parquet";
        let part_id = extract_part_id_from_parquet_path(path);
        assert_eq!(part_id, Some(98765432));
    }

    #[test]
    fn test_extract_part_id_from_invalid_path() {
        let path = "not-a-parquet-file.txt";
        let part_id = extract_part_id_from_parquet_path(path);
        assert_eq!(part_id, None);
    }

    #[test]
    fn test_changeset_totals() {
        let mut changeset = Changeset::new();
        changeset.added.push(FileChange {
            parquet_path: "file1.parquet".to_string(),
            size: 1000,
            part_id: None,
        });
        changeset.added.push(FileChange {
            parquet_path: "file2.parquet".to_string(),
            size: 2000,
            part_id: None,
        });
        changeset.removed.push(FileChange {
            parquet_path: "file3.parquet".to_string(),
            size: 500,
            part_id: None,
        });

        assert_eq!(changeset.total_bytes_added(), 3000);
        assert_eq!(changeset.total_bytes_removed(), 500);
    }
}
