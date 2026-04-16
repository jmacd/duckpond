// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Sync logic: diff manifests and apply changes to the pond.

use crate::git::{EntryKind, GitManifest, ManifestEntry};
use log::info;
use std::path::Path;
use tinyfs::EntryType;

/// A change to apply to the pond
#[derive(Debug)]
pub enum Change {
    /// Create or update a file
    WriteFile { path: String, oid: String },
    /// Create a directory
    CreateDir { path: String },
    /// Create or update a symlink
    WriteSymlink { path: String, oid: String },
    /// Remove an entry (file, symlink, or directory)
    Remove { path: String, kind: EntryKind },
}

/// Compute the changes needed to go from old_manifest to new_manifest
pub fn diff_manifests(old: &GitManifest, new: &GitManifest) -> Vec<Change> {
    let mut changes = Vec::new();

    // Detect removals (in old but not in new)
    for (path, entry) in &old.entries {
        if !new.entries.contains_key(path) {
            changes.push(Change::Remove {
                path: path.clone(),
                kind: entry.kind,
            });
        }
    }

    // Sort removals deepest-first for correct deletion order
    changes.sort_by(|a, b| {
        if let (Change::Remove { path: pa, .. }, Change::Remove { path: pb, .. }) = (a, b) {
            // Deeper paths first (more '/' separators = deeper)
            let depth_a = pa.matches('/').count();
            let depth_b = pb.matches('/').count();
            depth_b.cmp(&depth_a).then_with(|| pa.cmp(pb))
        } else {
            std::cmp::Ordering::Equal
        }
    });

    // Detect additions and modifications
    // Collect separately so we can sort: dirs shallowest-first, then files/symlinks
    let mut dir_adds = Vec::new();
    let mut file_adds = Vec::new();

    for (path, new_entry) in &new.entries {
        match old.entries.get(path) {
            None => {
                // New entry
                push_create(path, new_entry, &mut dir_adds, &mut file_adds);
            }
            Some(old_entry) => {
                if old_entry.kind != new_entry.kind {
                    // Type changed: remove old, create new
                    changes.push(Change::Remove {
                        path: path.clone(),
                        kind: old_entry.kind,
                    });
                    push_create(path, new_entry, &mut dir_adds, &mut file_adds);
                } else if old_entry.oid != new_entry.oid {
                    // Content changed (same type)
                    match new_entry.kind {
                        EntryKind::File => {
                            file_adds.push(Change::WriteFile {
                                path: path.clone(),
                                oid: new_entry.oid.clone(),
                            });
                        }
                        EntryKind::Symlink => {
                            file_adds.push(Change::WriteSymlink {
                                path: path.clone(),
                                oid: new_entry.oid.clone(),
                            });
                        }
                        EntryKind::Directory => {
                            // Directory OID changed but it still exists;
                            // child changes will be handled by their own entries
                        }
                    }
                }
                // else: unchanged, skip
            }
        }
    }

    // Sort dir creates shallowest-first
    dir_adds.sort_by(|a, b| {
        if let (Change::CreateDir { path: pa }, Change::CreateDir { path: pb }) = (a, b) {
            let depth_a = pa.matches('/').count();
            let depth_b = pb.matches('/').count();
            depth_a.cmp(&depth_b).then_with(|| pa.cmp(pb))
        } else {
            std::cmp::Ordering::Equal
        }
    });

    changes.extend(dir_adds);
    changes.extend(file_adds);

    changes
}

fn push_create(
    path: &str,
    entry: &ManifestEntry,
    dir_adds: &mut Vec<Change>,
    file_adds: &mut Vec<Change>,
) {
    match entry.kind {
        EntryKind::Directory => {
            dir_adds.push(Change::CreateDir {
                path: path.to_string(),
            });
        }
        EntryKind::File => {
            file_adds.push(Change::WriteFile {
                path: path.to_string(),
                oid: entry.oid.clone(),
            });
        }
        EntryKind::Symlink => {
            file_adds.push(Change::WriteSymlink {
                path: path.to_string(),
                oid: entry.oid.clone(),
            });
        }
    }
}

/// Apply a set of changes to the pond filesystem
pub async fn apply_changes(
    context: &tinyfs::FactoryContext,
    pond_path_on_disk: &Path,
    node_id: &str,
    config_pond_path: &str,
    changes: &[Change],
) -> Result<SyncStats, tinyfs::Error> {
    let fs = context.context.filesystem();
    let root = fs.root().await?;

    // Ensure the destination directory exists
    let _dest = root.create_dir_all(config_pond_path).await?;

    let mut stats = SyncStats::default();

    for change in changes {
        match change {
            Change::Remove { path, kind } => {
                let full_path = format!("{}/{}", config_pond_path, path);
                match remove_entry_at_path(&root, &full_path).await {
                    Ok(()) => {
                        info!("Removed {} ({:?}): {}", kind_str(*kind), kind, path);
                        stats.removed += 1;
                    }
                    Err(e) => {
                        log::warn!(
                            "Failed to remove {}: {} (may already be gone)",
                            full_path,
                            e
                        );
                    }
                }
            }
            Change::CreateDir { path } => {
                let full_path = format!("{}/{}", config_pond_path, path);
                let _dir = root.create_dir_all(&full_path).await?;
                info!("Created directory: {}", path);
                stats.dirs_created += 1;
            }
            Change::WriteFile { path, oid } => {
                let full_path = format!("{}/{}", config_pond_path, path);
                let content = crate::git::read_blob(pond_path_on_disk, node_id, oid)?;

                // Remove existing entry if present (type might have changed)
                let _ = remove_entry_at_path(&root, &full_path).await;

                // Ensure parent directory exists
                if let Some(parent) = Path::new(&full_path).parent() {
                    if parent != Path::new("") {
                        let _dir = root
                            .create_dir_all(parent.to_string_lossy().as_ref())
                            .await?;
                    }
                }

                // Create as FilePhysicalVersion
                use tokio::io::AsyncWriteExt;
                let (_, mut writer) = root
                    .create_file_path_streaming_with_type(
                        &full_path,
                        EntryType::FilePhysicalVersion,
                    )
                    .await?;
                writer
                    .write_all(&content)
                    .await
                    .map_err(|e| tinyfs::Error::Other(format!("Write failed: {}", e)))?;
                writer
                    .shutdown()
                    .await
                    .map_err(|e| tinyfs::Error::Other(format!("Shutdown failed: {}", e)))?;

                info!("Wrote file: {} ({} bytes)", path, content.len());
                stats.files_written += 1;
                stats.bytes_written += content.len() as u64;
            }
            Change::WriteSymlink { path, oid } => {
                let full_path = format!("{}/{}", config_pond_path, path);
                let target = crate::git::read_symlink_target(pond_path_on_disk, node_id, oid)?;

                // Remove existing entry if present
                let _ = remove_entry_at_path(&root, &full_path).await;

                // Ensure parent directory exists
                if let Some(parent) = Path::new(&full_path).parent() {
                    if parent != Path::new("") {
                        let _dir = root
                            .create_dir_all(parent.to_string_lossy().as_ref())
                            .await?;
                    }
                }

                let _node = root.create_symlink_path(&full_path, &target).await?;

                info!("Created symlink: {} -> {}", path, target);
                stats.symlinks_created += 1;
            }
        }
    }

    Ok(stats)
}

/// Remove an entry from the pond at the given path.
/// Handles the path decomposition into parent directory + entry name.
async fn remove_entry_at_path(root: &tinyfs::WD, full_path: &str) -> Result<(), tinyfs::Error> {
    let path = Path::new(full_path);
    let parent = path
        .parent()
        .ok_or_else(|| tinyfs::Error::Other("Cannot remove root".to_string()))?;
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| tinyfs::Error::Other("Invalid entry name".to_string()))?;

    let parent_wd = root.open_dir_path(parent).await?;
    parent_wd.remove_entry(name).await
}

fn kind_str(kind: EntryKind) -> &'static str {
    match kind {
        EntryKind::File => "file",
        EntryKind::Symlink => "symlink",
        EntryKind::Directory => "directory",
    }
}

/// Statistics from a sync operation
#[derive(Debug, Default)]
pub struct SyncStats {
    pub files_written: usize,
    pub bytes_written: u64,
    pub dirs_created: usize,
    pub symlinks_created: usize,
    pub removed: usize,
}

impl std::fmt::Display for SyncStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} files ({} bytes), {} dirs, {} symlinks written; {} removed",
            self.files_written,
            self.bytes_written,
            self.dirs_created,
            self.symlinks_created,
            self.removed
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn make_entry(oid: &str, kind: EntryKind, mode: u32) -> ManifestEntry {
        ManifestEntry {
            oid: oid.to_string(),
            kind,
            mode,
        }
    }

    #[test]
    fn test_diff_empty_to_populated() {
        let old = GitManifest::empty();
        let entries = BTreeMap::from([
            (
                "README.md".to_string(),
                make_entry("abc123", EntryKind::File, 0o100644),
            ),
            (
                "docs".to_string(),
                make_entry("def456", EntryKind::Directory, 0o40000),
            ),
        ]);
        let new = GitManifest {
            commit_sha: "commit1".to_string(),
            entries,
        };

        let changes = diff_manifests(&old, &new);
        // Should have 1 dir create + 1 file create
        assert_eq!(changes.len(), 2);
        assert!(matches!(&changes[0], Change::CreateDir { path } if path == "docs"));
        assert!(matches!(&changes[1], Change::WriteFile { path, .. } if path == "README.md"));
    }

    #[test]
    fn test_diff_no_changes() {
        let entries = BTreeMap::from([(
            "file.txt".to_string(),
            make_entry("abc123", EntryKind::File, 0o100644),
        )]);
        let old = GitManifest {
            commit_sha: "c1".to_string(),
            entries: entries.clone(),
        };
        let new = GitManifest {
            commit_sha: "c1".to_string(),
            entries,
        };

        let changes = diff_manifests(&old, &new);
        assert!(changes.is_empty());
    }

    #[test]
    fn test_diff_deletion_deepest_first() {
        let entries = BTreeMap::from([
            (
                "a".to_string(),
                make_entry("1", EntryKind::Directory, 0o40000),
            ),
            (
                "a/b".to_string(),
                make_entry("2", EntryKind::Directory, 0o40000),
            ),
            (
                "a/b/c.txt".to_string(),
                make_entry("3", EntryKind::File, 0o100644),
            ),
        ]);
        let old = GitManifest {
            commit_sha: "c1".to_string(),
            entries,
        };
        let new = GitManifest::empty();

        let changes = diff_manifests(&old, &new);
        // Removals should be deepest first: a/b/c.txt, a/b, a
        assert_eq!(changes.len(), 3);
        assert!(matches!(&changes[0], Change::Remove { path, .. } if path == "a/b/c.txt"));
        assert!(matches!(&changes[1], Change::Remove { path, .. } if path == "a/b"));
        assert!(matches!(&changes[2], Change::Remove { path, .. } if path == "a"));
    }
}
