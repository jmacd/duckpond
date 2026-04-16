// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Git operations using gix (pure Rust).
//!
//! Manages bare repo init/open/fetch and tree walking.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// An entry in the git tree manifest
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestEntry {
    /// Git object ID (hex string)
    pub oid: String,
    /// Entry kind
    pub kind: EntryKind,
    /// Unix file mode (e.g., 0o100644 for regular file, 0o120000 for symlink)
    pub mode: u32,
}

/// Kind of entry in the manifest
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryKind {
    File,
    Symlink,
    Directory,
}

/// Full manifest tracking the synced state of a git repo
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitManifest {
    /// The commit SHA that was last synced
    pub commit_sha: String,
    /// Map of relative path -> manifest entry
    pub entries: BTreeMap<String, ManifestEntry>,
}

impl GitManifest {
    /// Create an empty manifest (for first sync)
    #[must_use]
    pub fn empty() -> Self {
        Self {
            commit_sha: String::new(),
            entries: BTreeMap::new(),
        }
    }

    /// Deserialize manifest from JSON bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self, tinyfs::Error> {
        serde_json::from_slice(data)
            .map_err(|e| tinyfs::Error::Other(format!("Failed to parse manifest: {}", e)))
    }

    /// Serialize manifest to JSON bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>, tinyfs::Error> {
        serde_json::to_vec_pretty(self)
            .map_err(|e| tinyfs::Error::Other(format!("Failed to serialize manifest: {}", e)))
    }
}

/// Get the path to the bare repo for a given factory node
#[must_use]
pub fn bare_repo_path(pond_path: &Path, node_id: &str) -> PathBuf {
    pond_path.join("git").join(format!("{}.git", node_id))
}

/// Initialize or open a bare repo, fetch the remote, and return the
/// commit SHA for the configured ref.
pub fn fetch_and_resolve(
    repo_path: &Path,
    url: &str,
    git_ref: &str,
) -> Result<String, tinyfs::Error> {
    // Init or open the bare repo
    let repo = if repo_path.exists() {
        open_bare_repo(repo_path)?
    } else {
        init_bare_repo(repo_path, url)?
    };

    // Fetch from origin and resolve the ref from the remote's advertised refs
    fetch_and_resolve_ref(&repo, git_ref)
}

/// Walk the tree at the given commit and build a manifest
pub fn walk_tree(
    pond_path: &Path,
    node_id: &str,
    commit_sha: &str,
) -> Result<GitManifest, tinyfs::Error> {
    let repo_path = bare_repo_path(pond_path, node_id);
    let repo = open_bare_repo(&repo_path)?;

    let oid = gix::ObjectId::from_hex(commit_sha.as_bytes())
        .map_err(|e| tinyfs::Error::Other(format!("Invalid commit SHA: {}", e)))?;

    let commit = repo
        .find_object(oid)
        .map_err(|e| tinyfs::Error::Other(format!("Failed to find commit: {}", e)))?
        .try_into_commit()
        .map_err(|e| tinyfs::Error::Other(format!("Object is not a commit: {}", e)))?;

    let tree = commit
        .tree()
        .map_err(|e| tinyfs::Error::Other(format!("Failed to get tree: {}", e)))?;

    let mut entries = BTreeMap::new();
    walk_tree_recursive(&repo, &tree, "", &mut entries)?;

    Ok(GitManifest {
        commit_sha: commit_sha.to_string(),
        entries,
    })
}

/// Read the content of a blob by OID
pub fn read_blob(
    pond_path: &Path,
    node_id: &str,
    oid_hex: &str,
) -> Result<Vec<u8>, tinyfs::Error> {
    let repo_path = bare_repo_path(pond_path, node_id);
    let repo = open_bare_repo(&repo_path)?;

    let oid = gix::ObjectId::from_hex(oid_hex.as_bytes())
        .map_err(|e| tinyfs::Error::Other(format!("Invalid OID: {}", e)))?;

    let object = repo
        .find_object(oid)
        .map_err(|e| tinyfs::Error::Other(format!("Failed to find object: {}", e)))?;

    Ok(object.data.to_vec())
}

/// Read a symlink target (blob content interpreted as text)
pub fn read_symlink_target(
    pond_path: &Path,
    node_id: &str,
    oid_hex: &str,
) -> Result<String, tinyfs::Error> {
    let data = read_blob(pond_path, node_id, oid_hex)?;
    String::from_utf8(data)
        .map_err(|e| tinyfs::Error::Other(format!("Symlink target is not valid UTF-8: {}", e)))
}

// --- Internal helpers ---

fn init_bare_repo(
    path: &Path,
    url: &str,
) -> Result<gix::Repository, tinyfs::Error> {
    std::fs::create_dir_all(path)
        .map_err(|e| tinyfs::Error::Other(format!("Failed to create bare repo dir: {}", e)))?;

    let _repo = gix::init_bare(path)
        .map_err(|e| tinyfs::Error::Other(format!("Failed to init bare repo: {}", e)))?;

    // Write the remote config directly to the git config file
    let config_path = path.join("config");
    let config_content = format!(
        "[core]\n\trepositoryformatversion = 0\n\tfilemode = true\n\tbare = true\n[remote \"origin\"]\n\turl = {}\n\tfetch = +refs/*:refs/*\n",
        url
    );
    std::fs::write(&config_path, config_content)
        .map_err(|e| tinyfs::Error::Other(format!("Failed to write git config: {}", e)))?;

    // Re-open so the repo picks up the new config
    open_bare_repo(path)
}

fn open_bare_repo(path: &Path) -> Result<gix::Repository, tinyfs::Error> {
    gix::open(path)
        .map_err(|e| tinyfs::Error::Other(format!("Failed to open bare repo at {}: {}", path.display(), e)))
}

fn fetch_and_resolve_ref(
    repo: &gix::Repository,
    git_ref: &str,
) -> Result<String, tinyfs::Error> {
    log::info!("Fetching from origin");

    let remote = repo
        .find_remote("origin")
        .map_err(|e| tinyfs::Error::Other(format!("Failed to find remote 'origin': {}", e)))?;

    let outcome = remote
        .connect(gix::remote::Direction::Fetch)
        .map_err(|e| tinyfs::Error::Other(format!("Failed to connect to remote: {}", e)))?
        .prepare_fetch(gix::progress::Discard, Default::default())
        .map_err(|e| tinyfs::Error::Other(format!("Failed to prepare fetch: {}", e)))?
        .receive(gix::progress::Discard, &gix::interrupt::IS_INTERRUPTED)
        .map_err(|e| tinyfs::Error::Other(format!("Fetch failed: {}", e)))?;

    log::info!("Fetch complete");

    // Resolve the ref from the remote's advertised refs.
    let target_branch = format!("refs/heads/{}", git_ref);
    let target_tag = format!("refs/tags/{}", git_ref);

    log::debug!(
        "Looking for ref '{}': {} remote refs, {} mappings",
        git_ref,
        outcome.ref_map.remote_refs.len(),
        outcome.ref_map.mappings.len()
    );

    // Strategy 1: Check remote_refs (available for network protocols)
    for remote_ref in &outcome.ref_map.remote_refs {
        log::debug!("  remote ref: {:?}", remote_ref);
        let (name, oid) = match remote_ref {
            gix::protocol::handshake::Ref::Direct { full_ref_name, object } => {
                (full_ref_name.as_ref(), Some(*object))
            }
            gix::protocol::handshake::Ref::Symbolic { full_ref_name, object, .. } => {
                (full_ref_name.as_ref(), Some(*object))
            }
            gix::protocol::handshake::Ref::Peeled { full_ref_name, tag, .. } => {
                (full_ref_name.as_ref(), Some(*tag))
            }
            gix::protocol::handshake::Ref::Unborn { .. } => continue,
        };

        let name_str = std::str::from_utf8(name).unwrap_or("");
        if let Some(oid) = oid {
            if name_str == target_branch || name_str == target_tag || name_str == git_ref {
                return Ok(oid.to_hex().to_string());
            }
        }
    }

    // Strategy 2: Check mappings (populated by refspec matching)
    for mapping in &outcome.ref_map.mappings {
        log::debug!("  mapping: {:?}", mapping);
        if let Some(local) = &mapping.local {
            let local_str = std::str::from_utf8(local.as_ref()).unwrap_or("");
            if local_str == target_branch || local_str == target_tag {
                if let Some(oid) = mapping.remote.as_id() {
                    return Ok(oid.to_hex().to_string());
                }
            }
        }
    }

    // Strategy 3: Look up local refs (populated by fetch ref update)
    if let Ok(reference) = repo.find_reference(&target_branch)
        .or_else(|_| repo.find_reference(&target_tag))
        .or_else(|_| repo.find_reference(git_ref))
    {
        if let Ok(id) = reference.into_fully_peeled_id() {
            return Ok(id.to_hex().to_string());
        }
    }

    // Strategy 4: If git_ref looks like a full SHA, try it directly
    if git_ref.len() >= 40 {
        if let Ok(oid) = gix::ObjectId::from_hex(git_ref.as_bytes()) {
            if repo.find_object(oid).is_ok() {
                return Ok(git_ref.to_string());
            }
        }
    }

    Err(tinyfs::Error::Other(format!(
        "Failed to resolve ref '{}': not found in remote refs, mappings, or local refs",
        git_ref
    )))
}

fn walk_tree_recursive(
    repo: &gix::Repository,
    tree: &gix::Tree<'_>,
    prefix: &str,
    entries: &mut BTreeMap<String, ManifestEntry>,
) -> Result<(), tinyfs::Error> {
    for entry_ref in tree.iter() {
        let entry = entry_ref
            .map_err(|e| tinyfs::Error::Other(format!("Failed to read tree entry: {}", e)))?;

        let name = std::str::from_utf8(entry.filename())
            .map_err(|e| tinyfs::Error::Other(format!("Non-UTF8 filename: {}", e)))?;

        let path = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", prefix, name)
        };

        let mode = entry.mode().value() as u32;
        let oid = entry.oid().to_hex().to_string();

        match entry.mode().kind() {
            gix::object::tree::EntryKind::Tree => {
                // Record the directory entry
                let _ = entries.insert(
                    path.clone(),
                    ManifestEntry {
                        oid: oid.clone(),
                        kind: EntryKind::Directory,
                        mode,
                    },
                );

                // Recurse into subdirectory
                let sub_obj = repo
                    .find_object(entry.oid())
                    .map_err(|e| {
                        tinyfs::Error::Other(format!("Failed to find tree object: {}", e))
                    })?;
                let sub_tree = sub_obj
                    .try_into_tree()
                    .map_err(|e| {
                        tinyfs::Error::Other(format!("Object is not a tree: {}", e))
                    })?;
                walk_tree_recursive(repo, &sub_tree, &path, entries)?;
            }
            gix::object::tree::EntryKind::Link => {
                let _ = entries.insert(
                    path,
                    ManifestEntry {
                        oid,
                        kind: EntryKind::Symlink,
                        mode,
                    },
                );
            }
            gix::object::tree::EntryKind::Blob | gix::object::tree::EntryKind::BlobExecutable => {
                let _ = entries.insert(
                    path,
                    ManifestEntry {
                        oid,
                        kind: EntryKind::File,
                        mode,
                    },
                );
            }
            _ => {
                log::debug!("Skipping unsupported entry type at {}: mode={:#o}", path, mode);
            }
        }
    }
    Ok(())
}
