// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Content-graph fetch: the consumer side of the content-addressed remote
//! (design Section 8.5, Fork 2).
//!
//! This module implements the *fetch walk*: given a [`ContentRemote`] and its
//! tip commit, descend the object graph by `child_hash` and collect the
//! reachable, verified object closure.  It does no tlogfs rebuild yet; it
//! produces the in-memory [`FetchedGraph`] a rebuild consumes, and it is the
//! point at which content addressing is checked: every fetched object's bytes
//! are re-hashed and must equal the key they were fetched under.
//!
//! Descent is driven by [`EntryType`], exactly mirroring the producer's fold
//! (Section 9): physical directories are tree objects whose entries are
//! recursed into; physical files and symlinks are leaf blobs; series are
//! series objects whose version blobs are leaves; dynamic and computed nodes
//! are recipe leaves whose generated children are not in the graph.

use std::collections::BTreeMap;

use sync_store::ContentRemote;
use sync_store::content::{Commit, ObjectHash, TreeEntry, decode_series, decode_tree};
use tinyfs::EntryType;

use crate::StewardError;

/// A fetched content object, in the structured form a rebuild needs, alongside
/// its exact bytes (kept so the rebuild can write file content and re-verify).
#[derive(Debug, Clone)]
pub enum FetchedObject {
    /// A directory: its decoded, canonical-order entries.
    Tree(Vec<TreeEntry>),
    /// A leaf blob: a file version's bytes, a symlink target, or recipe bytes.
    Blob(Vec<u8>),
    /// A multi-version series: its ordered version blob hashes.
    Series(Vec<ObjectHash>),
}

/// The verified object closure reachable from a remote tip commit.
#[derive(Debug, Clone, Default)]
pub struct FetchedGraph {
    /// The tip commit's hash.
    pub tip: Option<ObjectHash>,
    /// The commit chain from the tip back toward genesis, tip first, limited to
    /// commits present on the remote.
    pub commits: Vec<(ObjectHash, Commit)>,
    /// Every reachable object keyed by content hash.  Each entry's bytes have
    /// been verified to hash to its key.
    pub objects: BTreeMap<ObjectHash, FetchedObject>,
    /// Raw bytes of every fetched object, keyed by content hash.
    pub bytes: BTreeMap<ObjectHash, Vec<u8>>,
}

impl FetchedGraph {
    /// The tip commit's root tree hash, or `None` if the graph is empty.
    #[must_use]
    pub fn root_tree_hash(&self) -> Option<ObjectHash> {
        self.commits.first().map(|(_, c)| c.root_tree_hash)
    }

    /// Total number of distinct objects fetched.
    #[must_use]
    pub fn len(&self) -> usize {
        self.objects.len()
    }

    /// True if no objects were fetched.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }
}

/// Fetch the verified object closure reachable from `ref_name`'s tip on
/// `remote`.
///
/// Returns an empty graph if the ref does not exist.  Otherwise fetches the tip
/// commit, walks its parent chain as far as the remote holds commits, and
/// descends the tip commit's root tree by `child_hash`, fetching every
/// reachable tree, blob, and series object exactly once.
///
/// # Errors
///
/// Returns an error if a referenced object is absent from the remote, if any
/// fetched object's bytes do not hash to the key it was fetched under, or if a
/// structured object fails to decode.
pub async fn fetch_object_graph(
    remote: &ContentRemote,
    ref_name: &str,
) -> Result<FetchedGraph, StewardError> {
    let Some(tip) = remote
        .get_tip(ref_name)
        .await
        .map_err(|e| StewardError::Content(e.to_string()))?
    else {
        return Ok(FetchedGraph::default());
    };

    let mut graph = FetchedGraph {
        tip: Some(tip),
        ..FetchedGraph::default()
    };

    // Walk the commit chain from the tip toward genesis, stopping at the first
    // commit the remote does not hold.
    let mut next = Some(tip);
    while let Some(commit_hash) = next {
        let Some(commit_bytes) = remote
            .get_object(commit_hash)
            .await
            .map_err(|e| StewardError::Content(e.to_string()))?
        else {
            break;
        };
        verify(commit_hash, &commit_bytes)?;
        let commit = Commit::decode(&commit_bytes)
            .map_err(|e| StewardError::Content(format!("decode commit: {e}")))?;
        next = commit.parent_commit_hash;
        graph.commits.push((commit_hash, commit));
    }

    // Descend the tip commit's root tree, fetching the full reachable closure.
    if let Some((_, tip_commit)) = graph.commits.first() {
        let root = tip_commit.root_tree_hash;
        fetch_tree(remote, root, &mut graph).await?;
    }

    Ok(graph)
}

/// Recursively fetch a tree object and everything reachable from its entries.
async fn fetch_tree(
    remote: &ContentRemote,
    tree_hash: ObjectHash,
    graph: &mut FetchedGraph,
) -> Result<(), StewardError> {
    // Iterative worklist to avoid async recursion on the directory tree.
    let mut stack = vec![tree_hash];
    while let Some(hash) = stack.pop() {
        if graph.objects.contains_key(&hash) {
            continue;
        }
        let bytes = fetch_verified(remote, hash).await?;
        let entries =
            decode_tree(&bytes).map_err(|e| StewardError::Content(format!("decode tree: {e}")))?;
        let _ = graph
            .objects
            .insert(hash, FetchedObject::Tree(entries.clone()));
        let _ = graph.bytes.insert(hash, bytes);

        for entry in entries {
            match entry.entry_type {
                EntryType::DirectoryPhysical => stack.push(entry.child_hash),
                EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries => {
                    fetch_series(remote, entry.child_hash, graph).await?;
                }
                EntryType::FilePhysicalVersion
                | EntryType::TablePhysicalVersion
                | EntryType::Symlink
                | EntryType::DirectoryDynamic
                | EntryType::FileDynamic
                | EntryType::TableDynamic => {
                    fetch_blob(remote, entry.child_hash, graph).await?;
                }
            }
        }
    }
    Ok(())
}

/// Fetch a series object and its version blobs.
async fn fetch_series(
    remote: &ContentRemote,
    series_hash: ObjectHash,
    graph: &mut FetchedGraph,
) -> Result<(), StewardError> {
    if graph.objects.contains_key(&series_hash) {
        return Ok(());
    }
    let bytes = fetch_verified(remote, series_hash).await?;
    let versions =
        decode_series(&bytes).map_err(|e| StewardError::Content(format!("decode series: {e}")))?;
    let _ = graph
        .objects
        .insert(series_hash, FetchedObject::Series(versions.clone()));
    let _ = graph.bytes.insert(series_hash, bytes);
    for version in versions {
        fetch_blob(remote, version, graph).await?;
    }
    Ok(())
}

/// Fetch a leaf blob object.
async fn fetch_blob(
    remote: &ContentRemote,
    hash: ObjectHash,
    graph: &mut FetchedGraph,
) -> Result<(), StewardError> {
    if graph.objects.contains_key(&hash) {
        return Ok(());
    }
    let bytes = fetch_verified(remote, hash).await?;
    let _ = graph
        .objects
        .insert(hash, FetchedObject::Blob(bytes.clone()));
    let _ = graph.bytes.insert(hash, bytes);
    Ok(())
}

/// Fetch an object's bytes and verify they hash to the requested key.
async fn fetch_verified(remote: &ContentRemote, hash: ObjectHash) -> Result<Vec<u8>, StewardError> {
    let bytes = remote
        .get_object(hash)
        .await
        .map_err(|e| StewardError::Content(e.to_string()))?
        .ok_or_else(|| {
            StewardError::Content(format!(
                "object {} is absent from the remote",
                hash.to_hex()
            ))
        })?;
    verify(hash, &bytes)?;
    Ok(bytes)
}

/// Enforce the content-addressing invariant: the bytes must hash to the key.
fn verify(hash: ObjectHash, bytes: &[u8]) -> Result<(), StewardError> {
    let actual = ObjectHash::of_bytes(bytes);
    if actual != hash {
        return Err(StewardError::Content(format!(
            "fetched object hashes to {} but was fetched as {}",
            actual.to_hex(),
            hash.to_hex()
        )));
    }
    Ok(())
}
