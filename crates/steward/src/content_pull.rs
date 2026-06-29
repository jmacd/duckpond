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

use std::collections::{BTreeMap, HashMap, VecDeque};

use sync_store::ContentRemote;
use sync_store::content::{
    Commit, ManifestEntry, ObjectHash, TreeEntry, decode_manifest, decode_recipe, decode_series,
    decode_tree,
};
use tinyfs::{EntryType, NodeID, WD};
use tlogfs::PondUserMetadata;
use tokio::io::AsyncWriteExt;

use crate::{Ship, StewardError};

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
    /// The tip commit's node manifest: one entry per node, recording the
    /// source's `node_id` alongside its parent, name, type, and content
    /// address (Section 4.5).  Empty when the graph is empty.  Kept out of
    /// `objects`/`bytes` because the manifest is pond-specific identity, not
    /// part of the dedup-shareable pure-content closure.
    pub manifest: Vec<ManifestEntry>,
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
        let manifest_hash = tip_commit.node_manifest_hash;
        fetch_tree(remote, root, &mut graph).await?;
        graph.manifest = fetch_manifest(remote, manifest_hash).await?;
    }

    Ok(graph)
}

/// Fetch and decode the tip commit's node manifest, verifying its bytes hash to
/// the commit's `node_manifest_hash` (Section 4.5).
async fn fetch_manifest(
    remote: &ContentRemote,
    manifest_hash: ObjectHash,
) -> Result<Vec<ManifestEntry>, StewardError> {
    let bytes = fetch_verified(remote, manifest_hash).await?;
    decode_manifest(&bytes).map_err(|e| StewardError::Content(format!("decode manifest: {e}")))
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

/// The result of rebuilding a pond from a fetched object graph.  Counts reflect
/// nodes *created* in this rebuild; an incremental pull that only versions or
/// renames existing nodes reports zeros here.
#[derive(Debug, Clone, Default)]
pub struct RebuildOutcome {
    /// The tip commit's root tree hash that was rebuilt.
    pub root_tree_hash: Option<ObjectHash>,
    /// Number of directories created.
    pub dirs: usize,
    /// Number of single-version files/tables created.
    pub files: usize,
    /// Number of symlinks created.
    pub symlinks: usize,
    /// Number of multi-version series created.
    pub series: usize,
    /// Number of dynamic nodes created.
    pub dynamic: usize,
}

/// One filesystem operation in an incremental rebuild plan, in apply order.
///
/// The plan is a `node_id`-keyed diff of the fetched source manifest against
/// the target's current node state (Decision D8).  Deletions come first
/// (deepest-first), then creates/renames/versions in breadth-first order so a
/// parent directory is always materialized before its children.
#[derive(Debug, Clone)]
enum ApplyOp {
    /// Rename a node within its parent (identity and history preserved).
    Rename {
        parent: String,
        old: String,
        new: String,
    },
    /// Ensure a directory exists under `parent` as `name` with the adopted
    /// `node_id`, then register its working directory for descent.  `create`
    /// distinguishes adopting a new node from opening an existing one.
    Dir {
        parent: String,
        name: String,
        node_id: String,
        create: bool,
    },
    /// Create (adopting `node_id`) or append to a physical file / table /
    /// series node.  `versions` are the version blobs to write in order: every
    /// version on create, only the appended suffix on update.  `entry_type`
    /// drives writer finalization (series infer temporal bounds).
    File {
        parent: String,
        name: String,
        node_id: String,
        create: bool,
        entry_type: EntryType,
        versions: Vec<Vec<u8>>,
    },
    /// Create (adopting `node_id`) or rewrite a symlink.  A rewrite re-adopts
    /// the same `node_id` after unlinking, so identity is preserved.
    Symlink {
        parent: String,
        name: String,
        node_id: String,
        create: bool,
        target: String,
    },
    /// Create (adopting `node_id`) or rewrite a dynamic node from its recipe.
    Dynamic {
        parent: String,
        name: String,
        node_id: String,
        create: bool,
        factory: String,
        config: Vec<u8>,
    },
    /// Unlink a target node that is absent from the source.
    Delete { parent_path: String, name: String },
}

/// Rebuild or incrementally update a tlogfs pond from a fetched object graph
/// (design Section 8.5).
///
/// The fetched node manifest carries the source's real `node_id`s; the consumer
/// adopts them so the rebuilt pond is row-identical to the source and every
/// later pull is a `node_id`-keyed diff (Decision D8).  The target need not be
/// empty: this computes the target's current node state, diffs it against the
/// source manifest by `node_id`, and applies the difference -- creating new
/// nodes (with adopted ids), appending file/series versions, renaming moved
/// nodes in place, and deleting nodes absent from the source -- in a single
/// transaction.
///
/// # Errors
///
/// Returns an error if the graph is empty or carries no manifest, if the graph
/// references an object it does not contain, if a node's `entry_type` changed
/// or it was reparented (both unsupported), if an incoming series is not an
/// append-only extension of the one held, if a symlink target is not valid
/// UTF-8, if a recipe fails to decode, or if a write fails.  After applying,
/// the read-side fold of `target` must equal the tip's root tree hash and the
/// rebuilt node manifest hash must equal the tip commit's `node_manifest_hash`;
/// a mismatch is an error.
pub async fn rebuild_pond(
    target: &mut Ship,
    graph: &FetchedGraph,
) -> Result<RebuildOutcome, StewardError> {
    let root = graph
        .root_tree_hash()
        .ok_or_else(|| StewardError::Content("cannot rebuild from an empty graph".to_string()))?;
    if graph.manifest.is_empty() {
        return Err(StewardError::Content(
            "fetched graph has no node manifest".to_string(),
        ));
    }
    let tip_manifest_hash = graph
        .commits
        .first()
        .map(|(_, c)| c.node_manifest_hash)
        .ok_or_else(|| StewardError::Content("fetched graph has no tip commit".to_string()))?;

    let (target_nodes, target_series) = crate::content_tree::build_target_state(target).await?;

    let (ops, outcome) = plan_node_diff(graph, root, &target_nodes, &target_series)?;

    let root_node_id = src_root_id(graph)?.to_string();
    target
        .write_transaction(
            &PondUserMetadata::new(vec!["pull".to_string()]),
            async move |fs| {
                let root_wd = fs.root().await?;
                apply_ops(&root_node_id, root_wd, &ops).await?;
                Ok(())
            },
        )
        .await?;

    let report = crate::compute_content_tree(target).await?;
    if report.root_tree_hash != root {
        return Err(StewardError::Content(format!(
            "rebuilt pond folds to {} but the tip root tree is {}",
            report.root_tree_hash.to_hex(),
            root.to_hex()
        )));
    }

    let pond_id = target.data_persistence().pond_id().to_string();
    let table = target.data_persistence().table().clone();
    let (_, manifest_root) =
        crate::content_tree::compute_commit_roots_for_table(table, &pond_id).await?;
    if manifest_root != tip_manifest_hash {
        return Err(StewardError::Content(format!(
            "rebuilt node manifest hashes to {} but the tip commit's manifest is {}",
            manifest_root.to_hex(),
            tip_manifest_hash.to_hex()
        )));
    }

    Ok(outcome)
}

/// Cross-pond import: rebuild a *foreign* pond's tree under its own `pond_id`
/// partition (Section 8.5.2, mount scoping), so a mount entry at the import
/// path resolves into it.  Unlike [`rebuild_pond`] -- which mirrors the source
/// at the local root and adopts the local pond_id -- this writes the source's
/// nodes beneath the foreign pond's well-known root, diffing against whatever of
/// the foreign tree is already present, and advances only the foreign pond's
/// seq allocator so the local pond's contiguous numbering is untouched.
///
/// # Errors
///
/// Same conditions as [`rebuild_pond`], computed over `foreign_pond_id`: the
/// graph must carry a manifest, references must resolve, and the rebuilt tree
/// must fold to the tip root tree hash with a matching node manifest.
pub async fn import_pond(
    target: &mut Ship,
    graph: &FetchedGraph,
    foreign_pond_id: uuid7::Uuid,
) -> Result<RebuildOutcome, StewardError> {
    let root = graph
        .root_tree_hash()
        .ok_or_else(|| StewardError::Content("cannot import an empty graph".to_string()))?;
    if graph.manifest.is_empty() {
        return Err(StewardError::Content(
            "fetched graph has no node manifest".to_string(),
        ));
    }
    let tip_manifest_hash = graph
        .commits
        .first()
        .map(|(_, c)| c.node_manifest_hash)
        .ok_or_else(|| StewardError::Content("fetched graph has no tip commit".to_string()))?;

    let foreign_id = foreign_pond_id.to_string();
    let (target_nodes, target_series) =
        crate::content_tree::build_target_state_for_pond(target, &foreign_id).await?;
    let (ops, outcome) = plan_node_diff(graph, root, &target_nodes, &target_series)?;

    let root_node_id = src_root_id(graph)?.to_string();
    let first_import = target_nodes.is_empty();
    target
        .write_transaction(
            &PondUserMetadata::new(vec!["pull".to_string(), "import".to_string()]),
            async move |fs| {
                if first_import {
                    fs.initialize_foreign_root(foreign_pond_id).await?;
                }
                let foreign_node = fs.foreign_root_node(foreign_pond_id).await?;
                let foreign_np = tinyfs::NodePath {
                    node: foreign_node,
                    path: "/".into(),
                };
                let root_wd = fs.wd(&foreign_np, foreign_np.clone()).await?;
                apply_ops(&root_node_id, root_wd, &ops).await?;
                Ok(())
            },
        )
        .await?;

    let table = target.data_persistence().table().clone();
    let report =
        crate::content_tree::compute_content_tree_for_table(table.clone(), &foreign_id).await?;
    if report.root_tree_hash != root {
        return Err(StewardError::Content(format!(
            "imported foreign tree folds to {} but the tip root tree is {}",
            report.root_tree_hash.to_hex(),
            root.to_hex()
        )));
    }

    let (_, manifest_root) =
        crate::content_tree::compute_commit_roots_for_table(table, &foreign_id).await?;
    if manifest_root != tip_manifest_hash {
        return Err(StewardError::Content(format!(
            "imported node manifest hashes to {} but the tip commit's manifest is {}",
            manifest_root.to_hex(),
            tip_manifest_hash.to_hex()
        )));
    }

    // Advance only the foreign pond's seq frontier so the local allocator stays
    // contiguous; the highest source seq is the foreign tip's commit seq.
    let foreign_seq = graph
        .commits
        .iter()
        .map(|(_, c)| c.provenance.seq)
        .max()
        .unwrap_or(0);
    target
        .data_persistence_mut()
        .sync_last_txn_seq(&foreign_id, foreign_seq);

    Ok(outcome)
}
fn src_root_id(graph: &FetchedGraph) -> Result<&str, StewardError> {
    graph
        .manifest
        .iter()
        .find(|e| e.parent_node_id.is_empty() && e.name.is_empty())
        .map(|e| e.node_id.as_str())
        .ok_or_else(|| StewardError::Content("manifest has no root entry".to_string()))
}

/// Diff the fetched source manifest against the target's current node state,
/// keyed by `node_id`, producing the ordered apply plan and the create counts.
fn plan_node_diff(
    graph: &FetchedGraph,
    root: ObjectHash,
    target_nodes: &HashMap<String, ManifestEntry>,
    target_series: &HashMap<String, Vec<ObjectHash>>,
) -> Result<(Vec<ApplyOp>, RebuildOutcome), StewardError> {
    let root_id = src_root_id(graph)?.to_string();

    // Index the source manifest by node_id and by parent for breadth-first
    // ordering (parents before children).
    let mut source_by_id: HashMap<&str, &ManifestEntry> = HashMap::new();
    let mut children: HashMap<&str, Vec<&ManifestEntry>> = HashMap::new();
    for entry in &graph.manifest {
        let _ = source_by_id.insert(entry.node_id.as_str(), entry);
        if entry.node_id != root_id {
            children
                .entry(entry.parent_node_id.as_str())
                .or_default()
                .push(entry);
        }
    }
    for kids in children.values_mut() {
        kids.sort_by(|a, b| a.name.cmp(&b.name));
    }

    let mut ops = Vec::new();
    let mut outcome = RebuildOutcome {
        root_tree_hash: Some(root),
        ..RebuildOutcome::default()
    };

    // Deletions first: target nodes absent from the source, deepest-first so a
    // directory is emptied before it is unlinked.
    let mut deletions: Vec<&ManifestEntry> = target_nodes
        .values()
        .filter(|t| t.node_id != root_id && !source_by_id.contains_key(t.node_id.as_str()))
        .collect();
    deletions.sort_by_key(|t| std::cmp::Reverse(target_depth(&t.node_id, target_nodes)));
    for t in deletions {
        ops.push(ApplyOp::Delete {
            parent_path: target_path(&t.parent_node_id, target_nodes),
            name: t.name.clone(),
        });
    }

    // Creates / renames / versions in breadth-first order from the root.
    let mut queue: VecDeque<&str> = VecDeque::new();
    queue.push_back(root_id.as_str());
    while let Some(parent_id) = queue.pop_front() {
        let Some(kids) = children.get(parent_id) else {
            continue;
        };
        for entry in kids {
            plan_one(
                entry,
                graph,
                target_nodes,
                target_series,
                &mut ops,
                &mut outcome,
            )?;
            if entry.entry_type == EntryType::DirectoryPhysical {
                queue.push_back(entry.node_id.as_str());
            }
        }
    }

    Ok((ops, outcome))
}

/// Plan the operations for a single source node against its target twin.
fn plan_one(
    entry: &ManifestEntry,
    graph: &FetchedGraph,
    target_nodes: &HashMap<String, ManifestEntry>,
    target_series: &HashMap<String, Vec<ObjectHash>>,
    ops: &mut Vec<ApplyOp>,
    outcome: &mut RebuildOutcome,
) -> Result<(), StewardError> {
    let existing = target_nodes.get(&entry.node_id);
    let create = existing.is_none();

    if let Some(t) = existing {
        if t.parent_node_id != entry.parent_node_id {
            return Err(StewardError::Content(format!(
                "node {} was reparented from {} to {}; reparenting is not supported",
                entry.node_id, t.parent_node_id, entry.parent_node_id
            )));
        }
        if t.entry_type != entry.entry_type {
            return Err(StewardError::Content(format!(
                "node {} changed entry type from {:?} to {:?}; this is not supported",
                entry.node_id, t.entry_type, entry.entry_type
            )));
        }
        if t.name != entry.name {
            ops.push(ApplyOp::Rename {
                parent: entry.parent_node_id.clone(),
                old: t.name.clone(),
                new: entry.name.clone(),
            });
        }
    }

    let content_changed = existing.is_none_or(|t| t.child_hash != entry.child_hash);

    match entry.entry_type {
        EntryType::DirectoryPhysical => {
            if create {
                outcome.dirs += 1;
            }
            ops.push(ApplyOp::Dir {
                parent: entry.parent_node_id.clone(),
                name: entry.name.clone(),
                node_id: entry.node_id.clone(),
                create,
            });
        }
        EntryType::FilePhysicalVersion | EntryType::TablePhysicalVersion => {
            if create {
                outcome.files += 1;
            }
            let versions = if content_changed {
                vec![blob_bytes(graph, entry.child_hash)?]
            } else {
                Vec::new()
            };
            ops.push(ApplyOp::File {
                parent: entry.parent_node_id.clone(),
                name: entry.name.clone(),
                node_id: entry.node_id.clone(),
                create,
                entry_type: entry.entry_type,
                versions,
            });
        }
        EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries => {
            if create {
                outcome.series += 1;
            }
            let versions =
                plan_series_versions(entry, graph, target_series, existing.map(|t| t.child_hash))?;
            ops.push(ApplyOp::File {
                parent: entry.parent_node_id.clone(),
                name: entry.name.clone(),
                node_id: entry.node_id.clone(),
                create,
                entry_type: entry.entry_type,
                versions,
            });
        }
        EntryType::Symlink => {
            if create {
                outcome.symlinks += 1;
            }
            if create || content_changed {
                let bytes = blob_bytes(graph, entry.child_hash)?;
                let target = String::from_utf8(bytes).map_err(|e| {
                    StewardError::Content(format!("symlink target is not utf-8: {e}"))
                })?;
                ops.push(ApplyOp::Symlink {
                    parent: entry.parent_node_id.clone(),
                    name: entry.name.clone(),
                    node_id: entry.node_id.clone(),
                    create,
                    target,
                });
            }
        }
        EntryType::DirectoryDynamic | EntryType::FileDynamic | EntryType::TableDynamic => {
            if create {
                outcome.dynamic += 1;
            }
            if create || content_changed {
                let bytes = blob_bytes(graph, entry.child_hash)?;
                let (factory, config) = decode_recipe(&bytes).map_err(|e| {
                    StewardError::Content(format!("decode recipe for {}: {e}", entry.name))
                })?;
                ops.push(ApplyOp::Dynamic {
                    parent: entry.parent_node_id.clone(),
                    name: entry.name.clone(),
                    node_id: entry.node_id.clone(),
                    create,
                    factory,
                    config,
                });
            }
        }
    }
    Ok(())
}

/// Decide which series version blobs to write: all of them on create, or only
/// the appended suffix on update.  An update requires the versions already held
/// to be a prefix of the incoming list -- series are append-only (Section
/// 8.5.3), so any divergence is a hard error.
fn plan_series_versions(
    entry: &ManifestEntry,
    graph: &FetchedGraph,
    target_series: &HashMap<String, Vec<ObjectHash>>,
    existing_child_hash: Option<ObjectHash>,
) -> Result<Vec<Vec<u8>>, StewardError> {
    let incoming = series_hashes(graph, entry.child_hash)?;

    let held = match existing_child_hash {
        None => &[][..],
        Some(child_hash) if child_hash == entry.child_hash => return Ok(Vec::new()),
        Some(_) => target_series
            .get(&entry.node_id)
            .map(Vec::as_slice)
            .ok_or_else(|| {
                StewardError::Content(format!(
                    "series node {} changed but its current versions are unknown",
                    entry.node_id
                ))
            })?,
    };

    if incoming.len() < held.len() || incoming[..held.len()] != *held {
        return Err(StewardError::Content(format!(
            "series node {} is not an append-only extension of the versions held ({} held, {} incoming)",
            entry.node_id,
            held.len(),
            incoming.len()
        )));
    }

    incoming[held.len()..]
        .iter()
        .map(|hash| blob_bytes(graph, *hash))
        .collect()
}

/// Apply an ordered plan within an open transaction, adopting source node ids.
async fn apply_ops(root_node_id: &str, root_wd: WD, ops: &[ApplyOp]) -> Result<(), StewardError> {
    let mut dir_wd: HashMap<String, WD> = HashMap::new();
    let _ = dir_wd.insert(root_node_id.to_string(), root_wd.clone());

    for op in ops {
        match op {
            ApplyOp::Delete { parent_path, name } => {
                let pwd = if parent_path.is_empty() {
                    root_wd.clone()
                } else {
                    root_wd.open_dir_path(parent_path).await?
                };
                pwd.remove_entry(name).await?;
            }
            ApplyOp::Rename { parent, old, new } => {
                parent_wd(&dir_wd, parent)?.rename_entry(old, new).await?;
            }
            ApplyOp::Dir {
                parent,
                name,
                node_id,
                create,
            } => {
                let pwd = parent_wd(&dir_wd, parent)?.clone();
                let child = if *create {
                    pwd.insert_directory_with_id(name, parse_node_id(node_id)?)
                        .await?
                } else {
                    pwd.open_dir_path(name).await?
                };
                let _ = dir_wd.insert(node_id.clone(), child);
            }
            ApplyOp::File {
                parent,
                name,
                node_id,
                create,
                entry_type,
                versions,
            } => {
                let pwd = parent_wd(&dir_wd, parent)?;
                let mut remaining = versions.iter();
                if *create {
                    // The first version is written through the writer returned
                    // at creation: a pending file has no row to re-resolve by
                    // path yet.  An adopted file always has at least one
                    // version, but tolerate an empty create defensively.
                    if let Some(first) = remaining.next() {
                        let mut writer = pwd
                            .create_file_with_id(name, parse_node_id(node_id)?)
                            .await?;
                        writer.write_all(first).await?;
                        finalize_writer(writer, *entry_type).await?;
                    }
                }
                for bytes in remaining {
                    let mut writer = pwd.async_writer_path_with_type(name, *entry_type).await?;
                    writer.write_all(bytes).await?;
                    finalize_writer(writer, *entry_type).await?;
                }
            }
            ApplyOp::Symlink {
                parent,
                name,
                node_id,
                create,
                target,
            } => {
                let pwd = parent_wd(&dir_wd, parent)?;
                if !create {
                    pwd.remove_entry(name).await?;
                }
                pwd.insert_symlink_with_id(name, parse_node_id(node_id)?, target)
                    .await?;
            }
            ApplyOp::Dynamic {
                parent,
                name,
                node_id,
                create,
                factory,
                config,
            } => {
                let pwd = parent_wd(&dir_wd, parent)?;
                if !create {
                    pwd.remove_entry(name).await?;
                }
                pwd.insert_dynamic_with_id(name, parse_node_id(node_id)?, factory, config.clone())
                    .await?;
            }
        }
    }
    Ok(())
}

/// Finalize a version writer: a table series infers its temporal bounds from
/// the parquet footer (which also shuts the writer down); every other kind just
/// closes.
async fn finalize_writer(
    mut writer: std::pin::Pin<Box<dyn tinyfs::FileMetadataWriter>>,
    entry_type: EntryType,
) -> Result<(), StewardError> {
    if entry_type == EntryType::TablePhysicalSeries {
        let _ = writer.infer_temporal_bounds().await?;
    } else {
        writer.shutdown().await?;
    }
    Ok(())
}

/// Look up a parent directory's working directory by `node_id`, erroring if it
/// was not materialized earlier in the breadth-first plan.
fn parent_wd<'a>(dir_wd: &'a HashMap<String, WD>, node_id: &str) -> Result<&'a WD, StewardError> {
    dir_wd.get(node_id).ok_or_else(|| {
        StewardError::Content(format!(
            "parent directory {node_id} was not materialized before its child"
        ))
    })
}

/// Parse a manifest `node_id` string into a [`NodeID`].
fn parse_node_id(node_id: &str) -> Result<NodeID, StewardError> {
    NodeID::from_hex_string(node_id)
        .map_err(|e| StewardError::Content(format!("invalid node_id {node_id}: {e}")))
}

/// Depth of a target node from the root (root is 0), by walking parents.
fn target_depth(node_id: &str, target_nodes: &HashMap<String, ManifestEntry>) -> usize {
    let mut depth = 0;
    let mut current = node_id;
    while let Some(entry) = target_nodes.get(current) {
        if entry.parent_node_id.is_empty() {
            break;
        }
        depth += 1;
        current = &entry.parent_node_id;
    }
    depth
}

/// Reconstruct the absolute path of a target directory node from its manifest
/// parent chain (empty string for the root).
fn target_path(node_id: &str, target_nodes: &HashMap<String, ManifestEntry>) -> String {
    let mut names = Vec::new();
    let mut current = node_id;
    while let Some(entry) = target_nodes.get(current) {
        if entry.parent_node_id.is_empty() {
            break;
        }
        names.push(entry.name.as_str());
        current = &entry.parent_node_id;
    }
    names.reverse();
    if names.is_empty() {
        String::new()
    } else {
        format!("/{}", names.join("/"))
    }
}

/// Look up a leaf blob's bytes in the fetched graph.
fn blob_bytes(graph: &FetchedGraph, hash: ObjectHash) -> Result<Vec<u8>, StewardError> {
    match graph.objects.get(&hash) {
        Some(FetchedObject::Blob(bytes)) => Ok(bytes.clone()),
        Some(_) => Err(StewardError::Content(format!(
            "expected a blob at {} but found a structured object",
            hash.to_hex()
        ))),
        None => Err(StewardError::Content(format!(
            "blob object {} missing from graph",
            hash.to_hex()
        ))),
    }
}

/// Resolve a series object to its ordered list of version blob hashes.
fn series_hashes(
    graph: &FetchedGraph,
    series_hash: ObjectHash,
) -> Result<&[ObjectHash], StewardError> {
    match graph.objects.get(&series_hash) {
        Some(FetchedObject::Series(versions)) => Ok(versions),
        Some(_) => Err(StewardError::Content(format!(
            "expected a series at {} but found a non-series object",
            series_hash.to_hex()
        ))),
        None => Err(StewardError::Content(format!(
            "series object {} missing from graph",
            series_hash.to_hex()
        ))),
    }
}
