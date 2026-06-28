// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Read-side content-tree computation (the SPACE layer over live state).
//!
//! This module reads a pond's live filesystem and folds it into a single
//! `root_tree_hash` using the content-addressed object model from
//! [`sync_store::content`].  It is the read-only counterpart to the
//! commit-time fold described in `docs/content-addressed-pond-design.md`
//! Section 5: it proves the object model against real ponds and answers the
//! comparison question (Goal 2) -- two ponds (or two subtrees) are identical
//! iff their tree hashes match -- without persisting anything.
//!
//! # How it reads live state
//!
//! Like [`crate::fsck`], it scans the data table once.  A directory's *latest*
//! `OplogEntry` row stores its complete live entry set (Arrow IPC of
//! [`tlogfs::DirectoryEntry`]), so the current tree is reconstructed directly
//! from the latest row per node with no operation replay.  The fold then runs
//! bottom-up from the local pond's root.
//!
//! # `child_hash` by node kind (design Section 9)
//!
//! | Node kind                                   | `child_hash`                          |
//! |---------------------------------------------|---------------------------------------|
//! | physical directory                          | recursive [`tree_hash`]               |
//! | physical file / table (single version)      | the version blob hash (`blake3`)      |
//! | physical series (multi-version)             | [`series_hash`] over version blobs    |
//! | symlink                                     | `blake3(target bytes)`                |
//! | dynamic dir / file / `table:dynamic`        | [`recipe_hash`] (factory + config)    |
//!
//! Dynamic nodes hash their stored definition (factory type plus config), not
//! their computed output, and their generated children are not folded in.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use datafusion::execution::context::SessionContext;

use sync_store::content::{
    ManifestEntry, ObjectHash, TreeEntry, encode_manifest, encode_recipe, encode_series,
    encode_tree, manifest_hash, recipe_hash, series_hash,
};
use tinyfs::{EntryType, ROOT_UUID};
use tlogfs::schema::{OplogEntry, decode_directory_entries};

use crate::{Ship, StewardError};

/// Result of a [`compute_content_tree`] run.
#[derive(Debug, Clone)]
pub struct ContentTreeReport {
    /// The content hash of the local pond's root directory tree.  Equal roots
    /// mean byte-identical content across the whole pond.
    pub root_tree_hash: ObjectHash,
    /// Number of distinct nodes folded into the root.
    pub nodes_hashed: usize,
}

/// The materialized content objects reachable from a pond's root tree.
///
/// Produced by [`materialize_content_objects`].  Per Decision D7 the objects
/// split by where their bytes live: small objects (trees, series manifests,
/// symlinks, recipes, and small blobs) carry their bytes inline and become
/// `objects` rows in a push; large blobs carry only their hash and transfer
/// via the external `_large_files` path.  Both are keyed by the same BLAKE3
/// hash, so reachability and dedup are uniform.
///
/// The node manifest (Section 4.5) is also included inline, since the commit
/// references it by hash and a consumer must fetch it to adopt the source's
/// node_ids.  Commit objects are NOT included here -- they are produced by the
/// commit path and added by the push layer on top of this closure.
#[derive(Debug, Clone, Default)]
pub struct MaterializedObjects {
    /// Objects whose bytes are carried inline, keyed by content hash.  These are
    /// pure content (trees, series, symlinks, recipes, small blobs) and so
    /// dedup across ponds; identity-bearing objects are kept out (see
    /// `manifest`).
    pub inline: BTreeMap<ObjectHash, Vec<u8>>,
    /// Large-blob hashes whose bytes transfer via the external path.
    pub external_blobs: BTreeSet<ObjectHash>,
    /// The node manifest object: its hash and bytes (Section 4.5).  Kept
    /// separate from `inline` because it carries the source's node_ids, so it
    /// is pond-specific and must not be counted as shareable content -- two
    /// ponds with identical content still have different manifests.  `None`
    /// only on a default-constructed value; a real fold always produces one.
    pub manifest: Option<(ObjectHash, Vec<u8>)>,
}

impl MaterializedObjects {
    /// Record an inline object (idempotent: re-recording a hash is a no-op).
    fn put_inline(&mut self, hash: ObjectHash, bytes: Vec<u8>) {
        let _ = self.inline.entry(hash).or_insert(bytes);
    }

    /// Record a large blob to transfer externally by hash.
    fn put_external(&mut self, hash: ObjectHash) {
        let _ = self.external_blobs.insert(hash);
    }

    /// Total number of distinct objects (inline, external, and the manifest).
    #[must_use]
    pub fn len(&self) -> usize {
        self.inline.len() + self.external_blobs.len() + usize::from(self.manifest.is_some())
    }

    /// True when no objects were materialized.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inline.is_empty() && self.external_blobs.is_empty() && self.manifest.is_none()
    }
}

/// One child entry of a directory, captured during the fold so that a later
/// comparison can descend by `child_hash` without re-reading the data table.
#[derive(Debug, Clone)]
pub(crate) struct ChildRef {
    /// The entry name within its parent directory.
    pub name: String,
    /// The entry kind (drives how the child contributes its `child_hash`).
    pub entry_type: EntryType,
    /// The content hash this child contributes to its parent's tree hash.
    pub child_hash: ObjectHash,
    /// The child node's own `node_id`, captured so the node manifest can record
    /// identity alongside the content tree (Section 4.5).
    pub child_node_id: String,
    /// The child directory's node key, present only when the child is a
    /// physical directory (the only kind a diff can descend into).
    pub child_dir_key: Option<NodeKey>,
}

/// An in-memory index of a pond's content tree: the root hash plus, for every
/// physical directory node, its sorted child entries with their child hashes.
///
/// Built once per pond by [`build_content_tree_for_table`]; consumed by the
/// content-tree comparison in [`crate::content_diff`], which walks two indices
/// top-down and prunes any subtree whose `child_hash` already matches.
pub(crate) struct ContentTreeIndex {
    /// The local pond's root directory tree hash.
    pub root_tree_hash: ObjectHash,
    /// The node key of the local pond's root directory.
    pub root_key: NodeKey,
    /// Per physical-directory child lists, in name order.
    pub dirs: HashMap<NodeKey, Vec<ChildRef>>,
    /// Per series node, its ordered version blob hashes (ascending version).
    /// Lets an incremental rebuild compute the suffix it must append to a
    /// series it already holds (design Section 8.5.3).
    pub series_versions: HashMap<NodeKey, Vec<ObjectHash>>,
    /// Number of distinct nodes folded into the root.
    pub nodes_hashed: usize,
}

/// Composite identity of a node within the data table: `(pond_id, node_id)`.
///
/// Keying by both keeps cross-pond imports correct, because every pond's root
/// shares the same well-known `node_id` and would otherwise collide.
pub(crate) type NodeKey = (String, String);

/// The latest-version facts about one node needed to hash it.
struct NodeFacts {
    /// Directory entry bytes (for directories) or the node's content (for
    /// inline files, symlink targets, and dynamic config).  `None` when the
    /// content is externalized (large file) or empty.
    content: Option<Vec<u8>>,
    /// The recorded `blake3` of this version, if any.
    blake3: Option<String>,
    /// The factory type for a dynamic node (`None` for physical nodes).  Folded
    /// into the recipe hash so the content commits to the factory, not just its
    /// config (Decision D4).
    factory: Option<String>,
}

/// One version of a series: its blob hash plus the inline bytes when the
/// version is small enough to be stored in-row.  `content` is `None` for an
/// externalized (large) version, whose bytes transfer via the external path.
struct VersionBlob {
    hash: ObjectHash,
    content: Option<Vec<u8>>,
}

/// Compute the local pond's `root_tree_hash` from its live filesystem state.
///
/// Reads the data table once, reconstructs the current tree, and folds it
/// bottom-up.  Pure and side-effect free.
///
/// # Errors
///
/// Returns an error if the data table cannot be read, if the local pond has no
/// root directory row, if a referenced child node is missing, or if directory
/// content cannot be decoded.
pub async fn compute_content_tree(ship: &Ship) -> Result<ContentTreeReport, StewardError> {
    let local_pond_id = ship.data_persistence().pond_id().to_string();
    let table = ship.data_persistence().table().clone();
    compute_content_tree_for_table(table, &local_pond_id).await
}

/// Compute a pond's `root_tree_hash` directly from a `DeltaTable` handle.
///
/// This is the table-level entry point used by the commit path, where no
/// active transaction is held: it opens a fresh `SessionContext`, reads the
/// data table once, reconstructs the current tree, and folds it bottom-up.
/// Pure and side-effect free.
///
/// # Errors
///
/// Returns an error if the data table cannot be read, if the named pond has no
/// root directory row, if a referenced child node is missing, or if directory
/// content cannot be decoded.
pub async fn compute_content_tree_for_table(
    table: deltalake::DeltaTable,
    local_pond_id: &str,
) -> Result<ContentTreeReport, StewardError> {
    let index = build_content_tree_for_table(table, local_pond_id).await?;
    Ok(ContentTreeReport {
        root_tree_hash: index.root_tree_hash,
        nodes_hashed: index.nodes_hashed,
    })
}

/// Build the full content-tree index for a pond from a `DeltaTable` handle.
///
/// Reads the data table once, reconstructs the current tree, and folds it
/// bottom-up while capturing every physical directory's child list (so a later
/// comparison can descend by `child_hash`).  Pure and side-effect free.
///
/// # Errors
///
/// Returns an error if the data table cannot be read, if the named pond has no
/// root directory row, if a referenced child node is missing, or if directory
/// content cannot be decoded.
pub(crate) async fn build_content_tree_for_table(
    table: deltalake::DeltaTable,
    local_pond_id: &str,
) -> Result<ContentTreeIndex, StewardError> {
    scan_and_fold(table, local_pond_id, None).await
}

/// Build the node manifest for a pond's content tree from an already-built
/// index: one [`ManifestEntry`] per node, recording the source's `node_id`
/// alongside its parent, name, type, and content address (Section 4.5).
///
/// Every non-root node appears exactly once as a child of its parent directory;
/// the root has no parent, so it is added explicitly with an empty parent and
/// name.  The manifest is the one place node identity is recorded, so a
/// consumer can adopt these ids and mirror the source row-for-row (Decision
/// D8).
pub(crate) fn node_manifest_entries(index: &ContentTreeIndex) -> Vec<ManifestEntry> {
    let local_pond = &index.root_key.0;
    let mut entries = Vec::with_capacity(index.nodes_hashed.max(1));
    entries.push(ManifestEntry::new(
        index.root_key.1.clone(),
        String::new(),
        String::new(),
        EntryType::DirectoryPhysical,
        index.root_tree_hash,
    ));
    for (dir_key, children) in &index.dirs {
        if &dir_key.0 != local_pond {
            continue;
        }
        let parent_node_id = &dir_key.1;
        for child in children {
            // Skip cross-pond mount points: a child resolving into a foreign
            // pond belongs to that pond's own manifest, and its root shares the
            // well-known ROOT_UUID, which would collide here.
            if let Some((child_pond, _)) = &child.child_dir_key
                && child_pond != local_pond
            {
                continue;
            }
            entries.push(ManifestEntry::new(
                child.child_node_id.clone(),
                parent_node_id.clone(),
                child.name.clone(),
                child.entry_type,
                child.child_hash,
            ));
        }
    }
    entries
}

/// Compute the two content roots a commit references: the `root_tree_hash` and
/// the `node_manifest_hash` (Section 4.3).  Both come from a single fold of the
/// data table, so they are guaranteed consistent with each other.
///
/// # Errors
///
/// Returns an error if the data table cannot be read or folded, or if the
/// manifest cannot be encoded (a duplicate `node_id`).
pub(crate) async fn compute_commit_roots_for_table(
    table: deltalake::DeltaTable,
    local_pond_id: &str,
) -> Result<(ObjectHash, ObjectHash), StewardError> {
    let index = build_content_tree_for_table(table, local_pond_id).await?;
    let manifest = node_manifest_entries(&index);
    let manifest_root = manifest_hash(&manifest).map_err(StewardError::Content)?;
    Ok((index.root_tree_hash, manifest_root))
}

/// Build a target pond's current node state for an incremental rebuild: a map
/// from `node_id` to its [`ManifestEntry`], and a map from each series
/// `node_id` to its ordered version blob hashes.
///
/// The maps are keyed by `node_id` alone (not the full `NodeKey`) because an
/// incremental pull operates within a single mirror pond; the diff against the
/// fetched source manifest is `node_id`-keyed (Decision D8).
///
/// # Errors
///
/// Returns an error if the data table cannot be read or folded.
pub(crate) async fn build_target_state(
    ship: &Ship,
) -> Result<
    (
        HashMap<String, ManifestEntry>,
        HashMap<String, Vec<ObjectHash>>,
    ),
    StewardError,
> {
    let local_pond_id = ship.data_persistence().pond_id().to_string();
    let table = ship.data_persistence().table().clone();
    let index = build_content_tree_for_table(table, &local_pond_id).await?;
    let by_id = node_manifest_entries(&index)
        .into_iter()
        .map(|e| (e.node_id.clone(), e))
        .collect();
    let series = index
        .series_versions
        .into_iter()
        .map(|((_pond, node_id), versions)| (node_id, versions))
        .collect();
    Ok((by_id, series))
}

/// Materialize the content objects reachable from a pond's root tree.
///
/// Reads the data table once and folds it exactly like the hash path, but also
/// captures each object's bytes: encoded tree objects, series manifests, and
/// small blob/symlink/recipe bytes inline, with large blobs recorded by hash
/// for external transfer (Decision D7).  Commit objects are added separately by
/// the push layer.  Pure and side-effect free.
///
/// # Errors
///
/// Returns an error if the data table cannot be read, if the named pond has no
/// root directory row, if a referenced child node is missing, or if directory
/// content cannot be decoded.
pub async fn materialize_content_objects(ship: &Ship) -> Result<MaterializedObjects, StewardError> {
    let local_pond_id = ship.data_persistence().pond_id().to_string();
    let table = ship.data_persistence().table().clone();
    let mut materialized = MaterializedObjects::default();
    let index = scan_and_fold(table, &local_pond_id, Some(&mut materialized)).await?;
    // The node manifest travels with the closure so a consumer can adopt the
    // source's node_ids (Section 4.5).  It is kept separate from the pure
    // content objects because it is pond-specific (it carries node_ids); the
    // commit references it by hash.
    let manifest = node_manifest_entries(&index);
    let manifest_bytes = encode_manifest(&manifest).map_err(StewardError::Content)?;
    materialized.manifest = Some((ObjectHash::of_bytes(&manifest_bytes), manifest_bytes));
    Ok(materialized)
}

/// Shared scan-and-fold core for the hash, index, and materialization paths.
///
/// When `sink` is `Some`, every folded object's bytes are recorded into it
/// (split inline vs external per Decision D7); when `None`, only hashes are
/// computed.  Either way the returned [`ContentTreeIndex`] is identical, so the
/// child-hash rules live in exactly one implementation.
async fn scan_and_fold(
    table: deltalake::DeltaTable,
    local_pond_id: &str,
    sink: Option<&mut MaterializedObjects>,
) -> Result<ContentTreeIndex, StewardError> {
    let ctx = SessionContext::new();
    let _previous = ctx
        .register_table("content_live", Arc::new(table))
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;

    let batches = ctx
        .sql("SELECT * FROM content_live ORDER BY pond_id, part_id, node_id, version")
        .await
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?
        .collect()
        .await
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;

    // Latest-version facts per node, and per-version blobs for series.
    // Rows arrive in ascending version order, so later rows overwrite earlier
    // ones for the latest-version snapshot.
    let mut latest: HashMap<NodeKey, NodeFacts> = HashMap::new();
    let mut series_versions: HashMap<NodeKey, BTreeMap<i64, VersionBlob>> = HashMap::new();

    for batch in &batches {
        let rows: Vec<OplogEntry> = serde_arrow::from_record_batch(batch)
            .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
        for row in rows {
            let key = (row.pond_id.clone(), row.node_id.to_string());

            if matches!(
                row.file_type,
                EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries
            ) {
                let hash = row_blob_hash(&row.blake3, row.content.as_deref());
                let _ = series_versions.entry(key.clone()).or_default().insert(
                    row.version,
                    VersionBlob {
                        hash,
                        content: row.content.clone(),
                    },
                );
            }

            let _ = latest.insert(
                key,
                NodeFacts {
                    content: row.content,
                    blake3: row.blake3,
                    factory: row.factory,
                },
            );
        }
    }

    let root_key = (local_pond_id.to_string(), ROOT_UUID.to_string());
    if !latest.contains_key(&root_key) {
        return Err(StewardError::DeltaLake(
            "local pond has no root directory row".to_string(),
        ));
    }

    let mut memo: HashMap<NodeKey, ObjectHash> = HashMap::new();
    let mut in_progress: Vec<NodeKey> = Vec::new();
    let mut dirs: HashMap<NodeKey, Vec<ChildRef>> = HashMap::new();
    let root_tree_hash = hash_directory(
        &root_key,
        &latest,
        &series_versions,
        &mut memo,
        &mut in_progress,
        &mut dirs,
        sink,
    )?;

    let series_version_hashes = series_versions
        .into_iter()
        .map(|(key, versions)| (key, versions.values().map(|v| v.hash).collect()))
        .collect();

    Ok(ContentTreeIndex {
        root_tree_hash,
        root_key,
        dirs,
        series_versions: series_version_hashes,
        nodes_hashed: memo.len(),
    })
}

/// Compute the blob hash of a file version: the recorded `blake3` when present
/// and well-formed, else a hash of the inline content (empty if externalized).
fn row_blob_hash(blake3: &Option<String>, content: Option<&[u8]>) -> ObjectHash {
    if let Some(hex) = blake3
        && let Ok(h) = ObjectHash::from_hex(hex)
    {
        return h;
    }
    ObjectHash::of_bytes(content.unwrap_or(&[]))
}

/// Fold one directory (by key) into its recursive [`tree_hash`], recording its
/// child list into `dirs` for later comparison.  When `sink` is `Some`, the
/// encoded tree object bytes (and, via `hash_child`, descendant object bytes)
/// are recorded for materialization.
#[allow(clippy::too_many_arguments)]
fn hash_directory(
    key: &NodeKey,
    latest: &HashMap<NodeKey, NodeFacts>,
    series_versions: &HashMap<NodeKey, BTreeMap<i64, VersionBlob>>,
    memo: &mut HashMap<NodeKey, ObjectHash>,
    in_progress: &mut Vec<NodeKey>,
    dirs: &mut HashMap<NodeKey, Vec<ChildRef>>,
    mut sink: Option<&mut MaterializedObjects>,
) -> Result<ObjectHash, StewardError> {
    if let Some(h) = memo.get(key) {
        return Ok(*h);
    }
    if in_progress.contains(key) {
        return Err(StewardError::DeltaLake(format!(
            "directory cycle detected at node {}/{}",
            key.0, key.1
        )));
    }

    let facts = latest.get(key).ok_or_else(|| {
        StewardError::DeltaLake(format!("missing directory node {}/{}", key.0, key.1))
    })?;
    let entries = decode_directory_entries(facts.content.as_deref().unwrap_or(&[]))
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;

    in_progress.push(key.clone());

    let mut tree_entries: Vec<TreeEntry> = Vec::with_capacity(entries.len());
    let mut children: Vec<ChildRef> = Vec::with_capacity(entries.len());
    for entry in entries {
        // A child belongs to its parent's pond unless it carries an explicit
        // foreign pond_id (a cross-pond import mount point).
        let child_pond = entry.pond_id.clone().unwrap_or_else(|| key.0.clone());
        let child_key = (child_pond, entry.child_node_id.to_string());
        // A foreign-pond mount whose subtree was not replicated here is opaque:
        // its content lives in its own pond's tree, so fold by mount identity
        // rather than recursing into rows that are absent locally.
        let is_unresolved_mount = child_key.0 != key.0 && !latest.contains_key(&child_key);
        let child_hash = if is_unresolved_mount {
            ObjectHash::of_bytes(format!("mount:{}/{}", child_key.0, child_key.1).as_bytes())
        } else {
            hash_child(
                &child_key,
                entry.entry_type,
                latest,
                series_versions,
                memo,
                in_progress,
                dirs,
                sink.as_deref_mut(),
            )?
        };
        let child_node_id = child_key.1.clone();
        let child_dir_key = if entry.entry_type == EntryType::DirectoryPhysical {
            Some(child_key)
        } else {
            None
        };
        children.push(ChildRef {
            name: entry.name.clone(),
            entry_type: entry.entry_type,
            child_hash,
            child_node_id,
            child_dir_key,
        });
        tree_entries.push(TreeEntry::new(entry.name, entry.entry_type, child_hash));
    }

    let _ = in_progress.pop();

    let encoded = encode_tree(&tree_entries).map_err(StewardError::DeltaLake)?;
    let hash = ObjectHash::of_bytes(&encoded);
    if let Some(sink) = sink {
        sink.put_inline(hash, encoded);
    }
    let _ = memo.insert(key.clone(), hash);
    let _ = dirs.insert(key.clone(), children);
    Ok(hash)
}

/// Compute the `child_hash` an entry of the given kind contributes to its
/// parent, dispatching on the entry type per design Section 9.  When `sink` is
/// `Some`, the child's object bytes are recorded for materialization.
#[allow(clippy::too_many_arguments)]
fn hash_child(
    key: &NodeKey,
    entry_type: EntryType,
    latest: &HashMap<NodeKey, NodeFacts>,
    series_versions: &HashMap<NodeKey, BTreeMap<i64, VersionBlob>>,
    memo: &mut HashMap<NodeKey, ObjectHash>,
    in_progress: &mut Vec<NodeKey>,
    dirs: &mut HashMap<NodeKey, Vec<ChildRef>>,
    sink: Option<&mut MaterializedObjects>,
) -> Result<ObjectHash, StewardError> {
    match entry_type {
        EntryType::DirectoryPhysical => {
            hash_directory(key, latest, series_versions, memo, in_progress, dirs, sink)
        }
        EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries => {
            let versions = series_versions.get(key).ok_or_else(|| {
                StewardError::DeltaLake(format!("missing series node {}/{}", key.0, key.1))
            })?;
            let ordered: Vec<ObjectHash> = versions.values().map(|v| v.hash).collect();
            let series = series_hash(&ordered);
            if let Some(sink) = sink {
                // The series manifest object, plus each version blob: small
                // versions inline, large (externalized) versions by hash (D7).
                sink.put_inline(series, encode_series(&ordered));
                for v in versions.values() {
                    record_blob(sink, v.hash, v.content.as_deref());
                }
            }
            Ok(series)
        }
        // Symlinks hash their target bytes; dynamic nodes hash their recipe
        // (factory type plus config), so the content commits to the factory and
        // a consumer can reconstruct which factory to instantiate (D4).
        EntryType::Symlink => {
            let facts = leaf_facts(key, latest)?;
            let bytes = facts.content.as_deref().unwrap_or(&[]);
            let hash = ObjectHash::of_bytes(bytes);
            if let Some(sink) = sink {
                // Symlink targets are small; always inline.
                sink.put_inline(hash, bytes.to_vec());
            }
            Ok(hash)
        }
        EntryType::DirectoryDynamic | EntryType::FileDynamic | EntryType::TableDynamic => {
            let facts = leaf_facts(key, latest)?;
            let factory = facts.factory.as_deref().ok_or_else(|| {
                StewardError::DeltaLake(format!(
                    "dynamic node {}/{} is missing its factory type",
                    key.0, key.1
                ))
            })?;
            let config = facts.content.as_deref().unwrap_or(&[]);
            let hash = recipe_hash(factory, config);
            if let Some(sink) = sink {
                // Recipes (factory + config) are small; always inline.
                sink.put_inline(hash, encode_recipe(factory, config));
            }
            Ok(hash)
        }
        // Single-version physical file or table: the version blob hash.
        EntryType::FilePhysicalVersion | EntryType::TablePhysicalVersion => {
            let facts = leaf_facts(key, latest)?;
            let hash = row_blob_hash(&facts.blake3, facts.content.as_deref());
            if let Some(sink) = sink {
                record_blob(sink, hash, facts.content.as_deref());
            }
            Ok(hash)
        }
    }
}

/// Record a file/version blob into the materialization sink: inline when the
/// bytes are in-row (small), external by hash when the content is `None`
/// (an externalized large file -- Decision D7).
fn record_blob(sink: &mut MaterializedObjects, hash: ObjectHash, content: Option<&[u8]>) {
    match content {
        Some(bytes) => sink.put_inline(hash, bytes.to_vec()),
        None => sink.put_external(hash),
    }
}

/// Look up a non-directory node's latest facts, erroring if it is missing.
fn leaf_facts<'a>(
    key: &NodeKey,
    latest: &'a HashMap<NodeKey, NodeFacts>,
) -> Result<&'a NodeFacts, StewardError> {
    latest
        .get(key)
        .ok_or_else(|| StewardError::DeltaLake(format!("missing node {}/{}", key.0, key.1)))
}
