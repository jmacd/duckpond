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
    Commit, ManifestEntry, ObjectHash, Provenance, TreeEntry, encode_manifest, encode_recipe,
    encode_series, encode_tree, manifest_hash, recipe_hash, series_hash,
};
use tinyfs::{EntryType, ROOT_UUID};
use tlogfs::schema::{OplogEntry, decode_directory_entries};

use crate::control_table::{CommitSpine, ControlTable};
use crate::{Ship, StewardError};

/// Compute the content-graph commit spine for a just-landed write or
/// compaction.
///
/// Folds the post-commit live tree into a `root_tree_hash`, looks up the
/// previous commit on this pond's chain (the `DataCommitted` record at
/// `txn_seq - 1`), builds the [`Commit`] object, and returns its hex spine.
/// The parent is `None` at genesis or wherever an earlier seq did not stamp a
/// spine; such gaps break the chain for that link but leave each commit's own
/// hash well-defined.
///
/// Shared by the guard's write commit and [`crate::Ship::compact`] so both
/// paths stamp an identical spine and every committed data write is reachable
/// via a content-graph commit for push.
pub(crate) async fn compute_commit_spine(
    table: deltalake::DeltaTable,
    control_table: &ControlTable,
    pond_id: uuid::Uuid,
    txn_seq: i64,
    request: String,
) -> Result<Option<CommitSpine>, StewardError> {
    let pond_id_str = pond_id.to_string();
    let (root_tree_hash, node_manifest_hash) =
        compute_commit_roots_for_table(table, &pond_id_str).await?;
    assemble_commit_spine(
        control_table,
        &pond_id_str,
        txn_seq,
        request,
        root_tree_hash,
        node_manifest_hash,
    )
    .await
}

/// The two content roots plus the partition checksums for a just-landed commit,
/// all derived from a single scan of the data table (design "Incremental
/// Content Tree", Tier 0).  Replaces the pre-Tier-0 pair of independent full
/// scans (one for the fold, one for the checksums).
pub(crate) struct CommitSnapshot {
    pub root_tree_hash: ObjectHash,
    pub node_manifest_hash: ObjectHash,
    pub partition_checksums: sync_steward::PartitionChecksums,
}

/// Scan a pond's live rows once and derive both content roots and the partition
/// checksums from the same rows, so the per-commit path reads the data table a
/// single time instead of twice.  `pond_id` is the local pond whose rows feed
/// the checksums and whose root the fold starts from.
///
/// # Errors
///
/// Returns an error if the data table cannot be scanned or folded, or if a row
/// cannot be hashed into its checksum leaf.
pub(crate) async fn compute_commit_snapshot(
    table: deltalake::DeltaTable,
    pond_id: uuid::Uuid,
) -> Result<CommitSnapshot, StewardError> {
    let pond_id_str = pond_id.to_string();
    let rows = scan_live_rows(table, false).await?;
    let partition_checksums =
        crate::remote_adapter::partition_checksums_from_rows(&rows, &pond_id_str)
            .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
    let index = fold_rows(rows, &pond_id_str, None)?;
    let manifest = node_manifest_entries(&index);
    let node_manifest_hash = manifest_hash(&manifest).map_err(StewardError::Content)?;
    Ok(CommitSnapshot {
        root_tree_hash: index.root_tree_hash,
        node_manifest_hash,
        partition_checksums,
    })
}

/// Assemble a [`CommitSpine`] from precomputed content roots: look up the
/// previous commit on this pond's chain, build the [`Commit`] object, and
/// return its hex spine.  Split from [`compute_commit_spine`] so the per-commit
/// path can reuse the single-scan roots from [`compute_commit_snapshot`]
/// instead of re-folding the tree.
pub(crate) async fn assemble_commit_spine(
    control_table: &ControlTable,
    pond_id_str: &str,
    txn_seq: i64,
    request: String,
    root_tree_hash: ObjectHash,
    node_manifest_hash: ObjectHash,
) -> Result<Option<CommitSpine>, StewardError> {
    let parent_commit_hash = parse_optional_parent_commit(
        control_table.commit_hash_at(txn_seq - 1).await?,
        txn_seq - 1,
        pond_id_str,
    )?;

    let provenance = Provenance {
        pond_id: pond_id_str.to_string(),
        seq: txn_seq,
        time_micros: chrono::Utc::now().timestamp_micros(),
        author: String::new(),
        request,
    };
    let commit = Commit::new(
        root_tree_hash,
        parent_commit_hash,
        node_manifest_hash,
        provenance,
    );

    Ok(Some(CommitSpine {
        root_tree_hash: root_tree_hash.to_hex(),
        parent_commit_hash: parent_commit_hash.map(|h| h.to_hex()),
        commit_hash: commit.hash().to_hex(),
        commit_object: hex::encode(commit.encode()),
    }))
}

/// Parse the parent commit's stored hex hash for a new commit's spine.
///
/// `None` (no record at `parent_seq`) is the legitimate chain-gap / genesis
/// case and stays `None`.  A present-but-malformed hex is corruption, not a
/// gap: it is a hard error rather than being silently collapsed to genesis,
/// which would recompute this commit's hash as if it had no parent and break
/// the chain invisibly.
fn parse_optional_parent_commit(
    stored: Option<String>,
    parent_seq: i64,
    pond_id_str: &str,
) -> Result<Option<ObjectHash>, StewardError> {
    match stored {
        None => Ok(None),
        Some(hex) => ObjectHash::from_hex(&hex).map(Some).map_err(|e| {
            StewardError::Content(format!(
                "corrupt parent commit hash at seq {parent_seq} for pond {pond_id_str}: {e} \
                 (value {hex:?})"
            ))
        }),
    }
}

/// Reconcile a pond's transparency-log tiles with its committed leaf sequence
/// and re-emit the checkpoint (design Decision D5).
///
/// The authoritative leaf sequence is the ordered `commit_object` bytes of the
/// spine-bearing `DataCommitted` records; the tile log is a derived,
/// re-materializable export.  The writer drives its next leaf position from the
/// committed leaf count, replaying every leaf the export is missing in commit
/// order, so a dropped append self-heals on the next commit.
///
/// Failures are logged and swallowed: the transparency log is a derived
/// publishing artifact and must not unwind an already-committed transaction.
/// Shared by the guard's write commit and [`crate::Ship::compact`].
pub(crate) async fn materialize_tlog(
    pond_path: &std::path::Path,
    control_table: &ControlTable,
    pond_id: uuid::Uuid,
) {
    let dir = crate::get_tlog_path(pond_path);
    let origin = format!("duckpond/{pond_id}");
    let log = sync_store::TileLog::new(dir, origin);

    let leaves = match control_table.commit_objects_in_order().await {
        Ok(l) => l,
        Err(e) => {
            log::error!("failed to read transparency-log leaf sequence: {e}");
            return;
        }
    };

    let exported = match log.size() {
        Ok(n) => n as usize,
        Err(e) => {
            log::error!("failed to read transparency-log checkpoint size: {e}");
            return;
        }
    };

    if exported >= leaves.len() {
        return;
    }

    let mut missing = Vec::with_capacity(leaves.len() - exported);
    for hex in &leaves[exported..] {
        match hex::decode(hex) {
            Ok(bytes) => missing.push(bytes),
            Err(e) => {
                log::error!("transparency-log leaf is not valid hex: {e}");
                return;
            }
        }
    }

    match log.append_leaf_data(missing) {
        Ok(checkpoint) => log::debug!(
            "transparency log checkpoint emitted (size={}, root={})",
            checkpoint.size,
            checkpoint.root.to_hex()
        ),
        Err(e) => log::error!("failed to materialize transparency-log tiles: {e}"),
    }
}

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
    // Hash/index paths never need blob bytes: file rows fold in via `blake3`,
    // and the only content the fold decodes (directories, symlinks, dynamic
    // node configs) has no `blake3`, so the narrow scan fetches exactly it.
    let rows = scan_live_rows(table, false).await?;
    fold_rows(rows, local_pond_id, None)
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

/// Build the full node-manifest bytes for the current in-transaction live state
/// and persist them into the reserved index node (design
/// `docs/incremental-content-tree-design.md` Section 4, Approach A / Phase 2).
///
/// `committed_table` is the pre-commit Delta table (its rows are read with the
/// same narrow projection as every read-side fold); `uncommitted` are this
/// transaction's pending records plus synthesized modified-directory rows (from
/// [`tlogfs::persistence::State::uncommitted_live_rows`]).  The two are merged
/// and ordered so the latest version per node wins, then folded exactly like
/// the post-commit path -- the reserved index node is excluded from the fold,
/// so the manifest never lists itself.  Phase 2 writes the complete manifest as
/// one index-node version per commit; Phase 4 will make it a touched-only delta.
///
/// # Errors
///
/// Returns an error if the committed table cannot be scanned, the merged rows
/// cannot be folded, or the manifest cannot be encoded (a duplicate `node_id`).
pub(crate) async fn in_txn_manifest_bytes(
    committed_table: deltalake::DeltaTable,
    uncommitted: Vec<OplogEntry>,
    local_pond_id: &str,
) -> Result<Vec<u8>, StewardError> {
    let mut rows = scan_live_rows(committed_table, false).await?;
    rows.extend(uncommitted);
    // fold_rows takes the latest version per node from ascending-version order;
    // pending and synthesized rows carry higher (or `i64::MAX`) versions, so
    // ordering by version per node makes them win over their committed rows.
    rows.sort_by(|a, b| {
        a.pond_id
            .cmp(&b.pond_id)
            .then_with(|| a.node_id.to_string().cmp(&b.node_id.to_string()))
            .then_with(|| a.version.cmp(&b.version))
    });
    let index = fold_rows(rows, local_pond_id, None)?;
    let manifest = node_manifest_entries(&index);
    encode_manifest(&manifest).map_err(StewardError::Content)
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
    build_target_state_for_pond(ship, &local_pond_id).await
}

/// Build the target's current node state for a named pond, keyed by `node_id`,
/// for a cross-pond import: the foreign pond's rows live under their own
/// `pond_id` partition, so the diff against the source manifest is computed
/// over that pond_id rather than the local one.  Returns empty maps when the
/// foreign pond has no root row yet (a first import has nothing to diff).
///
/// # Errors
///
/// Returns an error if the data table cannot be read or folded for a non-empty
/// foreign pond.
pub(crate) async fn build_target_state_for_pond(
    ship: &Ship,
    pond_id: &str,
) -> Result<
    (
        HashMap<String, ManifestEntry>,
        HashMap<String, Vec<ObjectHash>>,
    ),
    StewardError,
> {
    let table = ship.data_persistence().table().clone();
    let index = match build_content_tree_for_table(table, pond_id).await {
        Ok(index) => index,
        // A foreign pond with no root row yet: first import, empty target.
        Err(StewardError::DeltaLake(msg)) if msg.contains("no root directory row") => {
            return Ok((HashMap::new(), HashMap::new()));
        }
        Err(e) => return Err(e),
    };
    let by_id = node_manifest_entries(&index)
        .into_iter()
        .map(|e| (e.node_id.clone(), e))
        .collect();
    // Filter to the requested pond BEFORE dropping the pond_id component.  The
    // fold scans the whole data table and keys `series_versions` by
    // (pond_id, node_id); under D8 the source's node_ids are adopted verbatim,
    // so a mirror/import can hold the same series node_id under two different
    // pond_ids.  Collapsing to node_id-only without this filter lets a foreign
    // pond's version list win nondeterministically, corrupting the append-only
    // prefix used by incremental pull.  Mirrors the pond filter in
    // node_manifest_entries.
    let series = index
        .series_versions
        .into_iter()
        .filter(|((pond, _node_id), _versions)| pond == pond_id)
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
    // Materialization must read every blob so it can be transferred, so this
    // is the one caller that scans with content (`want_content = true`).
    let rows = scan_live_rows(table, true).await?;
    let index = fold_rows(rows, &local_pond_id, Some(&mut materialized))?;
    // The node manifest travels with the closure so a consumer can adopt the
    // source's node_ids (Section 4.5).  It is kept separate from the pure
    // content objects because it is pond-specific (it carries node_ids); the
    // commit references it by hash.
    let manifest = node_manifest_entries(&index);
    let manifest_bytes = encode_manifest(&manifest).map_err(StewardError::Content)?;
    materialized.manifest = Some((ObjectHash::of_bytes(&manifest_bytes), manifest_bytes));
    Ok(materialized)
}

/// Scan a pond's live rows once for the content fold (and, in the per-commit
/// path, the partition checksums computed from the same rows).
///
/// When `want_content` is false -- the per-commit hot path and every read-side
/// fold -- inline file `content` and `bao_outboard` bytes are NOT read from
/// parquet.  A file row already carries a `blake3` that stands in for its
/// content, and the only rows whose small `content` the fold decodes
/// (directories, symlinks, and dynamic-node configs) carry no `blake3`; those
/// are fetched by a second query filtered to `blake3 IS NULL`.  This keeps a
/// commit's read volume proportional to structural metadata, not to the inline
/// blob bytes of the whole pond (design "Incremental Content Tree", Tier 0).
///
/// When `want_content` is true -- materialization for push -- every row's
/// content is read so blobs can be transferred.
///
/// # Errors
///
/// Returns an error if the data table cannot be registered, queried, or
/// deserialized.
async fn scan_live_rows(
    table: deltalake::DeltaTable,
    want_content: bool,
) -> Result<Vec<OplogEntry>, StewardError> {
    let ctx = SessionContext::new();
    let _previous = ctx
        .register_table("content_live", Arc::new(table))
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
    scan_live_rows_ctx(&ctx, want_content).await
}

/// [`scan_live_rows`] re-exported for the checksum path in
/// [`crate::remote_adapter`], which owns its own `DeltaTable` handle.
pub(crate) async fn scan_live_rows_owned(
    table: deltalake::DeltaTable,
    want_content: bool,
) -> Result<Vec<OplogEntry>, StewardError> {
    scan_live_rows(table, want_content).await
}

/// Column list matching [`OplogEntry`]'s Arrow schema field order, but with the
/// two large byte columns replaced by typed NULL literals so the parquet reader
/// never materializes them while the batch still deserializes into a full
/// [`OplogEntry`] (with `content`/`bao_outboard` left `None`).
const NARROW_META_SQL: &str = "SELECT part_id, node_id, file_type, timestamp, version, \
     arrow_cast(NULL, 'Binary') AS content, blake3, size, min_event_time, max_event_time, \
     min_override, max_override, extended_attributes, factory, format, txn_seq, pond_id, \
     arrow_cast(NULL, 'Binary') AS bao_outboard, collapsed_through \
     FROM content_live ORDER BY pond_id, part_id, node_id, version";

/// Content of exactly the structural rows the fold decodes -- those without a
/// `blake3` (directories, symlinks, dynamic nodes).
const NARROW_CONTENT_SQL: &str =
    "SELECT pond_id, node_id, version, content FROM content_live WHERE blake3 IS NULL";

/// Just the structural `content` of a `blake3 IS NULL` row, keyed for splicing
/// back into its metadata row.
#[derive(serde::Deserialize)]
struct StructuralContent {
    pond_id: String,
    node_id: String,
    version: i64,
    content: Option<Vec<u8>>,
}

/// [`scan_live_rows`] against a session with `content_live` already registered
/// (split out so it can be exercised over an in-memory table in tests).
async fn scan_live_rows_ctx(
    ctx: &SessionContext,
    want_content: bool,
) -> Result<Vec<OplogEntry>, StewardError> {
    let sql = if want_content {
        "SELECT * FROM content_live ORDER BY pond_id, part_id, node_id, version"
    } else {
        NARROW_META_SQL
    };
    let batches = ctx
        .sql(sql)
        .await
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?
        .collect()
        .await
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
    let mut rows: Vec<OplogEntry> = Vec::new();
    for batch in &batches {
        let parsed: Vec<OplogEntry> = serde_arrow::from_record_batch(batch)
            .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
        rows.extend(parsed);
    }

    if want_content {
        return Ok(rows);
    }

    // Splice the small content of structural (blake3-free) rows back in.
    let content_batches = ctx
        .sql(NARROW_CONTENT_SQL)
        .await
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?
        .collect()
        .await
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
    let mut by_key: HashMap<(String, String, i64), Vec<u8>> = HashMap::new();
    for batch in &content_batches {
        let parsed: Vec<StructuralContent> = serde_arrow::from_record_batch(batch)
            .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
        for c in parsed {
            if let Some(bytes) = c.content {
                let _ = by_key.insert((c.pond_id, c.node_id, c.version), bytes);
            }
        }
    }
    for row in &mut rows {
        if row.blake3.is_none() {
            let key = (row.pond_id.clone(), row.node_id.to_string(), row.version);
            if let Some(bytes) = by_key.remove(&key) {
                row.content = Some(bytes);
            }
        }
    }
    Ok(rows)
}

/// Fold already-scanned live rows into a [`ContentTreeIndex`].
///
/// When `sink` is `Some`, every folded object's bytes are recorded into it
/// (split inline vs external per Decision D7); when `None`, only hashes are
/// computed.  Either way the returned [`ContentTreeIndex`] is identical, so the
/// child-hash rules live in exactly one implementation.  The rows must arrive
/// in ascending `version` order per node (the scan's `ORDER BY`), so later rows
/// overwrite earlier ones for the latest-version snapshot.
fn fold_rows(
    rows: Vec<OplogEntry>,
    local_pond_id: &str,
    sink: Option<&mut MaterializedObjects>,
) -> Result<ContentTreeIndex, StewardError> {
    // Latest-version facts per node, and per-version blobs for series.
    let mut latest: HashMap<NodeKey, NodeFacts> = HashMap::new();
    let mut series_versions: HashMap<NodeKey, BTreeMap<i64, VersionBlob>> = HashMap::new();
    // Highest `collapsed_through` sentinel seen per series node.  A series
    // compaction leaves the superseded per-version rows in the table beside a
    // merged row carrying this sentinel; the versions are pruned after the scan.
    let mut collapsed_through: HashMap<NodeKey, i64> = HashMap::new();

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
            if let Some(k) = row.collapsed_through {
                let entry = collapsed_through.entry(key.clone()).or_insert(k);
                *entry = (*entry).max(k);
            }
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

    // Drop versions superseded by a compaction.  The live series read path skips
    // every version at or below the highest `collapsed_through` sentinel (see
    // OpLogPersistence::async_file_reader_series); the content fold must match it
    // exactly, or a compacted series would fold in phantom superseded blobs and a
    // pulled mirror would reconstruct duplicated data whose fold still equals the
    // source's (both sides would fold the same dead rows).
    for (key, k) in &collapsed_through {
        if let Some(versions) = series_versions.get_mut(key) {
            versions.retain(|version, _| *version > *k);
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
        // A cross-pond mount point is a graft by reference, not this pond's
        // content: its subtree lives in the foreign pond's own content tree and
        // the push filters rows to this pond_id.  Omit it from the fold entirely
        // -- it contributes no tree entry and no child object -- so the content
        // tree is exactly this pond's own data.  This keeps the producer's
        // published tree consistent with what any consumer reconstructs (which
        // never receives the foreign subtree), and it is what blocks transitive
        // re-replication of a foreign mount across a multi-hop import (C imports
        // B imports A: C must not see A through B).  Because the omission happens
        // here, the node manifest excludes these mounts too (it is built from
        // the same child lists).
        if child_key.0 != key.0 {
            continue;
        }
        // The reserved node-manifest index node is a child of root but is
        // deliberately excluded from the content-tree fold and the node
        // manifest: its content is derived from the very hashes it stores, so
        // folding it in would be self-referential.  Skipping it here keeps
        // root's tree_hash and the manifest independent of the index node's
        // presence, exactly like a cross-pond mount (design
        // `docs/incremental-content-tree-design.md` Section 3).
        if child_key.1 == tinyfs::INDEX_NODE_UUID {
            continue;
        }
        let child_hash = hash_child(
            &child_key,
            entry.entry_type,
            latest,
            series_versions,
            memo,
            in_progress,
            dirs,
            sink.as_deref_mut(),
        )?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_optional_parent_commit_none_is_genesis() {
        // No record at the parent seq is a legitimate chain gap, not an error.
        let got = parse_optional_parent_commit(None, 0, "pond-x").expect("None is ok");
        assert!(got.is_none());
    }

    #[test]
    fn parse_optional_parent_commit_valid_hex_round_trips() {
        let want = ObjectHash::from_bytes([7u8; 32]);
        let got = parse_optional_parent_commit(Some(want.to_hex()), 3, "pond-x")
            .expect("valid hex parses")
            .expect("Some");
        assert_eq!(got, want);
    }

    #[test]
    fn parse_optional_parent_commit_malformed_hex_is_hard_error() {
        // A present-but-corrupt hash must NOT silently collapse to genesis.
        for bad in ["not-hex", "", "abc", &"zz".repeat(32)] {
            let err = parse_optional_parent_commit(Some(bad.to_string()), 5, "pond-x")
                .expect_err("corrupt hex must error");
            match err {
                StewardError::Content(msg) => {
                    assert!(msg.contains("corrupt parent commit hash"), "msg: {msg}");
                    assert!(msg.contains("seq 5"), "msg: {msg}");
                }
                other => panic!("expected Content error, got {other:?}"),
            }
        }
    }

    // Build an in-memory `content_live` table from OplogEntry rows so the narrow
    // scan can be exercised without a Delta table.
    fn register_rows(entries: &[OplogEntry]) -> SessionContext {
        use tlogfs::schema::ForArrow;
        let batch = serde_arrow::to_record_batch(&OplogEntry::for_arrow(), &entries)
            .expect("encode OplogEntry rows");
        let schema = batch.schema();
        let mem =
            datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).expect("memtable");
        let ctx = SessionContext::new();
        let _ = ctx
            .register_table("content_live", Arc::new(mem))
            .expect("register");
        ctx
    }

    #[tokio::test]
    async fn narrow_scan_drops_blob_content_but_keeps_structural() {
        use tinyfs::{EntryType, FileID};
        let pond = tinyfs::local_pond_uuid();

        // Directory: blake3 None, small structural content the fold decodes.
        let dir_id = FileID::new_physical_dir_id(pond);
        let dir_content = b"structural-directory-bytes".to_vec();
        let dir_row = OplogEntry::new_inline(dir_id, 1, 1, dir_content.clone(), 1);

        // Small file: blake3 Some, content redundant with the hash.
        let file_id =
            FileID::new_in_partition(dir_id.part_id(), EntryType::FilePhysicalVersion, pond);
        let file_content = b"redundant-blob-bytes".to_vec();
        let file_row = OplogEntry::new_small_file(file_id, 2, 1, file_content.clone(), 1);

        let ctx = register_rows(&[dir_row.clone(), file_row.clone()]);

        // Narrow scan: file blob content is dropped, structural content spliced.
        let narrow = scan_live_rows_ctx(&ctx, false).await.expect("narrow scan");
        let narrow_file = narrow
            .iter()
            .find(|r| r.blake3.is_some())
            .expect("file row present");
        assert!(
            narrow_file.content.is_none(),
            "blake3-bearing file content must not be read by the narrow scan"
        );
        assert!(narrow_file.bao_outboard.is_none());
        let narrow_dir = narrow
            .iter()
            .find(|r| r.blake3.is_none())
            .expect("dir row present");
        assert_eq!(
            narrow_dir.content.as_deref(),
            Some(dir_content.as_slice()),
            "structural (blake3-free) content must be spliced back for the fold"
        );

        // Full scan: every row keeps its content for materialization.
        let full = scan_live_rows_ctx(&ctx, true).await.expect("full scan");
        let full_file = full
            .iter()
            .find(|r| r.blake3.is_some())
            .expect("file row present");
        assert_eq!(full_file.content.as_deref(), Some(file_content.as_slice()));
    }

    #[tokio::test]
    async fn checksum_leaf_matches_between_full_and_narrow_rows() {
        // The redefined leaf digest must be identical whether computed from a
        // full row (as fsck does) or from a narrow-scan row (as the per-commit
        // checksum does): both normalize content/bao_outboard away for
        // blake3-bearing rows and keep structural content otherwise.
        use tinyfs::{EntryType, FileID};
        let pond = tinyfs::local_pond_uuid();
        let dir_id = FileID::new_physical_dir_id(pond);
        let dir_row = OplogEntry::new_inline(dir_id, 1, 1, b"dir-bytes".to_vec(), 1);
        let file_id =
            FileID::new_in_partition(dir_id.part_id(), EntryType::FilePhysicalVersion, pond);
        let file_row = OplogEntry::new_small_file(file_id, 2, 1, b"blob-bytes".to_vec(), 1);

        let ctx = register_rows(&[dir_row.clone(), file_row.clone()]);
        let narrow = scan_live_rows_ctx(&ctx, false).await.expect("narrow scan");

        for full in [&dir_row, &file_row] {
            let matching = narrow
                .iter()
                .find(|r| r.node_id == full.node_id && r.version == full.version)
                .expect("row present in narrow scan");
            let full_digest =
                crate::remote_adapter::row_leaf_digest(full).expect("full leaf digest");
            let narrow_digest =
                crate::remote_adapter::row_leaf_digest(matching).expect("narrow leaf digest");
            assert_eq!(
                full_digest, narrow_digest,
                "leaf digest must match between full and narrow rows for node {}",
                full.node_id
            );
        }
    }
}
