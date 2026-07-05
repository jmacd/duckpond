// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Reachable content-object inventory: the sync reachability primitive
//! (design Section 8).
//!
//! Sync over the content graph is git-shaped: "I have commit X, you have Y --
//! send me the objects reachable from Y that I lack, by hash."  Answering that
//! requires enumerating the set of content objects reachable from a pond's
//! root tree, keyed by their content hash.  This module produces that set from
//! the same read-side fold used by comparison, *without* touching the existing
//! bundle/frontier replication: it is the additive first step of the
//! content-addressed sync migration (the CA1 coexistence stage), letting the
//! object graph be built and verified alongside today's checksums before any
//! transfer path switches over.
//!
//! The inventory is content-only and lineage-independent: two ponds with
//! overlapping content share object hashes, so the set difference between a
//! local inventory and a remote's "have" set is exactly the bytes a transfer
//! must move.  The commit object itself (the spine head) is tracked separately
//! by the Phase 2b commit spine and is not part of this content inventory.

use std::collections::{BTreeMap, BTreeSet};

use sync_store::content::ObjectHash;
use tinyfs::EntryType;

use crate::content_tree::{ContentTreeIndex, build_content_tree_for_table};
use crate::{Ship, StewardError};

/// The kind of a content object, as it transfers and rehashes during sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ObjectKind {
    /// A directory tree object (the recursive `tree_hash` fold).
    Tree,
    /// A single-version file or table blob (`blake3` of the version bytes).
    Blob,
    /// A multi-version series object (the cumulative series hash).
    Series,
    /// A symlink target object (`blake3` of the target bytes).
    Symlink,
    /// A dynamic-node recipe object (`blake3` of the factory type plus config
    /// bytes; see [`sync_store::content::recipe_hash`]).
    Recipe,
}

/// The set of content objects reachable from a pond's root tree, keyed by their
/// content hash.
///
/// Built by [`inventory_content_objects`].  Because the keys are content
/// hashes, comparing two inventories (or an inventory against a remote "have"
/// set) is a pure set operation independent of pond identity or lineage.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectInventory {
    /// Each reachable object's hash mapped to its kind.
    pub objects: BTreeMap<ObjectHash, ObjectKind>,
}

impl ObjectInventory {
    /// Number of distinct reachable objects.
    #[must_use]
    pub fn len(&self) -> usize {
        self.objects.len()
    }

    /// `true` iff no objects are reachable (an impossible state for a live pond,
    /// whose root tree always exists, but defined for completeness).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }

    /// The set of object hashes in this inventory.
    #[must_use]
    pub fn hashes(&self) -> BTreeSet<ObjectHash> {
        self.objects.keys().copied().collect()
    }

    /// The objects this inventory has that a remote peer (identified by the set
    /// of hashes it already `have`s) lacks -- exactly the objects a transfer
    /// from here to that peer must send, in deterministic hash order.
    #[must_use]
    pub fn missing_from(&self, have: &BTreeSet<ObjectHash>) -> Vec<ObjectHash> {
        self.objects
            .keys()
            .filter(|h| !have.contains(*h))
            .copied()
            .collect()
    }
}

/// Map an entry kind to the content-object kind its `child_hash` names.
fn object_kind(entry_type: EntryType) -> ObjectKind {
    match entry_type {
        EntryType::DirectoryPhysical => ObjectKind::Tree,
        EntryType::FilePhysicalVersion | EntryType::TablePhysicalVersion => ObjectKind::Blob,
        EntryType::FilePhysicalSeries | EntryType::TablePhysicalSeries => ObjectKind::Series,
        EntryType::Symlink => ObjectKind::Symlink,
        EntryType::DirectoryDynamic | EntryType::FileDynamic | EntryType::TableDynamic => {
            ObjectKind::Recipe
        }
    }
}

/// Enumerate the content objects reachable from a pond's root tree.
///
/// Reads the data table once and folds it (the same read-side computation as
/// [`crate::compute_content_tree`]), then collects the root tree object plus
/// every child object named by a directory entry.  Every physical directory's
/// tree object appears either as the root or as a `Tree`-kind child of its
/// parent, so the union covers all reachable objects.
///
/// # Errors
///
/// Returns an error if the pond's data table cannot be read or folded.
pub async fn inventory_content_objects(ship: &Ship) -> Result<ObjectInventory, StewardError> {
    let index = build_content_tree_for_table(
        ship.data_persistence().table().clone(),
        ship.data_persistence().pond_id(),
    )
    .await?;
    Ok(inventory_from_index(&index))
}

/// Collect the reachable-object inventory from an already-built tree index.
fn inventory_from_index(index: &ContentTreeIndex) -> ObjectInventory {
    let mut objects: BTreeMap<ObjectHash, ObjectKind> = BTreeMap::new();
    // The root directory is itself a tree object.
    let _ = objects.insert(index.root_tree_hash, ObjectKind::Tree);
    for children in index.dirs.values() {
        for child in children {
            let _ = objects.insert(child.child_hash, object_kind(child.entry_type));
        }
    }
    ObjectInventory { objects }
}
