// SPDX-License-Identifier: Apache-2.0

//! Tree objects: a directory hashed as its sorted entries, content only.
//!
//! A tree commits to a directory's immediate children -- nothing else.  The
//! recursive fold (a subdirectory contributes its own `tree_hash` as its
//! `child_hash`) is what gives the design its load-bearing property: equal
//! `tree_hash` means an identical subtree in a single comparison, and any
//! change to any descendant changes every ancestor hash on the path to the
//! root.  See `docs/content-addressed-pond-design.md` Section 4.2.
//!
//! # `child_hash` by node kind (Section 9)
//!
//! The hash an entry contributes to its parent depends on the node kind:
//!
//! | Node kind                         | `child_hash`                                   |
//! |-----------------------------------|------------------------------------------------|
//! | file (physical version)           | `blake3(version bytes)` -- the blob hash       |
//! | series / multi-version file       | [`series_hash`] over all version blob hashes   |
//! | directory                         | the subtree's [`tree_hash`]                    |
//! | symlink                           | `blake3(target path)` -- [`ObjectHash::of_bytes`] |
//! | dynamic dir / `table:dynamic`     | `blake3(stored config bytes)` -- the recipe    |
//!
//! For files and directories the `child_hash` is simply the blob or subtree
//! hash already in hand, so no helper is needed.  For symlinks and dynamic
//! nodes the bytes (target path, config) are hashed untagged via
//! [`ObjectHash::of_bytes`].  Series use [`series_hash`].

use tinyfs::EntryType;

use super::{Cursor, ObjectHash, push_len_prefixed};

/// Magic header distinguishing a serialized tree from a raw blob (D2).
const TREE_MAGIC: &[u8] = b"dp.tree.1\n";

/// Magic header for the cumulative series hash (D2).
const SERIES_MAGIC: &[u8] = b"dp.series.1\n";

/// One entry in a tree: a named child with its type and content address.
///
/// The triple `(name, entry_type, child_hash)` is the entire value an entry
/// contributes to its parent's hash -- no node identity, no partition, no
/// timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeEntry {
    /// The child's name within this directory.
    pub name: String,
    /// The child's entry type.
    pub entry_type: EntryType,
    /// The child's content address (see the module table for how it is
    /// derived per node kind).
    pub child_hash: ObjectHash,
}

impl TreeEntry {
    /// Construct a tree entry.
    #[must_use]
    pub fn new(name: impl Into<String>, entry_type: EntryType, child_hash: ObjectHash) -> Self {
        Self {
            name: name.into(),
            entry_type,
            child_hash,
        }
    }
}

/// Serialize a directory's entries into the canonical tree wire format.
///
/// The layout is:
///
/// ```text
/// TREE_MAGIC
/// u32 LE  entry count
/// repeated, entries sorted by name bytes ascending:
///   u32 LE  name length
///   name bytes (UTF-8)
///   u8      entry_type discriminant
///   32      child_hash bytes
/// ```
///
/// The returned bytes *are* the tree object; its [`tree_hash`] is
/// `blake3` of these bytes.
///
/// # Errors
///
/// Returns an error if two entries share a name (a directory cannot hold two
/// children with the same name, and an ambiguous tree must not be silently
/// hashed).
pub fn encode_tree(entries: &[TreeEntry]) -> Result<Vec<u8>, String> {
    let mut sorted: Vec<&TreeEntry> = entries.iter().collect();
    sorted.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));

    for pair in sorted.windows(2) {
        if pair[0].name == pair[1].name {
            return Err(format!("duplicate entry name in tree: {:?}", pair[0].name));
        }
    }

    let mut buf = Vec::with_capacity(TREE_MAGIC.len() + 4 + entries.len() * 48);
    buf.extend_from_slice(TREE_MAGIC);
    let count = u32::try_from(sorted.len()).expect("entry count exceeds u32::MAX");
    buf.extend_from_slice(&count.to_le_bytes());
    for entry in sorted {
        push_len_prefixed(&mut buf, entry.name.as_bytes());
        buf.push(entry.entry_type as u8);
        buf.extend_from_slice(entry.child_hash.as_bytes());
    }
    Ok(buf)
}

/// Compute the recursive content hash of a directory from its entries.
///
/// This is `blake3` over the [`encode_tree`] serialization.  Because each
/// subdirectory entry carries its own `tree_hash` as `child_hash`, equal
/// results mean identical subtrees.
///
/// # Errors
///
/// Propagates the duplicate-name error from [`encode_tree`].
pub fn tree_hash(entries: &[TreeEntry]) -> Result<ObjectHash, String> {
    Ok(ObjectHash::of_bytes(&encode_tree(entries)?))
}

/// Encode a multi-version series into its content-object bytes.
///
/// A series entry must commit to its *whole* history so the hash is stable
/// across appends: appending a version extends the input deterministically.
/// The input is the ordered list of per-version blob hashes (oldest first):
///
/// ```text
/// SERIES_MAGIC
/// u32 LE  version count
/// repeated: 32 bytes per version blob hash, in order
/// ```
///
/// The returned bytes *are* the series object; its [`series_hash`] is
/// `blake3` of these bytes.  This is the simple-start encoding of the "stable
/// bao root over all versions" the design calls for (D2, not frozen).
#[must_use]
pub fn encode_series(version_hashes: &[ObjectHash]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(SERIES_MAGIC.len() + 4 + version_hashes.len() * 32);
    buf.extend_from_slice(SERIES_MAGIC);
    let count = u32::try_from(version_hashes.len()).expect("version count exceeds u32::MAX");
    buf.extend_from_slice(&count.to_le_bytes());
    for h in version_hashes {
        buf.extend_from_slice(h.as_bytes());
    }
    buf
}

/// Compute the cumulative content-and-history hash of a multi-version series.
///
/// This is `blake3` over the [`encode_series`] serialization.
#[must_use]
pub fn series_hash(version_hashes: &[ObjectHash]) -> ObjectHash {
    ObjectHash::of_bytes(&encode_series(version_hashes))
}

/// Decode a tree object back into its entries (the inverse of [`encode_tree`]).
///
/// The entries are returned in the canonical sorted-by-name order they were
/// serialized in.
///
/// # Errors
///
/// Returns an error if the magic header is wrong, the buffer is truncated, an
/// entry type byte is not a valid [`EntryType`] discriminant, a name is not
/// valid UTF-8, or there are trailing bytes after the declared entry count.
pub fn decode_tree(bytes: &[u8]) -> Result<Vec<TreeEntry>, String> {
    let mut cur = Cursor::new(bytes);
    cur.expect_tag(TREE_MAGIC)?;
    let count = cur.take_u32()? as usize;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let name = cur.take_len_prefixed_string()?;
        let entry_type = EntryType::try_from(cur.take_u8()?)?;
        let child_hash = cur.take_hash()?;
        entries.push(TreeEntry::new(name, entry_type, child_hash));
    }
    if !cur.is_empty() {
        return Err(format!("{} trailing byte(s) after tree", cur.remaining()));
    }
    Ok(entries)
}

/// Decode a series object back into its ordered version blob hashes (the
/// inverse of [`encode_series`]).
///
/// # Errors
///
/// Returns an error if the magic header is wrong, the buffer is truncated, or
/// there are trailing bytes after the declared version count.
pub fn decode_series(bytes: &[u8]) -> Result<Vec<ObjectHash>, String> {
    let mut cur = Cursor::new(bytes);
    cur.expect_tag(SERIES_MAGIC)?;
    let count = cur.take_u32()? as usize;
    let mut hashes = Vec::with_capacity(count);
    for _ in 0..count {
        hashes.push(cur.take_hash()?);
    }
    if !cur.is_empty() {
        return Err(format!("{} trailing byte(s) after series", cur.remaining()));
    }
    Ok(hashes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(s: &str) -> ObjectHash {
        ObjectHash::of_bytes(s.as_bytes())
    }

    fn file(name: &str, content: &str) -> TreeEntry {
        TreeEntry::new(name, EntryType::FilePhysicalVersion, h(content))
    }

    #[test]
    fn tree_hash_is_deterministic() {
        let a = vec![file("b", "2"), file("a", "1")];
        let b = vec![file("a", "1"), file("b", "2")];
        // Order of input entries must not matter; sorting is canonical.
        assert_eq!(tree_hash(&a).unwrap(), tree_hash(&b).unwrap());
    }

    #[test]
    fn tree_hash_changes_with_child_content() {
        let base = vec![file("a", "1"), file("b", "2")];
        let changed = vec![file("a", "1"), file("b", "CHANGED")];
        assert_ne!(tree_hash(&base).unwrap(), tree_hash(&changed).unwrap());
    }

    #[test]
    fn tree_hash_changes_with_entry_type() {
        let as_file = vec![file("a", "1")];
        let as_dir = vec![TreeEntry::new("a", EntryType::DirectoryPhysical, h("1"))];
        assert_ne!(tree_hash(&as_file).unwrap(), tree_hash(&as_dir).unwrap());
    }

    #[test]
    fn recursive_fold_detects_swapped_subtrees() {
        // Two sibling subtrees with the same local entry names but different
        // descendants. Folding the children in must make a swap detectable.
        let left = tree_hash(&[file("x", "left-data")]).unwrap();
        let right = tree_hash(&[file("x", "right-data")]).unwrap();

        let normal = vec![
            TreeEntry::new("dirA", EntryType::DirectoryPhysical, left),
            TreeEntry::new("dirB", EntryType::DirectoryPhysical, right),
        ];
        let swapped = vec![
            TreeEntry::new("dirA", EntryType::DirectoryPhysical, right),
            TreeEntry::new("dirB", EntryType::DirectoryPhysical, left),
        ];
        assert_ne!(tree_hash(&normal).unwrap(), tree_hash(&swapped).unwrap());
    }

    #[test]
    fn duplicate_names_rejected() {
        let dup = vec![file("a", "1"), file("a", "2")];
        assert!(tree_hash(&dup).is_err());
    }

    #[test]
    fn empty_tree_hashes() {
        // An empty directory has a well-defined, stable hash.
        let e1 = tree_hash(&[]).unwrap();
        let e2 = tree_hash(&[]).unwrap();
        assert_eq!(e1, e2);
        // And it differs from a non-empty tree.
        assert_ne!(e1, tree_hash(&[file("a", "1")]).unwrap());
    }

    #[test]
    fn series_hash_is_order_sensitive_and_stable() {
        let v1 = h("v1");
        let v2 = h("v2");
        assert_eq!(series_hash(&[v1, v2]), series_hash(&[v1, v2]));
        assert_ne!(series_hash(&[v1, v2]), series_hash(&[v2, v1]));
        // Appending a version changes the cumulative hash.
        assert_ne!(series_hash(&[v1]), series_hash(&[v1, v2]));
    }

    #[test]
    fn series_hash_differs_from_blob() {
        let v1 = h("v1");
        // A one-version series must not collide with the raw blob hash.
        assert_ne!(series_hash(&[v1]), v1);
    }

    #[test]
    fn decode_tree_round_trips_encode() {
        let entries = vec![
            file("b", "2"),
            file("a", "1"),
            TreeEntry::new("d", EntryType::DirectoryPhysical, h("dir")),
            TreeEntry::new("c", EntryType::Symlink, h("target")),
        ];
        let bytes = encode_tree(&entries).unwrap();
        let decoded = decode_tree(&bytes).unwrap();
        // Decoded entries come back in canonical sorted-by-name order.
        let mut expected = entries.clone();
        expected.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
        assert_eq!(decoded, expected);
        // And re-encoding the decoded entries reproduces the same bytes.
        assert_eq!(encode_tree(&decoded).unwrap(), bytes);
    }

    #[test]
    fn decode_tree_handles_empty() {
        let bytes = encode_tree(&[]).unwrap();
        assert!(decode_tree(&bytes).unwrap().is_empty());
    }

    #[test]
    fn decode_tree_rejects_bad_magic_and_trailing() {
        let mut bytes = encode_tree(&[file("a", "1")]).unwrap();
        let mut bad_magic = bytes.clone();
        bad_magic[0] ^= 0xff;
        assert!(decode_tree(&bad_magic).is_err());
        bytes.push(0);
        assert!(decode_tree(&bytes).is_err());
    }

    #[test]
    fn decode_series_round_trips_encode() {
        let versions = [h("v1"), h("v2"), h("v3")];
        let bytes = encode_series(&versions);
        assert_eq!(decode_series(&bytes).unwrap(), versions);
    }

    #[test]
    fn decode_series_rejects_truncation() {
        let bytes = encode_series(&[h("v1"), h("v2")]);
        assert!(decode_series(&bytes[..bytes.len() - 4]).is_err());
    }

    #[test]
    fn series_hash_is_blake3_of_encode_series() {
        // The encoded bytes ARE the series object; its hash is blake3 of them.
        let versions = [h("v1"), h("v2"), h("v3")];
        assert_eq!(
            series_hash(&versions),
            ObjectHash::of_bytes(&encode_series(&versions))
        );
        // The encoding round-trips structurally: magic + count + 32B per hash.
        let bytes = encode_series(&versions);
        assert_eq!(&bytes[..SERIES_MAGIC.len()], SERIES_MAGIC);
        assert_eq!(bytes.len(), SERIES_MAGIC.len() + 4 + versions.len() * 32);
    }
}
