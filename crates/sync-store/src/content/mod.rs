// SPDX-License-Identifier: Apache-2.0

//! Content-addressed object model (the SPACE and spine layers).
//!
//! This module implements the pure, lineage-independent hashing primitives
//! described in `docs/content-addressed-pond-design.md`.  It deals only in
//! bytes and hashes; it knows nothing about Delta Lake, partitions, or
//! transactions.  Three object kinds are modeled here:
//!
//! - **blob** -- the exact stored bytes of one file version.  A blob's
//!   identity is simply `blake3(bytes)`; there is no dedicated type because a
//!   blob *is* its bytes.  Use [`ObjectHash::of_bytes`].
//! - **tree** -- a directory as its sorted entries (see [`tree`]).
//! - **commit** -- one transaction, the only object carrying lineage (see
//!   [`commit`]).
//!
//! # The store invariant
//!
//! Every object's name is the BLAKE3 hash of its serialized bytes
//! (`name == blake3(bytes)`).  Raw blobs are hashed untagged so a file
//! version's content hash equals the value already recorded in the
//! filesystem.  Structured objects (tree, commit) embed a short magic
//! header in their serialization so their hashes cannot collide with a raw
//! blob that happens to share bytes.
//!
//! # Encoding stability (Decision D2)
//!
//! The byte encodings below are the simplest reasonable choice and are *not*
//! permanently frozen.  Because the project permits a clean reset, the
//! formats can be revised later by resetting all participating ponds.  Any
//! two ponds that sync must share the same encoding version, since hashes do
//! not match across versions.

mod commit;
mod tree;

pub use commit::{Commit, Provenance};
pub use tree::{TreeEntry, encode_series, encode_tree, series_hash, tree_hash};

use std::fmt;

/// A 32-byte BLAKE3 content address naming an immutable object.
///
/// An `ObjectHash` is the name of a blob, tree, or commit.  Equality is byte
/// equality of the 32-byte digest.  The hex form (lowercase, 64 chars) is the
/// canonical textual representation and round-trips via [`ObjectHash::to_hex`]
/// and [`ObjectHash::from_hex`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectHash([u8; 32]);

impl ObjectHash {
    /// Wrap raw digest bytes that are already a BLAKE3 output.
    #[must_use]
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The 32 raw digest bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Hash an arbitrary byte slice as an untagged blob.
    ///
    /// This is the identity of a file version, a symlink target, or a dynamic
    /// node's config bytes -- any object whose serialized form is exactly its
    /// raw bytes.
    #[must_use]
    pub fn of_bytes(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    /// Lowercase 64-character hex encoding of the digest.
    #[must_use]
    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(64);
        for b in &self.0 {
            s.push(char::from_digit(u32::from(b >> 4), 16).expect("nibble is valid hex digit"));
            s.push(char::from_digit(u32::from(b & 0x0f), 16).expect("nibble is valid hex digit"));
        }
        s
    }

    /// Parse a 64-character lowercase or uppercase hex string into a hash.
    ///
    /// # Errors
    ///
    /// Returns an error string if the input is not exactly 64 hex digits.
    pub fn from_hex(s: &str) -> Result<Self, String> {
        if s.len() != 64 {
            return Err(format!("expected 64 hex chars, got {}", s.len()));
        }
        let mut out = [0u8; 32];
        let bytes = s.as_bytes();
        for (i, slot) in out.iter_mut().enumerate() {
            let hi = hex_val(bytes[i * 2])?;
            let lo = hex_val(bytes[i * 2 + 1])?;
            *slot = (hi << 4) | lo;
        }
        Ok(Self(out))
    }
}

impl fmt::Display for ObjectHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

fn hex_val(c: u8) -> Result<u8, String> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        other => Err(format!("invalid hex digit: {:?}", other as char)),
    }
}

/// Append a `u32`-length-prefixed byte field to `buf`.
///
/// Shared by the tree and commit encoders to frame variable-length fields
/// (names, pond ids, free-form provenance strings) unambiguously.
pub(crate) fn push_len_prefixed(buf: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("field length exceeds u32::MAX");
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn of_bytes_matches_blake3() {
        let h = ObjectHash::of_bytes(b"hello");
        assert_eq!(h.as_bytes(), blake3::hash(b"hello").as_bytes());
    }

    #[test]
    fn hex_round_trips() {
        let h = ObjectHash::of_bytes(b"duckpond");
        let hex = h.to_hex();
        assert_eq!(hex.len(), 64);
        assert_eq!(ObjectHash::from_hex(&hex).unwrap(), h);
    }

    #[test]
    fn hex_accepts_uppercase() {
        let h = ObjectHash::of_bytes(b"x");
        let upper = h.to_hex().to_uppercase();
        assert_eq!(ObjectHash::from_hex(&upper).unwrap(), h);
    }

    #[test]
    fn hex_rejects_bad_input() {
        assert!(ObjectHash::from_hex("abc").is_err());
        assert!(ObjectHash::from_hex(&"z".repeat(64)).is_err());
    }

    #[test]
    fn distinct_inputs_differ() {
        assert_ne!(ObjectHash::of_bytes(b"a"), ObjectHash::of_bytes(b"b"));
    }
}
