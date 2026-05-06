// SPDX-License-Identifier: Apache-2.0

//! Per-partition content checksums.
//!
//! A *partition checksum* is a fingerprint of the live (post-tombstone)
//! key-value content of one partition.  It must satisfy:
//!
//! - **Deterministic.**  The same set of `(item_key, value_blake3)`
//!   leaves -> the same checksum bytes, every time.
//! - **Layout-independent.**  The checksum does not depend on which
//!   parquet files hold which rows, what order the items are inserted,
//!   or how many compactions the partition has undergone.  Compaction
//!   does not change the live set of items, so the checksum is invariant
//!   under compaction.
//! - **Sensitive.**  Any change to a key, a value, or the membership of
//!   the partition produces a different checksum (with overwhelming
//!   probability).
//!
//! Two implementations are provided.  Both implement [`PartitionChecksum`].
//!
//! - [`Merkle`]: BLAKE3-based binary Merkle tree over sorted leaves.
//!   Cryptographically conservative; computing from scratch is O(N).
//! - [`Homomorphic`]: sum of per-item BLAKE3 outputs in `Z/2^256 Z`.
//!   Permits O(1) incremental update (later todo) at the cost of weaker
//!   collision resistance against an active adversary; the threat model
//!   for this prototype is accidental divergence, not active attack.
//!
//! Both have a [`ChecksumKind`] tag that travels with the bytes so that
//! a Merkle checksum and a Homomorphic checksum of the same logical
//! state are NOT considered equal.

use serde::{Deserialize, Serialize};

mod homomorphic;
mod merkle;

pub use homomorphic::Homomorphic;
pub use merkle::Merkle;

/// Which strategy produced a [`Checksum`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChecksumKind {
    /// Merkle tree over sorted BLAKE3 leaves.  See [`Merkle`].
    Merkle,
    /// Sum of BLAKE3-derived field elements in `Z/2^256 Z`.  See [`Homomorphic`].
    Homomorphic,
}

impl ChecksumKind {
    /// Stable string name for use in serialized form (`"merkle"` /
    /// `"homomorphic"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            ChecksumKind::Merkle => "merkle",
            ChecksumKind::Homomorphic => "homomorphic",
        }
    }
}

/// A computed partition checksum.
///
/// Equality compares both the strategy tag and the bytes, so two
/// checksums produced by different strategies are never `==` even if
/// they happen to share bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Checksum {
    /// Which strategy produced these bytes.
    pub kind: ChecksumKind,
    /// The fixed-length output of the strategy.  Both built-in
    /// strategies produce 32 bytes.
    pub bytes: Vec<u8>,
}

impl Checksum {
    /// Construct a checksum directly.  Mostly useful for tests and
    /// deserialization.
    pub fn new(kind: ChecksumKind, bytes: Vec<u8>) -> Self {
        Self { kind, bytes }
    }

    /// Hex-encoded representation of the bytes (no kind prefix).
    pub fn hex(&self) -> String {
        let mut s = String::with_capacity(self.bytes.len() * 2);
        for b in &self.bytes {
            use std::fmt::Write;
            let _ = write!(&mut s, "{:02x}", b);
        }
        s
    }
}

/// One leaf for a partition checksum: an item's key plus its
/// already-computed BLAKE3-of-value digest.
///
/// Implementations pre-sort or otherwise canonicalize this slice as part
/// of [`PartitionChecksum::compute`]; callers do **not** need to sort.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Leaf<'a> {
    /// The item key.
    pub key: &'a str,
    /// 32-byte BLAKE3 of the item's value.
    pub value_blake3: &'a [u8; 32],
}

/// Strategy for computing a partition's content checksum.
///
/// Implementations are stateless and cheap to clone.  Each is a small
/// configuration object (no per-store state) so multiple [`Store`]s can
/// share one instance.
///
/// [`Store`]: crate::Store
pub trait PartitionChecksum: Send + Sync + 'static {
    /// Which strategy this is.
    fn kind(&self) -> ChecksumKind;

    /// Compute the checksum of a partition from its full set of live
    /// leaves.  Order does not matter -- implementations canonicalize.
    fn compute(&self, leaves: &[Leaf<'_>]) -> Checksum;
}

/// Helper to build a `[u8; 32]` from a slice without panicking on
/// bad-length input.  Returns `None` if `slice.len() != 32`.
#[allow(dead_code)] // used by the steward + remote crates in later todos
pub(crate) fn array_from_slice(slice: &[u8]) -> Option<[u8; 32]> {
    if slice.len() != 32 {
        return None;
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(slice);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(byte: u8) -> [u8; 32] {
        [byte; 32]
    }

    /// Property: identical leaf sets produce identical checksums under
    /// each strategy.  Tests run against a hand-coded leaf set so
    /// failures are easy to read.
    fn determinism<C: PartitionChecksum>(c: &C) {
        let h1 = h(1);
        let h2 = h(2);
        let leaves = vec![
            Leaf {
                key: "a",
                value_blake3: &h1,
            },
            Leaf {
                key: "b",
                value_blake3: &h2,
            },
        ];
        let a = c.compute(&leaves);
        let b = c.compute(&leaves);
        assert_eq!(a, b);
        assert_eq!(a.kind, c.kind());
    }

    /// Property: leaf order does not affect the checksum.
    fn layout_independence<C: PartitionChecksum>(c: &C) {
        let h1 = h(11);
        let h2 = h(22);
        let h3 = h(33);
        let in_order = vec![
            Leaf {
                key: "a",
                value_blake3: &h1,
            },
            Leaf {
                key: "b",
                value_blake3: &h2,
            },
            Leaf {
                key: "c",
                value_blake3: &h3,
            },
        ];
        let shuffled = vec![
            Leaf {
                key: "c",
                value_blake3: &h3,
            },
            Leaf {
                key: "a",
                value_blake3: &h1,
            },
            Leaf {
                key: "b",
                value_blake3: &h2,
            },
        ];
        assert_eq!(c.compute(&in_order), c.compute(&shuffled));
    }

    /// Property: empty input is well-defined and stable.
    fn empty_well_defined<C: PartitionChecksum>(c: &C) {
        let a = c.compute(&[]);
        let b = c.compute(&[]);
        assert_eq!(a, b);
        assert_eq!(a.kind, c.kind());
    }

    /// Property: changing any leaf changes the checksum.
    fn change_sensitivity<C: PartitionChecksum>(c: &C) {
        let h1 = h(7);
        let h2 = h(8);
        let h2_changed = h(9);
        let original = vec![
            Leaf {
                key: "k1",
                value_blake3: &h1,
            },
            Leaf {
                key: "k2",
                value_blake3: &h2,
            },
        ];
        let value_changed = vec![
            Leaf {
                key: "k1",
                value_blake3: &h1,
            },
            Leaf {
                key: "k2",
                value_blake3: &h2_changed,
            },
        ];
        let key_renamed = vec![
            Leaf {
                key: "k1",
                value_blake3: &h1,
            },
            Leaf {
                key: "k2_renamed",
                value_blake3: &h2,
            },
        ];
        let item_removed = vec![Leaf {
            key: "k1",
            value_blake3: &h1,
        }];
        let h_extra = h(10);
        let item_added = {
            let mut v = original.clone();
            v.push(Leaf {
                key: "k3",
                value_blake3: &h_extra,
            });
            v
        };
        let baseline = c.compute(&original);
        assert_ne!(baseline, c.compute(&value_changed));
        assert_ne!(baseline, c.compute(&key_renamed));
        assert_ne!(baseline, c.compute(&item_removed));
        assert_ne!(baseline, c.compute(&item_added));
    }

    /// Cross-strategy property: a Merkle and a Homomorphic checksum of
    /// the same logical content are NOT equal (different `kind`).
    #[test]
    fn cross_kind_not_equal() {
        let m = Merkle::new();
        let h_strategy = Homomorphic::new();
        let h1 = h(1);
        let leaves = vec![Leaf {
            key: "a",
            value_blake3: &h1,
        }];
        let a = m.compute(&leaves);
        let b = h_strategy.compute(&leaves);
        assert_ne!(a, b);
    }

    #[test]
    fn merkle_properties() {
        let c = Merkle::new();
        determinism(&c);
        layout_independence(&c);
        empty_well_defined(&c);
        change_sensitivity(&c);
    }

    #[test]
    fn homomorphic_properties() {
        let c = Homomorphic::new();
        determinism(&c);
        layout_independence(&c);
        empty_well_defined(&c);
        change_sensitivity(&c);
    }

    #[test]
    fn checksum_hex_format() {
        let c = Checksum::new(ChecksumKind::Merkle, vec![0x01, 0xab, 0xff, 0x00]);
        assert_eq!(c.hex(), "01abff00");
    }

    #[test]
    fn kind_as_str() {
        assert_eq!(ChecksumKind::Merkle.as_str(), "merkle");
        assert_eq!(ChecksumKind::Homomorphic.as_str(), "homomorphic");
    }

    #[test]
    fn array_from_slice_validates_length() {
        assert!(array_from_slice(&[0u8; 32]).is_some());
        assert!(array_from_slice(&[0u8; 31]).is_none());
        assert!(array_from_slice(&[0u8; 33]).is_none());
    }
}
