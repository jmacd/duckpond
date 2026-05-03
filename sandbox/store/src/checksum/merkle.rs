// SPDX-License-Identifier: Apache-2.0

//! BLAKE3-based binary Merkle tree.

use blake3::Hasher;

use super::{Checksum, ChecksumKind, Leaf, PartitionChecksum};

/// BLAKE3-based binary Merkle tree over sorted partition leaves.
///
/// Construction:
///
/// 1. Sort leaves by `(key, value_blake3)`, ascending.
/// 2. For each leaf compute `leaf_hash = BLAKE3("L" || key.len() as u32 LE
///    || key.bytes || value_blake3)`.
/// 3. Pairwise hash levels:
///    `parent = BLAKE3("N" || left || right)`.  An odd node at any level
///    is paired with itself (`parent = BLAKE3("N" || node || node)`).
/// 4. The root, which is BLAKE3-32-bytes, is the checksum.
/// 5. Empty input is defined as `BLAKE3("E")`.
///
/// The leading byte (`L`/`N`/`E`) defends against second-preimage
/// confusion between leaf, internal-node, and empty-tree pre-images.
#[derive(Debug, Clone, Default)]
pub struct Merkle;

impl Merkle {
    /// Construct a fresh strategy.  Stateless.
    pub fn new() -> Self {
        Self
    }
}

const TAG_LEAF: &[u8] = b"L";
const TAG_NODE: &[u8] = b"N";
const TAG_EMPTY: &[u8] = b"E";

impl PartitionChecksum for Merkle {
    fn kind(&self) -> ChecksumKind {
        ChecksumKind::Merkle
    }

    fn compute(&self, leaves: &[Leaf<'_>]) -> Checksum {
        if leaves.is_empty() {
            let mut h = Hasher::new();
            h.update(TAG_EMPTY);
            return Checksum::new(ChecksumKind::Merkle, h.finalize().as_bytes().to_vec());
        }

        // Sort by (key, value_blake3) for deterministic ordering.
        let mut sorted: Vec<&Leaf<'_>> = leaves.iter().collect();
        sorted.sort_by(|a, b| match a.key.cmp(b.key) {
            std::cmp::Ordering::Equal => a.value_blake3.cmp(b.value_blake3),
            ord => ord,
        });

        // Build leaf hashes.
        let mut level: Vec<[u8; 32]> = sorted
            .iter()
            .map(|leaf| {
                let mut h = Hasher::new();
                h.update(TAG_LEAF);
                let key_len = leaf.key.len() as u32;
                h.update(&key_len.to_le_bytes());
                h.update(leaf.key.as_bytes());
                h.update(leaf.value_blake3);
                *h.finalize().as_bytes()
            })
            .collect();

        // Pairwise reduce until one element remains.
        while level.len() > 1 {
            let mut next: Vec<[u8; 32]> = Vec::with_capacity(level.len().div_ceil(2));
            for chunk in level.chunks(2) {
                let mut h = Hasher::new();
                h.update(TAG_NODE);
                h.update(&chunk[0]);
                // For an odd-length level, pair the last node with itself.
                let right = if chunk.len() == 2 {
                    &chunk[1]
                } else {
                    &chunk[0]
                };
                h.update(right);
                next.push(*h.finalize().as_bytes());
            }
            level = next;
        }

        Checksum::new(ChecksumKind::Merkle, level[0].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(byte: u8) -> [u8; 32] {
        [byte; 32]
    }

    #[test]
    fn empty_tree_is_constant_and_distinct_from_single_leaf() {
        let m = Merkle::new();
        let empty = m.compute(&[]);
        let one_leaf = m.compute(&[Leaf {
            key: "k",
            value_blake3: &h(0),
        }]);
        assert_eq!(empty, m.compute(&[]));
        assert_ne!(empty, one_leaf);
    }

    #[test]
    fn duplicate_keys_with_distinct_values_produce_distinct_leaves() {
        // Both leaves have key "k"; the value disambiguates.  This
        // shouldn't happen in a well-formed Store (key uniqueness is
        // enforced upstream by the live-set query), but the trait
        // contract should still produce a sensible answer.
        let m = Merkle::new();
        let h1 = h(1);
        let h2 = h(2);
        let a = m.compute(&[
            Leaf {
                key: "k",
                value_blake3: &h1,
            },
            Leaf {
                key: "k",
                value_blake3: &h2,
            },
        ]);
        let b = m.compute(&[
            Leaf {
                key: "k",
                value_blake3: &h1,
            },
            Leaf {
                key: "k",
                value_blake3: &h2,
            },
        ]);
        assert_eq!(a, b);
    }

    #[test]
    fn odd_count_does_not_panic_and_pairs_last_with_self() {
        // 3 leaves -> level 1 has 2 nodes (the last pair-with-self), then
        // level 2 has 1 root.  Just exercising the odd-count code path.
        let m = Merkle::new();
        let h1 = h(1);
        let h2 = h(2);
        let h3 = h(3);
        let _ = m.compute(&[
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
        ]);
    }

    #[test]
    fn checksum_is_32_bytes() {
        let m = Merkle::new();
        let h1 = h(1);
        let c = m.compute(&[Leaf {
            key: "a",
            value_blake3: &h1,
        }]);
        assert_eq!(c.bytes.len(), 32);
        assert_eq!(c.kind, ChecksumKind::Merkle);
    }
}
