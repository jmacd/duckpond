// SPDX-License-Identifier: Apache-2.0

//! Homomorphic checksum: sum of per-item BLAKE3 digests in `Z/2^256 Z`.
//!
//! For each leaf, compute a 32-byte BLAKE3 digest of the canonical
//! encoding `("H" || key.len() LE || key || value_blake3)`.  Treat
//! that digest as a 256-bit unsigned integer in big-endian form.  The
//! partition's checksum is the sum of all per-item digests, modulo
//! `2^256`.  Adding or removing items is `O(1)` because addition is
//! associative and commutative; the per-item digest can be incrementally
//! added to or subtracted from the running total without recomputing the
//! whole.
//!
//! This is the well-known "set-XOR-with-sum" pattern in modular form.
//! It gives layout-independence by construction (sum is order-independent),
//! and its collision resistance is `~2^128` against a passive adversary
//! (birthday on 256-bit sum) -- comfortably more than enough for our
//! "detect accidental divergence" use case.  An *active* adversary who
//! controls multiple item insertions could craft a multi-set sum
//! collision in `~2^128` work; that's still infeasible and irrelevant
//! to our threat model.

use blake3::Hasher;

use super::{Checksum, ChecksumKind, Leaf, PartitionChecksum};

const TAG_HOMOMORPHIC: &[u8] = b"H";

/// Homomorphic per-partition checksum strategy.  Stateless.
#[derive(Debug, Clone, Default)]
pub struct Homomorphic;

impl Homomorphic {
    /// Construct a fresh strategy.
    pub fn new() -> Self {
        Self
    }

    /// Hash one leaf into a 256-bit field element (big-endian).
    fn leaf_digest(leaf: &Leaf<'_>) -> [u8; 32] {
        let mut h = Hasher::new();
        h.update(TAG_HOMOMORPHIC);
        let key_len = leaf.key.len() as u32;
        h.update(&key_len.to_le_bytes());
        h.update(leaf.key.as_bytes());
        h.update(leaf.value_blake3);
        *h.finalize().as_bytes()
    }
}

impl PartitionChecksum for Homomorphic {
    fn kind(&self) -> ChecksumKind {
        ChecksumKind::Homomorphic
    }

    fn compute(&self, leaves: &[Leaf<'_>]) -> Checksum {
        let mut acc = [0u8; 32];
        for leaf in leaves {
            let digest = Self::leaf_digest(leaf);
            add_assign_u256(&mut acc, &digest);
        }
        Checksum::new(ChecksumKind::Homomorphic, acc.to_vec())
    }
}

/// `acc = (acc + addend) mod 2^256`, big-endian byte order.
///
/// Pure modular addition with carry, ignoring final overflow.
fn add_assign_u256(acc: &mut [u8; 32], addend: &[u8; 32]) {
    let mut carry: u16 = 0;
    for i in (0..32).rev() {
        let sum = acc[i] as u16 + addend[i] as u16 + carry;
        acc[i] = sum as u8;
        carry = sum >> 8;
    }
    // carry is dropped (modular wrap)
    let _ = carry;
}

/// `acc = (acc - subtrahend) mod 2^256`, big-endian byte order.
///
/// Used for incremental updates (later todo).
#[allow(dead_code)]
fn sub_assign_u256(acc: &mut [u8; 32], subtrahend: &[u8; 32]) {
    let mut borrow: i16 = 0;
    for i in (0..32).rev() {
        let diff = acc[i] as i16 - subtrahend[i] as i16 - borrow;
        if diff < 0 {
            acc[i] = (diff + 256) as u8;
            borrow = 1;
        } else {
            acc[i] = diff as u8;
            borrow = 0;
        }
    }
    let _ = borrow;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(byte: u8) -> [u8; 32] {
        [byte; 32]
    }

    #[test]
    fn add_then_sub_is_identity() {
        let mut acc = [0u8; 32];
        let x = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x12, 0x34, 0x56, 0x78,
            0x9a, 0xbc, 0xde, 0xf0,
        ];
        add_assign_u256(&mut acc, &x);
        sub_assign_u256(&mut acc, &x);
        assert_eq!(acc, [0u8; 32]);
    }

    #[test]
    fn add_is_commutative_at_byte_level() {
        let x = [3u8; 32];
        let y = [5u8; 32];
        let mut a = [0u8; 32];
        add_assign_u256(&mut a, &x);
        add_assign_u256(&mut a, &y);
        let mut b = [0u8; 32];
        add_assign_u256(&mut b, &y);
        add_assign_u256(&mut b, &x);
        assert_eq!(a, b);
    }

    #[test]
    fn add_handles_carry() {
        let mut acc = [0xffu8; 32];
        let one = {
            let mut v = [0u8; 32];
            v[31] = 1;
            v
        };
        add_assign_u256(&mut acc, &one);
        // 0xff..ff + 1 = 0 (mod 2^256)
        assert_eq!(acc, [0u8; 32]);
    }

    #[test]
    fn sub_handles_borrow() {
        let mut acc = [0u8; 32];
        let one = {
            let mut v = [0u8; 32];
            v[31] = 1;
            v
        };
        sub_assign_u256(&mut acc, &one);
        // 0 - 1 = 0xff..ff (mod 2^256)
        assert_eq!(acc, [0xffu8; 32]);
    }

    #[test]
    fn order_independence_at_compute_level() {
        let c = Homomorphic::new();
        let h1 = h(1);
        let h2 = h(2);
        let h3 = h(3);
        let a = c.compute(&[
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
        let b = c.compute(&[
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
        ]);
        assert_eq!(a, b);
    }

    #[test]
    fn checksum_is_32_bytes() {
        let c = Homomorphic::new();
        let h1 = h(1);
        let cs = c.compute(&[Leaf {
            key: "a",
            value_blake3: &h1,
        }]);
        assert_eq!(cs.bytes.len(), 32);
        assert_eq!(cs.kind, ChecksumKind::Homomorphic);
    }

    #[test]
    fn identical_leaves_produce_identical_checksums() {
        // Reinforces determinism specifically for this strategy.
        let c = Homomorphic::new();
        let h1 = h(7);
        let leaves = vec![Leaf {
            key: "k",
            value_blake3: &h1,
        }];
        assert_eq!(c.compute(&leaves), c.compute(&leaves));
    }
}
