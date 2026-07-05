// SPDX-License-Identifier: Apache-2.0

//! RFC 6962 Merkle tree primitives: the TIME index over the commit spine.
//!
//! This is the append-order, binary, SHA-256 Merkle tree of RFC 6962 (the
//! Certificate Transparency construction).  It is deliberately a *different*
//! shape and hash function from the content graph: the content tree (SPACE) is
//! a name-sorted n-ary BLAKE3 fold below each commit, while this log (TIME)
//! folds SHA-256 *over* the linear sequence of commit leaves.  They share only
//! the leaf -- design Section 7.
//!
//! SHA-256 is used here, and only here, so that off-the-shelf transparency-log
//! witnesses and verifiers can interoperate at the publishing boundary.  The
//! rest of the system never leaves BLAKE3.
//!
//! # Hashing (RFC 6962 section 2.1)
//!
//! - empty tree:    `SHA-256("")`
//! - leaf `d`:      `SHA-256(0x00 || d)`
//! - node `l`, `r`: `SHA-256(0x01 || l || r)`
//!
//! The tree hash of `n > 1` leaves splits at `k`, the largest power of two
//! strictly less than `n`, and folds `node(MTH(left k), MTH(right n-k))`.

use sha2::{Digest, Sha256};

/// Domain-separation prefix for a leaf hash (RFC 6962).
const LEAF_PREFIX: u8 = 0x00;
/// Domain-separation prefix for an interior node hash (RFC 6962).
const NODE_PREFIX: u8 = 0x01;

/// A 32-byte SHA-256 digest in the transparency log (a node or leaf hash).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LogHash([u8; 32]);

impl LogHash {
    /// Wrap raw digest bytes.
    #[must_use]
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Borrow the raw digest bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Lower-case hex encoding of the digest.
    #[must_use]
    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(64);
        for b in &self.0 {
            s.push_str(&format!("{b:02x}"));
        }
        s
    }

    /// Parse a 64-character lower- or upper-case hex string into a digest.
    ///
    /// # Errors
    ///
    /// Returns an error string if the input is not exactly 64 hex characters.
    pub fn from_hex(s: &str) -> Result<Self, String> {
        if s.len() != 64 {
            return Err(format!("expected 64 hex chars, got {}", s.len()));
        }
        let mut out = [0u8; 32];
        for (i, byte) in out.iter_mut().enumerate() {
            let hi = hex_val(s.as_bytes()[2 * i])?;
            let lo = hex_val(s.as_bytes()[2 * i + 1])?;
            *byte = (hi << 4) | lo;
        }
        Ok(Self(out))
    }
}

fn hex_val(c: u8) -> Result<u8, String> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(format!("invalid hex character: {}", c as char)),
    }
}

/// The RFC 6962 hash of an empty tree: `SHA-256("")`.
#[must_use]
pub fn empty_root() -> LogHash {
    LogHash(Sha256::digest([]).into())
}

/// The RFC 6962 leaf hash of `leaf_data`: `SHA-256(0x00 || leaf_data)`.
#[must_use]
pub fn hash_leaf(leaf_data: &[u8]) -> LogHash {
    let mut hasher = Sha256::new();
    hasher.update([LEAF_PREFIX]);
    hasher.update(leaf_data);
    LogHash(hasher.finalize().into())
}

/// The RFC 6962 interior node hash: `SHA-256(0x01 || left || right)`.
#[must_use]
pub fn hash_children(left: &LogHash, right: &LogHash) -> LogHash {
    let mut hasher = Sha256::new();
    hasher.update([NODE_PREFIX]);
    hasher.update(left.0);
    hasher.update(right.0);
    LogHash(hasher.finalize().into())
}

/// The largest power of two strictly less than `n` (for `n >= 2`).
fn split_point(n: usize) -> usize {
    debug_assert!(n >= 2);
    let mut k = 1usize;
    while k << 1 < n {
        k <<= 1;
    }
    k
}

/// `true` iff `x` is a non-zero power of two.
fn is_power_of_two(x: usize) -> bool {
    x != 0 && (x & (x - 1)) == 0
}

/// The Merkle Tree Hash (RFC 6962 MTH) of a slice of leaf hashes.
pub(crate) fn mth(leaves: &[LogHash]) -> LogHash {
    match leaves.len() {
        0 => empty_root(),
        1 => leaves[0],
        n => {
            let k = split_point(n);
            hash_children(&mth(&leaves[..k]), &mth(&leaves[k..]))
        }
    }
}

/// An append-only RFC 6962 transparency log over a sequence of leaf hashes.
///
/// Leaves are stored already hashed (`hash_leaf(data)`); the structure folds
/// them into a root and produces inclusion and consistency proofs.  Appends are
/// O(1); root and proof computation are O(n) here (no tile caching yet -- that
/// is the deferred D5 publishing optimization).
#[derive(Debug, Default, Clone)]
pub struct TransparencyLog {
    leaves: Vec<LogHash>,
}

impl TransparencyLog {
    /// Create an empty log.
    #[must_use]
    pub fn new() -> Self {
        Self { leaves: Vec::new() }
    }

    /// Number of leaves appended so far (the tree size).
    #[must_use]
    pub fn len(&self) -> usize {
        self.leaves.len()
    }

    /// `true` iff no leaves have been appended.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.leaves.is_empty()
    }

    /// Append a leaf by its raw data (`SHA-256(0x00 || leaf_data)`), returning
    /// the new leaf's zero-based index.
    pub fn append(&mut self, leaf_data: &[u8]) -> usize {
        let index = self.leaves.len();
        self.leaves.push(hash_leaf(leaf_data));
        index
    }

    /// Append a pre-computed leaf hash, returning its zero-based index.
    pub fn append_leaf_hash(&mut self, leaf: LogHash) -> usize {
        let index = self.leaves.len();
        self.leaves.push(leaf);
        index
    }

    /// The leaf hash at `index`, if present.
    #[must_use]
    pub fn leaf_hash(&self, index: usize) -> Option<LogHash> {
        self.leaves.get(index).copied()
    }

    /// The current Merkle tree root hash.
    #[must_use]
    pub fn root(&self) -> LogHash {
        mth(&self.leaves)
    }

    /// An inclusion proof for the leaf at `index` against the current tree.
    ///
    /// The proof is the list of sibling hashes from the leaf up to the root, in
    /// bottom-up order, suitable for [`verify_inclusion`].
    ///
    /// # Errors
    ///
    /// Returns an error if `index` is out of range.
    pub fn inclusion_proof(&self, index: usize) -> Result<Vec<LogHash>, ProofError> {
        if index >= self.leaves.len() {
            return Err(ProofError::IndexOutOfRange {
                index,
                size: self.leaves.len(),
            });
        }
        Ok(path(index, &self.leaves))
    }

    /// A consistency proof that the current tree is an append-only extension of
    /// the earlier tree of size `old_size`, suitable for [`verify_consistency`].
    ///
    /// # Errors
    ///
    /// Returns an error if `old_size` exceeds the current size.
    pub fn consistency_proof(&self, old_size: usize) -> Result<Vec<LogHash>, ProofError> {
        let n = self.leaves.len();
        if old_size > n {
            return Err(ProofError::IndexOutOfRange {
                index: old_size,
                size: n,
            });
        }
        if old_size == 0 || old_size == n {
            return Ok(Vec::new());
        }
        Ok(subproof(old_size, &self.leaves, true))
    }
}

/// Errors from proof generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProofError {
    /// A leaf index or old size that does not fit the tree.
    IndexOutOfRange {
        /// The offending index or size.
        index: usize,
        /// The current tree size.
        size: usize,
    },
}

impl std::fmt::Display for ProofError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProofError::IndexOutOfRange { index, size } => {
                write!(f, "index {index} out of range for tree size {size}")
            }
        }
    }
}

impl std::error::Error for ProofError {}

/// RFC 6962 PATH(m, D[n]): the inclusion proof for leaf `m`, bottom-up.
fn path(m: usize, leaves: &[LogHash]) -> Vec<LogHash> {
    let n = leaves.len();
    if n == 1 {
        return Vec::new();
    }
    let k = split_point(n);
    if m < k {
        let mut proof = path(m, &leaves[..k]);
        proof.push(mth(&leaves[k..]));
        proof
    } else {
        let mut proof = path(m - k, &leaves[k..]);
        proof.push(mth(&leaves[..k]));
        proof
    }
}

/// RFC 6962 SUBPROOF(m, D[n], b): the consistency proof body, bottom-up.
fn subproof(m: usize, leaves: &[LogHash], b: bool) -> Vec<LogHash> {
    let n = leaves.len();
    if m == n {
        if b {
            return Vec::new();
        }
        return vec![mth(leaves)];
    }
    let k = split_point(n);
    if m <= k {
        let mut proof = subproof(m, &leaves[..k], b);
        proof.push(mth(&leaves[k..]));
        proof
    } else {
        let mut proof = subproof(m - k, &leaves[k..], false);
        proof.push(mth(&leaves[..k]));
        proof
    }
}

/// Verify an RFC 6962 inclusion proof.
///
/// Reconstructs the tree root from `leaf` (a leaf *hash*) at `index` within a
/// tree of `tree_size` leaves, using the bottom-up `proof`, and checks it
/// equals `root`.
#[must_use]
pub fn verify_inclusion(
    leaf: &LogHash,
    index: usize,
    tree_size: usize,
    proof: &[LogHash],
    root: &LogHash,
) -> bool {
    if index >= tree_size {
        return false;
    }
    match rebuild_root(index, tree_size, leaf, proof) {
        Some(computed) => &computed == root,
        None => false,
    }
}

/// Reconstruct the root from an inclusion proof, mirroring [`path`] exactly.
fn rebuild_root(m: usize, size: usize, leaf: &LogHash, proof: &[LogHash]) -> Option<LogHash> {
    if size == 1 {
        return proof.is_empty().then_some(*leaf);
    }
    let (sibling, rest) = proof.split_last()?;
    let k = split_point(size);
    if m < k {
        let left = rebuild_root(m, k, leaf, rest)?;
        Some(hash_children(&left, sibling))
    } else {
        let right = rebuild_root(m - k, size - k, leaf, rest)?;
        Some(hash_children(sibling, &right))
    }
}

/// Verify an RFC 6962 consistency proof between two tree sizes.
///
/// Checks that a tree of `new_size` leaves with root `new_root` is an
/// append-only extension of the tree of `old_size` leaves with root `old_root`,
/// using `proof`.  Follows the standard RFC 6962 section 2.1.4.2 verification
/// algorithm.
#[must_use]
pub fn verify_consistency(
    old_size: usize,
    new_size: usize,
    old_root: &LogHash,
    new_root: &LogHash,
    proof: &[LogHash],
) -> bool {
    if old_size > new_size {
        return false;
    }
    if old_size == new_size {
        return proof.is_empty() && old_root == new_root;
    }
    if old_size == 0 {
        // An empty tree is consistent with any tree; no proof needed.
        return proof.is_empty();
    }

    // If old_size is an exact power of two, the first proof node (old_root) is
    // implied and omitted by the prover; reintroduce it here.
    let mut nodes: Vec<LogHash> = Vec::with_capacity(proof.len() + 1);
    if is_power_of_two(old_size) {
        nodes.push(*old_root);
    }
    nodes.extend_from_slice(proof);

    let mut node = old_size - 1;
    let mut last_node = new_size - 1;
    while node % 2 == 1 {
        node >>= 1;
        last_node >>= 1;
    }

    let mut iter = nodes.iter();
    let Some(first) = iter.next() else {
        return false;
    };
    let mut hash1 = *first;
    let mut hash2 = *first;

    while node > 0 {
        if node % 2 == 1 {
            let Some(next) = iter.next() else {
                return false;
            };
            hash1 = hash_children(next, &hash1);
            hash2 = hash_children(next, &hash2);
        } else if node < last_node {
            let Some(next) = iter.next() else {
                return false;
            };
            hash2 = hash_children(&hash2, next);
        }
        node >>= 1;
        last_node >>= 1;
    }

    while last_node > 0 {
        let Some(next) = iter.next() else {
            return false;
        };
        hash2 = hash_children(&hash2, next);
        last_node >>= 1;
    }

    iter.next().is_none() && &hash1 == old_root && &hash2 == new_root
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leaf(i: usize) -> Vec<u8> {
        format!("commit-leaf-{i}").into_bytes()
    }

    fn log_of(n: usize) -> TransparencyLog {
        let mut log = TransparencyLog::new();
        for i in 0..n {
            let _ = log.append(&leaf(i));
        }
        log
    }

    #[test]
    fn empty_and_single_roots() {
        let empty = TransparencyLog::new();
        assert_eq!(empty.len(), 0);
        assert_eq!(empty.root(), empty_root());

        let mut one = TransparencyLog::new();
        let _ = one.append(&leaf(0));
        assert_eq!(one.root(), hash_leaf(&leaf(0)));
    }

    #[test]
    fn known_two_leaf_root() {
        let log = log_of(2);
        let expected = hash_children(&hash_leaf(&leaf(0)), &hash_leaf(&leaf(1)));
        assert_eq!(log.root(), expected);
    }

    #[test]
    fn known_three_leaf_root() {
        // RFC 6962: MTH of 3 leaves = node(node(l0,l1), l2).
        let log = log_of(3);
        let left = hash_children(&hash_leaf(&leaf(0)), &hash_leaf(&leaf(1)));
        let expected = hash_children(&left, &hash_leaf(&leaf(2)));
        assert_eq!(log.root(), expected);
    }

    #[test]
    fn hex_round_trip() {
        let h = hash_leaf(b"x");
        let s = h.to_hex();
        assert_eq!(s.len(), 64);
        assert_eq!(LogHash::from_hex(&s).unwrap(), h);
        assert!(LogHash::from_hex("zz").is_err());
    }

    #[test]
    fn inclusion_proofs_verify_for_all_sizes_and_indices() {
        for n in 1..=33 {
            let log = log_of(n);
            let root = log.root();
            for m in 0..n {
                let proof = log.inclusion_proof(m).expect("proof");
                let lh = log.leaf_hash(m).expect("leaf");
                assert!(
                    verify_inclusion(&lh, m, n, &proof, &root),
                    "inclusion must verify for n={n}, m={m}"
                );
                // A wrong leaf hash must not verify.
                let wrong = hash_leaf(b"not-the-leaf");
                assert!(
                    !verify_inclusion(&wrong, m, n, &proof, &root),
                    "tampered leaf must fail for n={n}, m={m}"
                );
            }
        }
    }

    #[test]
    fn inclusion_out_of_range_errors() {
        let log = log_of(4);
        assert!(log.inclusion_proof(4).is_err());
    }

    #[test]
    fn consistency_proofs_verify_for_all_size_pairs() {
        for n in 1..=33 {
            let new_log = log_of(n);
            let new_root = new_log.root();
            for m in 0..=n {
                let old_log = log_of(m);
                let old_root = old_log.root();
                let proof = new_log.consistency_proof(m).expect("proof");
                assert!(
                    verify_consistency(m, n, &old_root, &new_root, &proof),
                    "consistency must verify for m={m}, n={n}"
                );
            }
        }
    }

    #[test]
    fn consistency_rejects_tampered_old_root() {
        let new_log = log_of(9);
        let new_root = new_log.root();
        let proof = new_log.consistency_proof(5).expect("proof");
        let bogus = hash_leaf(b"bogus-old-root");
        assert!(!verify_consistency(5, 9, &bogus, &new_root, &proof));
    }

    #[test]
    fn consistency_rejects_shrinking_tree() {
        let log = log_of(4);
        let root = log.root();
        assert!(!verify_consistency(5, 4, &root, &root, &[]));
    }
}
