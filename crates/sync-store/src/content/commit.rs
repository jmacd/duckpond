// SPDX-License-Identifier: Apache-2.0

//! Commit objects: the spine, and the only place lineage lives.
//!
//! A commit wraps one transaction.  It names the `root_tree_hash` (the top of
//! the SPACE tree), the `parent_commit_hash` (making the single-writer chain a
//! hash chain), and the provenance.  Blobs and trees are pure content; *all*
//! provenance is isolated here so subtree hashes stay comparable across ponds.
//! See `docs/content-addressed-pond-design.md` Sections 4.3 and 5.3.

use super::{ObjectHash, push_len_prefixed};

/// Magic header distinguishing a serialized commit from a raw blob (D2).
const COMMIT_MAGIC: &[u8] = b"dp.commit.1\n";

/// The lineage and audit metadata recorded on a commit.
///
/// This is the only content in the object model that depends on `pond_id`,
/// sequence, or wall-clock time.  Keeping it isolated in the commit is the
/// inversion the whole design rests on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Provenance {
    /// The UUID of the pond that produced this commit.
    pub pond_id: String,
    /// The pond-local transaction sequence number.
    pub seq: i64,
    /// Commit time in microseconds since the Unix epoch.
    pub time_micros: i64,
    /// A human-meaningful author identifier.
    pub author: String,
    /// The original request that produced the transaction (for example, the
    /// CLI invocation), recorded verbatim for audit.
    pub request: String,
}

/// One commit: a transaction's content root plus its lineage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    /// The hash of this commit's root directory tree (top of SPACE).
    pub root_tree_hash: ObjectHash,
    /// The previous commit on this pond's linear chain, or `None` for the
    /// genesis commit.
    pub parent_commit_hash: Option<ObjectHash>,
    /// Lineage and audit metadata.
    pub provenance: Provenance,
}

impl Commit {
    /// Construct a commit.
    #[must_use]
    pub fn new(
        root_tree_hash: ObjectHash,
        parent_commit_hash: Option<ObjectHash>,
        provenance: Provenance,
    ) -> Self {
        Self {
            root_tree_hash,
            parent_commit_hash,
            provenance,
        }
    }

    /// Serialize the commit into its canonical wire format.
    ///
    /// The layout is:
    ///
    /// ```text
    /// COMMIT_MAGIC
    /// 32      root_tree_hash
    /// u8      parent present flag (0 or 1)
    /// 32      parent_commit_hash    (only if the flag is 1)
    /// u32 LE + bytes   pond_id
    /// i64 LE  seq
    /// i64 LE  time_micros
    /// u32 LE + bytes   author
    /// u32 LE + bytes   request
    /// ```
    ///
    /// The returned bytes *are* the commit object; its [`Commit::hash`] is
    /// `blake3` of these bytes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(COMMIT_MAGIC.len() + 128);
        buf.extend_from_slice(COMMIT_MAGIC);
        buf.extend_from_slice(self.root_tree_hash.as_bytes());
        match &self.parent_commit_hash {
            Some(parent) => {
                buf.push(1);
                buf.extend_from_slice(parent.as_bytes());
            }
            None => buf.push(0),
        }
        push_len_prefixed(&mut buf, self.provenance.pond_id.as_bytes());
        buf.extend_from_slice(&self.provenance.seq.to_le_bytes());
        buf.extend_from_slice(&self.provenance.time_micros.to_le_bytes());
        push_len_prefixed(&mut buf, self.provenance.author.as_bytes());
        push_len_prefixed(&mut buf, self.provenance.request.as_bytes());
        buf
    }

    /// The content address of this commit (`blake3` of [`Commit::encode`]).
    ///
    /// This hash is both the head of the SPACE tree (via `root_tree_hash`) and
    /// the leaf payload of the TIME transparency log.
    #[must_use]
    pub fn hash(&self) -> ObjectHash {
        ObjectHash::of_bytes(&self.encode())
    }

    /// Decode a commit from its canonical wire format (the inverse of
    /// [`Commit::encode`]).
    ///
    /// # Errors
    ///
    /// Returns an error if the magic header is wrong or the buffer is
    /// truncated or otherwise malformed.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let mut cur = Cursor::new(bytes);
        cur.expect_tag(COMMIT_MAGIC)?;
        let root_tree_hash = ObjectHash::from_bytes(cur.take_array()?);
        let parent_commit_hash = match cur.take_u8()? {
            0 => None,
            1 => Some(ObjectHash::from_bytes(cur.take_array()?)),
            other => return Err(format!("invalid parent flag {other}")),
        };
        let pond_id = cur.take_len_prefixed_string()?;
        let seq = i64::from_le_bytes(cur.take_array8()?);
        let time_micros = i64::from_le_bytes(cur.take_array8()?);
        let author = cur.take_len_prefixed_string()?;
        let request = cur.take_len_prefixed_string()?;
        if !cur.is_empty() {
            return Err(format!("{} trailing byte(s) after commit", cur.remaining()));
        }
        Ok(Self {
            root_tree_hash,
            parent_commit_hash,
            provenance: Provenance {
                pond_id,
                seq,
                time_micros,
                author,
                request,
            },
        })
    }
}

/// A minimal forward cursor over a commit byte buffer.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        if self.remaining() < n {
            return Err(format!(
                "truncated: need {n} byte(s), have {}",
                self.remaining()
            ));
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    fn expect_tag(&mut self, tag: &[u8]) -> Result<(), String> {
        let got = self.take(tag.len())?;
        if got != tag {
            return Err("bad magic header".to_string());
        }
        Ok(())
    }

    fn take_u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }

    fn take_array(&mut self) -> Result<[u8; 32], String> {
        let slice = self.take(32)?;
        let mut arr = [0u8; 32];
        arr.copy_from_slice(slice);
        Ok(arr)
    }

    fn take_array8(&mut self) -> Result<[u8; 8], String> {
        let slice = self.take(8)?;
        let mut arr = [0u8; 8];
        arr.copy_from_slice(slice);
        Ok(arr)
    }

    fn take_len_prefixed_string(&mut self) -> Result<String, String> {
        let len = u32::from_le_bytes(self.take_array4()?) as usize;
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|e| format!("invalid utf-8: {e}"))
    }

    fn take_array4(&mut self) -> Result<[u8; 4], String> {
        let slice = self.take(4)?;
        let mut arr = [0u8; 4];
        arr.copy_from_slice(slice);
        Ok(arr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prov() -> Provenance {
        Provenance {
            pond_id: "pond-uuid".to_string(),
            seq: 7,
            time_micros: 1_700_000_000_000_000,
            author: "jmacd".to_string(),
            request: "pond copy host:///x /y".to_string(),
        }
    }

    fn root() -> ObjectHash {
        ObjectHash::of_bytes(b"root-tree")
    }

    #[test]
    fn commit_hash_is_deterministic() {
        let c1 = Commit::new(root(), None, prov());
        let c2 = Commit::new(root(), None, prov());
        assert_eq!(c1.hash(), c2.hash());
    }

    #[test]
    fn parent_changes_hash() {
        let no_parent = Commit::new(root(), None, prov());
        let parent = ObjectHash::of_bytes(b"parent-commit");
        let with_parent = Commit::new(root(), Some(parent), prov());
        assert_ne!(no_parent.hash(), with_parent.hash());
    }

    #[test]
    fn provenance_changes_hash() {
        let base = Commit::new(root(), None, prov());
        let mut other = prov();
        other.seq = 8;
        let changed = Commit::new(root(), None, other);
        assert_ne!(base.hash(), changed.hash());
    }

    #[test]
    fn root_tree_changes_hash() {
        let base = Commit::new(root(), None, prov());
        let changed = Commit::new(ObjectHash::of_bytes(b"other-root"), None, prov());
        assert_ne!(base.hash(), changed.hash());
    }

    #[test]
    fn length_prefix_prevents_field_ambiguity() {
        // Moving a character across the author/request boundary must change
        // the hash, proving the framing is unambiguous.
        let mut a = prov();
        a.author = "ab".to_string();
        a.request = "c".to_string();
        let mut b = prov();
        b.author = "a".to_string();
        b.request = "bc".to_string();
        let ca = Commit::new(root(), None, a);
        let cb = Commit::new(root(), None, b);
        assert_ne!(ca.hash(), cb.hash());
    }

    #[test]
    fn commit_hash_differs_from_root_blob() {
        let c = Commit::new(root(), None, prov());
        assert_ne!(c.hash(), root());
    }

    #[test]
    fn decode_round_trips_encode() {
        let parent = ObjectHash::of_bytes(b"parent-commit");
        for c in [
            Commit::new(root(), None, prov()),
            Commit::new(root(), Some(parent), prov()),
        ] {
            let bytes = c.encode();
            let decoded = Commit::decode(&bytes).expect("decode");
            assert_eq!(decoded, c);
            assert_eq!(decoded.hash(), c.hash());
        }
    }

    #[test]
    fn decode_rejects_bad_magic() {
        let mut bytes = Commit::new(root(), None, prov()).encode();
        bytes[0] ^= 0xff;
        assert!(Commit::decode(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let mut bytes = Commit::new(root(), None, prov()).encode();
        bytes.push(0);
        assert!(Commit::decode(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_truncation() {
        let bytes = Commit::new(root(), None, prov()).encode();
        assert!(Commit::decode(&bytes[..bytes.len() - 4]).is_err());
    }
}
