// SPDX-License-Identifier: Apache-2.0

//! Tile materialization for the transparency log (design Decision D5).
//!
//! The RFC 6962 SHA-256 Merkle tree over the commit spine (see [`super`]) is
//! persisted as **tiles** in the C2SP `tlog-tiles` layout: fixed-size,
//! immutable, statically servable blocks of hashes.  A verifier fetches a
//! handful of tiles plus the signed [`Checkpoint`](super::Checkpoint) and
//! recomputes an O(log n) inclusion or consistency proof, never walking the
//! whole log.
//!
//! # Layout
//!
//! A tile at level `L` (>= 0) and index `N` holds up to [`TILE_WIDTH`] hashes
//! taken from tree level `8 * L`; each hash covers a complete subtree of
//! `2^(8L)` leaves.  Level 0 therefore holds the leaf hashes themselves.  Tiles
//! live under a log directory as:
//!
//! ```text
//! tile/<L>/<N>          full tile   (TILE_WIDTH hashes)
//! tile/<L>/<N>.p/<W>    partial tile (W < TILE_WIDTH hashes, rightmost only)
//! checkpoint            the tlog-checkpoint note body
//! ```
//!
//! `<N>` is encoded in base-1000 groups (`x001/x234/567`) per the spec so no
//! single directory holds more than 1000 entries.  A full tile is immutable:
//! once written it is never rewritten.  Only the rightmost (partial) tile at
//! each level changes as the log grows.
//!
//! # Complexity
//!
//! The writer recomputes tile hashes from the full leaf set on each append
//! (`O(n)`), matching the existing in-memory [`super::TransparencyLog`].  Full
//! tiles are written once (skip-if-exists), so I/O churn is bounded to the
//! rightmost tiles plus any newly completed full tiles.  An incremental
//! `O(log n)` writer that folds only the touched right spine is a later
//! optimization; the on-disk format does not change.

use std::io;
use std::path::{Path, PathBuf};

use super::checkpoint::Checkpoint;
use super::merkle::{LogHash, empty_root, hash_leaf, mth};

/// The number of hashes in a full tile (C2SP tile height 8, so `2^8`).
pub const TILE_WIDTH: u64 = 256;
/// Bits of tree level spanned by one tile level (C2SP tile height 8).
pub const TILE_HEIGHT_BITS: u32 = 8;
/// The byte length of one SHA-256 log hash.
const HASH_LEN: usize = 32;

/// A materialized, C2SP `tlog-tiles` view of the commit-spine transparency log.
///
/// Bound to a log directory (`{POND}/tlog`) and the log's origin identifier.
/// Appending leaves rewrites the affected tiles and re-emits the checkpoint.
#[derive(Debug, Clone)]
pub struct TileLog {
    dir: PathBuf,
    origin: String,
}

impl TileLog {
    /// Bind a tile log to directory `dir` with the given `origin` identifier.
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>, origin: impl Into<String>) -> Self {
        Self {
            dir: dir.into(),
            origin: origin.into(),
        }
    }

    /// The log directory.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The current tree size (leaf count) recorded in the checkpoint, or 0 if
    /// the log has not been materialized yet.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint exists but cannot be read or parsed.
    pub fn size(&self) -> Result<u64, super::CheckpointError> {
        Ok(Checkpoint::read(&self.dir)?.map(|c| c.size).unwrap_or(0))
    }

    /// Read every leaf hash from the level-0 tiles, in log order.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint or any expected level-0 tile is
    /// missing or malformed.
    pub fn load_leaves(&self) -> Result<Vec<LogHash>, super::CheckpointError> {
        let size = self.size()?;
        let mut leaves = Vec::with_capacity(size as usize);
        let full = size / TILE_WIDTH;
        for n in 0..full {
            self.read_tile_hashes(0, n, TILE_WIDTH as usize, &mut leaves)?;
        }
        let rem = (size % TILE_WIDTH) as usize;
        if rem > 0 {
            self.read_tile_hashes(0, full, rem, &mut leaves)?;
        }
        Ok(leaves)
    }

    /// Append leaves given by their raw leaf data, re-materialize the tiles, and
    /// emit a fresh checkpoint.  Returns the new checkpoint.
    ///
    /// Each entry of `leaf_data` is hashed with the RFC 6962 leaf rule
    /// (`SHA-256(0x00 || data)`) before being appended.
    ///
    /// # Errors
    ///
    /// Returns an error if existing leaves cannot be loaded or any tile /
    /// checkpoint write fails.
    pub fn append_leaf_data<I, B>(&self, leaf_data: I) -> Result<Checkpoint, super::CheckpointError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        let hashes = leaf_data.into_iter().map(|d| hash_leaf(d.as_ref()));
        self.append_leaf_hashes(hashes)
    }

    /// Append pre-computed leaf hashes, re-materialize the tiles, and emit a
    /// fresh checkpoint.  Returns the new checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if existing leaves cannot be loaded or any tile /
    /// checkpoint write fails.
    pub fn append_leaf_hashes<I>(&self, hashes: I) -> Result<Checkpoint, super::CheckpointError>
    where
        I: IntoIterator<Item = LogHash>,
    {
        let mut leaves = self.load_leaves()?;
        leaves.extend(hashes);
        Ok(self.materialize(&leaves)?)
    }

    /// Write all tiles for the leaf sequence `leaves` and emit the checkpoint.
    ///
    /// Full tiles already present on disk are left untouched (they are
    /// immutable); the rightmost partial tile at each level is rewritten
    /// atomically.  The checkpoint rename is the log's commit point.
    ///
    /// # Errors
    ///
    /// Returns any I/O error from writing a tile or the checkpoint.
    pub fn materialize(&self, leaves: &[LogHash]) -> io::Result<Checkpoint> {
        let t = leaves.len() as u64;
        let root = if t == 0 { empty_root() } else { mth(leaves) };

        // `t` is loop-invariant; the walk terminates when a level has no
        // complete subtree (`available == 0`).  Skip entirely for the empty log.
        let mut level: u8 = 0;
        if t > 0 {
            loop {
                let shift = TILE_HEIGHT_BITS * u32::from(level);
                let sub: u64 = if shift >= 64 { u64::MAX } else { 1u64 << shift };
                let available = if sub > t { 0 } else { t / sub };
                if available == 0 {
                    break;
                }
                let num_tiles = available.div_ceil(TILE_WIDTH);
                for tile_n in 0..num_tiles {
                    let first = tile_n * TILE_WIDTH;
                    let width = std::cmp::min(TILE_WIDTH, available - first);
                    self.write_tile(level, tile_n, first, width, sub, leaves)?;
                }
                level += 1;
            }
        }
        let checkpoint = Checkpoint::new(self.origin.clone(), t, root);
        checkpoint.write(&self.dir)?;
        Ok(checkpoint)
    }

    /// Compute the hashes for one tile and write it (skipping existing full
    /// tiles, which are immutable).
    fn write_tile(
        &self,
        level: u8,
        tile_n: u64,
        first_node: u64,
        width: u64,
        sub: u64,
        leaves: &[LogHash],
    ) -> io::Result<()> {
        let full = width == TILE_WIDTH;
        let target = self.dir.join(data_tile_rel(level, tile_n, width as usize));

        // A completed full tile is immutable: never recompute or rewrite it.
        if full && target.exists() {
            return Ok(());
        }

        // Drop any stale partial variants of this tile index before writing the
        // current one, so only one representation of the rightmost tile exists.
        let partial_dir = self.dir.join(format!("tile/{level}/{}.p", n_path(tile_n)));
        if partial_dir.exists() {
            std::fs::remove_dir_all(&partial_dir)?;
        }

        let mut buf = Vec::with_capacity(width as usize * HASH_LEN);
        for i in 0..width {
            let node = first_node + i;
            let start = (node * sub) as usize;
            let end = start + sub as usize;
            buf.extend_from_slice(mth(&leaves[start..end]).as_bytes());
        }
        write_atomic(&target, &buf)
    }

    /// Read `count` hashes from tile `(level, n)` into `out`.
    fn read_tile_hashes(
        &self,
        level: u8,
        n: u64,
        count: usize,
        out: &mut Vec<LogHash>,
    ) -> Result<(), super::CheckpointError> {
        let path = self.dir.join(data_tile_rel(level, n, count));
        let bytes = std::fs::read(&path)?;
        if bytes.len() < count * HASH_LEN {
            return Err(super::CheckpointError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "tile {} holds {} bytes, expected at least {}",
                    path.display(),
                    bytes.len(),
                    count * HASH_LEN
                ),
            )));
        }
        for chunk in bytes.chunks_exact(HASH_LEN).take(count) {
            let mut arr = [0u8; HASH_LEN];
            arr.copy_from_slice(chunk);
            out.push(LogHash::from_bytes(arr));
        }
        Ok(())
    }
}

/// The C2SP `tlog-tiles` index-path encoding of `n`: base-1000 groups, the most
/// significant groups prefixed with `x` and the last group zero-padded to three
/// digits (e.g. `567`, `x001/x234/567`).
#[must_use]
pub fn n_path(mut n: u64) -> String {
    let mut s = format!("{:03}", n % 1000);
    n /= 1000;
    while n > 0 {
        s = format!("x{:03}/{s}", n % 1000);
        n /= 1000;
    }
    s
}

/// The path of a data tile relative to the log directory: `tile/<L>/<N>` for a
/// full tile, `tile/<L>/<N>.p/<W>` for a partial one.
#[must_use]
pub fn data_tile_rel(level: u8, n: u64, width: usize) -> String {
    let base = format!("tile/{level}/{}", n_path(n));
    if width as u64 == TILE_WIDTH {
        base
    } else {
        format!("{base}.p/{width}")
    }
}

/// Atomically write `bytes` to `path`: create parent directories, write a
/// sibling temp file, then rename it into place so a crash never leaves a
/// half-written tile or checkpoint.
///
/// # Errors
///
/// Returns any I/O error from directory creation, writing, or renaming.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = match path.file_name() {
        Some(name) => {
            let mut n = name.to_os_string();
            n.push(".tmp");
            path.with_file_name(n)
        }
        None => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot write to a path with no file name",
            ));
        }
    };
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tlog::{TransparencyLog, verify_inclusion};

    #[test]
    fn n_path_groups_in_base_1000() {
        assert_eq!(n_path(0), "000");
        assert_eq!(n_path(7), "007");
        assert_eq!(n_path(999), "999");
        assert_eq!(n_path(1000), "x001/000");
        assert_eq!(n_path(1234), "x001/234");
        assert_eq!(n_path(1_234_567), "x001/x234/567");
    }

    #[test]
    fn data_tile_rel_marks_partial_tiles() {
        assert_eq!(data_tile_rel(0, 0, 256), "tile/0/000");
        assert_eq!(data_tile_rel(0, 1, 14), "tile/0/001.p/14");
        assert_eq!(data_tile_rel(1, 0, 1), "tile/1/000.p/1");
    }

    fn leaves(n: usize) -> Vec<LogHash> {
        (0..n)
            .map(|i| hash_leaf(format!("leaf-{i}").as_bytes()))
            .collect()
    }

    #[test]
    fn checkpoint_root_matches_reference_for_many_sizes() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Cover empty, sub-tile, exact-tile, and multi-tile-level sizes.
        for size in [0usize, 1, 2, 3, 255, 256, 257, 512, 600, 1000] {
            let sub = dir.path().join(format!("log-{size}"));
            let log = TileLog::new(&sub, "duckpond/test");
            let ls = leaves(size);
            let cp = log.materialize(&ls).expect("materialize");

            let mut reference = TransparencyLog::new();
            for l in &ls {
                let _ = reference.append_leaf_hash(*l);
            }
            assert_eq!(cp.size, size as u64, "size for {size}");
            assert_eq!(cp.root, reference.root(), "root for {size}");
        }
    }

    #[test]
    fn leaves_round_trip_through_tiles() {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = TileLog::new(dir.path(), "duckpond/test");
        let ls = leaves(600);
        let _ = log.materialize(&ls).expect("materialize");
        let loaded = log.load_leaves().expect("load");
        assert_eq!(loaded, ls);
    }

    #[test]
    fn incremental_append_equals_batch_and_full_tiles_are_stable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let inc = TileLog::new(dir.path().join("inc"), "duckpond/test");

        // Fill exactly one full level-0 tile, then verify that appending more
        // leaves does not disturb the now-immutable full tile bytes.
        let _ = inc
            .append_leaf_data((0..256).map(|i| format!("leaf-{i}")))
            .unwrap();
        let full_tile = inc.dir().join(data_tile_rel(0, 0, 256));
        let full_before = std::fs::read(&full_tile).expect("read full tile");

        let mut last = None;
        for i in 256..300 {
            last = Some(inc.append_leaf_data([format!("leaf-{i}")]).unwrap());
        }
        let cp = last.expect("appended");
        assert_eq!(cp.size, 300);

        let full_after = std::fs::read(&full_tile).expect("read full tile");
        assert_eq!(full_before, full_after, "full tile must be immutable");

        // The full-batch materialization of the same 300 leaves agrees.
        let batch = TileLog::new(dir.path().join("batch"), "duckpond/test");
        let batch_cp = batch.materialize(&leaves(300)).expect("materialize");
        assert_eq!(cp.root, batch_cp.root);
    }

    #[test]
    fn tile_leaves_prove_inclusion_against_checkpoint() {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = TileLog::new(dir.path(), "duckpond/test");
        let ls = leaves(257);
        let cp = log.materialize(&ls).expect("materialize");

        // Rebuild proofs from the tile-stored leaves and verify against the
        // published checkpoint root.
        let loaded = log.load_leaves().expect("load");
        let mut reference = TransparencyLog::new();
        for l in &loaded {
            let _ = reference.append_leaf_hash(*l);
        }
        for (i, l) in loaded.iter().enumerate() {
            let proof = reference.inclusion_proof(i).expect("proof");
            assert!(
                verify_inclusion(l, i, loaded.len(), &proof, &cp.root),
                "leaf {i} should prove against checkpoint root"
            );
        }
    }
}
