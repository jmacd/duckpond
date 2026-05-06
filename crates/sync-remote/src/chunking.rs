// SPDX-License-Identifier: Apache-2.0

//! File chunking for [`crate::push`].
//!
//! [Push] reads each parquet file from the source's Delta table,
//! splits it into chunks of at most [`CHUNK_SIZE_BYTES`] bytes, and
//! writes one [`schema::RowBody::DataAdd`] row per chunk.  This module
//! provides the splitting + per-chunk and per-file BLAKE3 hashing.
//!
//! [Push]: crate::Remote::push
//! [`schema::RowBody::DataAdd`]: crate::schema::RowBody::DataAdd

use std::path::Path;

use crate::error::Result;
use crate::schema::{BLAKE3_LEN, CHUNK_SIZE_BYTES};

/// One chunk of one file, ready to be inserted as a
/// [`crate::schema::RowBody::DataAdd`] row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkRecord {
    /// 0-based index of this chunk within its file.
    pub chunk_id: i64,
    /// The chunk's bytes; length is at most `chunk_size`.  Only the
    /// last chunk of a file may be short.
    pub chunk_data: Vec<u8>,
    /// BLAKE3 of [`Self::chunk_data`].
    pub chunk_blake3: [u8; BLAKE3_LEN],
}

/// A file split into chunks plus the file-level summary needed to
/// emit [`crate::schema::RowBody::DataAdd`] rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkedFile {
    /// Total file size in bytes.
    pub file_size: i64,
    /// BLAKE3 of the entire file's bytes.
    pub file_blake3: [u8; BLAKE3_LEN],
    /// One entry per chunk; `chunks.len() == chunk_count` and
    /// `chunks[i].chunk_id == i as i64`.
    pub chunks: Vec<ChunkRecord>,
}

impl ChunkedFile {
    /// Number of chunks; same as `self.chunks.len()` cast to `i64`.
    /// Stable wire field on every [`crate::schema::RowBody::DataAdd`] row.
    pub fn chunk_count(&self) -> i64 {
        self.chunks.len() as i64
    }
}

/// Split `bytes` into [`CHUNK_SIZE_BYTES`]-sized chunks and compute
/// per-chunk and file-level BLAKE3 hashes.
///
/// Empty input produces a single zero-length chunk so that every file
/// (including the empty one) has at least one [`crate::schema::RowBody::DataAdd`]
/// row on the wire and a consistent `chunk_count = max(1, ceil(size/chunk_size))`
/// invariant.
pub fn chunk_bytes(bytes: &[u8]) -> ChunkedFile {
    chunk_bytes_with_size(bytes, CHUNK_SIZE_BYTES)
}

/// Like [`chunk_bytes`] but with a caller-supplied chunk size.  Used
/// by tests to exercise boundary cases without allocating MiB-scale
/// buffers.  Production code should always use [`chunk_bytes`].
///
/// `chunk_size` must be greater than zero; passing zero panics in
/// debug builds and yields one giant chunk in release.
pub fn chunk_bytes_with_size(bytes: &[u8], chunk_size: usize) -> ChunkedFile {
    debug_assert!(chunk_size > 0, "chunk_size must be > 0");
    let chunk_size = chunk_size.max(1);

    let file_size = bytes.len() as i64;
    let file_blake3 = *blake3::hash(bytes).as_bytes();

    let mut chunks: Vec<ChunkRecord> = Vec::new();
    if bytes.is_empty() {
        // Empty file: emit one empty chunk so chunk_count >= 1.
        chunks.push(ChunkRecord {
            chunk_id: 0,
            chunk_data: Vec::new(),
            chunk_blake3: *blake3::hash(&[]).as_bytes(),
        });
    } else {
        for (i, slice) in bytes.chunks(chunk_size).enumerate() {
            chunks.push(ChunkRecord {
                chunk_id: i as i64,
                chunk_data: slice.to_vec(),
                chunk_blake3: *blake3::hash(slice).as_bytes(),
            });
        }
    }

    ChunkedFile {
        file_size,
        file_blake3,
        chunks,
    }
}

/// Read `path` into memory and call [`chunk_bytes`] on it.
pub fn chunk_file(path: &Path) -> Result<ChunkedFile> {
    let bytes = std::fs::read(path)?;
    Ok(chunk_bytes(&bytes))
}

/// Reverse of chunking: concatenate `chunks` (in `chunk_id` order)
/// into the original byte stream.  Returns an error if the BLAKE3 of
/// any chunk does not match its `chunk_blake3`, or if the BLAKE3 of
/// the assembled bytes does not match `file_blake3`, or if `chunks` is
/// not a contiguous 0..N sequence.
///
/// Used by `remote-pull` (consumer side) and verified by tests here.
pub fn assemble_file(
    chunks: &[ChunkRecord],
    file_size: i64,
    file_blake3: &[u8; BLAKE3_LEN],
) -> Result<Vec<u8>> {
    // Verify chunk_id sequence.
    for (i, c) in chunks.iter().enumerate() {
        if c.chunk_id != i as i64 {
            return Err(crate::error::RemoteError::Schema(format!(
                "chunks not in order: position {} has chunk_id {}",
                i, c.chunk_id,
            )));
        }
        let expected = blake3::hash(&c.chunk_data);
        if expected.as_bytes() != &c.chunk_blake3 {
            return Err(crate::error::RemoteError::Schema(format!(
                "chunk {} BLAKE3 mismatch",
                i,
            )));
        }
    }

    let total_capacity: usize = chunks.iter().map(|c| c.chunk_data.len()).sum();
    let mut out = Vec::with_capacity(total_capacity);
    for c in chunks {
        out.extend_from_slice(&c.chunk_data);
    }

    if out.len() as i64 != file_size {
        return Err(crate::error::RemoteError::Schema(format!(
            "assembled size {} != file_size {}",
            out.len(),
            file_size,
        )));
    }
    let actual = blake3::hash(&out);
    if actual.as_bytes() != file_blake3 {
        return Err(crate::error::RemoteError::Schema(
            "assembled file BLAKE3 mismatch".to_string(),
        ));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_yields_one_empty_chunk() {
        let cf = chunk_bytes(&[]);
        assert_eq!(cf.file_size, 0);
        assert_eq!(cf.chunk_count(), 1);
        assert_eq!(cf.chunks[0].chunk_id, 0);
        assert!(cf.chunks[0].chunk_data.is_empty());
        // BLAKE3 of empty bytes is well-defined.
        let expected = *blake3::hash(&[]).as_bytes();
        assert_eq!(cf.chunks[0].chunk_blake3, expected);
        assert_eq!(cf.file_blake3, expected);
    }

    #[test]
    fn file_smaller_than_chunk_size_is_one_short_chunk() {
        let bytes = b"hello".to_vec();
        let cf = chunk_bytes_with_size(&bytes, 16);
        assert_eq!(cf.file_size, 5);
        assert_eq!(cf.chunk_count(), 1);
        assert_eq!(cf.chunks[0].chunk_data, bytes);
    }

    #[test]
    fn file_exactly_chunk_size_is_one_full_chunk() {
        let bytes = vec![7u8; 16];
        let cf = chunk_bytes_with_size(&bytes, 16);
        assert_eq!(cf.file_size, 16);
        assert_eq!(cf.chunk_count(), 1);
        assert_eq!(cf.chunks[0].chunk_data.len(), 16);
    }

    #[test]
    fn file_larger_than_chunk_size_splits_into_multiple_chunks_last_short() {
        let bytes: Vec<u8> = (0u8..50).collect();
        let cf = chunk_bytes_with_size(&bytes, 16);
        assert_eq!(cf.file_size, 50);
        // ceil(50/16) = 4
        assert_eq!(cf.chunk_count(), 4);
        for (i, c) in cf.chunks.iter().enumerate() {
            assert_eq!(c.chunk_id, i as i64);
        }
        assert_eq!(cf.chunks[0].chunk_data.len(), 16);
        assert_eq!(cf.chunks[1].chunk_data.len(), 16);
        assert_eq!(cf.chunks[2].chunk_data.len(), 16);
        assert_eq!(cf.chunks[3].chunk_data.len(), 2);
    }

    #[test]
    fn file_size_multiple_of_chunk_size_yields_no_short_chunk() {
        let bytes = vec![0u8; 48];
        let cf = chunk_bytes_with_size(&bytes, 16);
        assert_eq!(cf.chunk_count(), 3);
        for c in &cf.chunks {
            assert_eq!(c.chunk_data.len(), 16);
        }
    }

    #[test]
    fn chunk_blake3_matches_chunk_data_for_each_chunk() {
        let bytes: Vec<u8> = (0u8..100).collect();
        let cf = chunk_bytes_with_size(&bytes, 16);
        for c in &cf.chunks {
            let expected = *blake3::hash(&c.chunk_data).as_bytes();
            assert_eq!(c.chunk_blake3, expected, "chunk {} hash", c.chunk_id);
        }
    }

    #[test]
    fn file_blake3_matches_full_input() {
        let bytes: Vec<u8> = (0u8..200).collect();
        let cf = chunk_bytes_with_size(&bytes, 32);
        let expected = *blake3::hash(&bytes).as_bytes();
        assert_eq!(cf.file_blake3, expected);
    }

    #[test]
    fn assemble_recovers_original_bytes() {
        let bytes: Vec<u8> = (0u8..123).collect();
        let cf = chunk_bytes_with_size(&bytes, 16);
        let recovered = assemble_file(&cf.chunks, cf.file_size, &cf.file_blake3).unwrap();
        assert_eq!(recovered, bytes);
    }

    #[test]
    fn assemble_detects_bad_chunk_hash() {
        let bytes: Vec<u8> = (0u8..50).collect();
        let cf = chunk_bytes_with_size(&bytes, 16);
        let mut tampered = cf.chunks.clone();
        tampered[1].chunk_blake3 = [0u8; BLAKE3_LEN];
        let err = assemble_file(&tampered, cf.file_size, &cf.file_blake3).unwrap_err();
        assert!(format!("{}", err).contains("BLAKE3 mismatch"));
    }

    #[test]
    fn assemble_detects_out_of_order_chunks() {
        let bytes: Vec<u8> = (0u8..50).collect();
        let cf = chunk_bytes_with_size(&bytes, 16);
        let mut shuffled = cf.chunks.clone();
        shuffled.swap(0, 1);
        let err = assemble_file(&shuffled, cf.file_size, &cf.file_blake3).unwrap_err();
        assert!(format!("{}", err).contains("not in order"));
    }

    #[test]
    fn assemble_detects_wrong_file_size() {
        let bytes: Vec<u8> = (0u8..50).collect();
        let cf = chunk_bytes_with_size(&bytes, 16);
        let err = assemble_file(&cf.chunks, 999, &cf.file_blake3).unwrap_err();
        assert!(format!("{}", err).contains("file_size"));
    }

    #[test]
    fn chunk_file_round_trips_via_temp_file() {
        use std::io::Write;
        use tempfile::NamedTempFile;
        let mut f = NamedTempFile::new().unwrap();
        let bytes: Vec<u8> = (0u8..200).map(|i| i.wrapping_mul(7)).collect();
        f.write_all(&bytes).unwrap();
        f.flush().unwrap();
        let cf = chunk_file(f.path()).unwrap();
        let recovered = assemble_file(&cf.chunks, cf.file_size, &cf.file_blake3).unwrap();
        assert_eq!(recovered, bytes);
    }
}
