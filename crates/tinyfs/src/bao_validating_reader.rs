// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Streaming bao-tree validating reader
//!
//! This module provides a reader that validates content against a bao-tree outboard
//! as it's read, block by block. Each 16KB block is validated before its data is
//! returned to the caller.
//!
//! This enables true streaming validation - only the blocks actually read are validated,
//! and corruption is detected immediately when the corrupted block is accessed.
//!
//! ## How it works
//!
//! For single-block files (≤16KB), the outboard is empty and we validate by checking
//! that the content hash matches the root hash (blake3).
//!
//! For multi-block files, we use `bao_tree::io::sync::valid_ranges()` to validate
//! specific chunk ranges against the outboard.

use bao_tree::io::outboard::PostOrderMemOutboard;
use bao_tree::{BaoTree, BlockSize, ChunkNum, ChunkRanges};
use std::io::{self, Read, Seek, SeekFrom};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

/// Block size for bao-tree validation (16KB = 2^4 * 1024 = 16384 bytes)
const BLOCK_SIZE_BYTES: usize = 16 * 1024;
const CHUNK_LOG: u8 = 4;
/// Each 16KB block contains 16 BLAKE3 chunks of 1024 bytes each
const CHUNKS_PER_BLOCK: u64 = 16;

/// A reader that validates content against a bao-tree outboard as blocks are read.
///
/// Each 16KB block is validated when first accessed. If validation fails,
/// an I/O error is returned immediately.
pub struct BaoValidatingReader {
    /// The underlying content
    content: Vec<u8>,
    /// Current read position
    position: u64,
    /// The bao-tree outboard for validation
    outboard: PostOrderMemOutboard<Vec<u8>>,
    /// Track which blocks have been validated (true = validated OK)
    validated_blocks: Vec<bool>,
    /// Root hash for single-block validation
    root_hash: blake3::Hash,
}

impl BaoValidatingReader {
    /// Create a new validating reader
    ///
    /// # Arguments
    /// * `content` - The file content to read
    /// * `outboard_data` - The serialized bao-tree outboard bytes
    /// * `root_hash` - The expected root hash (typically from blake3 metadata)
    ///
    /// # Errors
    /// Returns an error if the outboard data is invalid
    pub fn new(
        content: Vec<u8>,
        outboard_data: Vec<u8>,
        root_hash: blake3::Hash,
    ) -> io::Result<Self> {
        let size = content.len() as u64;
        let block_size = BlockSize::from_chunk_log(CHUNK_LOG);
        let tree = BaoTree::new(size, block_size);

        let outboard = PostOrderMemOutboard {
            root: root_hash,
            tree,
            data: outboard_data,
        };

        // Calculate number of blocks
        let num_blocks = (content.len() + BLOCK_SIZE_BYTES - 1) / BLOCK_SIZE_BYTES;
        let validated_blocks = vec![false; num_blocks.max(1)];

        Ok(Self {
            content,
            position: 0,
            outboard,
            validated_blocks,
            root_hash,
        })
    }

    /// Validate a specific block using bao-tree's valid_ranges function
    fn validate_block(&mut self, block_index: usize) -> io::Result<()> {
        if block_index >= self.validated_blocks.len() {
            return Ok(()); // Beyond content, nothing to validate
        }

        if self.validated_blocks[block_index] {
            return Ok(()); // Already validated
        }

        // For files with a single block (≤16KB), the outboard is empty
        // In this case, validate by checking the content hash matches the root hash
        if self.outboard.data.is_empty() {
            let computed_hash = blake3::hash(&self.content);
            if computed_hash != self.root_hash {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Bao-tree validation failed: block {} hash mismatch (single block file)",
                        block_index
                    ),
                ));
            }
            self.validated_blocks[block_index] = true;
            return Ok(());
        }

        // For multi-block files, use bao-tree's valid_ranges to validate
        // Calculate the chunk range for this block
        let start_chunk = ChunkNum((block_index as u64) * CHUNKS_PER_BLOCK);
        let end_chunk = ChunkNum(((block_index + 1) as u64) * CHUNKS_PER_BLOCK);
        let range_to_validate = ChunkRanges::from(start_chunk..end_chunk);

        // Use valid_ranges to check if this block is valid
        // This is the public API for validation - it returns ranges that are valid
        let mut has_valid_chunks = false;
        for result in bao_tree::io::sync::valid_ranges(&self.outboard, &self.content[..], &range_to_validate) {
            match result {
                Ok(_range) => {
                    has_valid_chunks = true;
                    break;
                }
                Err(e) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Bao-tree validation error for block {}: {}", block_index, e),
                    ));
                }
            }
        }

        // If no valid chunks were returned for our range, the block is corrupted
        if !has_valid_chunks {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Bao-tree validation failed: block {} data corruption detected",
                    block_index
                ),
            ));
        }

        self.validated_blocks[block_index] = true;
        Ok(())
    }

    /// Get the content length
    #[must_use]
    pub fn len(&self) -> u64 {
        self.content.len() as u64
    }

    /// Check if content is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.content.is_empty()
    }
}

impl Read for BaoValidatingReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.position >= self.content.len() as u64 {
            return Ok(0); // EOF
        }

        // Figure out which block we need to read from
        let start_block = (self.position as usize) / BLOCK_SIZE_BYTES;

        // Validate the current block before reading
        self.validate_block(start_block)?;

        // Calculate how much we can read from the current block
        let block_start = start_block * BLOCK_SIZE_BYTES;
        let block_end = ((start_block + 1) * BLOCK_SIZE_BYTES).min(self.content.len());
        let position_in_block = self.position as usize - block_start;
        let bytes_remaining_in_block = block_end - block_start - position_in_block;

        // Read up to the end of the current block or the buffer size
        let bytes_to_read = buf.len().min(bytes_remaining_in_block);
        let start = self.position as usize;
        let end = start + bytes_to_read;

        buf[..bytes_to_read].copy_from_slice(&self.content[start..end]);
        self.position += bytes_to_read as u64;

        Ok(bytes_to_read)
    }
}

impl Seek for BaoValidatingReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(p) => p as i64,
            SeekFrom::End(p) => self.content.len() as i64 + p,
            SeekFrom::Current(p) => self.position as i64 + p,
        };
        
        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Seek before start of file",
            ));
        }
        
        self.position = new_pos as u64;
        Ok(self.position)
    }
}

// Async wrapper for BaoValidatingReader
impl AsyncRead for BaoValidatingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut temp_buf = vec![0u8; buf.remaining()];
        match self.read(&mut temp_buf) {
            Ok(n) => {
                buf.put_slice(&temp_buf[..n]);
                Poll::Ready(Ok(()))
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl AsyncSeek for BaoValidatingReader {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let _ = self.seek(position)?;
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.position))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bao_tree::io::outboard::PostOrderMemOutboard;

    fn create_test_content(size: usize) -> Vec<u8> {
        (0..size).map(|i| (i % 256) as u8).collect()
    }

    fn create_outboard(content: &[u8]) -> (blake3::Hash, Vec<u8>) {
        let block_size = BlockSize::from_chunk_log(CHUNK_LOG);
        let outboard = PostOrderMemOutboard::create(content, block_size);
        (outboard.root, outboard.data)
    }

    #[test]
    fn test_small_file_validates() {
        // Small file (< 16KB) - uses blake3 hash directly
        let content = create_test_content(1000);
        let (root_hash, outboard_data) = create_outboard(&content);

        let mut reader =
            BaoValidatingReader::new(content.clone(), outboard_data, root_hash).unwrap();

        let mut buf = Vec::new();
        let _ = reader.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, content);
    }

    #[test]
    fn test_large_file_validates() {
        // Large file (> 16KB) - uses bao-tree outboard
        let content = create_test_content(32 * 1024); // 32KB = 2 blocks
        let (root_hash, outboard_data) = create_outboard(&content);

        let mut reader =
            BaoValidatingReader::new(content.clone(), outboard_data, root_hash).unwrap();

        let mut buf = Vec::new();
        let _ = reader.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, content);
    }

    #[test]
    fn test_small_file_corruption_detected() {
        let mut content = create_test_content(1000);
        let (root_hash, outboard_data) = create_outboard(&content);

        // Corrupt the content after creating the outboard
        content[500] ^= 0xFF;

        let mut reader = BaoValidatingReader::new(content, outboard_data, root_hash).unwrap();

        let mut buf = vec![0u8; 1000];
        let result = reader.read(&mut buf);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Bao-tree validation failed"));
    }

    #[test]
    fn test_large_file_first_block_corruption_detected() {
        let mut content = create_test_content(32 * 1024);
        let (root_hash, outboard_data) = create_outboard(&content);

        // Corrupt first block
        content[100] ^= 0xFF;

        let mut reader = BaoValidatingReader::new(content, outboard_data, root_hash).unwrap();

        // Try to read from first block - should fail
        let mut buf = vec![0u8; 100];
        let result = reader.read(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_large_file_second_block_corruption_detected() {
        let mut content = create_test_content(32 * 1024);
        let (root_hash, outboard_data) = create_outboard(&content);

        // Corrupt second block (position 16KB + 100)
        content[16 * 1024 + 100] ^= 0xFF;

        let mut reader = BaoValidatingReader::new(content, outboard_data, root_hash).unwrap();

        // First block should read successfully
        let mut buf = vec![0u8; 16 * 1024];
        let result = reader.read(&mut buf);
        assert!(result.is_ok());

        // Second block should fail
        let mut buf2 = vec![0u8; 100];
        let result2 = reader.read(&mut buf2);
        assert!(result2.is_err());
    }

    #[test]
    fn test_seek_and_read_validates_correct_block() {
        let mut content = create_test_content(48 * 1024); // 3 blocks
        let (root_hash, outboard_data) = create_outboard(&content);

        // Corrupt second block
        content[16 * 1024 + 100] ^= 0xFF;

        let mut reader = BaoValidatingReader::new(content, outboard_data, root_hash).unwrap();

        // Seek to third block (past the corruption) - should work
        let _ = reader.seek(SeekFrom::Start(32 * 1024)).unwrap();
        let mut buf = vec![0u8; 100];
        let result = reader.read(&mut buf);
        assert!(result.is_ok());

        // Seek back to second block - should fail
        let _ = reader.seek(SeekFrom::Start(16 * 1024)).unwrap();
        let mut buf2 = vec![0u8; 100];
        let result2 = reader.read(&mut buf2);
        assert!(result2.is_err());
    }
}
