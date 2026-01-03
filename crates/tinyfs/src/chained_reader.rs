// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Chained reader for concatenating multiple async readers.
//!
//! Used by FilePhysicalSeries to concatenate all versions in oldest-to-newest order.
//! This is a general-purpose utility that chains multiple AsyncRead sources together.

use std::io::{self, Cursor, SeekFrom};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

/// A reader that chains multiple AsyncRead sources together.
///
/// Reads from each source in order until EOF, then moves to the next source.
/// Used to concatenate all versions of a FilePhysicalSeries entry.
pub struct ChainedReader {
    /// The readers to chain together (in order)
    readers: Vec<Pin<Box<dyn AsyncRead + Send>>>,
    /// Current reader index
    current_index: usize,
    /// Total bytes read so far (for seek support)
    position: u64,
    /// Total size of all readers combined
    total_size: u64,
}

impl ChainedReader {
    /// Create a new chained reader from a list of readers and their sizes.
    ///
    /// The readers should be in the order they should be read (e.g., oldest first).
    /// The sizes are needed for seek support.
    #[must_use]
    pub fn new(readers: Vec<Pin<Box<dyn AsyncRead + Send>>>, sizes: Vec<u64>) -> Self {
        assert_eq!(
            readers.len(),
            sizes.len(),
            "readers and sizes must have same length"
        );

        let total_size: u64 = sizes.iter().sum();

        Self {
            readers,
            current_index: 0,
            position: 0,
            total_size,
        }
    }

    /// Create a chained reader from in-memory byte vectors.
    ///
    /// This is useful for tests and for small file versions stored inline.
    #[must_use]
    pub fn from_bytes(chunks: Vec<Vec<u8>>) -> Self {
        let sizes: Vec<u64> = chunks.iter().map(|c| c.len() as u64).collect();
        let readers: Vec<Pin<Box<dyn AsyncRead + Send>>> = chunks
            .into_iter()
            .map(|c| Box::pin(Cursor::new(c)) as Pin<Box<dyn AsyncRead + Send>>)
            .collect();
        Self::new(readers, sizes)
    }
}

impl AsyncRead for ChainedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            // Check if we've exhausted all readers
            if self.current_index >= self.readers.len() {
                return Poll::Ready(Ok(())); // EOF
            }

            // Get the current reader
            let current_idx = self.current_index;
            let reader = &mut self.readers[current_idx];
            let before_len = buf.filled().len();

            match Pin::new(reader).poll_read(cx, buf) {
                Poll::Ready(Ok(())) => {
                    let bytes_read = buf.filled().len() - before_len;
                    if bytes_read > 0 {
                        self.position += bytes_read as u64;
                        return Poll::Ready(Ok(()));
                    } else {
                        // EOF on current reader, move to next
                        self.current_index += 1;
                        // Continue loop to try next reader
                    }
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl AsyncSeek for ChainedReader {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let new_pos = match position {
            SeekFrom::Start(pos) => pos,
            SeekFrom::End(offset) => {
                if offset > 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Cannot seek past end",
                    ));
                }
                (self.total_size as i64 + offset) as u64
            }
            SeekFrom::Current(offset) => (self.position as i64 + offset) as u64,
        };

        if new_pos > self.total_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot seek past end",
            ));
        }

        // For chained readers, we can't actually seek within the underlying readers
        // since they may be streaming. We can only support seek to start (position 0)
        // for re-reading, or return error for other positions.
        //
        // This is a limitation: FilePhysicalSeries readers don't support arbitrary seeking.
        // Format providers that need seeking should use a different approach.
        if new_pos != 0 && new_pos != self.position {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "ChainedReader only supports seek to start (position 0)",
            ));
        }

        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.position))
    }
}

// Implement Unpin to satisfy the blanket impl for AsyncReadSeek
impl Unpin for ChainedReader {}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_chained_reader_basic() {
        let chunks = vec![b"Hello, ".to_vec(), b"World".to_vec(), b"!".to_vec()];

        let mut reader = ChainedReader::from_bytes(chunks);

        let mut result = String::new();
        let _ = reader.read_to_string(&mut result).await.unwrap();

        assert_eq!(result, "Hello, World!");
    }

    #[tokio::test]
    async fn test_chained_reader_empty_chunks() {
        let chunks = vec![
            b"A".to_vec(),
            b"".to_vec(), // Empty chunk
            b"B".to_vec(),
            b"".to_vec(), // Another empty chunk
            b"C".to_vec(),
        ];

        let mut reader = ChainedReader::from_bytes(chunks);

        let mut result = String::new();
        let _ = reader.read_to_string(&mut result).await.unwrap();

        assert_eq!(result, "ABC");
    }

    #[tokio::test]
    async fn test_chained_reader_single_chunk() {
        let chunks = vec![b"Single chunk content".to_vec()];

        let mut reader = ChainedReader::from_bytes(chunks);

        let mut result = String::new();
        let _ = reader.read_to_string(&mut result).await.unwrap();

        assert_eq!(result, "Single chunk content");
    }

    #[tokio::test]
    async fn test_chained_reader_no_chunks() {
        let chunks: Vec<Vec<u8>> = vec![];

        let mut reader = ChainedReader::from_bytes(chunks);

        let mut result = Vec::new();
        let _ = reader.read_to_end(&mut result).await.unwrap();

        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_chained_reader_csv_lines() {
        // Simulate CSV data split across multiple versions
        let chunks = vec![
            b"name,value\n".to_vec(),
            b"alice,100\n".to_vec(),
            b"bob,200\n".to_vec(),
            b"carol,300\n".to_vec(),
        ];

        let mut reader = ChainedReader::from_bytes(chunks);

        let mut result = String::new();
        let _ = reader.read_to_string(&mut result).await.unwrap();

        assert_eq!(result, "name,value\nalice,100\nbob,200\ncarol,300\n");
    }
}
