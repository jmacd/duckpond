// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Large file corruption detection tests for tlogfs
//!
//! These tests verify that corruption in chunked parquet large files is detected
//! and that corruption is isolated to the affected chunks.
//!
//! ## Background
//!
//! Large files (≥64KB) are stored as chunked parquet files in `_large_files/`.
//! Each chunk (default 16MB) has its own SeriesOutboard for verification.
//! Corruption in one chunk should not affect reading other chunks.
//!
//! ## Test Strategy
//!
//! 1. Write a large file (32MB = 2 chunks)
//! 2. Locate the parquet file on disk
//! 3. Find the byte offsets of chunk_data columns in the parquet file
//! 4. Corrupt specific chunk data using raw file I/O
//! 5. Verify:
//!    - Reading corrupted chunk fails with verification error
//!    - Reading uncorrupted chunk succeeds

use crate::large_files::LARGE_FILE_THRESHOLD;
use crate::persistence::OpLogPersistence;
use log::debug;
use std::path::PathBuf;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use utilities::chunked_files::CHUNK_SIZE_DEFAULT;

fn test_dir() -> (TempDir, String) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let store_path = temp_dir
        .path()
        .join("test_store")
        .to_string_lossy()
        .to_string();
    debug!("Test directory: {}", store_path);
    (temp_dir, store_path)
}

/// Find the parquet file for a large file by blake3 hash
async fn find_large_file_parquet(pond_path: &str, blake3: &str) -> Option<PathBuf> {
    let large_files_dir = PathBuf::from(pond_path).join("_large_files");

    // Check flat structure first
    let flat_path = large_files_dir.join(format!("blake3={}.parquet", blake3));
    if flat_path.exists() {
        return Some(flat_path);
    }

    // Check hierarchical structure
    let prefix = &blake3[0..4];
    let hierarchical_path = large_files_dir
        .join(format!("blake3_16={}", prefix))
        .join(format!("blake3={}.parquet", blake3));
    if hierarchical_path.exists() {
        return Some(hierarchical_path);
    }

    None
}

/// Information about where chunk_data is stored in the parquet file
#[derive(Debug)]
#[allow(dead_code)]
struct ChunkDataLocation {
    /// Row group index
    row_group: usize,
    /// Column index (chunk_data is column 6)
    column: usize,
    /// Byte offset in file where the data page starts
    offset: u64,
    /// Compressed size of the data page
    compressed_size: u64,
    /// Which chunk_id this corresponds to (derived from row group)
    chunk_id: i64,
}

/// Find the byte locations of chunk_data column pages in a parquet file
///
/// This uses parquet metadata to find exactly where chunk binary data is stored,
/// so we can corrupt it without breaking parquet structure/metadata.
fn find_chunk_data_locations(parquet_path: &PathBuf) -> std::io::Result<Vec<ChunkDataLocation>> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::fs::File;

    let file = File::open(parquet_path)?;
    let reader = SerializedFileReader::new(file)
        .map_err(|e| std::io::Error::other(format!("Failed to open parquet: {}", e)))?;

    let metadata = reader.metadata();
    let file_metadata = metadata.file_metadata();
    let num_row_groups = metadata.num_row_groups();

    debug!(
        "Parquet file has {} row groups, {} columns",
        num_row_groups,
        file_metadata.schema_descr().num_columns()
    );

    let mut locations = Vec::new();

    // chunk_data is column index 6 in our schema
    const CHUNK_DATA_COLUMN: usize = 6;

    for rg_idx in 0..num_row_groups {
        let row_group = metadata.row_group(rg_idx);

        if CHUNK_DATA_COLUMN < row_group.num_columns() {
            let column = row_group.column(CHUNK_DATA_COLUMN);

            // Get the byte range of this column chunk
            let (offset, compressed_size) = column.byte_range();

            debug!(
                "Row group {}: chunk_data at offset {}, size {} bytes",
                rg_idx, offset, compressed_size
            );

            locations.push(ChunkDataLocation {
                row_group: rg_idx,
                column: CHUNK_DATA_COLUMN,
                offset,
                compressed_size,
                chunk_id: rg_idx as i64,
            });
        }
    }

    Ok(locations)
}

/// Corrupt bytes within a specific chunk's data in the parquet file
///
/// This corrupts bytes at a safe offset within the chunk_data column's data page,
/// avoiding the parquet metadata so the file remains structurally valid.
fn corrupt_chunk_data(
    parquet_path: &PathBuf,
    location: &ChunkDataLocation,
    corruption_offset: u64, // Offset within the data page
    corruption_size: usize,
) -> std::io::Result<()> {
    use std::io::{Seek, SeekFrom, Write};

    // Make sure we're corrupting within the data page bounds
    if corruption_offset + corruption_size as u64 > location.compressed_size {
        return Err(std::io::Error::other(format!(
            "Corruption offset {} + size {} exceeds data page size {}",
            corruption_offset, corruption_size, location.compressed_size
        )));
    }

    let file_offset = location.offset + corruption_offset;

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(parquet_path)?;

    let _ = file.seek(SeekFrom::Start(file_offset))?;
    let corruption = vec![0xFF; corruption_size];
    file.write_all(&corruption)?;
    file.flush()?;

    debug!(
        "Corrupted {} bytes at file offset {} (chunk {} data page offset {})",
        corruption_size, file_offset, location.chunk_id, corruption_offset
    );

    Ok(())
}

/// Test that corruption in a large file is detected during read
#[tokio::test]
async fn test_large_file_corruption_detected() -> Result<(), Box<dyn std::error::Error>> {
    let _ = env_logger::builder().is_test(true).try_init();

    debug!("=== Testing Large File Corruption Detection ===");

    let (_temp_dir, store_path) = test_dir();
    // Use uncompressed mode so we can corrupt specific bytes
    let mut persistence = OpLogPersistence::create_test_uncompressed(&store_path).await?;

    // Create a large file that will be stored as chunked parquet
    // Use 2 chunks worth of data (32MB with 16MB chunks)
    let chunk_size = CHUNK_SIZE_DEFAULT;
    let file_size = chunk_size * 2;

    // Fill with distinctive pattern: 0xAA for chunk 0, 0xBB for chunk 1
    let mut large_content = vec![0xAA; chunk_size];
    large_content.extend(vec![0xBB; chunk_size]);

    assert!(file_size >= LARGE_FILE_THRESHOLD);
    debug!(
        "Creating large file with {} bytes ({} chunks)",
        file_size, 2
    );

    // Store the file
    let tx = persistence.begin_test().await?;
    let wd = tx.root().await?;

    _ = tinyfs::async_helpers::convenience::create_file_path(
        &wd,
        "/large_test.dat",
        &large_content,
    )
    .await?;
    tx.commit_test().await?;

    debug!("✅ Large file stored successfully");

    // Find the blake3 hash by reading the file metadata
    let tx2 = persistence.begin_test().await?;
    let wd2 = tx2.root().await?;
    let file_node = wd2.get_node_path("/large_test.dat").await?;
    let file_handle = file_node.as_file().await?;
    let metadata = file_handle.metadata().await?;
    let blake3 = metadata
        .blake3
        .clone()
        .expect("Large file should have blake3 hash");

    debug!("File blake3: {}", blake3);
    tx2.commit_test().await?;

    // Find the parquet file on disk
    let parquet_path = find_large_file_parquet(&store_path, &blake3)
        .await
        .expect("Should find parquet file for large file");

    debug!("Found parquet file: {}", parquet_path.display());

    // File is already uncompressed (using create_test_uncompressed)

    // Find where chunk data is stored in the parquet file
    // With 2 chunks and no compression, we should see ~16MB per chunk in the data column
    let locations = find_chunk_data_locations(&parquet_path)?;
    debug!("Found {} row group(s) with chunk_data", locations.len());
    assert!(!locations.is_empty(), "Should have chunk_data locations");

    // The chunk_data column contains raw uncompressed binary data for all chunks
    // Corrupt bytes in the second half (chunk 1's data region)
    let location = &locations[0];
    debug!(
        "chunk_data column: offset={}, size={} bytes",
        location.offset, location.compressed_size
    );

    // Corrupt at ~75% into the chunk_data (should be in chunk 1's region)
    let corruption_offset = (location.compressed_size * 3) / 4;
    corrupt_chunk_data(&parquet_path, location, corruption_offset, 1024)?;

    debug!(
        "✅ Corrupted chunk_data at offset {} within data column",
        corruption_offset
    );

    // Now try to read the file - corruption should be detected
    let tx3 = persistence.begin_test().await?;
    let wd3 = tx3.root().await?;
    let file_node3 = wd3.get_node_path("/large_test.dat").await?;
    let file_handle3 = file_node3.as_file().await?;

    let mut reader = file_handle3.async_reader().await?;

    // Read chunk 0 first - should succeed (not corrupted)
    let mut chunk0_data = vec![0u8; 1024];
    let result0 = reader.read_exact(&mut chunk0_data).await;
    debug!("Chunk 0 read result: {:?}", result0.is_ok());

    // Seek to chunk 1 and try to read - should fail
    let chunk1_start = chunk_size as u64;
    let _ = reader.seek(std::io::SeekFrom::Start(chunk1_start)).await?;

    let mut chunk1_data = vec![0u8; 1024];
    let result1 = reader.read_exact(&mut chunk1_data).await;

    debug!("Chunk 1 read result: {:?}", result1);

    // At minimum, the corruption should be detected somewhere
    let corruption_detected = result0.is_err() || result1.is_err();
    assert!(
        corruption_detected,
        "Corruption should be detected in at least one chunk"
    );

    if let Err(e) = &result1 {
        debug!("Corruption error: {}", e);
    }

    tx3.commit_test().await?;

    debug!("SUCCESS: Corruption was detected");
    Ok(())
}

/// Test that chunk isolation works - corrupting one chunk doesn't affect others
#[tokio::test]
async fn test_large_file_chunk_isolation() -> Result<(), Box<dyn std::error::Error>> {
    let _ = env_logger::builder().is_test(true).try_init();

    debug!("=== Testing Large File Chunk Isolation ===");

    let (_temp_dir, store_path) = test_dir();
    // Use uncompressed mode so we can corrupt specific bytes
    let mut persistence = OpLogPersistence::create_test_uncompressed(&store_path).await?;

    // Create a 32MB file (2 chunks)
    let chunk_size = CHUNK_SIZE_DEFAULT;
    let file_size = chunk_size * 2;

    // Distinctive patterns for each chunk
    let mut large_content = vec![0x11; chunk_size]; // Chunk 0: 0x11
    large_content.extend(vec![0x22; chunk_size]); // Chunk 1: 0x22

    debug!("Creating {} byte file ({} chunks)", file_size, 2);

    let tx = persistence.begin_test().await?;
    let wd = tx.root().await?;

    _ = tinyfs::async_helpers::convenience::create_file_path(
        &wd,
        "/isolation_test.dat",
        &large_content,
    )
    .await?;
    tx.commit_test().await?;

    // Get blake3 and find parquet file
    let tx2 = persistence.begin_test().await?;
    let wd2 = tx2.root().await?;
    let file_node = wd2.get_node_path("/isolation_test.dat").await?;
    let file_handle = file_node.as_file().await?;
    let metadata = file_handle.metadata().await?;
    let blake3 = metadata
        .blake3
        .clone()
        .expect("Large file should have blake3 hash");
    tx2.commit_test().await?;

    let parquet_path = find_large_file_parquet(&store_path, &blake3)
        .await
        .expect("Should find parquet file");

    // Find where chunk data is stored and corrupt chunk 0
    let locations = find_chunk_data_locations(&parquet_path)?;
    debug!("Found {} chunk data locations", locations.len());
    assert!(locations.len() >= 2, "Should have at least 2 chunks");

    // Corrupt chunk 0's data
    let chunk0_location = &locations[0];
    let corruption_offset = chunk0_location.compressed_size / 2;
    corrupt_chunk_data(&parquet_path, chunk0_location, corruption_offset, 64)?;

    debug!("Corrupted chunk 0 data");

    // Now read chunks - chunk 0 should fail, chunk 1 should succeed
    let tx3 = persistence.begin_test().await?;
    let wd3 = tx3.root().await?;
    let file_node3 = wd3.get_node_path("/isolation_test.dat").await?;
    let file_handle3 = file_node3.as_file().await?;

    let mut reader = file_handle3.async_reader().await?;

    // Try chunk 0 - should fail
    let mut buf0 = vec![0u8; 1024];
    let result0 = reader.read_exact(&mut buf0).await;
    debug!("Chunk 0 (corrupted) read: {:?}", result0.is_err());
    assert!(result0.is_err(), "Corrupted chunk 0 should fail to read");

    // Try chunk 1 - should succeed (isolation)
    // Need a fresh reader since the previous one may be in error state
    let mut reader2 = file_handle3.async_reader().await?;
    let chunk1_start = chunk_size as u64;
    let _ = reader2.seek(std::io::SeekFrom::Start(chunk1_start)).await?;

    let mut buf1 = vec![0u8; 1024];
    let result1 = reader2.read_exact(&mut buf1).await;

    debug!("Chunk 1 (uncorrupted) read: {:?}", result1.is_ok());
    assert!(
        result1.is_ok(),
        "Uncorrupted chunk 1 should read successfully"
    );
    assert_eq!(buf1, vec![0x22; 1024], "Chunk 1 data should be 0x22");

    tx3.commit_test().await?;

    debug!("SUCCESS: Chunk isolation verified - corruption in chunk 0 didn't affect chunk 1");
    Ok(())
}

/// Test baseline: uncorrupted large file reads correctly
#[tokio::test]
async fn test_large_file_baseline_no_corruption() -> Result<(), Box<dyn std::error::Error>> {
    let _ = env_logger::builder().is_test(true).try_init();

    debug!("=== Testing Large File Baseline (No Corruption) ===");

    let (_temp_dir, store_path) = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path).await?;

    // Create a 32MB file
    let chunk_size = CHUNK_SIZE_DEFAULT;
    let file_size = chunk_size * 2;

    let mut large_content = vec![0xAA; chunk_size];
    large_content.extend(vec![0xBB; chunk_size]);

    debug!("Creating {} byte file", file_size);

    let tx = persistence.begin_test().await?;
    let wd = tx.root().await?;

    _ = tinyfs::async_helpers::convenience::create_file_path(
        &wd,
        "/baseline_test.dat",
        &large_content,
    )
    .await?;
    tx.commit_test().await?;

    // Read it back completely
    let tx2 = persistence.begin_test().await?;
    let wd2 = tx2.root().await?;
    let file_node = wd2.get_node_path("/baseline_test.dat").await?;
    let file_handle = file_node.as_file().await?;

    // Check metadata reports correct size
    let metadata = file_handle.metadata().await?;
    debug!(
        "File metadata size: {:?}, blake3: {:?}",
        metadata.size, metadata.blake3
    );

    let mut reader = file_handle.async_reader().await?;

    // Read all content
    let mut all_content = Vec::new();
    let bytes_read = reader.read_to_end(&mut all_content).await?;

    debug!(
        "Read {} bytes, all_content.len() = {}",
        bytes_read,
        all_content.len()
    );

    assert_eq!(all_content.len(), file_size, "Should read full file");
    assert_eq!(
        &all_content[0..chunk_size],
        &vec![0xAA; chunk_size][..],
        "Chunk 0 should be 0xAA"
    );
    assert_eq!(
        &all_content[chunk_size..],
        &vec![0xBB; chunk_size][..],
        "Chunk 1 should be 0xBB"
    );

    tx2.commit_test().await?;

    debug!("SUCCESS: Baseline test - uncorrupted file reads correctly");
    Ok(())
}
