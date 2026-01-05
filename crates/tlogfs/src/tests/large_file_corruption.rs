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

/// Corrupt bytes in a parquet file at a specific offset
/// 
/// This directly modifies the parquet file's binary content to inject corruption.
/// The offset should point to somewhere within the chunk_data binary column.
async fn corrupt_parquet_file(
    parquet_path: &PathBuf,
    offset: u64,
    corruption_bytes: &[u8],
) -> std::io::Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(parquet_path)?;
    
    let _ = file.seek(SeekFrom::Start(offset))?;
    file.write_all(corruption_bytes)?;
    file.flush()?;
    
    debug!(
        "Corrupted {} bytes at offset {} in {}",
        corruption_bytes.len(),
        offset,
        parquet_path.display()
    );
    
    Ok(())
}

/// Find the actual byte offset of chunk_data for a specific chunk in the parquet file
/// 
/// Parquet stores data in row groups with complex encoding. This function searches
/// for the actual binary pattern to locate where chunk data is stored.
fn find_chunk_data_offset_by_pattern(
    parquet_path: &PathBuf,
    chunk_id: i64,
    _chunk_size: usize,
    pattern_byte: u8,
) -> std::io::Result<Option<u64>> {
    use std::io::Read;
    
    let mut file = std::fs::File::open(parquet_path)?;
    let mut contents = Vec::new();
    let _ = file.read_to_end(&mut contents)?;
    
    // Look for runs of the pattern byte that are close to the expected chunk size
    // This is a heuristic that works because we fill chunks with a known pattern
    let min_run_length = 1024 * 1024; // At least 1MB of the pattern
    
    let mut current_run_start = None;
    let mut current_run_length = 0;
    let mut found_runs: Vec<(usize, usize)> = Vec::new();
    
    for (i, &byte) in contents.iter().enumerate() {
        if byte == pattern_byte {
            if current_run_start.is_none() {
                current_run_start = Some(i);
                current_run_length = 0;
            }
            current_run_length += 1;
        } else {
            if current_run_length >= min_run_length {
                if let Some(start) = current_run_start {
                    found_runs.push((start, current_run_length));
                }
            }
            current_run_start = None;
            current_run_length = 0;
        }
    }
    
    // Check final run
    if current_run_length >= min_run_length {
        if let Some(start) = current_run_start {
            found_runs.push((start, current_run_length));
        }
    }
    
    debug!("Found {} runs of pattern byte 0x{:02X}", found_runs.len(), pattern_byte);
    for (i, (start, len)) in found_runs.iter().enumerate() {
        debug!("  Run {}: offset={}, length={}", i, start, len);
    }
    
    // Return the offset for the requested chunk_id
    if (chunk_id as usize) < found_runs.len() {
        let (start, _len) = found_runs[chunk_id as usize];
        Ok(Some(start as u64))
    } else {
        Ok(None)
    }
}

/// Test that corruption in a large file is detected during read
#[tokio::test]
async fn test_large_file_corruption_detected() -> Result<(), Box<dyn std::error::Error>> {
    let _ = env_logger::builder().is_test(true).try_init();
    
    debug!("=== Testing Large File Corruption Detection ===");
    
    let (_temp_dir, store_path) = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path).await?;
    
    // Create a large file that will be stored as chunked parquet
    // Use 2 chunks worth of data (32MB with 16MB chunks)
    let chunk_size = CHUNK_SIZE_DEFAULT;
    let file_size = chunk_size * 2;
    
    // Fill with distinctive pattern: 0xAA for chunk 0, 0xBB for chunk 1
    let mut large_content = vec![0xAA; chunk_size];
    large_content.extend(vec![0xBB; chunk_size]);
    
    assert!(file_size >= LARGE_FILE_THRESHOLD);
    debug!("Creating large file with {} bytes ({} chunks)", file_size, 2);
    
    // Store the file
    let tx = persistence.begin_test().await?;
    let wd = tx.root().await?;
    
    _ = tinyfs::async_helpers::convenience::create_file_path(
        &wd,
        "/large_test.dat",
        &large_content,
    ).await?;
    tx.commit_test().await?;
    
    debug!("✅ Large file stored successfully");
    
    // Find the blake3 hash by reading the file metadata
    let tx2 = persistence.begin_test().await?;
    let wd2 = tx2.root().await?;
    let file_node = wd2.get_node_path("/large_test.dat").await?;
    let file_handle = file_node.as_file().await?;
    let metadata = file_handle.metadata().await?;
    let blake3 = metadata.blake3.clone().expect("Large file should have blake3 hash");
    
    debug!("File blake3: {}", blake3);
    tx2.commit_test().await?;
    
    // Find the parquet file on disk
    let parquet_path = find_large_file_parquet(&store_path, &blake3).await
        .expect("Should find parquet file for large file");
    
    debug!("Found parquet file: {}", parquet_path.display());
    
    // Find where chunk 1's data is stored (the 0xBB pattern)
    let chunk1_offset = find_chunk_data_offset_by_pattern(&parquet_path, 1, chunk_size, 0xBB)?
        .expect("Should find chunk 1 data in parquet file");
    
    debug!("Chunk 1 data found at offset {}", chunk1_offset);
    
    // Corrupt chunk 1 by changing some bytes
    let corruption = vec![0xFF; 1024]; // Corrupt 1KB
    corrupt_parquet_file(&parquet_path, chunk1_offset + 1000, &corruption).await?;
    
    debug!("✅ Corrupted chunk 1 at offset {}", chunk1_offset + 1000);
    
    // Now try to read the file - should detect corruption when reading chunk 1
    let tx3 = persistence.begin_test().await?;
    let wd3 = tx3.root().await?;
    let file_node3 = wd3.get_node_path("/large_test.dat").await?;
    let file_handle3 = file_node3.as_file().await?;
    
    let mut reader = file_handle3.async_reader().await?;
    
    // Read chunk 0 - should succeed (it's not corrupted)
    let mut buffer = vec![0u8; 1024];
    let result0 = reader.read_exact(&mut buffer).await;
    
    debug!("Chunk 0 read result: {:?}", result0.is_ok());
    assert!(result0.is_ok(), "Reading uncorrupted chunk 0 should succeed");
    assert_eq!(buffer, vec![0xAA; 1024], "Chunk 0 data should be intact");
    
    // Seek to chunk 1 and try to read - should fail with verification error
    let chunk1_start = chunk_size as u64;
    let _ = reader.seek(std::io::SeekFrom::Start(chunk1_start)).await?;
    
    let mut buffer1 = vec![0u8; 1024];
    let result1 = reader.read_exact(&mut buffer1).await;
    
    debug!("Chunk 1 read result: {:?}", result1);
    
    // The read should fail due to corruption detection
    assert!(result1.is_err(), "Reading corrupted chunk 1 should fail");
    
    let err_msg = result1.unwrap_err().to_string();
    debug!("Corruption error message: {}", err_msg);
    
    // Should mention verification or hash mismatch
    assert!(
        err_msg.contains("verification") || 
        err_msg.contains("mismatch") ||
        err_msg.contains("Chunk"),
        "Error should indicate verification failure: {}",
        err_msg
    );
    
    tx3.commit_test().await?;
    
    debug!("SUCCESS: Corruption in chunk 1 was detected, chunk 0 remained readable");
    Ok(())
}

/// Test that chunk isolation works - corrupting one chunk doesn't affect others
#[tokio::test]
async fn test_large_file_chunk_isolation() -> Result<(), Box<dyn std::error::Error>> {
    let _ = env_logger::builder().is_test(true).try_init();
    
    debug!("=== Testing Large File Chunk Isolation ===");
    
    let (_temp_dir, store_path) = test_dir();
    let mut persistence = OpLogPersistence::create_test(&store_path).await?;
    
    // Create a 32MB file (2 chunks)
    let chunk_size = CHUNK_SIZE_DEFAULT;
    let file_size = chunk_size * 2;
    
    // Distinctive patterns for each chunk
    let mut large_content = vec![0x11; chunk_size]; // Chunk 0: 0x11
    large_content.extend(vec![0x22; chunk_size]);   // Chunk 1: 0x22
    
    debug!("Creating {} byte file ({} chunks)", file_size, 2);
    
    let tx = persistence.begin_test().await?;
    let wd = tx.root().await?;
    
    _ = tinyfs::async_helpers::convenience::create_file_path(
        &wd,
        "/isolation_test.dat",
        &large_content,
    ).await?;
    tx.commit_test().await?;
    
    // Get blake3 and find parquet file
    let tx2 = persistence.begin_test().await?;
    let wd2 = tx2.root().await?;
    let file_node = wd2.get_node_path("/isolation_test.dat").await?;
    let file_handle = file_node.as_file().await?;
    let metadata = file_handle.metadata().await?;
    let blake3 = metadata.blake3.clone().expect("Large file should have blake3 hash");
    tx2.commit_test().await?;
    
    let parquet_path = find_large_file_parquet(&store_path, &blake3).await
        .expect("Should find parquet file");
    
    // Corrupt chunk 0 (the 0x11 pattern)
    let chunk0_offset = find_chunk_data_offset_by_pattern(&parquet_path, 0, chunk_size, 0x11)?
        .expect("Should find chunk 0 data");
    
    let corruption = vec![0xFF; 512];
    corrupt_parquet_file(&parquet_path, chunk0_offset + 500, &corruption).await?;
    
    debug!("Corrupted chunk 0 at offset {}", chunk0_offset + 500);
    
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
    assert!(result1.is_ok(), "Uncorrupted chunk 1 should read successfully");
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
    ).await?;
    tx.commit_test().await?;
    
    // Read it back completely
    let tx2 = persistence.begin_test().await?;
    let wd2 = tx2.root().await?;
    let file_node = wd2.get_node_path("/baseline_test.dat").await?;
    let file_handle = file_node.as_file().await?;
    
    // Check metadata reports correct size
    let metadata = file_handle.metadata().await?;
    debug!("File metadata size: {:?}, blake3: {:?}", metadata.size, metadata.blake3);
    
    let mut reader = file_handle.async_reader().await?;
    
    // Read all content
    let mut all_content = Vec::new();
    let bytes_read = reader.read_to_end(&mut all_content).await?;
    
    debug!("Read {} bytes, all_content.len() = {}", bytes_read, all_content.len());
    
    assert_eq!(all_content.len(), file_size, "Should read full file");
    assert_eq!(&all_content[0..chunk_size], &vec![0xAA; chunk_size][..], "Chunk 0 should be 0xAA");
    assert_eq!(&all_content[chunk_size..], &vec![0xBB; chunk_size][..], "Chunk 1 should be 0xBB");
    
    tx2.commit_test().await?;
    
    debug!("SUCCESS: Baseline test - uncorrupted file reads correctly");
    Ok(())
}
