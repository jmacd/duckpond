// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Corruption detection tests for bao-tree and blake3 validation
//!
//! These tests verify that tinyfs detects corrupted file content using
//! the bao-tree outboard data and blake3 hashes stored during writes.
//!
//! ## Implementation
//!
//! - Uses proper bao_tree crate (PostOrderMemOutboard) for outboard computation and validation
//! - Tests use MemoryPersistence.corrupt_file_content() to inject corruption after normal writes
//! - Validation happens automatically in MemoryFile::async_reader() before returning reader
//! - Both bao-tree (for files >16KB) and blake3 (for all files) validation layers
//!
//! ## File Size Requirements
//!
//! Files must be >16KB to have non-empty bao-tree outboards. With chunk_log=4,
//! bao-tree blocks are 16KB (16 BLAKE3 chunks of 1KB each). A single block or less
//! has no parent nodes in the merkle tree, so no outboard data. Smaller files
//! only use blake3 validation.
//!
//! ## Test Coverage
//!
//! - First block corruption detection
//! - Middle block corruption detection
//! - Last block corruption detection  
//! - Block boundary corruption detection
//! - Multiple corruption points detection
//! - Baseline (no false positives for uncorrupted files)

use crate::error::Result;
use crate::memory::MemoryPersistence;
use crate::{EntryType, FS};

// =============================================================================
// Test Fixture Helpers
// =============================================================================

/// Large file threshold - files >= this size are stored as chunked parquet
const LARGE_FILE_THRESHOLD: usize = 64 * 1024;

/// Configuration for corruption tests
struct CorruptionTestConfig {
    /// Content size in bytes
    size: usize,
    /// Entry type to create
    entry_type: EntryType,
    /// Byte pattern to fill content with
    fill_byte: u8,
    /// Byte positions to test corruption at (relative to version content)
    corruption_offsets: Vec<usize>,
    /// File path to use
    path: &'static str,
}

impl CorruptionTestConfig {
    /// Create test config for small inline files (<64KB)
    fn small_file(size: usize) -> Self {
        assert!(size < LARGE_FILE_THRESHOLD, "Use large_file() for files >= 64KB");
        Self {
            size,
            entry_type: EntryType::FilePhysicalVersion,
            fill_byte: 0xAA,
            corruption_offsets: vec![0, size / 2, size.saturating_sub(1)],
            path: "/test.dat",
        }
    }
    
    /// Create test config for large chunked files (>=64KB)
    fn large_file(size: usize) -> Self {
        assert!(size >= LARGE_FILE_THRESHOLD, "Use small_file() for files < 64KB");
        Self {
            size,
            entry_type: EntryType::FilePhysicalVersion,
            fill_byte: 0xBB,
            corruption_offsets: vec![
                0,                          // First byte
                16 * 1024,                  // First bao block boundary
                size / 2,                   // Middle
                size.saturating_sub(1),     // Last byte
            ],
            path: "/large_test.dat",
        }
    }
    
    /// Set custom corruption offsets
    fn with_offsets(mut self, offsets: Vec<usize>) -> Self {
        self.corruption_offsets = offsets;
        self
    }
    
    /// Set custom fill byte
    fn with_fill_byte(mut self, byte: u8) -> Self {
        self.fill_byte = byte;
        self
    }
}

/// Test fixture for corruption detection tests
struct CorruptionTestFixture {
    persistence: MemoryPersistence,
    fs: FS,
    config: CorruptionTestConfig,
    content: Vec<u8>,
}

impl CorruptionTestFixture {
    /// Create a new fixture and write the initial file
    async fn setup(config: CorruptionTestConfig) -> Result<Self> {
        let persistence = MemoryPersistence::default();
        let fs = FS::new(persistence.clone()).await?;
        let content = vec![config.fill_byte; config.size];
        
        // Write the file
        let root = fs.root().await?;
        use tokio::io::AsyncWriteExt;
        let mut writer = root
            .async_writer_path_with_type(config.path, config.entry_type.clone())
            .await?;
        writer.write_all(&content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
        writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
        
        Ok(Self { persistence, fs, config, content })
    }
    
    /// Verify uncorrupted file reads correctly (baseline)
    async fn verify_baseline(&self) -> Result<()> {
        let root = self.fs.root().await?;
        let file_node = root.get_node_path(self.config.path).await?;
        let file = file_node.as_file().await?;
        
        let mut reader = file.async_reader().await?;
        let mut buffer = Vec::new();
        use tokio::io::AsyncReadExt;
        let _ = reader.read_to_end(&mut buffer).await.map_err(|e| crate::Error::Other(e.to_string()))?;
        
        assert_eq!(buffer.len(), self.content.len(), "Size mismatch");
        assert_eq!(buffer, self.content, "Content mismatch");
        Ok(())
    }
    
    /// Test that corruption at a specific offset is detected
    async fn verify_corruption_detected(&self, offset: usize) -> Result<()> {
        let root = self.fs.root().await?;
        let file_node = root.get_node_path(self.config.path).await?;
        let file_id = file_node.id();
        
        // Corrupt the byte
        let corrupt_byte = self.config.fill_byte.wrapping_add(1);
        self.persistence.corrupt_file_content(file_id, 1, offset, corrupt_byte).await?;
        
        // Try to read - should detect corruption
        let root = self.fs.root().await?;
        let file_node = root.get_node_path(self.config.path).await?;
        let file = file_node.as_file().await?;
        let read_result = file.async_reader().await;
        
        // Restore before asserting
        self.persistence.corrupt_file_content(file_id, 1, offset, self.config.fill_byte).await?;
        
        assert!(
            read_result.is_err(),
            "Should detect corruption at offset {} in {} byte file",
            offset,
            self.config.size
        );
        
        Ok(())
    }
    
    /// Run all configured corruption tests
    async fn run_all_corruption_tests(&self) -> Result<()> {
        // First verify baseline
        self.verify_baseline().await?;
        
        // Test each corruption offset
        for &offset in &self.config.corruption_offsets {
            if offset < self.config.size {
                self.verify_corruption_detected(offset).await?;
            }
        }
        
        // Final baseline check
        self.verify_baseline().await?;
        
        Ok(())
    }
}

/// Test corruption of first block in a FilePhysicalVersion
#[tokio::test]
async fn test_corruption_first_block() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // Create test content - 32KB (2 bao-tree blocks of 16KB each)
    // With chunk_log=4, blocks are 16KB. Need >16KB for non-empty outboard.
    let content = vec![0xAA; 32768];
    
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type("/test.dat", EntryType::FilePhysicalVersion)
        .await?;
    writer.write_all(&content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    // Get the file ID
    let file_node = root.get_node_path("/test.dat").await?;
    let file_id = file_node.id();
    
    // Corrupt the first byte using test-only API
    persistence.corrupt_file_content(file_id, 1, 0, 0xFF).await?;
    
    // Try to read - should detect corruption via bao-tree/blake3 validation
    let root = fs.root().await?;
    let file_node = root.get_node_path("/test.dat").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    
    // Should fail with validation error
    assert!(read_result.is_err(), "Should detect corruption during validation");
    
    let err_msg = match read_result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("Should have failed with corruption error"),
    };
    
    assert!(
        err_msg.contains("validation failed") || err_msg.contains("mismatch"),
        "Error should mention validation failure: {}",
        err_msg
    );
    
    Ok(())
}

/// Test corruption of middle block
#[tokio::test]
async fn test_corruption_middle_block() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // Create 48KB file (3 bao-tree blocks of 16KB each)
    let content = vec![0xBB; 49152];
    
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type("/test.dat", EntryType::FilePhysicalVersion)
        .await?;
    writer.write_all(&content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    let file_node = root.get_node_path("/test.dat").await?;
    let file_id = file_node.id();
    
    // Corrupt middle block (byte 1500 is in block 1)
    persistence.corrupt_file_content(file_id, 1, 1500, 0xCC).await?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/test.dat").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    assert!(read_result.is_err(), "Should detect corruption in middle block");
    
    Ok(())
}

/// Test corruption of last block
#[tokio::test]
async fn test_corruption_last_block() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // Create 32KB file (2 bao-tree blocks of 16KB each)
    let content = vec![0xDD; 32768];
    
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type("/test.dat", EntryType::FilePhysicalVersion)
        .await?;
    writer.write_all(&content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    let file_node = root.get_node_path("/test.dat").await?;
    let file_id = file_node.id();
    
    // Corrupt near end (byte 32000 in last part)
    persistence.corrupt_file_content(file_id, 1, 32000, 0x11).await?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/test.dat").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    assert!(read_result.is_err(), "Should detect corruption in last block");
    
    Ok(())
}

/// Test corruption at block boundary
#[tokio::test]
async fn test_corruption_block_boundary() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // Create 32KB file (2 bao-tree blocks of 16KB each)
    let content = vec![0xEE; 32768];
    
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type("/test.dat", EntryType::FilePhysicalVersion)
        .await?;
    writer.write_all(&content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    let file_node = root.get_node_path("/test.dat").await?;
    let file_id = file_node.id();
    
    // Corrupt at 16KB bao-tree block boundary
    persistence.corrupt_file_content(file_id, 1, 16384, 0x22).await?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/test.dat").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    assert!(read_result.is_err(), "Should detect corruption at block boundary");
    
    Ok(())
}

/// Test multiple corruption points
#[tokio::test]
async fn test_multiple_corruptions() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // Create 64KB file (4 bao-tree blocks of 16KB each)
    let content = vec![0xFF; 65536];
    
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type("/test.dat", EntryType::FilePhysicalVersion)
        .await?;
    writer.write_all(&content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    let file_node = root.get_node_path("/test.dat").await?;
    let file_id = file_node.id();
    
    // Corrupt multiple locations across different 16KB blocks
    persistence.corrupt_file_content(file_id, 1, 100, 0x44).await?;
    persistence.corrupt_file_content(file_id, 1, 20000, 0x55).await?;
    persistence.corrupt_file_content(file_id, 1, 50000, 0x66).await?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/test.dat").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    assert!(read_result.is_err(), "Should detect multiple corruptions");
    
    Ok(())
}

/// Test that uncorrupted file reads correctly
#[tokio::test]
async fn test_no_corruption_baseline() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // Create 32KB file (2 bao-tree blocks of 16KB each) to ensure bao-tree validation
    let content = vec![0x42; 32768];
    
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type("/test.dat", EntryType::FilePhysicalVersion)
        .await?;
    writer.write_all(&content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/test.dat").await?;
    let file = file_node.as_file().await?;
    
    // Should succeed - no corruption
    let mut reader = file.async_reader().await?;
    let mut buffer = Vec::new();
    use tokio::io::AsyncReadExt;
    let bytes_read = reader.read_to_end(&mut buffer).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    assert_eq!(bytes_read, 32768);
    assert_eq!(buffer.len(), 32768);
    assert_eq!(buffer, content);
    
    Ok(())
}

/// Test that corruption of ANY single byte in a file crossing block boundaries is detected
/// This verifies that both bao-tree (block-level) and blake3 (whole-file) validation work
#[tokio::test]
async fn test_every_byte_corruption_detected() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // Create 17409 byte file: 16KB + 1KB + 1 byte
    // This spans two bao-tree blocks (first complete 16KB, second partial)
    let file_size = 17409;
    let content = vec![0xAB; file_size];
    
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type("/test.dat", EntryType::FilePhysicalVersion)
        .await?;
    writer.write_all(&content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    let file_node = root.get_node_path("/test.dat").await?;
    let file_id = file_node.id();
    
    // Test corruption at strategic positions:
    // - First byte (position 0)
    // - Last byte of first block (position 16383)
    // - First byte of second block (position 16384)
    // - Middle of second block (position 17000)
    // - Last byte (position 17408)
    let test_positions = vec![0, 16383, 16384, 17000, 17408];
    
    for byte_offset in test_positions {
        // Corrupt one byte
        persistence.corrupt_file_content(file_id, 1, byte_offset, 0xFF).await?;
        
        // Try to read - should detect corruption
        let root = fs.root().await?;
        let file_node = root.get_node_path("/test.dat").await?;
        let file = file_node.as_file().await?;
        
        let read_result = file.async_reader().await;
        assert!(
            read_result.is_err(),
            "Should detect corruption at byte offset {} (file size {})",
            byte_offset,
            file_size
        );
        
        // Restore the byte for next test
        persistence.corrupt_file_content(file_id, 1, byte_offset, 0xAB).await?;
    }
    
    // Verify file reads correctly when not corrupted
    let root = fs.root().await?;
    let file_node = root.get_node_path("/test.dat").await?;
    let file = file_node.as_file().await?;
    let mut reader = file.async_reader().await?;
    let mut buffer = Vec::new();
    use tokio::io::AsyncReadExt;
    let _bytes_read = reader.read_to_end(&mut buffer).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    assert_eq!(buffer, content, "Uncorrupted file should read successfully");
    
    Ok(())
}

// =============================================================================
// FilePhysicalSeries Corruption Tests (Multi-Version)
// =============================================================================

/// Test corruption in first version of a FilePhysicalSeries
#[tokio::test]
async fn test_series_corruption_first_version() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // Create FilePhysicalSeries with multiple versions
    // Each version 12KB - when concatenated, cumulative spans multiple bao blocks
    let version1 = vec![0xAA; 12 * 1024];
    let version2 = vec![0xBB; 12 * 1024];
    let version3 = vec![0xCC; 12 * 1024]; // Total: 36KB = 2+ bao blocks
    
    use tokio::io::AsyncWriteExt;
    
    // Write version 1
    let mut writer = root
        .async_writer_path_with_type("/series.dat", EntryType::FilePhysicalSeries)
        .await?;
    writer.write_all(&version1).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    // Write version 2
    let mut writer = root
        .async_writer_path_with_type("/series.dat", EntryType::FilePhysicalSeries)
        .await?;
    writer.write_all(&version2).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    // Write version 3
    let mut writer = root
        .async_writer_path_with_type("/series.dat", EntryType::FilePhysicalSeries)
        .await?;
    writer.write_all(&version3).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    // Verify it reads correctly first
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file = file_node.as_file().await?;
    let mut reader = file.async_reader().await?;
    let mut buffer = Vec::new();
    use tokio::io::AsyncReadExt;
    let _ = reader.read_to_end(&mut buffer).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    assert_eq!(buffer.len(), 36 * 1024, "Should concatenate all versions");
    
    // Get file ID
    let file_node = root.get_node_path("/series.dat").await?;
    let file_id = file_node.id();
    
    // Corrupt first version (version 1) at byte 100
    persistence.corrupt_file_content(file_id, 1, 100, 0xFF).await?;
    
    // Try to read - should detect corruption via cumulative outboard validation
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    assert!(read_result.is_err(), "Should detect corruption in first version");
    
    let err_msg = match read_result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("Should have failed with corruption error"),
    };
    assert!(
        err_msg.contains("hash mismatch") || err_msg.contains("validation failed") || err_msg.contains("Bao-tree"),
        "Error should mention validation failure: {}",
        err_msg
    );
    
    Ok(())
}

/// Test corruption in middle version of a FilePhysicalSeries
#[tokio::test]
async fn test_series_corruption_middle_version() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // Create FilePhysicalSeries with 3 versions
    let version1 = vec![0xAA; 12 * 1024];
    let version2 = vec![0xBB; 12 * 1024];
    let version3 = vec![0xCC; 12 * 1024];
    
    use tokio::io::AsyncWriteExt;
    
    for content in [&version1, &version2, &version3] {
        let mut writer = root
            .async_writer_path_with_type("/series.dat", EntryType::FilePhysicalSeries)
            .await?;
        writer.write_all(content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
        writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    }
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file_id = file_node.id();
    
    // Corrupt middle version (version 2) at byte 500
    persistence.corrupt_file_content(file_id, 2, 500, 0xFF).await?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    assert!(read_result.is_err(), "Should detect corruption in middle version");
    
    Ok(())
}

/// Test corruption in last version of a FilePhysicalSeries
#[tokio::test]
async fn test_series_corruption_last_version() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // Create FilePhysicalSeries with 3 versions
    let version1 = vec![0xAA; 12 * 1024];
    let version2 = vec![0xBB; 12 * 1024];
    let version3 = vec![0xCC; 12 * 1024];
    
    use tokio::io::AsyncWriteExt;
    
    for content in [&version1, &version2, &version3] {
        let mut writer = root
            .async_writer_path_with_type("/series.dat", EntryType::FilePhysicalSeries)
            .await?;
        writer.write_all(content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
        writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    }
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file_id = file_node.id();
    
    // Corrupt last version (version 3) near end
    persistence.corrupt_file_content(file_id, 3, 12000, 0xFF).await?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    assert!(read_result.is_err(), "Should detect corruption in last version");
    
    Ok(())
}

/// Test corruption at version boundary (where cumulative outboard validates across versions)
#[tokio::test]
async fn test_series_corruption_at_version_boundary() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // Version 1: exactly 16KB (one bao block)
    // Version 2: 1KB (partial second block)
    // Corruption at boundary between them exercises cross-version validation
    let version1 = vec![0xAA; 16 * 1024];
    let version2 = vec![0xBB; 1024];
    
    use tokio::io::AsyncWriteExt;
    
    for content in [&version1, &version2] {
        let mut writer = root
            .async_writer_path_with_type("/series.dat", EntryType::FilePhysicalSeries)
            .await?;
        writer.write_all(content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
        writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    }
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file_id = file_node.id();
    
    // Corrupt last byte of version 1 (byte 16383 - boundary)
    persistence.corrupt_file_content(file_id, 1, 16383, 0xFF).await?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    assert!(read_result.is_err(), "Should detect corruption at version boundary");
    
    Ok(())
}

/// Test series validation with many small versions (cumulative < 16KB, no bao, relies on blake3)
#[tokio::test]
async fn test_series_small_versions_blake3_validation() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // Multiple tiny versions - cumulative < 16KB, no bao-tree outboard
    // Must rely on blake3 validation only
    let versions: Vec<Vec<u8>> = (0..10).map(|i| vec![i as u8; 100]).collect();
    
    use tokio::io::AsyncWriteExt;
    
    for content in &versions {
        let mut writer = root
            .async_writer_path_with_type("/series.dat", EntryType::FilePhysicalSeries)
            .await?;
        writer.write_all(content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
        writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    }
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file_id = file_node.id();
    
    // Corrupt version 5 at byte 50
    persistence.corrupt_file_content(file_id, 5, 50, 0xFF).await?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    // Small files may not have bao-tree validation, but should still fail
    // because the cumulative content won't match stored hashes
    assert!(read_result.is_err(), "Should detect corruption even in small series (via cumulative validation)");
    
    Ok(())
}

/// Test series validation baseline - no corruption, should read successfully
#[tokio::test]
async fn test_series_no_corruption_baseline() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // Create series with multiple versions > 16KB total
    let version1 = vec![0xAA; 20 * 1024];
    let version2 = vec![0xBB; 20 * 1024];
    
    use tokio::io::AsyncWriteExt;
    
    for content in [&version1, &version2] {
        let mut writer = root
            .async_writer_path_with_type("/series.dat", EntryType::FilePhysicalSeries)
            .await?;
        writer.write_all(content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
        writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    }
    
    // Should read successfully
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file = file_node.as_file().await?;
    let mut reader = file.async_reader().await?;
    let mut buffer = Vec::new();
    use tokio::io::AsyncReadExt;
    let _ = reader.read_to_end(&mut buffer).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    assert_eq!(buffer.len(), 40 * 1024);
    assert_eq!(&buffer[..20 * 1024], &version1[..]);
    assert_eq!(&buffer[20 * 1024..], &version2[..]);
    
    Ok(())
}

// =============================================================================
// Small Version Series Tests (pending bytes spanning multiple versions)
// =============================================================================
// These tests verify correct handling of FilePhysicalSeries where versions
// are smaller than 16KB (bao block size), causing pending bytes to span
// multiple versions.

/// Test many small versions where pending spans multiple versions
/// Versions: 5KB each, 10 versions = 50KB total
/// Block boundaries at 16KB, 32KB, 48KB - pending crosses version boundaries
#[tokio::test]
async fn test_series_many_small_versions_baseline() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // 10 versions of 5KB each = 50KB total
    // Block boundaries: 0-16KB (v1+v2+partial v3), 16-32KB (partial v3+v4+v5+partial v6), etc.
    let versions: Vec<Vec<u8>> = (0..10u8).map(|i| vec![i; 5 * 1024]).collect();
    
    use tokio::io::AsyncWriteExt;
    
    for content in &versions {
        let mut writer = root
            .async_writer_path_with_type("/series.dat", EntryType::FilePhysicalSeries)
            .await?;
        writer.write_all(content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
        writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    }
    
    // Read and verify
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file = file_node.as_file().await?;
    let mut reader = file.async_reader().await?;
    let mut buffer = Vec::new();
    use tokio::io::AsyncReadExt;
    let _ = reader.read_to_end(&mut buffer).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    assert_eq!(buffer.len(), 50 * 1024);
    
    // Verify each version's content
    for (i, version) in versions.iter().enumerate() {
        let start = i * 5 * 1024;
        let end = start + 5 * 1024;
        assert_eq!(&buffer[start..end], &version[..], "Version {} mismatch", i);
    }
    
    Ok(())
}

/// Test corruption in small version that contributes to pending bytes
/// Corrupt version 4 (bytes 20KB-25KB), which is in the middle of block 2 (16KB-32KB)
#[tokio::test]
async fn test_series_small_version_corruption_mid_block() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // 10 versions of 5KB each
    let versions: Vec<Vec<u8>> = (0..10u8).map(|i| vec![i; 5 * 1024]).collect();
    
    use tokio::io::AsyncWriteExt;
    
    for content in &versions {
        let mut writer = root
            .async_writer_path_with_type("/series.dat", EntryType::FilePhysicalSeries)
            .await?;
        writer.write_all(content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
        writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    }
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file_id = file_node.id();
    
    // Corrupt version 5 (0-indexed version 4) at byte 100
    // This is at cumulative offset 20KB + 100 = in block 2
    persistence.corrupt_file_content(file_id, 5, 100, 0xFF).await?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    assert!(read_result.is_err(), "Should detect corruption in small version within block");
    
    Ok(())
}

/// Test corruption at exact block boundary crossing between small versions
/// Block 1 ends at 16KB, which is in the middle of version 4 (15KB-20KB)
#[tokio::test]
async fn test_series_small_version_corruption_at_block_boundary() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // 10 versions of 5KB each
    let versions: Vec<Vec<u8>> = (0..10u8).map(|i| vec![i; 5 * 1024]).collect();
    
    use tokio::io::AsyncWriteExt;
    
    for content in &versions {
        let mut writer = root
            .async_writer_path_with_type("/series.dat", EntryType::FilePhysicalSeries)
            .await?;
        writer.write_all(content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
        writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    }
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file_id = file_node.id();
    
    // Corrupt version 4 (0-indexed version 3) at byte 1024
    // Version 4 spans 15KB-20KB, byte 1024 = cumulative offset 16KB = exact block boundary
    persistence.corrupt_file_content(file_id, 4, 1024, 0xFF).await?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    assert!(read_result.is_err(), "Should detect corruption at block boundary within version");
    
    Ok(())
}

/// Test corruption in very first small version
#[tokio::test]
async fn test_series_small_version_corruption_first() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // 10 versions of 5KB each
    let versions: Vec<Vec<u8>> = (0..10u8).map(|i| vec![i; 5 * 1024]).collect();
    
    use tokio::io::AsyncWriteExt;
    
    for content in &versions {
        let mut writer = root
            .async_writer_path_with_type("/series.dat", EntryType::FilePhysicalSeries)
            .await?;
        writer.write_all(content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
        writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    }
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file_id = file_node.id();
    
    // Corrupt first version at byte 50
    persistence.corrupt_file_content(file_id, 1, 50, 0xFF).await?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    assert!(read_result.is_err(), "Should detect corruption in first small version");
    
    Ok(())
}

/// Test corruption in last small version
#[tokio::test]
async fn test_series_small_version_corruption_last() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // 10 versions of 5KB each
    let versions: Vec<Vec<u8>> = (0..10u8).map(|i| vec![i; 5 * 1024]).collect();
    
    use tokio::io::AsyncWriteExt;
    
    for content in &versions {
        let mut writer = root
            .async_writer_path_with_type("/series.dat", EntryType::FilePhysicalSeries)
            .await?;
        writer.write_all(content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
        writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    }
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file_id = file_node.id();
    
    // Corrupt last version (version 10) at byte 50
    persistence.corrupt_file_content(file_id, 10, 50, 0xFF).await?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    assert!(read_result.is_err(), "Should detect corruption in last small version");
    
    Ok(())
}

/// Test tiny versions (100 bytes each) - all within single bao block
/// Total 20 versions * 100 bytes = 2KB, well under 16KB block
#[tokio::test]
async fn test_series_tiny_versions_single_block() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // 20 versions of 100 bytes each = 2KB total (single bao block)
    let versions: Vec<Vec<u8>> = (0..20u8).map(|i| vec![i; 100]).collect();
    
    use tokio::io::AsyncWriteExt;
    
    for content in &versions {
        let mut writer = root
            .async_writer_path_with_type("/series.dat", EntryType::FilePhysicalSeries)
            .await?;
        writer.write_all(content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
        writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    }
    
    // Baseline: should read successfully
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file = file_node.as_file().await?;
    let mut reader = file.async_reader().await?;
    let mut buffer = Vec::new();
    use tokio::io::AsyncReadExt;
    let _ = reader.read_to_end(&mut buffer).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    assert_eq!(buffer.len(), 2000);
    
    Ok(())
}

/// Test tiny versions with corruption - should be detected via cumulative checksum
#[tokio::test]
async fn test_series_tiny_versions_corruption() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // 20 versions of 100 bytes each
    let versions: Vec<Vec<u8>> = (0..20u8).map(|i| vec![i; 100]).collect();
    
    use tokio::io::AsyncWriteExt;
    
    for content in &versions {
        let mut writer = root
            .async_writer_path_with_type("/series.dat", EntryType::FilePhysicalSeries)
            .await?;
        writer.write_all(content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
        writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    }
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file_id = file_node.id();
    
    // Corrupt version 10 at byte 50
    persistence.corrupt_file_content(file_id, 10, 50, 0xFF).await?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    assert!(read_result.is_err(), "Should detect corruption in tiny version via cumulative checksum");
    
    Ok(())
}

/// Test versions that exactly fill blocks (16KB each)
/// This is the ideal case - each version is exactly one block
#[tokio::test]
async fn test_series_exact_block_versions() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // 4 versions of exactly 16KB each = 64KB total (4 blocks)
    let versions: Vec<Vec<u8>> = (0..4u8).map(|i| vec![i; 16 * 1024]).collect();
    
    use tokio::io::AsyncWriteExt;
    
    for content in &versions {
        let mut writer = root
            .async_writer_path_with_type("/series.dat", EntryType::FilePhysicalSeries)
            .await?;
        writer.write_all(content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
        writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    }
    
    // Baseline read
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file = file_node.as_file().await?;
    let mut reader = file.async_reader().await?;
    let mut buffer = Vec::new();
    use tokio::io::AsyncReadExt;
    let _ = reader.read_to_end(&mut buffer).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    assert_eq!(buffer.len(), 64 * 1024);
    
    Ok(())
}

/// Test exact block versions with corruption
#[tokio::test]
async fn test_series_exact_block_versions_corruption() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // 4 versions of exactly 16KB each
    let versions: Vec<Vec<u8>> = (0..4u8).map(|i| vec![i; 16 * 1024]).collect();
    
    use tokio::io::AsyncWriteExt;
    
    for content in &versions {
        let mut writer = root
            .async_writer_path_with_type("/series.dat", EntryType::FilePhysicalSeries)
            .await?;
        writer.write_all(content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
        writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    }
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file_id = file_node.id();
    
    // Corrupt version 3 (block 3) at byte 100
    persistence.corrupt_file_content(file_id, 3, 100, 0xFF).await?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/series.dat").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    assert!(read_result.is_err(), "Should detect corruption in exact-block-sized version");
    
    Ok(())
}

// =============================================================================
// Small File Tests (< 16KB, blake3 only validation)
// =============================================================================

/// Test corruption detection in small files (< 16KB, uses blake3 only)
#[tokio::test]
async fn test_corruption_small_file_blake3_only() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // Create small file (< 16KB block size, no bao-tree outboard)
    let content = vec![0x42; 1000];
    
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type("/small.dat", EntryType::FilePhysicalVersion)
        .await?;
    writer.write_all(&content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    let file_node = root.get_node_path("/small.dat").await?;
    let file_id = file_node.id();
    
    // Corrupt middle byte
    persistence.corrupt_file_content(file_id, 1, 500, 0xFF).await?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/small.dat").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    assert!(read_result.is_err(), "Should detect corruption via blake3");
    
    let err_msg = match read_result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("Should have failed"),
    };
    assert!(
        err_msg.contains("Blake3 mismatch"),
        "Error should mention blake3: {}",
        err_msg
    );
    
    Ok(())
}

/// Test corruption at exact 16KB boundary (one full block, no outboard)
#[tokio::test]
async fn test_corruption_exactly_one_block() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // Exactly 16KB = one full bao block, outboard may be empty
    let content = vec![0x42; 16384];
    
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type("/oneblock.dat", EntryType::FilePhysicalVersion)
        .await?;
    writer.write_all(&content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    let file_node = root.get_node_path("/oneblock.dat").await?;
    let file_id = file_node.id();
    
    // Corrupt last byte
    persistence.corrupt_file_content(file_id, 1, 16383, 0xFF).await?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/oneblock.dat").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    assert!(read_result.is_err(), "Should detect corruption via blake3 fallback");
    
    Ok(())
}

// =============================================================================
// Large File Tests (spanning many bao blocks)
// =============================================================================

/// Test corruption detection in large file (multiple bao blocks)
#[tokio::test]
async fn test_corruption_large_file_many_blocks() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // Create 256KB file (16 bao blocks of 16KB each)
    let content = vec![0x99; 256 * 1024];
    
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type("/large.dat", EntryType::FilePhysicalVersion)
        .await?;
    writer.write_all(&content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    let file_node = root.get_node_path("/large.dat").await?;
    let file_id = file_node.id();
    
    // Test corruption at various block boundaries
    let test_offsets = vec![
        0,                    // First byte
        16384,                // Second block start
        16384 * 8,            // Middle block
        16384 * 15,           // Last complete block
        256 * 1024 - 1,       // Last byte
    ];
    
    for offset in test_offsets {
        persistence.corrupt_file_content(file_id, 1, offset, 0xFF).await?;
        
        let root = fs.root().await?;
        let file_node = root.get_node_path("/large.dat").await?;
        let file = file_node.as_file().await?;
        
        let read_result = file.async_reader().await;
        assert!(
            read_result.is_err(),
            "Should detect corruption at offset {} in 256KB file",
            offset
        );
        
        // Restore for next test
        persistence.corrupt_file_content(file_id, 1, offset, 0x99).await?;
    }
    
    // Verify uncorrupted reads work
    let root = fs.root().await?;
    let file_node = root.get_node_path("/large.dat").await?;
    let file = file_node.as_file().await?;
    let mut reader = file.async_reader().await?;
    let mut buffer = Vec::new();
    use tokio::io::AsyncReadExt;
    let _ = reader.read_to_end(&mut buffer).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    assert_eq!(buffer, content);
    
    Ok(())
}

// =============================================================================
// Edge Cases
// =============================================================================

/// Test empty file has no validation issues
#[tokio::test]
async fn test_empty_file_no_validation_issues() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // Create empty file
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type("/empty.dat", EntryType::FilePhysicalVersion)
        .await?;
    writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    // Should read successfully (empty)
    let root = fs.root().await?;
    let file_node = root.get_node_path("/empty.dat").await?;
    let file = file_node.as_file().await?;
    let mut reader = file.async_reader().await?;
    let mut buffer = Vec::new();
    use tokio::io::AsyncReadExt;
    let _ = reader.read_to_end(&mut buffer).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    assert!(buffer.is_empty());
    
    Ok(())
}

/// Test single byte file
#[tokio::test]
async fn test_single_byte_file_corruption() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // Create 1-byte file
    let content = vec![0x42];
    
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type("/tiny.dat", EntryType::FilePhysicalVersion)
        .await?;
    writer.write_all(&content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    let file_node = root.get_node_path("/tiny.dat").await?;
    let file_id = file_node.id();
    
    // Corrupt the only byte
    persistence.corrupt_file_content(file_id, 1, 0, 0xFF).await?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/tiny.dat").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    assert!(read_result.is_err(), "Should detect corruption in 1-byte file via blake3");
    
    Ok(())
}

/// Test file at 16KB + 1 byte boundary (exactly 2 blocks with minimal second block)
#[tokio::test]
async fn test_minimal_second_block() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // 16KB + 1 byte - creates 2 bao blocks, second has just 1 byte
    let content = vec![0x77; 16385];
    
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type("/boundary.dat", EntryType::FilePhysicalVersion)
        .await?;
    writer.write_all(&content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    let file_node = root.get_node_path("/boundary.dat").await?;
    let file_id = file_node.id();
    
    // Corrupt the single byte in second block
    persistence.corrupt_file_content(file_id, 1, 16384, 0xFF).await?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/boundary.dat").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    assert!(read_result.is_err(), "Should detect corruption in minimal second block");
    
    Ok(())
}
// =============================================================================
// TablePhysicalVersion Corruption Tests
// =============================================================================

/// Test corruption detection in TablePhysicalVersion (large table >16KB)
#[tokio::test]
async fn test_table_version_corruption_large() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // Create large table content (32KB - spans 2 bao blocks)
    // Tables are stored as parquet, but we can write raw bytes for testing
    let content = vec![0xAA; 32768];
    
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type("/table.parquet", EntryType::TablePhysicalVersion)
        .await?;
    writer.write_all(&content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    let file_node = root.get_node_path("/table.parquet").await?;
    let file_id = file_node.id();
    
    // Corrupt first byte
    persistence.corrupt_file_content(file_id, 1, 0, 0xFF).await?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/table.parquet").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    assert!(read_result.is_err(), "Should detect corruption in TablePhysicalVersion");
    
    let err_msg = match read_result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("Should have failed with corruption error"),
    };
    assert!(
        err_msg.contains("validation failed") || err_msg.contains("mismatch"),
        "Error should mention validation failure: {}",
        err_msg
    );
    
    Ok(())
}

/// Test corruption detection in small TablePhysicalVersion (<16KB, blake3 only)
#[tokio::test]
async fn test_table_version_corruption_small() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // Small table content
    let content = vec![0xBB; 1000];
    
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type("/small_table.parquet", EntryType::TablePhysicalVersion)
        .await?;
    writer.write_all(&content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    let file_node = root.get_node_path("/small_table.parquet").await?;
    let file_id = file_node.id();
    
    // Corrupt middle byte
    persistence.corrupt_file_content(file_id, 1, 500, 0xFF).await?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/small_table.parquet").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    assert!(read_result.is_err(), "Should detect corruption via blake3");
    
    Ok(())
}

/// Test TablePhysicalVersion baseline - no corruption
#[tokio::test]
async fn test_table_version_no_corruption_baseline() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    let content = vec![0xCC; 32768];
    
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type("/table.parquet", EntryType::TablePhysicalVersion)
        .await?;
    writer.write_all(&content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/table.parquet").await?;
    let file = file_node.as_file().await?;
    
    let mut reader = file.async_reader().await?;
    let mut buffer = Vec::new();
    use tokio::io::AsyncReadExt;
    let _ = reader.read_to_end(&mut buffer).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    assert_eq!(buffer, content);
    
    Ok(())
}

// =============================================================================
// TablePhysicalSeries Corruption Tests
// =============================================================================

/// Test corruption in TablePhysicalSeries (multi-version table)
#[tokio::test]
async fn test_table_series_corruption_first_version() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // Create table series with multiple versions
    let version1 = vec![0xAA; 12 * 1024];
    let version2 = vec![0xBB; 12 * 1024];
    let version3 = vec![0xCC; 12 * 1024]; // Total: 36KB
    
    use tokio::io::AsyncWriteExt;
    
    for content in [&version1, &version2, &version3] {
        let mut writer = root
            .async_writer_path_with_type("/table_series.parquet", EntryType::TablePhysicalSeries)
            .await?;
        writer.write_all(content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
        writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    }
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/table_series.parquet").await?;
    let file_id = file_node.id();
    
    // Corrupt first version at byte 100
    persistence.corrupt_file_content(file_id, 1, 100, 0xFF).await?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/table_series.parquet").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    assert!(read_result.is_err(), "Should detect corruption in table series first version");
    
    Ok(())
}

/// Test corruption in middle version of TablePhysicalSeries
#[tokio::test]
async fn test_table_series_corruption_middle_version() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    let version1 = vec![0xAA; 12 * 1024];
    let version2 = vec![0xBB; 12 * 1024];
    let version3 = vec![0xCC; 12 * 1024];
    
    use tokio::io::AsyncWriteExt;
    
    for content in [&version1, &version2, &version3] {
        let mut writer = root
            .async_writer_path_with_type("/table_series.parquet", EntryType::TablePhysicalSeries)
            .await?;
        writer.write_all(content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
        writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    }
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/table_series.parquet").await?;
    let file_id = file_node.id();
    
    // Corrupt middle version
    persistence.corrupt_file_content(file_id, 2, 500, 0xFF).await?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/table_series.parquet").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    assert!(read_result.is_err(), "Should detect corruption in table series middle version");
    
    Ok(())
}

/// Test TablePhysicalSeries baseline - no corruption
#[tokio::test]
async fn test_table_series_no_corruption_baseline() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    let version1 = vec![0xAA; 20 * 1024];
    let version2 = vec![0xBB; 20 * 1024];
    
    use tokio::io::AsyncWriteExt;
    
    for content in [&version1, &version2] {
        let mut writer = root
            .async_writer_path_with_type("/table_series.parquet", EntryType::TablePhysicalSeries)
            .await?;
        writer.write_all(content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
        writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    }
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/table_series.parquet").await?;
    let file = file_node.as_file().await?;
    let mut reader = file.async_reader().await?;
    let mut buffer = Vec::new();
    use tokio::io::AsyncReadExt;
    let _ = reader.read_to_end(&mut buffer).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    assert_eq!(buffer.len(), 40 * 1024);
    
    Ok(())
}

// =============================================================================
// Symlink Corruption Tests
// =============================================================================
// Note: In the current MemoryPersistence implementation, symlinks don't store
// blake3 hashes (symlink.rs returns blake3: None). For tlogfs, symlinks also
// don't validate blake3 (schema.rs skips validation for Symlink type).
//
// This is by design - symlinks are small trusted metadata. If we wanted to
// add symlink validation, we'd need to:
// 1. Store blake3 of target path at creation time
// 2. Verify on readlink()
//
// For now, these tests document the current behavior (no corruption detection).

/// Test symlink target is stored correctly (baseline - no corruption detection)
#[tokio::test]
async fn test_symlink_baseline() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // Create a target file
    let content = vec![0x42; 100];
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type("/target.dat", EntryType::FilePhysicalVersion)
        .await?;
    writer.write_all(&content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    // Create symlink to it - create_symlink_path returns the symlink node itself
    let link_node = root.create_symlink_path("/link", "/target.dat").await?;
    
    // Get symlink info from the returned node (not via get_node_path which follows symlinks)
    let symlink = link_node.as_symlink().await?;
    let target = symlink.readlink().await?;
    
    assert_eq!(target.to_str().unwrap(), "/target.dat");
    
    // Also verify we can follow the symlink to read the target
    let read_content = root.read_file_path_to_vec("/link").await?;
    assert_eq!(read_content, content);
    
    Ok(())
}

// =============================================================================
// DirectoryPhysical Corruption Tests  
// =============================================================================
// Note: Directories in MemoryPersistence are stored as Node objects with
// children tracked separately. They don't have byte content that could be
// corrupted in the same way as files.
//
// In tlogfs, directories have content (serialized children) but no blake3
// validation. Corruption would be detected via structural inconsistencies
// (e.g., child node doesn't exist).
//
// These tests document that directory structure integrity is maintained.

/// Test directory structure integrity (baseline)
#[tokio::test]
async fn test_directory_baseline() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // Create nested directory structure
    let _ = root.create_dir_path("/dir1").await?;
    let _ = root.create_dir_path("/dir1/dir2").await?;
    let _ = root.create_dir_path("/dir1/dir2/dir3").await?;
    
    // Create file in nested directory
    let content = vec![0x42; 100];
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type("/dir1/dir2/dir3/file.dat", EntryType::FilePhysicalVersion)
        .await?;
    writer.write_all(&content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    // Verify we can traverse the structure
    let dir1 = root.get_node_path("/dir1").await?;
    assert!(dir1.into_dir().await.is_some());
    
    let dir3 = root.get_node_path("/dir1/dir2/dir3").await?;
    assert!(dir3.into_dir().await.is_some());
    
    let file = root.get_node_path("/dir1/dir2/dir3/file.dat").await?;
    assert!(file.into_file().await.is_some());
    
    // Read file content
    let file_handle = file.as_file().await?;
    let mut reader = file_handle.async_reader().await?;
    let mut buffer = Vec::new();
    use tokio::io::AsyncReadExt;
    let _ = reader.read_to_end(&mut buffer).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    assert_eq!(buffer, content);
    
    Ok(())
}

/// Test directory listing integrity
#[tokio::test]
async fn test_directory_listing_integrity() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // Create directory with multiple children
    let _ = root.create_dir_path("/testdir").await?;
    
    use tokio::io::AsyncWriteExt;
    for i in 0u8..5 {
        let path = format!("/testdir/file{}.dat", i);
        let mut writer = root
            .async_writer_path_with_type(&path, EntryType::FilePhysicalVersion)
            .await?;
        writer.write_all(&[i; 100]).await.map_err(|e| crate::Error::Other(e.to_string()))?;
        writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    }
    
    // List directory using entries stream
    let testdir = root.open_dir_path("/testdir").await?;
    use futures::StreamExt;
    let mut entry_stream = testdir.entries().await?;
    let mut count = 0;
    while let Some(result) = entry_stream.next().await {
        let _entry = result?;
        count += 1;
    }
    
    assert_eq!(count, 5);
    
    // Verify all files can be read with correct content
    for i in 0u8..5 {
        let path = format!("/testdir/file{}.dat", i);
        let file_node = root.get_node_path(&path).await?;
        let file = file_node.as_file().await?;
        let mut reader = file.async_reader().await?;
        let mut buffer = Vec::new();
        use tokio::io::AsyncReadExt;
        let _ = reader.read_to_end(&mut buffer).await.map_err(|e| crate::Error::Other(e.to_string()))?;
        
        assert_eq!(buffer, vec![i; 100]);
    }
    
    Ok(())
}

// =============================================================================
// FilePhysicalVersion Fixture-Based Tests (Small + Large)
// =============================================================================

/// Test corruption detection using fixture: tiny file (1 byte)
#[tokio::test]
async fn test_fixture_tiny_file() -> Result<()> {
    let config = CorruptionTestConfig::small_file(1);
    let fixture = CorruptionTestFixture::setup(config).await?;
    fixture.run_all_corruption_tests().await
}

/// Test corruption detection using fixture: small file (1KB)
#[tokio::test]
async fn test_fixture_small_1kb() -> Result<()> {
    let config = CorruptionTestConfig::small_file(1024);
    let fixture = CorruptionTestFixture::setup(config).await?;
    fixture.run_all_corruption_tests().await
}

/// Test corruption detection using fixture: exactly one bao block (16KB)
#[tokio::test]
async fn test_fixture_one_bao_block() -> Result<()> {
    let config = CorruptionTestConfig::small_file(16 * 1024);
    let fixture = CorruptionTestFixture::setup(config).await?;
    fixture.run_all_corruption_tests().await
}

/// Test corruption detection using fixture: just over one bao block (16KB + 1)
#[tokio::test]
async fn test_fixture_over_one_bao_block() -> Result<()> {
    let config = CorruptionTestConfig::small_file(16 * 1024 + 1)
        .with_offsets(vec![0, 16383, 16384]); // Boundary testing
    let fixture = CorruptionTestFixture::setup(config).await?;
    fixture.run_all_corruption_tests().await
}

/// Test corruption detection using fixture: two bao blocks (32KB)
#[tokio::test]
async fn test_fixture_two_bao_blocks() -> Result<()> {
    let config = CorruptionTestConfig::small_file(32 * 1024)
        .with_offsets(vec![0, 8192, 16383, 16384, 24576, 32767]);
    let fixture = CorruptionTestFixture::setup(config).await?;
    fixture.run_all_corruption_tests().await
}

/// Test corruption detection using fixture: just under large threshold (63KB)
#[tokio::test]
async fn test_fixture_under_large_threshold() -> Result<()> {
    let config = CorruptionTestConfig::small_file(63 * 1024)
        .with_offsets(vec![0, 16 * 1024, 32 * 1024, 48 * 1024, 63 * 1024 - 1]);
    let fixture = CorruptionTestFixture::setup(config).await?;
    fixture.run_all_corruption_tests().await
}

/// Test corruption detection using fixture: exactly at large threshold (64KB)
/// This file will be stored as chunked parquet
#[tokio::test]
async fn test_fixture_at_large_threshold() -> Result<()> {
    let config = CorruptionTestConfig::large_file(64 * 1024)
        .with_offsets(vec![0, 16 * 1024, 32 * 1024, 48 * 1024, 64 * 1024 - 1]);
    let fixture = CorruptionTestFixture::setup(config).await?;
    fixture.run_all_corruption_tests().await
}

/// Test corruption detection using fixture: large file (128KB - 8 bao blocks)
/// Stored as chunked parquet in _large_files/
#[tokio::test]
async fn test_fixture_large_128kb() -> Result<()> {
    let config = CorruptionTestConfig::large_file(128 * 1024)
        .with_offsets(vec![
            0,                      // First byte
            16 * 1024 - 1,          // End of first bao block
            16 * 1024,              // Start of second bao block
            64 * 1024,              // 4 bao blocks in
            100 * 1024,             // Middle-ish
            128 * 1024 - 1,         // Last byte
        ]);
    let fixture = CorruptionTestFixture::setup(config).await?;
    fixture.run_all_corruption_tests().await
}

/// Test corruption detection using fixture: large file (256KB - 16 bao blocks)
#[tokio::test]
async fn test_fixture_large_256kb() -> Result<()> {
    let config = CorruptionTestConfig::large_file(256 * 1024);
    let fixture = CorruptionTestFixture::setup(config).await?;
    fixture.run_all_corruption_tests().await
}

/// Test corruption detection using fixture: large file (1MB - 64 bao blocks)
#[tokio::test]
async fn test_fixture_large_1mb() -> Result<()> {
    let config = CorruptionTestConfig::large_file(1024 * 1024)
        .with_offsets(vec![
            0,
            256 * 1024,
            512 * 1024,
            768 * 1024,
            1024 * 1024 - 1,
        ]);
    let fixture = CorruptionTestFixture::setup(config).await?;
    fixture.run_all_corruption_tests().await
}

/// Test corruption at every bao block boundary in a 256KB file
#[tokio::test]
async fn test_fixture_every_block_boundary() -> Result<()> {
    // Create offsets at every 16KB boundary plus first/last bytes of each block
    let mut offsets = Vec::new();
    for block in 0..16 {
        let block_start = block * 16 * 1024;
        offsets.push(block_start);           // First byte of block
        if block > 0 {
            offsets.push(block_start - 1);   // Last byte of previous block
        }
    }
    offsets.push(256 * 1024 - 1);            // Last byte
    
    let config = CorruptionTestConfig::large_file(256 * 1024)
        .with_offsets(offsets);
    let fixture = CorruptionTestFixture::setup(config).await?;
    fixture.run_all_corruption_tests().await
}

// =============================================================================
// FilePhysicalVersion: Specific corruption patterns for large files
// =============================================================================

/// Test that corruption in first chunk of large file is detected
#[tokio::test]
async fn test_large_file_first_chunk_corruption() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // 100KB file - stored in chunked parquet
    let content = vec![0xCC; 100 * 1024];
    
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type("/chunked.dat", EntryType::FilePhysicalVersion)
        .await?;
    writer.write_all(&content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    let file_node = root.get_node_path("/chunked.dat").await?;
    let file_id = file_node.id();
    
    // Corrupt first byte
    persistence.corrupt_file_content(file_id, 1, 0, 0xFF).await?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/chunked.dat").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    assert!(read_result.is_err(), "Should detect corruption in first chunk of large file");
    
    Ok(())
}

/// Test that corruption in last chunk of large file is detected
#[tokio::test]
async fn test_large_file_last_chunk_corruption() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // 100KB file - stored in chunked parquet
    let content = vec![0xDD; 100 * 1024];
    
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type("/chunked.dat", EntryType::FilePhysicalVersion)
        .await?;
    writer.write_all(&content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    let file_node = root.get_node_path("/chunked.dat").await?;
    let file_id = file_node.id();
    
    // Corrupt last byte
    persistence.corrupt_file_content(file_id, 1, 100 * 1024 - 1, 0xFF).await?;
    
    let root = fs.root().await?;
    let file_node = root.get_node_path("/chunked.dat").await?;
    let file = file_node.as_file().await?;
    
    let read_result = file.async_reader().await;
    assert!(read_result.is_err(), "Should detect corruption in last byte of large file");
    
    Ok(())
}

/// Test large file baseline (no corruption should succeed)
#[tokio::test]
async fn test_large_file_baseline() -> Result<()> {
    let persistence = MemoryPersistence::default();
    let fs = FS::new(persistence.clone()).await?;
    let root = fs.root().await?;
    
    // 100KB file - stored in chunked parquet
    let content = vec![0xEE; 100 * 1024];
    
    use tokio::io::AsyncWriteExt;
    let mut writer = root
        .async_writer_path_with_type("/chunked.dat", EntryType::FilePhysicalVersion)
        .await?;
    writer.write_all(&content).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    writer.shutdown().await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    // Should read successfully with no corruption
    let root = fs.root().await?;
    let file_node = root.get_node_path("/chunked.dat").await?;
    let file = file_node.as_file().await?;
    let mut reader = file.async_reader().await?;
    let mut buffer = Vec::new();
    use tokio::io::AsyncReadExt;
    let _ = reader.read_to_end(&mut buffer).await.map_err(|e| crate::Error::Other(e.to_string()))?;
    
    assert_eq!(buffer.len(), 100 * 1024, "Size should match");
    assert_eq!(buffer, content, "Content should match");
    
    Ok(())
}