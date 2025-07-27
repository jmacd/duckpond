# Large File Storage Implementation Plan

**Date**: July 18, 2025  
**Status**: Ready to implement  
**Context**: OpLog/TLogFS merge complete, Phase 1 streaming support complete

## Overview

Implement large file storage for DuckPond's unified TLogFS crate to handle files >= 64 KiB efficiently by storing them outside Delta Lake records.

## Current State

### ✅ **Foundation Complete**
- **Unified TLogFS**: OpLog and TLogFS crates merged successfully
- **Streaming Support**: AsyncRead/AsyncWrite working with write protection
- **Arrow Integration**: Parquet roundtrip tests passing
- **All Tests Passing**: 113 tests across all crates, zero warnings

### 🎯 **Ready to Implement**: Large File Storage

## Design Goals

1. **64 KiB Threshold**: Files >= 64 KiB stored externally, smaller files inline in Delta Lake
2. **Content Addressing**: SHA256-based deduplication in `<pond>/_large_files/` 
3. **Optional Content**: OplogEntry.content becomes `Option<Vec<u8>>`
4. **SHA256 Field**: New `OplogEntry.sha256: Option<String>` field
5. **Atomic Operations**: Temp file + rename for consistency
6. **Garbage Collection**: Clean up unreferenced large files

## Implementation Plan

### Step 1: Update OplogEntry Schema

**File**: `crates/tlogfs/src/lib.rs`

```rust
/// Updated OplogEntry with large file support
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OplogEntry {
    pub part_id: String,
    pub node_id: String,
    pub timestamp: String,
    pub entry_type: String,
    
    // UPDATED: Content is now optional for large files
    pub content: Option<Vec<u8>>,       // None for large files (>= 64 KiB)
    
    // NEW: SHA256 checksum for large files
    pub sha256: Option<String>,         // Some() for large files, None for small files
}

impl OplogEntry {
    /// Create entry for small file (< 64 KiB)
    pub fn new_small_file(
        part_id: String, 
        node_id: String, 
        timestamp: String, 
        entry_type: String, 
        content: Vec<u8>
    ) -> Self {
        Self {
            part_id,
            node_id,
            timestamp,
            entry_type,
            content: Some(content),
            sha256: None,
        }
    }
    
    /// Create entry for large file (>= 64 KiB)
    pub fn new_large_file(
        part_id: String, 
        node_id: String, 
        timestamp: String,
        entry_type: String, 
        sha256: String
    ) -> Self {
        Self {
            part_id,
            node_id,
            timestamp,
            entry_type,
            content: None,
            sha256: Some(sha256),
        }
    }
    
    /// Check if this entry represents a large file
    pub fn is_large_file(&self) -> bool {
        self.content.is_none() && self.sha256.is_some()
    }
}
```

### Step 2: Large File Utilities and Hybrid Writer

**File**: `crates/tlogfs/src/large_files.rs` (new)

```rust
use sha2::{Sha256, Digest};
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::File;
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// Threshold for storing files separately: 64 KiB
pub const LARGE_FILE_THRESHOLD: usize = 64 * 1024;

/// Memory threshold for spilling to temp file: 1 MiB
const MEMORY_BUFFER_THRESHOLD: usize = 1024 * 1024;

/// Get large file path in pond directory
pub fn large_file_path(pond_path: &str, sha256: &str) -> PathBuf {
    PathBuf::from(pond_path)
        .join("_large_files")
        .join(format!("{}.data", sha256))
}

/// Check if content should be stored as large file
pub fn should_store_as_large_file(content: &[u8]) -> bool {
    content.len() >= LARGE_FILE_THRESHOLD
}

/// Result of hybrid writer finalization
pub struct HybridWriterResult {
    pub content: Vec<u8>,
    pub sha256: String,
    pub size: usize,
}

/// Hybrid writer that implements AsyncWrite with incremental hashing and spillover
pub struct HybridWriter {
    /// Memory buffer for small files
    memory_buffer: Option<Vec<u8>>,
    /// Temporary file for large files
    temp_file: Option<File>,
    /// Path to temporary file
    temp_path: Option<PathBuf>,
    /// Incremental SHA256 hasher
    hasher: Sha256,
    /// Total bytes written
    total_written: usize,
    /// Target pond directory for final file
    pond_path: String,
}

impl HybridWriter {
    pub fn new(pond_path: String) -> Self {
        Self {
            memory_buffer: Some(Vec::new()),
            temp_file: None,
            temp_path: None,
            hasher: Sha256::new(),
            total_written: 0,
            pond_path,
        }
    }
    
    /// Finalize the writer and return content strategy decision
    pub async fn finalize(mut self) -> std::io::Result<HybridWriterResult> {
        // Finalize hash computation
        let sha256 = format!("{:x}", self.hasher.finalize());
        
        let content = if let Some(buffer) = self.memory_buffer {
            // Small file: return memory buffer
            buffer
        } else if let Some(temp_path) = self.temp_path {
            // Large file: move temp file to final location with content-addressed name
            
            // Ensure _large_files directory exists
            let large_files_dir = PathBuf::from(&self.pond_path).join("_large_files");
            tokio::fs::create_dir_all(&large_files_dir).await?;
            
            // Final content-addressed path
            let final_path = large_file_path(&self.pond_path, &sha256);
            
            // Atomic rename: temp file -> final content-addressed file
            tokio::fs::rename(&temp_path, &final_path).await?;
            
            // Return empty Vec to indicate large file (content stored externally)
            Vec::new()
        } else {
            Vec::new()
        };
        
        Ok(HybridWriterResult {
            content,
            sha256,
            size: self.total_written,
        })
    }
    
    /// Spill memory buffer to temporary file
    async fn spill_to_temp_file(&mut self) -> std::io::Result<()> {
        if self.temp_file.is_some() {
            return Ok(()); // Already spilled
        }
        
        // Create temporary file with uuid7 name in pond directory
        let temp_dir = PathBuf::from(&self.pond_path);
        let temp_filename = format!("temp_{}.data", uuid7::uuid7());
        let temp_path = temp_dir.join(temp_filename);
        
        // Create async File
        let mut async_file = File::create(&temp_path).await?;
        
        // Write existing buffer to temp file
        if let Some(buffer) = self.memory_buffer.take() {
            async_file.write_all(&buffer).await?;
            async_file.flush().await?;
        }
        
        self.temp_file = Some(async_file);
        self.temp_path = Some(temp_path);
        
        Ok(())
    }
}
}

impl AsyncWrite for HybridWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let this = &mut *self;
        
        // Update incremental hash
        this.hasher.update(buf);
        this.total_written += buf.len();
        
        // Check if we need to spill to temp file
        if this.memory_buffer.is_some() && 
           this.total_written > MEMORY_BUFFER_THRESHOLD {
            
            // Need to spill - use async block with polling
            let spill_future = this.spill_to_temp_file();
            tokio::pin!(spill_future);
            
            match spill_future.poll(cx) {
                Poll::Ready(Ok(())) => {
                    // Successfully spilled, continue with file write below
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
        
        // Write to appropriate destination
        if let Some(ref mut buffer) = this.memory_buffer {
            // Still in memory mode
            buffer.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        } else if let Some(ref mut temp_file) = this.temp_file {
            // In temp file mode
            Pin::new(temp_file).poll_write(cx, buf)
        } else {
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Writer in invalid state"
            )))
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        if let Some(ref mut temp_file) = self.temp_file {
            Pin::new(temp_file).poll_flush(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        if let Some(ref mut temp_file) = self.temp_file {
            Pin::new(temp_file).poll_shutdown(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }
}
```

### Step 3: Update Persistence Layer for Hybrid Writing

**File**: `crates/tlogfs/src/delta_table_manager.rs`

```rust
use crate::large_files::*;

impl DeltaTableManager {
    /// Create a hybrid writer for streaming file content
    pub fn create_hybrid_writer(&self) -> HybridWriter {
        HybridWriter::new(self.table_path.clone())
    }
    
    /// Store file content from hybrid writer result
    pub async fn store_file_from_hybrid_writer(
        &mut self, 
        node_id: &str, 
        part_id: &str, 
        result: HybridWriterResult
    ) -> Result<(), TLogFSError> {
        if result.size >= LARGE_FILE_THRESHOLD {
            // Large file: content already stored, just create OplogEntry with SHA256
            let entry = OplogEntry::new_large_file(
                part_id.to_string(),
                node_id.to_string(),
                self.generate_timestamp(),
                "FileData".to_string(),
                result.sha256,
            );
            
            self.append_oplog_entry(entry).await
        } else {
            // Small file: store content directly in Delta Lake
            let entry = OplogEntry::new_small_file(
                part_id.to_string(),
                node_id.to_string(),
                self.generate_timestamp(),
                "FileData".to_string(),
                result.content,
            );
            
            self.append_oplog_entry(entry).await
        }
    }
    
    /// Store file content using size-based strategy (legacy method)
    pub async fn store_file_content(
        &mut self, 
        node_id: &str, 
        part_id: &str, 
        content: &[u8]
    ) -> Result<(), TLogFSError> {
        if should_store_as_large_file(content) {
            self.store_large_file(node_id, part_id, content).await
        } else {
            self.store_small_file(node_id, part_id, content).await
        }
    }
    
    /// Store large file directly (for backward compatibility)
    async fn store_large_file(
        &mut self, 
        node_id: &str, 
        part_id: &str, 
        content: &[u8]
    ) -> Result<(), TLogFSError> {
        // Use hybrid writer for consistency
        let mut writer = self.create_hybrid_writer();
        
        use tokio::io::AsyncWriteExt;
        writer.write_all(content).await?;
        writer.shutdown().await?;
        
        let result = writer.finalize().await?;
        self.store_file_from_hybrid_writer(node_id, part_id, result).await
    }
    
    /// Store small file directly in Delta Lake
    async fn store_small_file(
        &mut self, 
        node_id: &str, 
        part_id: &str, 
        content: &[u8]
    ) -> Result<(), TLogFSError> {
        let entry = OplogEntry::new_small_file(
            part_id.to_string(),
            node_id.to_string(),
            self.generate_timestamp(),
            "FileData".to_string(),
            content.to_vec(),
        );
        
        self.append_oplog_entry(entry).await
    }
    
    /// Load file content using size-based strategy
    pub async fn load_file_content(
        &self, 
        node_id: &str, 
        part_id: &str
    ) -> Result<Vec<u8>, TLogFSError> {
        let entry = self.load_latest_oplog_entry(node_id, part_id).await?;
        
        if entry.is_large_file() {
            // Large file: read from separate storage
            let sha256 = entry.sha256.unwrap();
            let large_file_path = large_file_path(&self.table_path, &sha256);
            
            tokio::fs::read(&large_file_path).await
                .map_err(|e| TLogFSError::LargeFileNotFound {
                    sha256,
                    path: large_file_path.display().to_string(),
                    source: e,
                })
        } else {
            // Small file: content stored inline
            Ok(entry.content.unwrap_or_default())
        }
    }
}
```

### Step 4: Error Handling

**File**: `crates/tlogfs/src/error.rs`

```rust
#[derive(Debug, thiserror::Error)]
pub enum TLogFSError {
    // ...existing errors...
    
    #[error("Large file not found: {sha256} at path {path}")]
    LargeFileNotFound {
        sha256: String,
        path: String,
        #[source]
        source: std::io::Error,
    },
    
    #[error("Large file integrity check failed: expected {expected}, got {actual}")]
    LargeFileIntegrityError {
        expected: String,
        actual: String,
    },
}
```

### Step 5: Testing with Hybrid Writer

**File**: `crates/tlogfs/src/tests/large_files_tests.rs` (new)

```rust
use super::*;
use crate::large_files::*;
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn test_hybrid_writer_small_file() {
    let mut manager = create_test_manager().await;
    
    // Small file (< 64 KiB) via hybrid writer
    let content = vec![42u8; 1024]; // 1 KiB
    
    let mut writer = manager.create_hybrid_writer();
    writer.write_all(&content).await.unwrap();
    writer.shutdown().await.unwrap();
    
    let result = writer.finalize().await.unwrap();
    
    // Should be small file (content in memory)
    assert_eq!(result.content, content);
    assert_eq!(result.size, 1024);
    assert!(result.size < LARGE_FILE_THRESHOLD);
    
    // Store via hybrid result
    manager.store_file_from_hybrid_writer("node1", "part1", result).await.unwrap();
    
    // Verify stored inline
    let entry = manager.load_latest_oplog_entry("node1", "part1").await.unwrap();
    assert!(!entry.is_large_file());
    assert_eq!(entry.content.unwrap(), content);
    assert!(entry.sha256.is_none());
}

#[tokio::test]
async fn test_hybrid_writer_large_file() {
    let mut manager = create_test_manager().await;
    
    // Large file (>= 64 KiB) via hybrid writer
    let content = vec![42u8; 65536]; // 64 KiB
    
    let mut writer = manager.create_hybrid_writer();
    writer.write_all(&content).await.unwrap();
    writer.shutdown().await.unwrap();
    
    let result = writer.finalize().await.unwrap();
    
    // Should be large file (empty content, file stored externally)
    assert!(result.content.is_empty());
    assert_eq!(result.size, 65536);
    assert!(result.size >= LARGE_FILE_THRESHOLD);
    
    // Verify large file exists at content-addressed path
    let large_file_path = large_file_path(&manager.table_path, &result.sha256);
    assert!(tokio::fs::metadata(&large_file_path).await.is_ok());
    
    // Store via hybrid result
    manager.store_file_from_hybrid_writer("node1", "part1", result.clone()).await.unwrap();
    
    // Verify stored as large file
    let entry = manager.load_latest_oplog_entry("node1", "part1").await.unwrap();
    assert!(entry.is_large_file());
    assert!(entry.content.is_none());
    assert_eq!(entry.sha256.unwrap(), result.sha256);
    
    // Verify retrieval
    let loaded = manager.load_file_content("node1", "part1").await.unwrap();
    assert_eq!(loaded, content);
}

#[tokio::test]
async fn test_hybrid_writer_incremental_hash() {
    let mut manager = create_test_manager().await;
    
    // Write content in chunks to test incremental hashing
    let chunk1 = vec![1u8; 32768]; // 32 KiB
    let chunk2 = vec![2u8; 32768]; // 32 KiB
    let total_content = [chunk1.clone(), chunk2.clone()].concat();
    
    let mut writer = manager.create_hybrid_writer();
    
    // Write in chunks
    writer.write_all(&chunk1).await.unwrap();
    writer.write_all(&chunk2).await.unwrap();
    writer.shutdown().await.unwrap();
    
    let result = writer.finalize().await.unwrap();
    
    // Verify hash matches content written in chunks
    use sha2::{Sha256, Digest};
    let mut expected_hasher = Sha256::new();
    expected_hasher.update(&total_content);
    let expected_hash = format!("{:x}", expected_hasher.finalize());
    
    assert_eq!(result.sha256, expected_hash);
    assert_eq!(result.size, 65536);
    
    // Verify file stored correctly
    let large_file_path = large_file_path(&manager.table_path, &result.sha256);
    let stored_content = tokio::fs::read(&large_file_path).await.unwrap();
    assert_eq!(stored_content, total_content);
}

#[tokio::test]
async fn test_hybrid_writer_spillover() {
    let mut manager = create_test_manager().await;
    
    // Write more than memory threshold to test spillover
    let large_content = vec![42u8; 2 * 1024 * 1024]; // 2 MiB
    
    let mut writer = manager.create_hybrid_writer();
    
    // Write in chunks to trigger spillover
    let chunk_size = 256 * 1024; // 256 KiB chunks
    for chunk in large_content.chunks(chunk_size) {
        writer.write_all(chunk).await.unwrap();
    }
    writer.shutdown().await.unwrap();
    
    let result = writer.finalize().await.unwrap();
    
    // Should be large file
    assert!(result.content.is_empty());
    assert_eq!(result.size, large_content.len());
    
    // Verify file stored correctly
    let large_file_path = large_file_path(&manager.table_path, &result.sha256);
    let stored_content = tokio::fs::read(&large_file_path).await.unwrap();
    assert_eq!(stored_content, large_content);
}

#[tokio::test]
async fn test_hybrid_writer_deduplication() {
    let mut manager = create_test_manager().await;
    
    // Create two identical large files via hybrid writer
    let content = vec![42u8; 65536]; // 64 KiB
    
    // First file
    let mut writer1 = manager.create_hybrid_writer();
    writer1.write_all(&content).await.unwrap();
    writer1.shutdown().await.unwrap();
    let result1 = writer1.finalize().await.unwrap();
    
    // Second file with same content
    let mut writer2 = manager.create_hybrid_writer();
    writer2.write_all(&content).await.unwrap();
    writer2.shutdown().await.unwrap();
    let result2 = writer2.finalize().await.unwrap();
    
    // Should have same hash (deduplication)
    assert_eq!(result1.sha256, result2.sha256);
    
    // Both files should exist at same path (second overwrites first, but content is identical)
    let large_file_path = large_file_path(&manager.table_path, &result1.sha256);
    assert!(tokio::fs::metadata(&large_file_path).await.is_ok());
    
    // Store both entries
    manager.store_file_from_hybrid_writer("node1", "part1", result1).await.unwrap();
    manager.store_file_from_hybrid_writer("node2", "part2", result2).await.unwrap();
    
    // Verify both entries read the same data
    let data1 = manager.load_file_content("node1", "part1").await.unwrap();
    let data2 = manager.load_file_content("node2", "part2").await.unwrap();
    assert_eq!(data1, data2);
    assert_eq!(data1, content);
}

// Legacy tests for backward compatibility
#[tokio::test]
async fn test_small_file_storage() {
    let mut manager = create_test_manager().await;
    
    // Small file (< 64 KiB)
    let content = vec![42u8; 1024]; // 1 KiB
    
    manager.store_file_content("node1", "part1", &content).await.unwrap();
    
    // Verify stored inline
    let entry = manager.load_latest_oplog_entry("node1", "part1").await.unwrap();
    assert!(!entry.is_large_file());
    assert_eq!(entry.content.unwrap(), content);
    assert!(entry.sha256.is_none());
    
    // Verify retrieval
    let loaded = manager.load_file_content("node1", "part1").await.unwrap();
    assert_eq!(loaded, content);
}

#[tokio::test]
async fn test_large_file_storage() {
    let mut manager = create_test_manager().await;
    
    // Large file (>= 64 KiB)
    let content = vec![42u8; 65536]; // 64 KiB
    
    manager.store_file_content("node1", "part1", &content).await.unwrap();
    
    // Verify stored as large file
    let entry = manager.load_latest_oplog_entry("node1", "part1").await.unwrap();
    assert!(entry.is_large_file());
    assert!(entry.content.is_none());
    assert!(entry.sha256.is_some());
    
    // Verify large file exists
    let sha256 = entry.sha256.unwrap();
    let large_file_path = large_file_path(&manager.table_path, &sha256);
    assert!(tokio::fs::metadata(&large_file_path).await.is_ok());
    
    // Verify retrieval
    let loaded = manager.load_file_content("node1", "part1").await.unwrap();
    assert_eq!(loaded, content);
}

#[tokio::test]
async fn test_threshold_boundary() {
    let mut manager = create_test_manager().await;
    
    // Just under threshold
    let small_content = vec![42u8; LARGE_FILE_THRESHOLD - 1];
    manager.store_file_content("node1", "part1", &small_content).await.unwrap();
    
    let entry1 = manager.load_latest_oplog_entry("node1", "part1").await.unwrap();
    assert!(!entry1.is_large_file());
    
    // Just at threshold
    let large_content = vec![42u8; LARGE_FILE_THRESHOLD];
    manager.store_file_content("node2", "part2", &large_content).await.unwrap();
    
    let entry2 = manager.load_latest_oplog_entry("node2", "part2").await.unwrap();
    assert!(entry2.is_large_file());
}
```

## Directory Structure

After implementation, pond directories will have:

```
my_pond/
├── _delta_log/                 # Delta Lake transaction log
│   ├── 00000000000000000000.json
│   └── 00000000000000000001.json
├── _large_files/               # Large file storage (64 KiB+)
│   ├── a1b2c3d4e5f6...abc.data # Content-addressed files
│   ├── f6e5d4c3b2a1...def.data # Flat directory structure
│   └── 9876543210ab...123.data
└── part-00000-*.parquet        # Delta Lake data files
```

## Implementation Checklist

### 🎯 **Phase 1: Core Large File Storage**
- [ ] Update OplogEntry schema with optional content and sha256 fields
- [ ] Create large_files.rs module with utilities and constants
- [ ] Update DeltaTableManager with size-based storage logic
- [ ] Add LargeFileNotFound and integrity error types
- [ ] Test small file storage (< 64 KiB)
- [ ] Test large file storage (>= 64 KiB) 
- [ ] Test content deduplication
- [ ] Test threshold boundary conditions

### 📋 **Phase 2: Integration & Polish**
- [ ] Update existing file operations to use new storage strategy
- [ ] Verify all existing tests still pass
- [ ] Add garbage collection for unreferenced large files
- [ ] Add integrity verification on large file reads
- [ ] Performance testing with large Parquet files
- [ ] Update memory bank documentation

## Next Steps

1. **Start with Schema Update**: Update OplogEntry in `crates/tlogfs/src/lib.rs`
2. **Add Utilities**: Create `large_files.rs` module 
3. **Update Storage Logic**: Modify DeltaTableManager
4. **Test Incrementally**: Add tests for each component
5. **Integration Testing**: Ensure existing functionality works

This approach maintains the clean foundation from Phase 1 while adding the large file capability you specified.
