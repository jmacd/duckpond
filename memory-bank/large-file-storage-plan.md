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

### Step 2: Large File Utilities

**File**: `crates/tlogfs/src/large_files.rs` (new)

```rust
use sha2::{Sha256, Digest};
use std::path::PathBuf;

/// Threshold for storing files separately: 64 KiB
pub const LARGE_FILE_THRESHOLD: usize = 64 * 1024;

/// Compute SHA256 hash of content
pub fn compute_content_hash(content: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content);
    format!("{:x}", hasher.finalize())
}

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
```

### Step 3: Update Persistence Layer

**File**: `crates/tlogfs/src/delta_table_manager.rs`

```rust
impl DeltaTableManager {
    /// Store file content using size-based strategy
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
    
    /// Store large file externally with SHA256 reference
    async fn store_large_file(
        &mut self, 
        node_id: &str, 
        part_id: &str, 
        content: &[u8]
    ) -> Result<(), TLogFSError> {
        // 1. Compute SHA256 hash
        let sha256 = compute_content_hash(content);
        
        // 2. Store large file in pond directory
        let large_file_path = large_file_path(&self.table_path, &sha256);
        tokio::fs::create_dir_all(large_file_path.parent().unwrap()).await?;
        
        // 3. Atomic write: use temp file then rename
        let temp_path = format!("{}.tmp", large_file_path.display());
        tokio::fs::write(&temp_path, content).await?;
        tokio::fs::rename(&temp_path, &large_file_path).await?;
        
        // 4. Create OplogEntry with SHA256 reference
        let entry = OplogEntry::new_large_file(
            part_id.to_string(),
            node_id.to_string(),
            self.generate_timestamp(),
            "FileData".to_string(),
            sha256,
        );
        
        // 5. Append to Delta Lake
        self.append_oplog_entry(entry).await
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

### Step 5: Testing

**File**: `crates/tlogfs/src/tests/large_files_tests.rs` (new)

```rust
use super::*;
use crate::large_files::*;

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
async fn test_content_deduplication() {
    let mut manager = create_test_manager().await;
    
    // Create the same large content twice
    let content = vec![42u8; 65536]; // 64 KiB
    
    manager.store_file_content("node1", "part1", &content).await.unwrap();
    manager.store_file_content("node2", "part2", &content).await.unwrap();
    
    // Verify only one large file was created (deduplication)
    let large_files_dir = PathBuf::from(&manager.table_path).join("_large_files");
    let mut entries = tokio::fs::read_dir(&large_files_dir).await.unwrap();
    
    let mut file_count = 0;
    while entries.next_entry().await.unwrap().is_some() {
        file_count += 1;
    }
    assert_eq!(file_count, 1); // Only one actual large file
    
    // Verify both entries read the same data
    let data1 = manager.load_file_content("node1", "part1").await.unwrap();
    let data2 = manager.load_file_content("node2", "part2").await.unwrap();
    assert_eq!(data1, data2);
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
