# Metadata Consolidation Plan for TLogFS/TinyFS System

**Created**: July 19, 2025  
**Status**: Planning Phase  
**Priority**: High - Foundation for improved metadata access patterns

## Overview

Consolidate the currently inconsistent metadata approach in the tlogfs/tinyfs system by creating a structured metadata API that replaces the generic `metadata_u64(&self, name: &str)` method with a comprehensive metadata struct.

## Current State Analysis

### Existing Metadata Types

1. **Node Version**: Available via `metadata_u64("version")` - returns `record.version as u64`
2. **File Size**: Currently implicitly calculated from content or large-file store
3. **File Entry Type**: Stored as `file_type: tinyfs::EntryType` in `OplogEntry`  
4. **SHA256 Value**: Currently only present for large files (`sha256: Option<String>`)

### Current Issues

- **Inconsistent access**: Generic string-based API is not type-safe
- **Missing size metadata**: File sizes must be derived from filesystem or content length
- **SHA256 logic coupling**: Large file detection tied to SHA256 presence instead of content absence
- **No unified metadata view**: Four different access patterns for related metadata

## Proposed Solution

### 1. Consolidated Metadata Struct

```rust
/// Consolidated metadata for filesystem nodes
#[derive(Debug, Clone, PartialEq)]
pub struct NodeMetadata {
    /// Node version (incremented on each modification)
    pub version: u64,
    
    /// File size in bytes (None for directories and symlinks)
    pub size: Option<u64>,
    
    /// SHA256 checksum (Some for all files, None for directories/symlinks)
    pub sha256: Option<String>,
    
    /// Entry type (file, directory, symlink with file format variants)
    pub entry_type: tinyfs::EntryType,
}
```

### 2. Enhanced OplogEntry Schema

**Changes to `crates/tlogfs/src/schema.rs`**:

#### Add Size Field
```rust
pub struct OplogEntry {
    // ... existing fields ...
    pub sha256: Option<String>,
    /// File size in bytes (Some() for all files, None for directories/symlinks)
    pub size: Option<u64>,  // NEW FIELD
}
```

#### Update Arrow Schema
```rust
impl ForArrow for OplogEntry {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            // ... existing fields ...
            Arc::new(Field::new("sha256", DataType::Utf8, true)),
            Arc::new(Field::new("size", DataType::UInt64, true)), // NEW FIELD
        ]
    }
}
```

#### Update Constructor Methods
```rust
impl OplogEntry {
    /// Create entry for small file with size
    pub fn new_small_file(
        part_id: String, 
        node_id: String, 
        file_type: tinyfs::EntryType,
        timestamp: i64,
        version: i64,
        content: Vec<u8>
    ) -> Self {
        let size = content.len() as u64;
        Self {
            part_id,
            node_id,
            file_type,
            timestamp,
            version,
            content: Some(content),
            sha256: Some(compute_sha256(&content)), // NEW: Always compute SHA256
            size: Some(size), // NEW: Store size explicitly
        }
    }
    
    /// Create entry for large file with size
    pub fn new_large_file(
        part_id: String, 
        node_id: String, 
        file_type: tinyfs::EntryType,
        timestamp: i64,
        version: i64,
        sha256: String,
        size: u64  // NEW PARAMETER
    ) -> Self {
        Self {
            part_id,
            node_id,
            file_type,
            timestamp,
            version,
            content: None,
            sha256: Some(sha256),
            size: Some(size), // NEW: Store size explicitly
        }
    }
}
```

#### Update Large File Detection Logic
```rust
impl OplogEntry {
    /// Check if this entry represents a large file (based on content absence)
    pub fn is_large_file(&self) -> bool {
        self.content.is_none() && self.file_type.is_file()
    }
    
    /// Get file size (guaranteed for files, None for directories/symlinks)
    pub fn file_size(&self) -> Option<u64> {
        self.size
    }
    
    /// Extract consolidated metadata
    pub fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            version: self.version as u64,
            size: self.size,
            sha256: self.sha256.clone(),
            entry_type: self.file_type,
        }
    }
}
```

### 3. Enhanced Large File Naming Convention

**Changes to `crates/tlogfs/src/large_files.rs`**:

#### New Filename Format
```rust
/// Generate large file path with size in filename
/// Format: sha256=<hash>_size=<bytes>.file
pub fn large_file_path_with_size(pond_path: &str, sha256: &str, size: u64) -> PathBuf {
    PathBuf::from(pond_path)
        .join("_large_files")
        .join(format!("sha256={}_size={}.file", sha256, size))
}

/// Parse size from large file filename
pub fn parse_size_from_filename(filename: &str) -> Option<u64> {
    // Extract size from: sha256=<hash>_size=<bytes>.file
    let parts: Vec<&str> = filename.split('_').collect();
    if parts.len() >= 2 {
        if let Some(size_part) = parts.iter().find(|p| p.starts_with("size=")) {
            if let Some(size_str) = size_part.strip_prefix("size=") {
                if let Some(size_only) = size_str.strip_suffix(".file") {
                    return size_only.parse().ok();
                }
            }
        }
    }
    None
}

/// Validate large file consistency without reading content
pub async fn validate_large_file_size(file_path: &Path) -> Result<bool, std::io::Error> {
    let filename = file_path.file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid filename"))?;
    
    if let Some(expected_size) = parse_size_from_filename(filename) {
        let actual_size = tokio::fs::metadata(file_path).await?.len();
        Ok(actual_size == expected_size)
    } else {
        Ok(false) // Cannot validate without size in filename
    }
}
```

### 4. Updated TinyFS Metadata API

**Changes to `crates/tinyfs/src/metadata.rs`**:

#### New Metadata Trait
```rust
use crate::EntryType;

/// Consolidated metadata for filesystem nodes
#[derive(Debug, Clone, PartialEq)]
pub struct NodeMetadata {
    /// Node version (incremented on each modification)
    pub version: u64,
    
    /// File size in bytes (None for directories and symlinks)
    pub size: Option<u64>,
    
    /// SHA256 checksum (Some for all files, None for directories/symlinks)  
    pub sha256: Option<String>,
    
    /// Entry type (file, directory, symlink with file format variants)
    pub entry_type: EntryType,
}

#[async_trait]
pub trait Metadata: Send + Sync {
    /// Get consolidated metadata for this node
    async fn metadata(&self) -> Result<NodeMetadata>;
    
    /// Get a u64 metadata value by name (legacy compatibility)
    /// 
    /// Supported names: "version", "size"
    async fn metadata_u64(&self, name: &str) -> Result<Option<u64>> {
        let metadata = self.metadata().await?;
        match name {
            "version" => Ok(Some(metadata.version)),
            "size" => Ok(metadata.size),
            _ => Ok(None),
        }
    }
}
```

### 5. Implementation Updates

#### TLogFS Persistence Layer
**Changes to `crates/tlogfs/src/persistence.rs`**:

```rust
impl OpLogPersistence {
    /// Store file content with size tracking
    async fn store_file_content_with_type(
        &self, 
        node_id: NodeID, 
        part_id: NodeID, 
        content: &[u8],
        entry_type: tinyfs::EntryType
    ) -> Result<(), TLogFSError> {
        let content_len = content.len();
        let sha256 = compute_sha256(content);
        
        if should_store_as_large_file(content) {
            // Store as large file with size in filename
            let large_file_path = large_file_path_with_size(&self.store_path, &sha256, content_len as u64);
            self.store_large_file_at_path(&large_file_path, content).await?;
            
            // Create large file entry with size
            let entry = OplogEntry::new_large_file(
                part_id.to_hex_string(),
                node_id.to_hex_string(),
                entry_type,
                Utc::now().timestamp_micros(),
                1,
                sha256,
                content_len as u64, // NEW: Include size
            );
            
            self.pending_records.lock().await.push(entry);
        } else {
            // Store as small file with size and SHA256
            let entry = OplogEntry::new_small_file(
                part_id.to_hex_string(),
                node_id.to_hex_string(),
                entry_type,
                Utc::now().timestamp_micros(),
                1,
                content.to_vec(),
            );
            
            self.pending_records.lock().await.push(entry);
        }
        Ok(())
    }
}
```

#### SHA256 Computation for All Files
```rust
use sha2::{Sha256, Digest};

/// Compute SHA256 for any content (small or large files)
pub fn compute_sha256(content: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content);
    format!("{:x}", hasher.finalize())
}
```

#### Memory Implementation Updates
**Changes to `crates/tinyfs/src/memory_persistence.rs`**:

```rust
#[async_trait]
impl Metadata for MemoryFile {
    async fn metadata(&self) -> Result<NodeMetadata> {
        let content = self.content.lock().await;
        let size = content.len() as u64;
        let sha256 = compute_sha256(&content);
        
        Ok(NodeMetadata {
            version: 1, // Memory files don't track versions
            size: Some(size),
            sha256: Some(sha256),
            entry_type: EntryType::FileData, // Default for memory files
        })
    }
}
```

## Implementation Strategy

### Phase 1: Schema Updates (Foundation)
1. **Add size field to OplogEntry**
   - Update struct definition
   - Update Arrow schema
   - Update constructor methods
   - Add metadata extraction method

2. **Update large file naming convention**
   - Implement size-embedded filenames
   - Add parsing and validation utilities
   - Update path generation functions

3. **Add NodeMetadata struct to TinyFS**
   - Define consolidated metadata struct
   - Update Metadata trait with new method
   - Maintain backward compatibility with metadata_u64

### Phase 2: Persistence Layer Updates
1. **Update TLogFS implementation**
   - Modify file storage to always compute SHA256
   - Update large file creation with size
   - Implement metadata extraction from OplogEntry

2. **Update memory implementation**  
   - Add on-the-fly SHA256 computation
   - Implement NodeMetadata extraction

### Phase 3: Large File Management
1. **Migrate existing large files**
   - Scan existing `_large_files` directory
   - Read file sizes and rename with new format
   - Update consistency checking tools

2. **Enhanced validation tools**
   - Implement size-based consistency checks
   - Add large file orphan detection
   - Build repair utilities for size mismatches

### Phase 4: API Migration
1. **Update all metadata access**
   - Replace metadata_u64 calls with metadata() where beneficial
   - Add type-safe size access patterns
   - Implement efficient SHA256 caching

2. **Testing and validation**
   - Test metadata consistency across implementations  
   - Validate large file handling with size tracking
   - Performance test metadata access patterns

## Benefits

### Immediate Benefits
- **Type-safe metadata access**: No more string-based field lookup
- **Efficient size access**: No filesystem calls needed for file sizes
- **Consistent SHA256 availability**: All files have checksums, not just large ones
- **Self-documenting large files**: Size visible in filename for debugging

### Long-term Benefits  
- **Better consistency checking**: Validate large file sizes without content reads
- **Improved debugging**: File sizes visible in directory listings
- **Foundation for caching**: Structured metadata enables intelligent caching
- **Cleaner API evolution**: Structured approach supports future metadata additions

## Risks and Mitigations

### Schema Evolution Risk
- **Risk**: Adding fields to OplogEntry requires Delta Lake schema evolution
- **Mitigation**: Nullable fields ensure backward compatibility; existing records work unchanged

### Large File Migration Risk  
- **Risk**: Renaming large files could break references during migration
- **Mitigation**: Support both old and new naming conventions during transition period

### Performance Impact
- **Risk**: Computing SHA256 for small files adds overhead
- **Mitigation**: SHA256 computation is fast for small files; benefits outweigh costs

### Memory Usage
- **Risk**: Storing size redundantly for small files increases memory usage
- **Mitigation**: u64 size field adds minimal overhead compared to content storage

## Success Metrics

1. **API Consistency**: All metadata accessible through unified interface
2. **Performance**: No regression in file access performance  
3. **Reliability**: Size mismatches detectable without content validation
4. **Maintainability**: Reduced complexity in metadata handling code

## Next Steps

1. **Review and approval** of this plan
2. **Create implementation tasks** for each phase
3. **Begin with Phase 1**: Schema updates and foundation changes
4. **Incremental testing** after each phase completion

This plan provides a comprehensive approach to consolidating metadata while maintaining system performance and reliability.
