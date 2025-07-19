# Arrow Record Batch Implementation Plan for Table and Series Files

## Overview

This document outlines the implementation plan for adding Arrow Record Batch support to DuckPond's TLogFS for `FileTable` and `FileSeries` entry types. These files will store data as Parquet format while maintaining TinyFS as a byte-oriented abstraction.

**Updated**: July 18, 2025 - Reflects oplog/tlogfs merge completion and large file storage strategy

## Design Principles

1. **TinyFS remains byte-oriented** - No Arrow types in TinyFS core interfaces
2. **Files are byte containers** - Table/Series files contain Parquet bytes
3. **Streaming support** - Use standard AsyncRead/AsyncWrite traits
4. **Large file handling** - External storage with 64 KiB threshold
5. **Keep TinyFS focused** - Arrow convenience methods are extensions, not core features
6. **Clean foundation** - Built on the unified tlogfs crate post-merge

## Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   User Code     │    │  WD Interface    │    │  TinyFS Core    │
│                 │    │  (Arrow Ext)     │    │  (Bytes Only)   │
│ RecordBatch     │───▶│ create_table_*   │───▶│ create_file_*   │
│ Stream<Batch>   │    │ read_series_*    │    │ read_to_vec     │
│                 │    │                  │    │ async_reader    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                 │
                                 ▼
                       ┌──────────────────┐
                       │  TLogFS Backend  │
                       │   (Unified)      │
                       │ Small: DeltaLake │
                       │ Large: Separate  │
                       │  Files (64KiB+)  │
                       └──────────────────┘
```

## Current System State

### ✅ **Phase 2 Complete**: OpLog/TLogFS Merge (July 18, 2025)

The oplog and tlogfs crates have been successfully merged into a unified `tlogfs` crate:

- **Eliminated double nesting**: OplogEntry now stores content directly
- **Unified error handling**: Single TLogFSError hierarchy  
- **Clean imports**: `use tlogfs::{Record, DeltaTableManager}` instead of separate crates
- **All tests passing**: 113 tests across all crates with zero warnings
- **Clean foundation**: Ready for Arrow integration and large file storage

## Current State Summary & Updated Plan

### ✅ **What's Complete**
1. **Phase 1: Core TinyFS Streaming Support** - Complete with async read/write and write protection
2. **Phase 2: Large File Storage** - Complete with content-addressed storage, 64 KiB threshold, hierarchical directories
3. **OpLog/TLogFS Merge** - Complete with unified crate and clean error handling
4. **EntryType System** - Complete with FileTable/FileSeries/FileData variants
5. **ForArrow Trait** - Complete in tlogfs crate for schema definitions

### 🎯 **Ready to Implement: Phase 3 Arrow Integration**

**Updated Approach Based on Your Preferences**:

1. **Arrow Module Structure**: `crates/tinyfs/src/arrow/` - keeps dependencies flowing correctly
2. **ForArrow Trait Migration**: Move `ForArrow` trait from tlogfs to `tinyfs/src/arrow/schema.rs`
3. **Parquet Support**: Add parquet functionality in `tinyfs/src/arrow/parquet.rs`
4. **FileTable Focus**: Set `EntryType::FileTable`, defer FileSeries for later
5. **1000-Row Batches**: Default batch size for memory efficiency
6. **Two-Level API**:
   - **Low-level**: Direct `RecordBatch` operations
   - **High-level**: `ForArrow` collections with automatic batching

### 📋 **Implementation Plan**

#### **Next Priority: Arrow Integration**
```rust
// ForArrow trait in tinyfs/src/arrow/schema.rs
pub trait ForArrow {
    fn for_arrow() -> Vec<FieldRef>;
    fn for_delta() -> Vec<DeltaStructField> { /* default impl */ }
}

// Low-level API in tinyfs/src/arrow/parquet.rs
trait ParquetExt {
    async fn create_table_from_batch(&self, path, batch: &RecordBatch) -> Result<()>;
    async fn read_table_as_batch(&self, path) -> Result<RecordBatch>;
}

// High-level API  
impl ParquetExt {
    async fn create_table_from_items<T: ForArrow + Serialize>(&self, path, items: Vec<T>) -> Result<()>;
    async fn read_table_as_items<T: ForArrow + DeserializeOwned>(&self, path) -> Result<Vec<T>>;
}
```

#### **Key Design Decisions Confirmed**:
- ✅ **No TinyFS core changes** - arrow module approach maintains clean separation
- ✅ **ForArrow in tinyfs** - move trait from tlogfs to avoid circular dependency
- ✅ **FileTable only** - focus on single-table files, defer series for later  
- ✅ **1000-row batches** - sensible default for memory vs efficiency balance
- ✅ **Large file compatible** - automatically works with existing 64 KiB threshold system

## Next Steps: Phase 3 Implementation

## Implementation Phases

### Phase 1: Core TinyFS Streaming Support ✅ **COMPLETE** (July 14, 2025)

**STATUS**: Successfully implemented with comprehensive write protection

#### 1.1 ✅ Handle Architecture with Write Protection

The current implementation provides write-protected streaming through a clean Handle architecture:

```rust
/// File handle with integrated write protection
pub struct Handle {
    inner: Arc<tokio::sync::Mutex<Box<dyn File>>>,
    state: Arc<tokio::sync::RwLock<FileState>>, // Write protection
}

#[derive(Debug, Clone, PartialEq)]
enum FileState {
    Ready,   // Available for read/write operations
    Writing, // Being written via streaming - reads return error
}

impl Handle {
    /// Get an async reader with write protection
    pub async fn async_reader(&self) -> error::Result<Pin<Box<dyn AsyncRead + Send>>> {
        // Check if file is being written - fails with clear error
        let state = self.state.read().await;
        if *state == FileState::Writing {
            return Err(error::Error::Other("File is currently being written".to_string()));
        }
        drop(state);
        
        let file = self.inner.lock().await;
        file.async_reader().await
    }
    
    /// Get an async writer with exclusive write lock
    pub async fn async_writer(&self) -> error::Result<StreamingFileWriter> {
        // Acquire write lock - prevents concurrent reads/writes
        let mut state = self.state.write().await;
        if *state == FileState::Writing {
            return Err(error::Error::Other("File is already being written".to_string()));
        }
        *state = FileState::Writing;
        
        Ok(StreamingFileWriter::new(self.clone(), WriteGuard::new(self.state.clone())))
    }
}
```

#### 1.2 ✅ Simple Memory Buffering for Phase 1

**ACTUAL IMPLEMENTATION**: Simple memory buffering for Phase 1, with automatic write lock management:

```rust
/// Write-protected streaming writer with simple memory buffering
pub struct StreamingFileWriter {
    handle: Handle,
    buffer: Vec<u8>,                    // Simple memory buffer for Phase 1
    _write_guard: WriteGuard,           // Automatic write lock management
    write_result_rx: Option<oneshot::Receiver<Result<(), Error>>>,
}

impl AsyncWrite for StreamingFileWriter {
    fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, std::io::Error>> {
        // Simple: append to memory buffer
        self.buffer.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }
    
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        // Coordinate background write via tokio::spawn + oneshot channel
        // Write lock automatically released when WriteGuard is dropped
    }
}

/// Automatic write lock management
struct WriteGuard {
    state: Arc<tokio::sync::RwLock<FileState>>,
}

impl Drop for WriteGuard {
    fn drop(&mut self) {
        // Automatically reset state to Ready when writer is dropped
    }
}
```

#### 1.3 ✅ Comprehensive Testing

**COMPLETE**: 10 comprehensive tests validating streaming and protection:

- ✅ `test_async_reader_basic` - Basic streaming read functionality  
- ✅ `test_async_writer_basic` - Basic streaming write functionality
- ✅ `test_async_writer_memory_buffering` - Memory buffering verification
- ✅ `test_async_writer_large_data` - Large file handling (1MB+)
- ✅ `test_parquet_roundtrip_single_batch` - Arrow/Parquet integration
- ✅ `test_parquet_roundtrip_multiple_batches` - Complex Arrow workflows
- ✅ `test_memory_bounded_large_parquet` - Large Parquet file handling
- ✅ `test_concurrent_writers` - Concurrent operation safety
- ✅ `test_concurrent_read_write_protection` - **Write protection verification**
- ✅ `test_write_protection_with_completed_write` - **Lock lifecycle testing**

**Key Achievements**:
- **Write protection**: Reads blocked during writes with clear errors
- **Automatic lock management**: WriteGuard ensures cleanup even on panic/drop
- **Arrow integration**: Full Parquet roundtrip working via AsyncArrowWriter
- **Memory bounded**: Simple buffering strategy for Phase 1
- **Clean API**: Users just need to scope writers properly with `{ }` blocks

### Phase 2: Large File Storage Architecture 🎯 **READY TO IMPLEMENT** (Next)

**STATUS**: Ready to implement with new OplogEntry schema for 64 KiB+ files

#### 2.1 Updated OplogEntry Schema with Large File Support

**File**: `crates/tlogfs/src/schema.rs`

Update OplogEntry to support optional content and SHA256 checksums:

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
    pub fn new_small_file(part_id: String, node_id: String, timestamp: String, 
                         entry_type: String, content: Vec<u8>) -> Self {
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
    pub fn new_large_file(part_id: String, node_id: String, timestamp: String,
                         entry_type: String, sha256: String) -> Self {
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

#### 2.2 Large File Storage Constants and Utilities

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

#### 2.3 Updated TLogFS Persistence Layer

**File**: `crates/tlogfs/src/persistence.rs`

Update the persistence layer to handle large files:

```rust
impl OpLogPersistence {
    /// Store file content using size-based strategy
    async fn store_file_content_with_strategy(
        &self, 
        node_id: NodeID, 
        part_id: NodeID, 
        content: &[u8]
    ) -> TLogFSResult<()> {
        if should_store_as_large_file(content) {
            // Large file: store separately and save reference
            let sha256 = compute_content_hash(content);
            
            // Store large file in pond directory
            let large_file_path = large_file_path(&self.store_path, &sha256);
            tokio::fs::create_dir_all(large_file_path.parent().unwrap()).await?;
            
            // Atomic write: use temp file then rename
            let temp_path = format!("{}.tmp", large_file_path.display());
            tokio::fs::write(&temp_path, content).await?;
            tokio::fs::rename(&temp_path, &large_file_path).await?;
            
            // Create OplogEntry with SHA256 reference
            let entry = OplogEntry::new_large_file(
                part_id.to_string(),
                node_id.to_string(),
                self.generate_timestamp(),
                "FileData".to_string(),
                sha256,
            );
            
            self.append_oplog_entry(entry).await
        } else {
            // Small file: store directly in Delta Lake
            let entry = OplogEntry::new_small_file(
                part_id.to_string(),
                node_id.to_string(),
                self.generate_timestamp(),
                "FileData".to_string(),
                content.to_vec(),
            );
            
            self.append_oplog_entry(entry).await
        }
    }
    
    /// Load file content using size-based strategy
    async fn load_file_content_with_strategy(
        &self, 
        node_id: NodeID, 
        part_id: NodeID
    ) -> TLogFSResult<Vec<u8>> {
        let entry = self.load_latest_oplog_entry(node_id, part_id).await?;
        
        if entry.is_large_file() {
            // Large file: read from separate storage
            let sha256 = entry.sha256.unwrap();
            let large_file_path = large_file_path(&self.store_path, &sha256);
            
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
    
    /// Garbage collect unused large files
    pub async fn gc_large_files(&self) -> TLogFSResult<GCStats> {
        // Scan all OplogEntry records to find referenced SHA256 hashes
        let referenced_hashes = self.scan_referenced_large_files().await?;
        
        // Scan _large_files directory for actual files
        let large_files_dir = PathBuf::from(&self.store_path).join("_large_files");
        let existing_files = self.scan_large_files_directory(&large_files_dir).await?;
        
        // Delete unreferenced files
        let mut deleted_count = 0;
        let mut freed_bytes = 0;
        
        for file_path in existing_files {
            let file_name = file_path.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("");
            
            if !referenced_hashes.contains(file_name) {
                let metadata = tokio::fs::metadata(&file_path).await?;
                freed_bytes += metadata.len();
                
                tokio::fs::remove_file(&file_path).await?;
                deleted_count += 1;
            }
        }
        
        Ok(GCStats { deleted_count, freed_bytes })
    }
}

#[derive(Debug)]
pub struct GCStats {
    pub deleted_count: usize,
    pub freed_bytes: u64,
}
```

#### 2.4 Directory Structure

With the updated strategy, the pond directory structure becomes:

```
my_pond/
├── _delta_log/                 # Delta Lake transaction log
│   ├── 00000000000000000000.json
│   └── 00000000000000000001.json
├── _large_files/               # Large file storage (64 KiB+)
│   ├── a1b2c3d4e5f6.../data   # Content-addressed files
│   ├── f6e5d4c3b2a1.../data   # Flat directory structure
│   └── 9876543210ab.../data
└── part-00000-*.parquet        # Delta Lake data files
```

**Key Benefits**:
- **64 KiB threshold**: Balances Delta Lake efficiency vs external storage
- **Content addressing**: Automatic deduplication via SHA256
- **Flat directory**: Simple structure, no subdirectory complexity
- **Atomic operations**: Temp file + rename for consistency
- **Garbage collection**: Clean up unreferenced large files

### Phase 3: Arrow Integration Layer 🚀 **READY TO IMPLEMENT** (Next Priority)

**STATUS**: Ready to implement with Phase 1 streaming foundation complete

With Phase 1's streaming support complete, Arrow integration becomes straightforward using the existing AsyncRead/AsyncWrite infrastructure via a tinyfs sub-module.

#### 3.1 Arrow Module Structure with ForArrow Migration

**File**: `crates/tinyfs/src/arrow/mod.rs` (new arrow module)

Following TinyFS architectural principles, this will be a complete arrow module containing both schema and parquet functionality:

```rust
//! Arrow integration module for TinyFS
//! 
//! This module provides Arrow RecordBatch integration without modifying
//! the core TinyFS interfaces. Contains both schema definitions (ForArrow trait)
//! and parquet support to avoid circular dependencies.

pub mod schema;
pub mod parquet;

pub use schema::ForArrow;
pub use parquet::ParquetExt;
```

**File**: `crates/tinyfs/src/arrow/schema.rs`

Move ForArrow trait from tlogfs to avoid circular dependency:

```rust
//! Schema definitions for Arrow integration

use arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use std::sync::Arc;
use std::collections::HashMap;
use deltalake::kernel::{
    DataType as DeltaDataType, PrimitiveType, StructField as DeltaStructField,
};

/// Trait for converting data structures to Arrow and Delta Lake schemas
/// 
/// This trait was moved from tlogfs to tinyfs to avoid circular dependencies
/// while allowing both tinyfs arrow support and tlogfs persistence to use it.
pub trait ForArrow {
    /// Define the Arrow schema for this type
    fn for_arrow() -> Vec<FieldRef>;

    /// Default implementation that converts Arrow schema to Delta Lake schema
    /// This enables compatibility with Delta Lake storage in tlogfs
    fn for_delta() -> Vec<DeltaStructField> {
        let afs = Self::for_arrow();

        afs.into_iter()
            .map(|af| {
                let prim_type = match af.data_type() {
                    DataType::Timestamp(TimeUnit::Microsecond, _) => PrimitiveType::Timestamp,
                    DataType::Utf8 => PrimitiveType::String,
                    DataType::Binary => PrimitiveType::Binary,
                    DataType::Int64 => PrimitiveType::Long,
                    _ => panic!("configure this type: {:?}", af.data_type()),
                };

                DeltaStructField {
                    name: af.name().to_string(),
                    data_type: DeltaDataType::Primitive(prim_type),
                    nullable: af.is_nullable(),
                    metadata: HashMap::new(),
                }
            })
            .collect()
    }
}
```

**File**: `crates/tinyfs/src/arrow/parquet.rs`

Complete parquet implementation using the local ForArrow trait:

```rust
//! Parquet support for TinyFS using Arrow integration
//!
//! This module provides both low-level Arrow RecordBatch operations
//! and high-level ForArrow collection serialization.

use arrow_array::RecordBatch;
use arrow::datatypes::Schema;
use parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use futures::Stream;
use std::path::Path;
use std::sync::Arc;
use crate::{WD, Result, EntryType};
use super::schema::ForArrow;
use serde_arrow::{to_arrow, from_arrow};

/// Default batch size for iterating over collections
const DEFAULT_BATCH_SIZE: usize = 1000;

/// Extension trait for Arrow RecordBatch operations on WD
/// This provides convenience methods as an arrow module extension
#[async_trait::async_trait]
pub trait ParquetExt {
    /// Create a Table file from a RecordBatch
    async fn create_table_from_batch<P: AsRef<Path> + Send>(
        &self,
        path: P,
        batch: &RecordBatch,
    ) -> Result<()>;
    
    /// Create a Table file from a collection implementing ForArrow
    async fn create_table_from_items<P: AsRef<Path> + Send, T, I>(
        &self,
        path: P,
        items: I,
    ) -> Result<()>
    where
        T: ForArrow + serde::Serialize + Send,
        I: IntoIterator<Item = T> + Send,
        I::IntoIter: Send;
    
    /// Read a Table file as a single RecordBatch
    async fn read_table_as_batch<P: AsRef<Path> + Send>(
        &self,
        path: P,
    ) -> Result<RecordBatch>;
    
    /// Read a Table file as a collection of items implementing ForArrow
    async fn read_table_as_items<P: AsRef<Path> + Send, T>(
        &self,
        path: P,
    ) -> Result<Vec<T>>
    where
        T: ForArrow + serde::de::DeserializeOwned + Send;
}
```

#### 3.2 ParquetExt Implementation Using Local ForArrow

**File**: `crates/tinyfs/src/arrow/parquet.rs` (continued)

Implementation leverages the existing AsyncRead/AsyncWrite streaming infrastructure with local ForArrow trait:

```rust
use serde_arrow::to_arrow;
use serde_arrow::from_arrow;

#[async_trait::async_trait]
impl ParquetExt for WD {
    async fn create_table_from_batch<P: AsRef<Path> + Send>(
        &self,
        path: P,
        batch: &RecordBatch,
    ) -> Result<()> {
        // 1. Create streaming AsyncWrite from TinyFS
        let writer = self.async_writer_path(&path).await?;
        
        // 2. Use AsyncArrowWriter to serialize RecordBatch to Parquet
        let compat_writer = writer.compat_write();
        let mut arrow_writer = AsyncArrowWriter::try_new(compat_writer, batch.schema(), None)?;
        arrow_writer.write(batch).await?;
        arrow_writer.close().await?;
        
        // 3. Set entry type to FileTable for proper detection
        self.set_entry_type(&path, EntryType::FileTable).await?;
        
        Ok(())
    }
    
    async fn create_table_from_items<P: AsRef<Path> + Send, T, I>(
        &self,
        path: P,
        items: I,
    ) -> Result<()>
    where
        T: ForArrow + serde::Serialize + Send,
        I: IntoIterator<Item = T> + Send,
        I::IntoIter: Send,
    {
        // 1. Convert iterator to Vec for batching
        let items_vec: Vec<T> = items.into_iter().collect();
        
        // 2. Create Arrow schema from ForArrow trait
        let arrow_schema = Arc::new(Schema::new(T::for_arrow()));
        
        // 3. Process items in batches of DEFAULT_BATCH_SIZE (1000)
        let mut batches = Vec::new();
        for chunk in items_vec.chunks(DEFAULT_BATCH_SIZE) {
            // Use serde_arrow to convert chunk to RecordBatch
            let arrays = to_arrow(&arrow_schema, chunk)?;
            let batch = RecordBatch::try_new(arrow_schema.clone(), arrays)?;
            batches.push(batch);
        }
        
        // 4. Create streaming AsyncWrite from TinyFS
        let writer = self.async_writer_path(&path).await?;
        let compat_writer = writer.compat_write();
        let mut arrow_writer = AsyncArrowWriter::try_new(compat_writer, arrow_schema, None)?;
        
        // 5. Write all batches
        for batch in batches {
            arrow_writer.write(&batch).await?;
        }
        arrow_writer.close().await?;
        
        // 6. Set entry type to FileTable for proper detection
        self.set_entry_type(&path, EntryType::FileTable).await?;
        
        Ok(())
    }
    
    async fn read_table_as_batch<P: AsRef<Path> + Send>(
        &self,
        path: P,
    ) -> Result<RecordBatch> {
        // 1. Create streaming AsyncRead from TinyFS
        let reader = self.async_reader_path(&path).await?;
        
        // 2. Use ParquetRecordBatchStreamBuilder for async reading
        let compat_reader = reader.compat();
        let builder = ParquetRecordBatchStreamBuilder::new(compat_reader).await?;
        let mut stream = builder.build()?;
        
        // 3. Collect all batches and concatenate them
        let mut all_batches = Vec::new();
        while let Some(batch_result) = stream.next().await {
            all_batches.push(batch_result?);
        }
        
        if all_batches.is_empty() {
            return Err(Error::Other("Empty table file".to_string()));
        }
        
        // 4. Concatenate all batches into single RecordBatch
        if all_batches.len() == 1 {
            Ok(all_batches.into_iter().next().unwrap())
        } else {
            let schema = all_batches[0].schema();
            let batch_refs: Vec<&RecordBatch> = all_batches.iter().collect();
            arrow::compute::concat_batches(&schema, &batch_refs)
                .map_err(|e| Error::Other(format!("Failed to concatenate batches: {}", e)))
        }
    }
    
    async fn read_table_as_items<P: AsRef<Path> + Send, T>(
        &self,
        path: P,
    ) -> Result<Vec<T>>
    where
        T: ForArrow + serde::de::DeserializeOwned + Send,
    {
        // 1. Read as RecordBatch first
        let batch = self.read_table_as_batch(path).await?;
        
        // 2. Use serde_arrow to convert from RecordBatch to Vec<T>
        let items: Vec<T> = from_arrow(&batch)?;
        
        Ok(items)
    }
}
```

#### 3.3 Integration with Large File Storage

Arrow integration automatically benefits from Phase 2 large file storage:

```rust
impl ParquetExt for WD {
    async fn create_large_table_from_items<P: AsRef<Path> + Send, T, I>(
        &self,
        path: P,
        items: I,
    ) -> Result<()>
    where
        T: ForArrow + serde::Serialize + Send,
        I: IntoIterator<Item = T> + Send,
        I::IntoIter: Send,
    {
        // Create table file using standard method
        self.create_table_from_items(path, items).await?;
        
        // Large file storage happens automatically:
        // 1. AsyncArrowWriter streams Parquet data to AsyncWrite
        // 2. TinyFS buffers in memory until flush/close
        // 3. If final size >= 64 KiB, TLogFS stores as large file
        // 4. SHA256 computed and stored in OplogEntry
        // 5. Content stored in _large_files/ directory
        
        Ok(())
    }
}
```

**Key Benefits**:
- **Seamless integration**: Arrow works directly with Phase 1 streaming
- **Automatic large file handling**: Phase 2 storage kicks in transparently
- **Memory bounded**: Streaming prevents loading huge files into RAM
- **Type-aware**: EntryType distinguishes FileTable files
- **Clean separation**: Arrow logic separate from TinyFS core
- **ForArrow trait**: Defined locally in tinyfs to avoid circular dependencies
- **Proper dependency flow**: tinyfs → tlogfs (no reverse dependencies)

### Phase 4: Entry Type Metadata Integration

#### 4.1 Enhanced Entry Type Support

**File**: `crates/tinyfs/src/entry_type.rs`

Add methods for large file thresholds:

```rust
impl EntryType {
    /// Check if this entry type supports large file storage
    pub fn supports_large_files(&self) -> bool {
        matches!(self, EntryType::FileData | EntryType::FileTable | EntryType::FileSeries)
    }
    
    /// Get the appropriate threshold for large file storage based on entry type
    pub fn large_file_threshold(&self) -> Option<usize> {
        match self {
            EntryType::FileData => Some(64 * 1024),      // 64 KiB for data files
            EntryType::FileTable => Some(64 * 1024),     // 64 KiB for table files  
            EntryType::FileSeries => Some(64 * 1024),    // 64 KiB for series files
            _ => None,
        }
    }
}
```

#### 4.2 Directory Entry Type Storage

Need to implement metadata storage for EntryType in directory entries. This requires updates to:

- Directory implementation to store entry types
- WD interface to set/get entry types
- TLogFS persistence to store directory metadata

### Phase 5: Testing Infrastructure

#### 5.1 Arrow Test Utilities

**File**: `crates/tinyfs/src/tests/arrow_tests.rs`

```rust
use crate::arrow::{ForArrow, ParquetExt};
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow_array::{Int64Array, StringArray, Float64Array, RecordBatch};
use std::sync::Arc;

#[tokio::test] // No feature flags needed
async fn test_create_and_read_table_file() {
    let fs = FS::new_oplog("test_table").await.unwrap();
    let root = fs.root().await.unwrap();
    
    // Create test RecordBatch
    let batch = create_test_batch();
    
    // Store as table file
    root.create_table_from_batch("table.parquet", &batch).await.unwrap();
    
    // Read back as RecordBatch
    let result = root.read_table_as_batch("table.parquet").await.unwrap();
    
    assert_eq!(batch.schema(), result.schema());
    assert_eq!(batch.num_rows(), result.num_rows());
}

#[tokio::test]
async fn test_create_table_from_forarrow_items() {
    #[derive(serde::Serialize, serde::Deserialize)]
    struct TestRecord {
        id: i64,
        name: String,
        value: f64,
    }
    
    impl ForArrow for TestRecord {
        fn for_arrow() -> Vec<FieldRef> {
            vec![
                Arc::new(Field::new("id", DataType::Int64, false)),
                Arc::new(Field::new("name", DataType::Utf8, false)),
                Arc::new(Field::new("value", DataType::Float64, false)),
            ]
        }
    }
    
    let fs = FS::new_oplog("test_forarrow").await.unwrap();
    let root = fs.root().await.unwrap();
    
    // Create test data collection (>1000 items to test batching)
    let items: Vec<TestRecord> = (0..2500).map(|i| TestRecord {
        id: i,
        name: format!("item_{}", i),
        value: i as f64 * 1.5,
    }).collect();
    
    // Store as table file using ForArrow
    root.create_table_from_items("data.parquet", items.clone()).await.unwrap();
    
    // Read back as items
    let result: Vec<TestRecord> = root.read_table_as_items("data.parquet").await.unwrap();
    
    assert_eq!(items.len(), result.len());
    assert_eq!(items[0].id, result[0].id);
    assert_eq!(items[2499].name, result[2499].name);
}

#[tokio::test]
async fn test_table_file_entry_type() {
    let fs = FS::new_oplog("test_entry_type").await.unwrap();
    let root = fs.root().await.unwrap();
    
    // Create table file
    let batch = create_test_batch();
    root.create_table_from_batch("table.parquet", &batch).await.unwrap();
    
    // Verify entry type is set correctly
    let entry_type = root.get_entry_type("table.parquet").await.unwrap();
    assert_eq!(entry_type, EntryType::FileTable);
    assert!(entry_type.is_table_file());
    assert!(entry_type.is_parquet_file());
}

fn create_test_batch() -> RecordBatch {
    let ids = Int64Array::from(vec![1, 2, 3, 4, 5]);
    let names = StringArray::from(vec!["alice", "bob", "charlie", "diana", "eve"]);
    let values = Float64Array::from(vec![1.1, 2.2, 3.3, 4.4, 5.5]);
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    
    RecordBatch::try_new(schema, vec![
        Arc::new(ids),
        Arc::new(names),
        Arc::new(values),
    ]).unwrap()
}

#[tokio::test]
async fn test_large_parquet_file_storage() {
    let fs = FS::new_oplog("test_large").await.unwrap();
    let root = fs.root().await.unwrap();
    
    // Create large batch (> 64 KiB when serialized)
    let large_batch = create_large_test_batch();
    
    root.create_table_from_batch("large_table.parquet", &large_batch).await.unwrap();
    
    // Verify stored as large file
    let entry = fs.persistence.load_latest_oplog_entry(/* node_id, part_id */).await.unwrap();
    assert!(entry.is_large_file());
    assert!(entry.sha256.is_some());
    assert!(entry.content.is_none());
    
    // Verify large file exists
    let sha256 = entry.sha256.unwrap();
    let large_file_path = large_file_path(&fs.store_path, &sha256);
    assert!(tokio::fs::metadata(&large_file_path).await.is_ok());
}

fn create_large_test_batch() -> RecordBatch {
    // Create a batch with enough data to exceed 64 KiB when serialized to Parquet
    let size = 10000;
    let ids: Vec<i64> = (0..size).collect();
    let names: Vec<String> = (0..size).map(|i| format!("name_{}", i)).collect();
    let values: Vec<f64> = (0..size).map(|i| i as f64 * 1.1).collect();
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    
    RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(StringArray::from(names)),
        Arc::new(Float64Array::from(values)),
    ]).unwrap()
}
```
```

#### 5.2 Memory and Large File Tests

```rust
#[tokio::test]
async fn test_content_addressable_deduplication() {
    let fs = FS::new_oplog("test_dedup").await.unwrap();
    let root = fs.root().await.unwrap();
    
    // Create the same Arrow data twice
    let batch = create_large_test_batch(); // > 64 KiB
    
    root.create_table_from_batch("table1.parquet", &batch).await.unwrap();
    root.create_table_from_batch("table2.parquet", &batch).await.unwrap();
    
    // Verify only one large file was created (deduplication)
    let large_files_dir = PathBuf::from(&fs.store_path).join("_large_files");
    let entries = tokio::fs::read_dir(&large_files_dir).await.unwrap();
    let file_count = entries.count().await;
    assert_eq!(file_count, 1); // Only one actual large file
    
    // Verify both tables read the same data
    let data1 = root.read_table_as_batch("table1.parquet").await.unwrap();
    let data2 = root.read_table_as_batch("table2.parquet").await.unwrap();
    assert_eq!(data1.schema(), data2.schema());
    assert_eq!(data1.num_rows(), data2.num_rows());
}

#[tokio::test]
async fn test_large_series_streaming() {
    let fs = FS::new_oplog("test_streaming").await.unwrap();
    let root = fs.root().await.unwrap();
    
    // Create a large series file by streaming many batches
    let batch_stream = generate_large_batch_stream(100); // 100 batches
    root.create_series_from_batches("series.parquet", batch_stream).await.unwrap();
    
    // Verify we can stream it back without loading everything into memory
    let read_stream = root.read_series_as_stream("series.parquet").await.unwrap();
    let mut count = 0;
    
    tokio::pin!(read_stream);
    while let Some(batch) = read_stream.next().await {
        let _batch = batch.unwrap();
        count += 1;
        
        // Verify memory stays bounded during streaming
        // (Implementation would need actual memory monitoring)
    }
    
    assert_eq!(count, 100);
}
```

## Implementation Checklist

### ✅ Phase 1: Core Streaming Support **COMPLETE** (July 14, 2025)
- ✅ Handle architecture with write protection
- ✅ StreamingFileWriter with memory buffering  
- ✅ AsyncRead/AsyncWrite trait implementations
- ✅ Comprehensive test suite (10 tests)
- ✅ Arrow/Parquet integration working
- ✅ Write protection validation
- ✅ Memory bounded operation

### 🎯 Phase 2: Large File Storage **READY TO IMPLEMENT** (Next Priority)
- [ ] Update OplogEntry schema with optional content and sha256 fields
- [ ] Create large_files.rs module with 64 KiB threshold
- [ ] Implement content-addressed storage in `<pond>/_large_files/`
- [ ] Update TLogFS persistence layer for size-based storage strategy
- [ ] Add garbage collection for unreferenced large files
- [ ] Test large file round-trip and deduplication
- [ ] Test atomic operations and consistency

### 🚀 Phase 3: Arrow Integration **READY AFTER PHASE 2**
- [ ] Create arrow module with schema and parquet sub-modules
- [ ] Move ForArrow trait from tlogfs to tinyfs/src/arrow/schema.rs
- [ ] Add ParquetExt trait in tinyfs/src/arrow/parquet.rs with core Arrow methods
- [ ] Implement create_table_from_batch() and read_table_as_batch() (low-level API)
- [ ] Implement create_table_from_items() and read_table_as_items() (high-level ForArrow API)
- [ ] Integration with EntryType system for FileTable detection
- [ ] Update tlogfs to use tinyfs::arrow::ForArrow instead of local definition
- [ ] Test Arrow roundtrips with large Parquet files
- [ ] Test ForArrow-based serialization with 1000-row batches
- [ ] Validate automatic large file handling

### 📋 Phase 4: Entry Type Integration **PLANNED**
- [ ] Add large file threshold methods to EntryType
- [ ] Implement entry type metadata storage in directories  
- [ ] Add WD methods for setting/getting entry types
- [ ] Update from_node_type to detect Parquet content

### 📋 Phase 5: Testing & Documentation **ONGOING**
- [ ] Comprehensive Arrow integration tests
- [ ] Large file storage tests (Phase 2)
- [ ] Performance benchmarks
- [ ] Update documentation with examples
- [ ] CLI integration for table/series commands

## Next Steps: Phase 2 Implementation

**IMMEDIATE PLAN**: Implement Arrow integration with the updated approach:

1. **Create Arrow Module**: Add `crates/tinyfs/src/arrow/` module with schema and parquet sub-modules
2. **Move ForArrow Trait**: Migrate trait from tlogfs to tinyfs/src/arrow/schema.rs
3. **Implement ParquetExt Trait**: Two-level API with ForArrow integration
4. **Low-Level API**: Direct RecordBatch operations
5. **High-Level API**: ForArrow-based collections with 1000-row batches
6. **EntryType Integration**: Automatic FileTable detection
7. **Update TLogFS**: Change imports to use tinyfs::arrow::ForArrow
8. **Comprehensive Testing**: ForArrow roundtrips, batching, large files

**Foundation Goals**:
- ✅ Arrow module approach keeps TinyFS core unchanged
- ✅ ForArrow trait migration avoids circular dependencies
- ✅ 1000-row default batch size for memory efficiency
- ✅ FileTable entry type for proper file detection
- ✅ Clean separation between low-level Arrow API and high-level ForArrow API
- ✅ Proper dependency flow: tinyfs → tlogfs (no reverse dependencies)

**After Implementation**: The Arrow integration will work seamlessly with large file storage, automatically handling large Parquet files transparently through the existing Phase 2 infrastructure.

## Key Benefits

1. **Scalability**: Large files stored separately from Delta Lake records
2. **Performance**: Streaming support for large datasets without memory issues
3. **Deduplication**: Content-addressed storage prevents duplicate storage
4. **Compatibility**: Existing byte-oriented APIs unchanged
5. **Type Safety**: EntryType system distinguishes file formats
6. **Focused Architecture**: TinyFS core stays simple, Arrow is convenience layer
7. **Clean Foundation**: Built on unified tlogfs crate post-merge

## Future Enhancements

1. **Schema Evolution**: Support schema changes in Series files over time
2. **Compression**: Configurable compression for Parquet files
3. **Indexing**: Add Arrow Flight integration for query pushdown
4. **Caching**: Smart caching for frequently accessed large files
5. **Replication**: Cross-region replication for large files
6. **Advanced GC**: Configurable retention policies for large files

## Dependencies on Arrow Crates

- **arrow-array**: RecordBatch and Array types
- **arrow-schema**: Schema definitions
- **parquet**: AsyncArrowWriter, ParquetRecordBatchStreamBuilder
- **futures**: Stream trait for async iteration
- **tokio-util**: AsyncRead/AsyncWrite compatibility adapters

## Risks and Mitigations

1. **Large File Cleanup**: Implemented robust garbage collection for large files
2. **Memory Usage**: Simple buffering bounds memory to content size (Phase 1), future HybridWriter will add spillover (Phase 2+)
3. **Atomic Operations**: Use temp file + rename for consistency
4. **Concurrent Access**: Write protection prevents data races during streaming
5. **Error Handling**: Comprehensive error handling for I/O operations
6. **Performance**: Benchmark and optimize streaming paths
7. **Storage Growth**: Monitor large file directory size and implement retention policies

## Architectural Approach: Clean Separation

The key insight is **architectural separation** while leveraging existing dependencies:

### What stays in TinyFS core:
- `File` trait with `read_to_vec()`, `write_from_slice()`, `async_reader()`, `async_writer()`
- `EntryType` enum for distinguishing file formats
- Streaming support via standard `AsyncRead`/`AsyncWrite` traits
- Large file storage strategies in TLogFS persistence layer

### What goes in extension layers:
- `ParquetExt` trait with `create_table_from_batch()`, `read_table_as_items()`
- `ForArrow` trait in `tinyfs/src/arrow/schema.rs` for schema definitions
- Conversion between `RecordBatch` and Parquet bytes
- Arrow-specific error handling and schema management

### Dependencies:
- **TinyFS**: Arrow types used in arrow module, no imports in core trait definitions
- **Extensions**: Arrow types used in extension traits and WD layer
- **Project**: Arrow already available via Delta Lake and DataFusion dependencies
- **Proper flow**: tinyfs (with arrow module) → tlogfs (uses tinyfs::arrow::ForArrow)

This keeps TinyFS focused on file storage primitives while providing rich Arrow integration at higher layers, all built on the clean foundation of the unified tlogfs crate.
    /// Create an AsyncRead stream for the file content
    /// Implementations handle their own concurrent read protection
    async fn async_reader(&self) -> error::Result<Pin<Box<dyn AsyncRead + Send>>>;
    
    /// Create an AsyncWrite stream for the file content  
    /// Implementations handle their own write exclusivity
    async fn async_writer(&self) -> error::Result<Pin<Box<dyn AsyncWrite + Send>>>;
    
    /// Check if file is currently being written (optional)
    async fn is_being_written(&self) -> bool {
        false // Default implementation for simple files
    }
}

impl Handle {
    pub fn new(file: Arc<tokio::sync::Mutex<Box<dyn File>>>) -> Self {
        Self(file)
    }
    
    /// Get an async reader - delegated to implementation
    pub async fn async_reader(&self) -> error::Result<Pin<Box<dyn AsyncRead + Send>>> {
        let file = self.0.lock().await;
        file.async_reader().await
    }
    
    /// Get an async writer - delegated to implementation  
    pub async fn async_writer(&self) -> error::Result<Pin<Box<dyn AsyncWrite + Send>>> {
        let mut file = self.0.lock().await;
        file.async_writer().await
    }
}
```

#### 1.2 Memory Implementation with Integrated State

**File**: `crates/tinyfs/src/memory/file.rs`

**NEW APPROACH**: Move write protection into MemoryFile implementation:

```rust
use tokio::sync::{Mutex, RwLock};

/// Memory file with integrated write protection
pub struct MemoryFile {
    content: Arc<Mutex<Vec<u8>>>,
    write_state: Arc<RwLock<WriteState>>,
}

#[derive(Debug, Clone, PartialEq)]
enum WriteState {
    Ready,
    Writing,
}

impl MemoryFile {
    pub fn new<T: AsRef<[u8]>>(content: T) -> Self {
        Self {
            content: Arc::new(Mutex::new(content.as_ref().to_vec())),
            write_state: Arc::new(RwLock::new(WriteState::Ready)),
        }
    }
}

#[async_trait]
impl File for MemoryFile {
    async fn async_reader(&self) -> error::Result<Pin<Box<dyn AsyncRead + Send>>> {
        // Check write state
        let state = self.write_state.read().await;
        if *state == WriteState::Writing {
            return Err(error::Error::Other("File is currently being written".to_string()));
        }
        drop(state);
        
        let content = self.content.lock().await;
        Ok(Box::pin(std::io::Cursor::new(content.clone())))
    }
    
    async fn async_writer(&self) -> error::Result<Pin<Box<dyn AsyncWrite + Send>>> {
        // Acquire write lock
        let mut state = self.write_state.write().await;
        if *state == WriteState::Writing {
            return Err(error::Error::Other("File is already being written".to_string()));
        }
        *state = WriteState::Writing;
        drop(state);
        
        Ok(Box::pin(MemoryFileWriter::new(
            self.content.clone(),
            self.write_state.clone()
        )))
    }
    
    async fn is_being_written(&self) -> bool {
        let state = self.write_state.read().await;
        *state == WriteState::Writing
    }
}

/// Writer that resets state on drop
struct MemoryFileWriter {
    content: Arc<Mutex<Vec<u8>>>,
    write_state: Arc<RwLock<WriteState>>,
    buffer: Vec<u8>,
}

impl AsyncWrite for MemoryFileWriter {
    // ... implementation with automatic state reset in Drop
}
```

#### 1.3 TLogFS Implementation with Transaction-Integrated State

**File**: `crates/tlogfs/src/file.rs`

**NEW APPROACH**: Integrate write state with Delta Lake transactions:

```rust
/// TLogFS file with transaction-integrated state management
pub struct OpLogFile {
    node_id: NodeID,
    parent_node_id: NodeID,
    persistence: Arc<dyn PersistenceLayer>,
    /// Transaction-bound write state
    transaction_state: Arc<RwLock<TransactionWriteState>>,
}

#[derive(Debug, Clone, PartialEq)]
enum TransactionWriteState {
    Ready,
    WritingInTransaction(i64), // Transaction ID
}

#[async_trait]
impl File for OpLogFile {
    async fn async_reader(&self) -> error::Result<Pin<Box<dyn AsyncRead + Send>>> {
        // Check transaction state
        let state = self.transaction_state.read().await;
        if let TransactionWriteState::WritingInTransaction(_) = *state {
            return Err(error::Error::Other("File is being written in active transaction".to_string()));
        }
        drop(state);
        
        let content = self.persistence.load_file_content(self.node_id, self.parent_node_id).await
            .map_err(|e| error::Error::Other(format!("Failed to load file content: {}", e)))?;
        
        Ok(Box::pin(std::io::Cursor::new(content)))
    }
    
    async fn async_writer(&self) -> error::Result<Pin<Box<dyn AsyncWrite + Send>>> {
        // Get current transaction ID from persistence layer
        let transaction_id = self.persistence.current_transaction_id().await
            .map_err(|e| error::Error::Other(format!("No active transaction: {}", e)))?;
        
        // Acquire write lock with transaction ID
        let mut state = self.transaction_state.write().await;
        match *state {
            TransactionWriteState::WritingInTransaction(existing_tx) if existing_tx == transaction_id => {
                return Err(error::Error::Other("File is already being written in this transaction".to_string()));
            }
            TransactionWriteState::WritingInTransaction(other_tx) => {
                return Err(error::Error::Other(format!("File is being written in transaction {}", other_tx)));
            }
            TransactionWriteState::Ready => {
                *state = TransactionWriteState::WritingInTransaction(transaction_id);
            }
        }
        drop(state);
        
        Ok(Box::pin(OpLogFileWriter::new(
            self.node_id,
            self.parent_node_id,
            self.persistence.clone(),
            self.transaction_state.clone(),
            transaction_id,
        )))
    }
    
    async fn is_being_written(&self) -> bool {
        let state = self.transaction_state.read().await;
        matches!(*state, TransactionWriteState::WritingInTransaction(_))
    }
}

/// Writer integrated with Delta Lake transactions
struct OpLogFileWriter {
    // ... implementation that resets transaction state when complete
}
```

#### 1.4 Update WD Interface for Simple Delegation

**File**: `crates/tinyfs/src/wd.rs`

**NEW APPROACH**: Simple delegation to implementations:

```rust
impl WD {
    /// Get an async reader for streaming file content
    pub async fn async_reader_path<P: AsRef<Path>>(&self, path: P) -> Result<Pin<Box<dyn AsyncRead + Send>>> {
        let node_path = self.get_node_path(path).await?;
        let file_handle = node_path.node.as_file()?;
        file_handle.async_reader().await
    }
    
    /// Get an async writer for streaming content to file
    pub async fn async_writer_path<P: AsRef<Path>>(&self, path: P) -> Result<Pin<Box<dyn AsyncWrite + Send>>> {
        let (node_path, writer) = self.create_file_path_streaming(path).await?;
        Ok(writer)
    }
    
    /// Buffer helper: Read entire file into memory
    pub async fn read_file_path_to_vec<P: AsRef<Path>>(&self, path: P) -> Result<Vec<u8>> {
        let reader = self.async_reader_path(path).await?;
        buffer_helpers::read_all_to_vec(reader).await
    }
    
    /// Buffer helper: Write data from slice
    pub async fn write_file_path_from_slice<P: AsRef<Path>>(&self, path: P, content: &[u8]) -> Result<NodePath> {
        let mut writer = self.async_writer_path(path).await?;
        use tokio::io::AsyncWriteExt;
        writer.write_all(content).await?;
        writer.shutdown().await?;
        Ok(node_path)
    }
}
```

#### 1.5 Migration and Testing Plan

**IMPLEMENTATION STEPS**:

1. **Remove External State Management**:
   - Remove `state` field from `Handle` struct
   - Remove `FileState`, `WriteGuard`, `StreamingFileWriter` types
   - Simplify `Handle` to simple wrapper: `Handle(Arc<Mutex<Box<dyn File>>>)`

2. **Update File Trait**:
   - Change `async_writer(&mut self)` to `async_writer(&self)`
   - Implementations handle their own mutability via interior mutability

3. **Rebuild MemoryFile**:
   - Add internal `write_state: Arc<RwLock<WriteState>>`
   - Implement `MemoryFileWriter` with automatic state cleanup
   - Test concurrent access protection

4. **Rebuild OpLogFile**:
   - Add internal `transaction_state: Arc<RwLock<TransactionWriteState>>`
   - Integrate write locks with Delta Lake transaction IDs
   - Test transaction-bound write protection

5. **Update All Usage Sites**:
   - Update WD interface methods
   - Update all test files
   - Verify steward operations still work

**TESTING STRATEGY**:
- All existing tests should continue to pass
- Add tests for implementation-specific write protection
- Test transaction integration in TLogFS
- Verify memory management in MemoryFile
    state: Arc<tokio::sync::RwLock<FileState>>, // NEW: Write protection
}

#[derive(Debug, Clone, PartialEq)]
enum FileState {
    Ready,   // Available for read/write operations
    Writing, // Being written via streaming - reads return error
}

#[async_trait]
pub trait File: Metadata + Send + Sync {
    // ...existing methods...
    
    /// Create an AsyncRead stream for the file content
    /// Default implementation wraps read_to_vec() for backward compatibility
    async fn async_reader(&self) -> error::Result<Pin<Box<dyn AsyncRead + Send>>> {
        let content = self.read_to_vec().await?;
        Ok(Box::pin(std::io::Cursor::new(content)))
    }
    
    // NOTE: async_writer was REMOVED from trait - cleaner design
    // Only available on Handle which provides necessary coordination
}

impl Handle {
    /// Get an async reader with write protection
    pub async fn async_reader(&self) -> error::Result<Pin<Box<dyn AsyncRead + Send>>> {
        // Check if file is being written - fails with clear error
        let state = self.state.read().await;
        if *state == FileState::Writing {
            return Err(error::Error::Other("File is currently being written".to_string()));
        }
        drop(state);
        
        let file = self.inner.lock().await;
        file.async_reader().await
    }
    
    /// Get an async writer with exclusive write lock
    pub async fn async_writer(&self) -> error::Result<StreamingFileWriter> {
        // Acquire write lock - prevents concurrent reads/writes
        let mut state = self.state.write().await;
        if *state == FileState::Writing {
            return Err(error::Error::Other("File is already being written".to_string()));
        }
        *state = FileState::Writing;
        
        Ok(StreamingFileWriter::new(self.clone(), WriteGuard::new(self.state.clone())))
    }
}
```

#### 1.2 Simple Memory Buffering (Phase 1 Approach)

**ACTUAL IMPLEMENTATION**: We implemented simple memory buffering for Phase 1, deferring complex hybrid storage to Phase 2:

```rust
/// Write-protected streaming writer with simple memory buffering
pub struct StreamingFileWriter {
    handle: Handle,
    buffer: Vec<u8>,                    // Simple memory buffer for Phase 1
    _write_guard: WriteGuard,           // Automatic write lock management
    write_result_rx: Option<oneshot::Receiver<Result<(), Error>>>,
}

impl AsyncWrite for StreamingFileWriter {
    fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, std::io::Error>> {
        // Simple: append to memory buffer
        self.buffer.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }
    
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        // Coordinate background write via tokio::spawn + oneshot channel
        // Write lock automatically released when WriteGuard is dropped
    }
}

/// Automatic write lock management
struct WriteGuard {
    state: Arc<tokio::sync::RwLock<FileState>>,
}

impl Drop for WriteGuard {
    fn drop(&mut self) {
        // Automatically reset state to Ready when writer is dropped
        if let Ok(mut state) = self.state.try_write() {
            *state = FileState::Ready;
        }
    }
}
```

#### 1.3 Comprehensive Protection and Testing ✅

**ACTUAL IMPLEMENTATION**: We built 10 comprehensive tests including protection verification:

- ✅ `test_async_reader_basic` - Basic streaming read functionality  
- ✅ `test_async_writer_basic` - Basic streaming write functionality
- ✅ `test_async_writer_memory_buffering` - Memory buffering verification
- ✅ `test_async_writer_large_data` - Large file handling (1MB+)
- ✅ `test_parquet_roundtrip_single_batch` - Arrow/Parquet integration
- ✅ `test_parquet_roundtrip_multiple_batches` - Complex Arrow workflows
- ✅ `test_memory_bounded_large_parquet` - Large Parquet file handling
- ✅ `test_concurrent_writers` - Concurrent operation safety
- ✅ `test_concurrent_read_write_protection` - **Write protection verification**
- ✅ `test_write_protection_with_completed_write` - **Lock lifecycle testing**

**Key Achievements**:
- **Write protection**: Reads blocked during writes with clear errors
- **Automatic lock management**: WriteGuard ensures cleanup even on panic/drop
- **Arrow integration**: Full Parquet roundtrip working via AsyncArrowWriter
- **Memory bounded**: Simple buffering strategy for Phase 1
- **Clean API**: Users just need to scope writers properly with `{ }` blocks

### Phase 2: Large File Storage Architecture (PLANNED)

**STATUS**: Will be implemented after Phase 1 redesign

#### 2.1 Planned: Hybrid Memory/File Buffering

**File**: `crates/tinyfs/src/file.rs` (Future enhancement)

For Phase 2, we'll implement the HybridWriter that can handle very large files:

```rust
use tokio::fs::File as TokioFile;
use tempfile::NamedTempFile;

/// Memory threshold for switching to file-based buffering (e.g., 1MB)
const MEMORY_BUFFER_THRESHOLD: usize = 1024 * 1024;

/// Hybrid writer that buffers in memory up to threshold, then spills to temp file
pub struct HybridWriter {
    memory_buffer: Option<Vec<u8>>,
    temp_file: Option<TokioFile>,
    temp_path: Option<std::path::PathBuf>,
    total_written: usize,
}

impl HybridWriter {
    pub fn new() -> Self {
        Self {
            memory_buffer: Some(Vec::new()),
            temp_file: None,
            temp_path: None,
            total_written: 0,
        }
    }
    
    /// Get the final data - either from memory buffer or temp file
    pub async fn into_data(mut self) -> std::io::Result<Vec<u8>> {
        if let Some(buffer) = self.memory_buffer {
            // Small file - return memory buffer
            Ok(buffer)
        } else if let Some(temp_path) = self.temp_path {
            // Large file - read from temp file
            let data = tokio::fs::read(&temp_path).await?;
            // Clean up temp file
            let _ = tokio::fs::remove_file(&temp_path).await;
            Ok(data)
        } else {
            Ok(Vec::new())
        }
    }
    
    /// Spill memory buffer to temporary file
    async fn spill_to_temp_file(&mut self) -> std::io::Result<()> {
        if self.temp_file.is_some() {
            return Ok(()); // Already spilled
        }
        
        // Create temporary file
        let temp_file = NamedTempFile::new()?;
        let temp_path = temp_file.path().to_path_buf();
        let mut tokio_file = TokioFile::create(&temp_path).await?;
        
        // Write existing buffer to temp file
        if let Some(buffer) = self.memory_buffer.take() {
            use tokio::io::AsyncWriteExt;
            tokio_file.write_all(&buffer).await?;
            tokio_file.flush().await?;
        }
        
        self.temp_file = Some(tokio_file);
        self.temp_path = Some(temp_path);
        
        Ok(())
    }
}

impl AsyncWrite for HybridWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let this = &mut *self;
        
        // Check if we need to spill to temp file
        if this.memory_buffer.is_some() && 
           this.total_written + buf.len() > MEMORY_BUFFER_THRESHOLD {
            
            // Need to spill - but we can't do async work in poll_write
            // So we'll use a waker-based approach
            let mut spill_future = Box::pin(this.spill_to_temp_file());
            match spill_future.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    // Successfully spilled, continue with file write
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
            this.total_written += buf.len();
            Poll::Ready(Ok(buf.len()))
        } else if let Some(ref mut temp_file) = this.temp_file {
            // In temp file mode
            use tokio::io::AsyncWriteExt;
            let mut write_future = Box::pin(temp_file.write_all(buf));
            match write_future.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    this.total_written += buf.len();
                    Poll::Ready(Ok(buf.len()))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Writer in invalid state"
            )))
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        if let Some(ref mut temp_file) = self.temp_file {
            use tokio::io::AsyncWriteExt;
            let mut flush_future = Box::pin(temp_file.flush());
            flush_future.as_mut().poll(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        if let Some(ref mut temp_file) = self.temp_file {
            use tokio::io::AsyncWriteExt;
            let mut shutdown_future = Box::pin(temp_file.shutdown());
            shutdown_future.as_mut().poll(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

/// File writer that uses hybrid buffering strategy
pub struct FileWriter {
    handle: file::Handle,
    writer: Option<HybridWriter>,
}

impl FileWriter {
    pub fn new(handle: file::Handle) -> Self {
        Self {
            handle,
            writer: Some(HybridWriter::new()),
        }
    }
}

impl AsyncWrite for FileWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        if let Some(ref mut writer) = self.writer {
            Pin::new(writer).poll_write(cx, buf)
        } else {
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Writer already closed"
            )))
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        if let Some(ref mut writer) = self.writer {
            Pin::new(writer).poll_flush(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        let this = &mut *self;
        
        // First shutdown the hybrid writer
        if let Some(ref mut writer) = this.writer {
            if let Poll::Pending = Pin::new(writer).poll_shutdown(cx)? {
                return Poll::Pending;
            }
        }
        
        // Extract data and write to file
        if let Some(writer) = this.writer.take() {
            let handle = this.handle.clone();
            
            // Spawn task to extract data and write to file
            tokio::spawn(async move {
                match writer.into_data().await {
                    Ok(data) => {
                        if let Err(e) = handle.write_file(&data).await {
                            eprintln!("Failed to write file: {}", e);
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to extract writer data: {}", e);
                    }
                }
            });
        }
        
        Poll::Ready(Ok(()))
    }
}
```

#### 1.3 Update Memory Backend

**File**: `crates/tinyfs/src/memory.rs`

Implement streaming support in MemoryFile:

```rust
#[async_trait]
impl File for MemoryFile {
    // ...existing methods...
    
    async fn async_reader(&self) -> error::Result<Pin<Box<dyn AsyncRead + Send>>> {
        Ok(Box::pin(std::io::Cursor::new(self.content.clone())))
    }
    
    async fn async_writer(&mut self) -> error::Result<Pin<Box<dyn AsyncWrite + Send>>> {
        self.content.clear();
        Ok(Box::pin(MemoryWriter::new(&mut self.content)))
    }
}
```

### Phase 2: Large File Storage Architecture

#### 2.1 Planned: Storage Strategy Definitions

**File**: `crates/tinyfs/src/storage_strategy.rs` (new)

```rust
/// Threshold for storing files separately (e.g., 10MB)
pub const LARGE_FILE_THRESHOLD: usize = 10 * 1024 * 1024;

/// Reference to a large file stored outside Delta Lake
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LargeFileReference {
    /// Content-addressed path in large file storage
    pub path: PathBuf,
    /// SHA-256 hash for integrity verification
    pub hash: String,
    /// File size in bytes
    pub size: u64,
}

/// Storage strategy decision based on content size
#[derive(Debug)]
pub enum StorageStrategy {
    /// Store content directly in Delta Lake record
    Inline(Vec<u8>),
    /// Store content separately with reference
    LargeFile(LargeFileReference),
}
```

#### 2.2 Planned: Update OpLog Persistence Layer

**File**: `crates/tlogfs/src/persistence.rs`

Add large file handling:

```rust
impl OpLogPersistence {
    /// Store file content using size-based strategy
    async fn store_file_content_with_strategy(
        &self, 
        node_id: NodeID, 
        part_id: NodeID, 
        content: &[u8]
    ) -> TinyFSResult<()> {
        let (file_type, stored_content) = if content.len() >= LARGE_FILE_THRESHOLD {
            // Large file: store as separate file with reference
            let large_file_ref = self.store_large_file(node_id, content).await?;
            let ref_bytes = serde_json::to_vec(&large_file_ref)?;
            (tinyfs::EntryType::FileData, ref_bytes)
        } else {
            // Small file: store directly in Delta Lake
            (tinyfs::EntryType::FileData, content.to_vec())
        };
        
        // Continue with existing storage logic...
    }
    
    /// Store large file as separate file in filesystem
    async fn store_large_file(&self, node_id: NodeID, content: &[u8]) -> Result<LargeFileReference> {
        // Content-addressed storage with deduplication
        // Directory structure: table_path/_large_files/xx/hash.data
    }
    
    /// Load file content using size-based strategy
    async fn load_file_content_with_strategy(&self, node_id: NodeID, part_id: NodeID) -> TinyFSResult<Vec<u8>> {
        // Check if content is LargeFileReference or inline bytes
    }
}
```

#### 2.3 Planned: Directory Structure

```
my_table/
├── _delta_log/                 # Delta Lake transaction log
├── _large_files/               # Large file storage
│   ├── a1/                     # First 2 chars of hash
│   │   └── a1b2c3d4e5f6.data  # Content-addressed files
│   └── f6/
│       └── f6e5d4c3b2a1.data
└── part-00000-*.parquet        # Delta Lake data files
```

### Phase 3: Arrow Integration Layer 🚀 **READY AFTER PHASE 1** (July 14, 2025)

**STATUS**: Ready to implement after Phase 1 redesign is complete

With Phase 1's simple delegation architecture, Arrow integration becomes straightforward.

#### 3.1 Add Arrow Convenience Extensions

**File**: `crates/tinyfs/Cargo.toml`

Since Arrow is already a transitive dependency via Delta Lake and DataFusion, we don't need feature flags to include it. Instead, we structure the code to keep Arrow types out of TinyFS core:

```toml
[dependencies]
# ...existing dependencies...
# Arrow is already available via delta-rs and datafusion
arrow-array = { workspace = true }
arrow-schema = { workspace = true } 
parquet = { workspace = true, features = ["async"] }
futures = { version = "0.3" }
```

The key is **architectural separation**:
- **TinyFS core**: Only works with `Vec<u8>`, `AsyncRead`, `AsyncWrite`  
- **Arrow extensions**: Convert between `RecordBatch` and bytes externally
- **WD interface**: Provides Arrow convenience methods that use TinyFS core

#### 3.2 Arrow Extension Trait for WD

**File**: `crates/tinyfs/src/arrow_support.rs` (new)

```rust
// No feature flags needed - Arrow is already available via workspace dependencies
use arrow_array::RecordBatch;
use parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};

/// Extension trait for Arrow Record Batch operations on WD
/// This provides convenience methods but doesn't change TinyFS core interfaces
#[async_trait]
pub trait WDArrowExt {
    /// Create a Table file from a RecordBatch
    async fn create_table_from_batch<P: AsRef<Path> + Send>(
        &self,
        path: P,
        batch: &RecordBatch,
    ) -> Result<()>;
    
    /// Create a Series file from multiple RecordBatches
    async fn create_series_from_batches<P: AsRef<Path> + Send, S>(
        &self,
        path: P,
        batches: S,
    ) -> Result<()>
    where
        S: Stream<Item = Result<RecordBatch>> + Send;
    
    /// Read a Table file as a single RecordBatch
    async fn read_table_as_batch<P: AsRef<Path> + Send>(
        &self,
        path: P,
    ) -> Result<RecordBatch>;
    
    /// Read a Series file as a stream of RecordBatches
    async fn read_series_as_stream<P: AsRef<Path> + Send>(
        &self,
        path: P,
    ) -> Result<impl Stream<Item = Result<RecordBatch>>>;
}
```

#### 3.3 Arrow Implementation

Key streaming patterns from Arrow crates:

- **ParquetRecordBatchStreamBuilder**: Creates async readers from AsyncFileReader
- **AsyncArrowWriter**: Handles streaming writes to AsyncFileWriter
- **ParquetRecordBatchStream**: Implements Stream<Item = Result<RecordBatch>>

```rust
// No feature flags needed - just architectural separation
#[async_trait]
impl WDArrowExt for WD {
    async fn create_table_from_batch<P: AsRef<Path> + Send>(
        &self,
        path: P,
        batch: &RecordBatch,
    ) -> Result<()> {
        // 1. Create AsyncWrite stream from file
        let node_path = self.create_file_path(&path, &[]).await?;
        let file_handle = node_path.node.as_file()?;
        let writer = file_handle.async_writer().await?;
        
        // 2. Use AsyncArrowWriter to serialize RecordBatch
        let mut arrow_writer = AsyncArrowWriter::try_new(writer, batch.schema(), None)?;
        arrow_writer.write(batch).await?;
        arrow_writer.close().await?;
        
        // 3. Set entry type to FileTable
        self.set_entry_type(&path, EntryType::FileTable).await?;
        
        Ok(())
    }
    
    async fn read_series_as_stream<P: AsRef<Path> + Send>(
        &self,
        path: P,
    ) -> Result<impl Stream<Item = Result<RecordBatch>>> {
        // 1. Get AsyncRead stream from file
        let node_path = self.get_node_path(path).await?;
        let file_handle = node_path.node.as_file()?;
        let reader = file_handle.async_reader().await?;
        
        // 2. Create ParquetRecordBatchStream
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
        let stream = builder.build()?;
        
        Ok(stream.map(|result| {
            result.map_err(|e| Error::Other(format!("Parquet read failed: {}", e)))
        }))
    }
}
```

### Phase 4: Entry Type Metadata Integration

#### 4.1 Enhanced Entry Type Support

**File**: `crates/tinyfs/src/entry_type.rs`

Add methods for large file thresholds:

```rust
impl EntryType {
    /// Check if this entry type supports large file storage
    pub fn supports_large_files(&self) -> bool {
        matches!(self, EntryType::FileData | EntryType::FileTable | EntryType::FileSeries)
    }
    
    /// Get the appropriate threshold for large file storage based on entry type
    pub fn large_file_threshold(&self) -> Option<usize> {
        match self {
            EntryType::FileData => Some(64 * 1024),      // 64KB for data files
            EntryType::FileTable => Some(1024 * 1024),   // 1MB for table files
            EntryType::FileSeries => Some(1024 * 1024),  // 1MB for series files
            _ => None,
        }
    }
}
```

#### 4.2 Directory Entry Type Storage

Need to implement metadata storage for EntryType in directory entries. This requires updates to:

- Directory implementation to store entry types
- WD interface to set/get entry types
- OpLog persistence to store directory metadata

### Phase 5: Testing Infrastructure

#### 5.1 Arrow Test Utilities

**File**: `crates/tinyfs/src/tests/arrow_tests.rs`

```rust
#[tokio::test] // No feature flags needed
async fn test_create_and_read_table_file() {
        // Test table file round-trip
    }
    
    #[tokio::test]
    async fn test_series_file_streaming() {
        // Test series file with multiple batches
    }
    
    #[tokio::test]
    async fn test_large_parquet_file_storage() {
        // Test large file threshold behavior
    }
}
```

#### 5.2 Memory and Large File Tests

```rust
#[tokio::test]
async fn test_hybrid_writer_memory_bounds() {
    // Test that HybridWriter uses bounded memory
    let writer = HybridWriter::new();
    
    // Write 10MB of data in 1KB chunks
    for _ in 0..10240 {
        let chunk = vec![0u8; 1024];
        writer.write_all(&chunk).await.unwrap();
        
        // Verify memory usage stays bounded
        let memory_usage = get_process_memory(); // hypothetical
        assert!(memory_usage < 2 * 1024 * 1024); // < 2MB total
    }
    
    let final_data = writer.into_data().await.unwrap();
    assert_eq!(final_data.len(), 10 * 1024 * 1024);
}

#[tokio::test]
async fn test_content_addressable_deduplication() {
    let fs = FS::new_oplog("test_table").await.unwrap();
    let root = fs.root().await.unwrap();
    
    // Create the same Arrow data twice
    let batch = create_large_test_batch(); // > 1MB
    
    root.create_table_from_batch("table1.parquet", &batch).await.unwrap();
    root.create_table_from_batch("table2.parquet", &batch).await.unwrap();
    
    // Verify only one large file was created (deduplication)
    let large_files = list_large_files("test_table/_large_files").await.unwrap();
    assert_eq!(large_files.len(), 1);
    
    // Verify both tables read the same data
    let data1 = root.read_table_as_batch("table1.parquet").await.unwrap();
    let data2 = root.read_table_as_batch("table2.parquet").await.unwrap();
    assert_eq!(data1, data2);
}

#[tokio::test]
async fn test_large_series_streaming() {
    let fs = FS::new_oplog("test_series").await.unwrap();
    let root = fs.root().await.unwrap();
    
    // Create a large series file by streaming many batches
    let batch_stream = generate_large_batch_stream(1000); // 1000 batches
    root.create_series_from_batches("series.parquet", batch_stream).await.unwrap();
    
    // Verify we can stream it back without loading everything into memory
    let read_stream = root.read_series_as_stream("series.parquet").await.unwrap();
    let mut count = 0;
    
    tokio::pin!(read_stream);
    while let Some(batch) = read_stream.next().await {
        let _batch = batch.unwrap();
        count += 1;
        
        // Verify memory stays bounded during streaming
        let memory_usage = get_process_memory();
        assert!(memory_usage < 10 * 1024 * 1024); // < 10MB
    }
    
    assert_eq!(count, 1000);
}
```

## Memory Management Strategy

## Phase 1 Redesign: Current State and Plan

### ✅ **CURRENT STATE**: External State Management (July 14, 2025)

The current implementation has external state management that needs to be simplified:

```rust
// Current: Complex external state coordination
pub struct Handle {
    inner: Arc<tokio::sync::Mutex<Box<dyn File>>>,
    state: Arc<tokio::sync::RwLock<FileState>>, // ← Remove this
}

// Current: Wrapper manages state externally
impl Handle {
    pub async fn async_writer(&self) -> error::Result<StreamingFileWriter> {
        // Complex external state management
        let mut state = self.state.write().await;
        if *state == FileState::Writing {
            return Err(error::Error::Other("File is already being written".to_string()));
        }
        *state = FileState::Writing;
        Ok(StreamingFileWriter::new(self.clone(), WriteGuard::new(self.state.clone())))
    }
}
```

### 🎯 **TARGET STATE**: Implementation-Integrated State

```rust
// Target: Simple wrapper, no external state
pub struct Handle(Arc<tokio::sync::Mutex<Box<dyn File>>>);

// Target: Pure delegation to implementations
impl Handle {
    pub async fn async_writer(&self) -> error::Result<Pin<Box<dyn AsyncWrite + Send>>> {
        let file = self.0.lock().await;
        file.async_writer().await // Implementation handles state
    }
}

// Each implementation manages its own state
impl File for MemoryFile {
    async fn async_writer(&self) -> error::Result<Pin<Box<dyn AsyncWrite + Send>>> {
        // MemoryFile handles its own write protection
        let mut state = self.write_state.write().await;
        if *state == WriteState::Writing {
            return Err(error::Error::Other("File is already being written".to_string()));
        }
        *state = WriteState::Writing;
        // Return writer that resets state on drop
    }
}
```

## Implementation Checklist

### Phase 1: Core Streaming Redesign 🎯 **PLANNED** (July 14, 2025)
- [ ] Remove external state management from Handle
- [ ] Simplify Handle to wrapper: `Handle(Arc<Mutex<Box<dyn File>>>)`
- [ ] Update File trait: `async_writer(&self)` instead of `async_writer(&mut self)`
- [ ] Rebuild MemoryFile with internal write protection
- [ ] Rebuild OpLogFile with transaction-integrated state
- [ ] Update WD interface for simple delegation
- [ ] Update all usage sites and tests
- [ ] Verify steward operations still work

### Phase 2: Large File Storage 📋 **PLANNED** 
- [ ] Create StorageStrategy and LargeFileReference types
- [ ] Implement HybridWriter with memory/temp file spillover
- [ ] Update OpLog persistence for size-based storage
- [ ] Implement content-addressed large file storage
- [ ] Add large file garbage collection
- [ ] Test large file round-trip and deduplication

### Phase 3: Arrow Integration 🚀 **PLANNED**
- [ ] Add Arrow convenience extensions (no feature flags needed)
- [ ] Create WDArrowExt trait with core methods
- [ ] Implement RecordBatch serialization/deserialization via streaming
- [ ] Add streaming support for Series files
- [ ] Test large Parquet file handling with streaming interface
- [ ] Integrate with EntryType system for FileTable/FileSeries detection

### Phase 4: Entry Type Integration 📋 **PLANNED**
- [ ] Add large file threshold methods to EntryType
- [ ] Implement entry type metadata storage in directories  
- [ ] Add WD methods for setting/getting entry types
- [ ] Update from_node_type to detect Parquet content

### Phase 5: Testing & Documentation 📋 **PLANNED**
- [ ] Comprehensive Arrow integration tests
- [ ] Large file storage tests (Phase 2)
- [ ] Performance benchmarks
- [ ] Update documentation with examples
- [ ] CLI integration for table/series commands

## Next Steps: Phase 1 Redesign

**IMMEDIATE PLAN**: Clean up the architecture by removing external state management:

1. **Simplify Handle Structure**: Remove the complex `state` field and `StreamingFileWriter` wrapper
2. **Push State Into Implementations**: Let each File implementation handle its own concurrency model  
3. **Clean Trait Design**: Simple `async_reader()` and `async_writer()` delegation
4. **Implementation-Specific Benefits**:
   - **MemoryFile**: Simpler internal `RwLock<WriteState>` 
   - **OpLogFile**: Transaction-integrated write state
   - **Future HybridFile**: Storage strategy decisions at implementation level

**Foundation Goals**:
- ✅ Simple, clean `Handle(Arc<Mutex<Box<dyn File>>>>` wrapper
- ✅ Each implementation manages its own write protection
- ✅ TLogFS integrates write state with Delta Lake transactions
- ✅ All existing tests continue to pass
- ✅ Clean foundation for Arrow integration and hybrid storage

**After Phase 1**: Arrow integration and large file storage become straightforward to implement on the clean foundation.

### Phase 5: Testing & Documentation 📋 **PLANNED**
- [ ] Comprehensive Arrow integration tests
- [ ] Large file storage tests (Phase 2)
- [ ] Performance benchmarks
- [ ] Update documentation with examples
- [ ] CLI integration for table/series commands

## Next Steps: Phase 3 Arrow Integration

**READY TO PROCEED**: With Phase 1's write-protected streaming complete, we can immediately start implementing:

1. **WDArrowExt trait** - Arrow convenience methods for WD
2. **create_table_from_batch()** - Store RecordBatch as Parquet via streaming
3. **read_table_as_batch()** - Load Parquet as RecordBatch via streaming  
4. **create_series_from_batches()** - Multi-batch streaming writes
5. **read_series_as_stream()** - Streaming reads of large Series files

**Foundation Ready**:
- ✅ AsyncWrite working with AsyncArrowWriter
- ✅ AsyncRead working with ParquetRecordBatchStreamBuilder
- ✅ Write protection prevents data races during streaming
- ✅ Memory buffering handles files up to memory limits
- ✅ All streaming tests passing (10/10)

**Phase 2 Deferral**: Complex hybrid file storage can be added later without affecting the Arrow API.

## Key Benefits

1. **Scalability**: Large files stored separately from Delta Lake records
2. **Performance**: Streaming support for large datasets
3. **Deduplication**: Content-addressed storage prevents duplication
4. **Compatibility**: Existing byte-oriented APIs unchanged
5. **Type Safety**: EntryType system distinguishes file formats
6. **Focused Architecture**: TinyFS core stays simple, Arrow is convenience layer

## Future Enhancements

1. **Schema Evolution**: Support schema changes in Series files over time
2. **Compression**: Configurable compression for Parquet files
3. **Indexing**: Add Arrow Flight integration for query pushdown
4. **Caching**: Smart caching for frequently accessed large files
5. **Replication**: Cross-region replication for large files

## Dependencies on Arrow Crates

- **arrow-array**: RecordBatch and Array types
- **arrow-schema**: Schema definitions
- **parquet**: AsyncArrowWriter, ParquetRecordBatchStreamBuilder
- **futures**: Stream trait for async iteration

## Risks and Mitigations

1. **Large File Cleanup**: Implement robust garbage collection for large files
2. **Memory Usage**: HybridWriter bounds memory to ~1MB per concurrent write operation
3. **Temporary File Cleanup**: Use tempfile crate for automatic cleanup on drop/panic
4. **Concurrent Access**: Ensure thread safety for large file operations
5. **Error Handling**: Comprehensive error handling for I/O operations
6. **Performance**: Benchmark and optimize streaming paths

## Architectural Approach: Keeping TinyFS Focused

The key insight is **architectural separation**, not dependency isolation:

### What stays in TinyFS core:
- `File` trait with `read_to_vec()`, `write_from_slice()`, `async_reader()`, `async_writer()`
- `EntryType` enum for distinguishing file formats
- Streaming support via standard `AsyncRead`/`AsyncWrite` traits
- Large file storage strategies

### What goes in extension layers:
- `WDArrowExt` trait with `create_table_from_batch()`, `read_series_as_stream()` 
- Conversion between `RecordBatch` and Parquet bytes
- Arrow-specific error handling and schema management

### Dependencies:
- **TinyFS**: No direct Arrow imports in core trait definitions
- **Extensions**: Arrow types are used in extension traits and WD layer
- **Project**: Arrow already available via Delta Lake and DataFusion dependencies

This keeps TinyFS focused on file storage primitives while providing rich Arrow integration at higher layers.

This plan provides a comprehensive roadmap for implementing Arrow Record Batch support while maintaining the clean architecture principles of DuckPond.

### Integration with Large File Storage

The hybrid writer integrates perfectly with the large file storage strategy:

```rust
// Enhanced TLogFS integration
impl OpLogPersistence {
    async fn store_file_from_hybrid_writer(&self, writer: HybridWriter) -> Result<StorageStrategy> {
        // Extract data from hybrid writer (could be memory buffer or temp file)
        let data = writer.into_data().await?;
        
        // Compute content hash for deduplication
        let content_hash = self.compute_content_hash(&data);
        
        // Apply size-based storage strategy
        if data.len() >= LARGE_FILE_THRESHOLD {
            // Large file: check if we already have this content
            if let Some(existing_ref) = self.find_existing_large_file(&content_hash).await? {
                // Deduplication: reuse existing file
                Ok(StorageStrategy::LargeFile(existing_ref))
            } else {
                // Store as new large file with content-addressable path
                let large_file_ref = self.store_large_file_with_hash(&data, content_hash).await?;
                Ok(StorageStrategy::LargeFile(large_file_ref))
            }
        } else {
            // Small file: store inline in Delta Lake
            Ok(StorageStrategy::Inline(data))
        }
    }
    
    /// Store large file using content-addressable naming
    async fn store_large_file_with_hash(&self, data: &[u8], content_hash: String) -> Result<LargeFileReference> {
        let table_path = PathBuf::from(&self.store_path);
        let large_files_dir = table_path.join("_large_files");
        
        // Content-addressable path: _large_files/ab/abcdef123456.data
        let hash_prefix = &content_hash[..2];
        let file_dir = large_files_dir.join(hash_prefix);
        tokio::fs::create_dir_all(&file_dir).await?;
        
        let file_name = format!("{}.data", content_hash);
        let file_path = file_dir.join(&file_name);
        let relative_path = format!("_large_files/{}/{}", hash_prefix, file_name);
        
        // Write file only if it doesn't exist (atomic deduplication)
        if !file_path.exists() {
            tokio::fs::write(&file_path, data).await?;
        }
        
        Ok(LargeFileReference {
            path: PathBuf::from(relative_path),
            hash: content_hash,
            size: data.len() as u64,
        })
    }
}
```

**Key Benefits**:
- **Automatic deduplication**: Same content = same hash = same file
- **Content integrity**: Hash verification on read
- **Efficient storage**: Large files stored once, referenced many times
- **Bounded memory**: HybridWriter ensures we never load huge files into RAM
