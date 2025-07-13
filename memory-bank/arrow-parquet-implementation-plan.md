# Arrow Record Batch Implementation Plan for Table and Series Files

## Overview

This document outlines the implementation plan for adding Arrow Record Batch support to DuckPond's TinyFS for `FileTable` and `FileSeries` entry types. These files will store data as Parquet format while maintaining TinyFS as a byte-oriented abstraction.

## Design Principles

1. **TinyFS remains byte-oriented** - No Arrow types in TinyFS core interfaces
2. **Files are byte containers** - Table/Series files contain Parquet bytes
3. **Streaming support** - Use standard AsyncRead/AsyncWrite traits
4. **Large file handling** - Hybrid storage with size thresholds
5. **Keep TinyFS focused** - Arrow convenience methods are extensions, not core features
6. **Backward compatibility** - Existing APIs unchanged

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
                       │  OpLog Backend   │
                       │                  │
                       │ Small: DeltaLake │
                       │ Large: Separate  │
                       │       Files      │
                       └──────────────────┘
```

## Implementation Phases

### Phase 1: Core TinyFS Streaming Support

#### 1.1 Enhance File Trait with AsyncRead/AsyncWrite

**File**: `crates/tinyfs/src/file.rs`

Add streaming methods to the File trait:

```rust
#[async_trait]
pub trait File: Metadata + Send + Sync {
    // ...existing methods...
    
    /// Create an AsyncRead stream for the file content
    /// Default implementation wraps read_to_vec() for backward compatibility
    async fn async_reader(&self) -> error::Result<Pin<Box<dyn AsyncRead + Send>>> {
        let content = self.read_to_vec().await?;
        Ok(Box::pin(std::io::Cursor::new(content)))
    }
    
    /// Create an AsyncWrite stream for the file content
    /// Default implementation collects to Vec and calls write_from_slice
    async fn async_writer(&mut self) -> error::Result<Pin<Box<dyn AsyncWrite + Send>>> {
        Ok(Box::pin(FileWriter::new(self as *mut dyn File)))
    }
}
```

Add corresponding methods to Handle:

```rust
impl Handle {
    // ...existing methods...
    
    /// Get an async reader for streaming the file content
    pub async fn async_reader(&self) -> error::Result<Pin<Box<dyn AsyncRead + Send>>> {
        let file = self.0.lock().await;
        file.async_reader().await
    }
    
    /// Get an async writer for streaming content to the file
    pub async fn async_writer(&self) -> error::Result<Pin<Box<dyn AsyncWrite + Send>>> {
        let mut file = self.0.lock().await;
        file.async_writer().await
    }
}
```

#### 1.2 Implement FileWriter for AsyncWrite

**File**: `crates/tinyfs/src/file.rs`

```rust
/// AsyncWrite wrapper that collects data and writes to File trait
struct FileWriter {
    file_ptr: *mut dyn File,
    buffer: Vec<u8>,
}

impl AsyncWrite for FileWriter {
    // Implementation that buffers writes and commits on shutdown
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

#### 2.1 Storage Strategy Definitions

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

#### 2.2 Update OpLog Persistence Layer

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

#### 2.3 Directory Structure

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

### Phase 3: Arrow Integration Layer

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

#### 5.2 Large File Storage Tests

```rust
#[tokio::test]
async fn test_large_file_threshold() {
    // Create file larger than threshold
    // Verify it's stored as separate file
    // Verify content integrity
}

#[tokio::test]
async fn test_large_file_deduplication() {
    // Create multiple files with same content
    // Verify only one copy is stored
    // Verify both files read correctly
}
```

## Implementation Checklist

### Phase 1: Core Streaming ✓ (Planned)
- [ ] Add async_reader/async_writer to File trait
- [ ] Implement FileWriter for AsyncWrite buffering
- [ ] Update Handle with streaming methods
- [ ] Update MemoryFile implementation
- [ ] Add basic streaming tests

### Phase 2: Large File Storage ✓ (Planned)
- [ ] Create StorageStrategy and LargeFileReference types
- [ ] Update OpLog persistence for size-based storage
- [ ] Implement content-addressed large file storage
- [ ] Add large file garbage collection
- [ ] Test large file round-trip and deduplication

### Phase 3: Arrow Integration ✓ (Planned)
- [ ] Add Arrow convenience extensions (no feature flags needed)
- [ ] Create WDArrowExt trait with core methods
- [ ] Implement RecordBatch serialization/deserialization
- [ ] Add streaming support for Series files
- [ ] Integrate with ParquetRecordBatchStream

### Phase 4: Entry Type Integration ✓ (Planned)
- [ ] Add large file threshold methods to EntryType
- [ ] Implement entry type metadata storage in directories
- [ ] Add WD methods for setting/getting entry types
- [ ] Update from_node_type to detect Parquet content

### Phase 5: Testing & Documentation ✓ (Planned)
- [ ] Comprehensive Arrow integration tests
- [ ] Large file storage tests
- [ ] Performance benchmarks
- [ ] Update documentation with examples
- [ ] CLI integration for table/series commands

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

1. **Large File Cleanup**: Implement robust garbage collection
2. **Concurrent Access**: Ensure thread safety for large file operations
3. **Error Handling**: Comprehensive error handling for I/O operations
4. **Performance**: Benchmark and optimize streaming paths

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
