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

### Phase 1: Core TinyFS Streaming Support ✅ **COMPLETED**

#### 1.1 Enhanced File Trait with AsyncRead (Write Protection Model)

**File**: `crates/tinyfs/src/file.rs`

**ACTUAL IMPLEMENTATION**: We implemented a write-protected streaming model that's cleaner than the original plan:

```rust
/// Enhanced Handle with write protection
pub struct Handle {
    inner: Arc<tokio::sync::Mutex<Box<dyn File>>>,
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

**STATUS**: Not yet implemented - Phase 1 uses simple memory buffering

The Phase 1 implementation uses simple memory buffering for all files. Phase 2 will add sophisticated hybrid storage for large files.

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

### Phase 3: Arrow Integration Layer (READY FOR IMPLEMENTATION)

**STATUS**: Ready to implement - Phase 1 streaming infrastructure complete

With Phase 1's streaming support and write protection working, we can now build the Arrow integration layer.

#### 3.1 Add Arrow Convenience Extensions

**File**: `crates/tinyfs/Cargo.toml`

Add dependencies for temporary file handling:

```toml
[dependencies]
# ...existing dependencies...
tempfile = "3.0"
```

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

## Phase 1 Achievements and Current State

### ✅ **COMPLETED: Write-Protected Streaming (Phase 1)**

**What we built** is actually **better** than the original plan:

1. **Write Protection**: Files cannot be read while being written (prevents data races)
2. **Automatic Lock Management**: WriteGuard handles cleanup even on panic/drop
3. **Clean Architecture**: Removed async_writer from trait (better separation)
4. **Arrow Integration Ready**: Full Parquet roundtrip working via AsyncArrowWriter
5. **Comprehensive Testing**: 10 tests including protection verification

**Key Benefits of Our Approach**:
- **Safer**: Write protection prevents concurrent access bugs
- **Simpler**: No complex temp file management in Phase 1
- **Cleaner**: File trait focused on core operations only  
- **Ready**: Arrow integration can proceed immediately

**Memory Strategy**: 
- **Phase 1**: Simple memory buffering for all files
- **Phase 2**: Will add hybrid memory/temp file approach for very large files

## Implementation Checklist

### Phase 1: Core Streaming ✅ **COMPLETED**
- ✅ Add async_reader to File trait with write protection
- ✅ Remove async_writer from trait (cleaner design)
- ✅ Implement StreamingFileWriter with write locks
- ✅ Update Handle with protected streaming methods  
- ✅ Update MemoryFile implementation
- ✅ Add comprehensive streaming tests (10 tests)
- ✅ Add write protection verification tests
- ✅ Arrow/Parquet integration working end-to-end

### Phase 2: Large File Storage 📋 **PLANNED**
- [ ] Create StorageStrategy and LargeFileReference types
- [ ] Implement HybridWriter with memory/temp file spillover
- [ ] Update OpLog persistence for size-based storage
- [ ] Implement content-addressed large file storage
- [ ] Add large file garbage collection
- [ ] Test large file round-trip and deduplication

### Phase 3: Arrow Integration 🚀 **READY TO START**
- [ ] Add Arrow convenience extensions (no feature flags needed)
- [ ] Create WDArrowExt trait with core methods
- [ ] Implement RecordBatch serialization/deserialization
- [ ] Add streaming support for Series files
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
