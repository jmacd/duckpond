# Bao-Tree Utility Library Design

This document describes the design of the `bao_outboard` module in the `utilities` crate, which provides incremental BLAKE3 hash computation using bao-tree structures for verified streaming and data integrity.

## Overview

The bao-tree library enables:
- **Verified streaming**: Validate data block-by-block as it's read, without loading the entire file into memory
- **Incremental checksumming**: Append new data without re-hashing the entire file
- **Corruption detection**: Identify which 16KB block contains corrupted data

## Block Size

Uses **16KB blocks** (`chunk_log=4`), which means:
- Each block contains 16 BLAKE3 chunks (1KB each)
- Overhead is ~0.4% (one hash pair per block, minus one)
- Good balance between granularity and efficiency

## Unified `blake3` Field Semantics

The `blake3` metadata field stores the **bao-tree root hash** (not simple `blake3::hash(content)`):

| Entry Type | `blake3` field contains |
|------------|------------------------|
| `FilePhysicalVersion` | `bao_root(version_content, offset=0)` |
| `FilePhysicalSeries` | `bao_root(v1\|\|v2\|\|...\|\|vN)` - cumulative |
| `TablePhysicalVersion` | `bao_root(version_content, offset=0)` |
| `TablePhysicalSeries` | `bao_root(version_content, offset=0)` - per-version |
| `DirectoryPhysical` | `bao_root(content, offset=0)` |
| `Symlink` | `blake3::hash(target_path)` - simple hash (always < 16KB) |

**Key insight**: Data is hashed exactly once at write time. For series types, each version's `blake3` is the cumulative root through that version - no redundant hashing.

## Entry Type Validation Strategies

Different TinyFS entry types use different validation approaches:

| EntryType              | Strategy                   | Notes                              |
|------------------------|----------------------------|------------------------------------|
| `DirectoryPhysical`    | Fresh `compute_outboard()` | Arrow IPC encoding in contents     |
| `DirectoryDynamic`     | None                       | Factory-generated on demand        |
| `Symlink`              | `blake3::hash()` only      | Always < 16KB, no outboard needed  |
| `FilePhysicalVersion`  | Fresh `compute_outboard()` | Each version starts at byte 0      |
| `FilePhysicalSeries`   | **Incremental resume**     | Cumulative across versions         |
| `FileDynamic`          | None                       | Factory-generated on demand        |
| `TablePhysicalVersion` | Fresh `compute_outboard()` | Each version starts at byte 0      |
| `TablePhysicalSeries`  | Fresh `compute_outboard()` | Each version independent (parquet) |
| `TableDynamic`         | None                       | Factory-generated on demand        |

**Key insight**: Only `FilePhysicalSeries` uses incremental checksumming with resume capability. `TablePhysicalSeries` versions are independent parquet files that cannot be concatenated, so each version is verified at offset 0. All other physical types also compute fresh outboards starti\
ng at offset 0.

## Validation for Series Types

### FilePhysicalSeries (Cumulative)

Readers read the entire concatenated `FilePhysicalSeries`. Validation computes the cumulative bao-tree root and compares against the stored `blake3`:

```rust
let mut state = IncrementalHashState::new();
for version in all_versions_in_order {
    state.ingest(&version.content);
}
assert_eq!(state.root_hash().to_hex(), metadata.blake3);
```

Individual versions are **not** verified in isolation - they're verified as part of the cumulative chain. This is acceptable because readers always read the concatenated series.

### TablePhysicalSeries (Per-Version)

Each version of a `TablePhysicalSeries` is an independent parquet file. Unlike raw file data, parquet files cannot be concatenated - each is a self-contained unit with its own schema, row groups, and footer.

Validation is per-version at offset 0:

```rust
// Each version verified independently
let outboard = compute_outboard(&version.content);
assert_eq!(outboard.root_hash().to_hex(), version.blake3);
```

## Large File Storage and Validation

Large files (≥1MB) are stored as chunked parquet with `SeriesOutboard` per chunk. The chunk offset within a `FilePhysicalSeries` determines bao-tree offset:

```
FilePhysicalSeries with large file at version 2:
  Version 0: 50KB inline
  Version 1: 30KB inline  
  Version 2: 100MB large file → stored as chunked parquet
    Chunk 0: offset = 80KB (cumulative size after v0+v1)
    Chunk 1: offset = 80KB + 16MB
    Chunk 2: offset = 80KB + 32MB
    ...
```

**Validation moves to persistence layer** because:
1. Persistence layer knows storage format (inline vs chunked parquet)
2. Persistence layer stores the outboard data (in OpLogEntry or parquet rows)
3. TinyFS just needs validated bytes - doesn't care HOW validation happens

```
┌─────────────────────────────────────────────────────────┐
│                      TinyFS                              │
│  - Calls persistence.read_file_series()                 │
│  - Gets back VALIDATED bytes via AsyncRead              │
│  - Does NOT know about bao-tree storage format          │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│              Persistence Layer                          │
│  Implements validation based on storage format:         │
│                                                         │
│  Memory:     BaoValidatingReader(whole_version_bytes)   │
│  tlogfs:     BaoValidatingReader(inline_content)        │
│  tlogfs+lg:  ChunkedBaoReader(parquet, cumulative_off)  │
│                                                         │
│  All return AsyncRead that yields validated bytes       │
└─────────────────────────────────────────────────────────┘
```

## Core Data Structures

### `VersionOutboard`

Simple outboard for `FilePhysicalVersion` entries:

```rust
pub struct VersionOutboard {
    pub outboard: Vec<u8>,  // Post-order hash pairs
    pub size: u64,          // Content size
}
```

### `SeriesOutboard`

Outboard for `FilePhysicalSeries` entries with incremental checksumming support:

```rust
pub struct SeriesOutboard {
    pub version_outboard: Vec<u8>,     // Offset=0 outboard for this version (large files only)
    pub cumulative_outboard: Vec<u8>,  // Outboard for v1..vN concatenated content
    pub frontier: Vec<(u32, [u8; 32], u64)>,  // Rightmost path for resumption
    pub version_size: u64,
    pub cumulative_size: u64,
}
```

The `frontier` field stores the rightmost path of the Merkle tree:
- Each entry: `(level, hash, start_block)`
- Level 0 = single 16KB block, level 1 = 2 blocks merged, etc.
- This is O(log N) where N is the number of complete blocks

### `IncrementalHashState`

Internal state for building the Merkle tree incrementally:

```rust
pub struct IncrementalHashState {
    pub total_size: u64,
    pub pending_block: Vec<u8>,  // 0 to 16KB-1 bytes not yet in a complete block
    completed_subtrees: Vec<(u32, blake3::Hash, u64)>,  // The frontier
}
```

## The Pending Bytes Problem

When computing cumulative checksums across versions, blocks may span version boundaries:

```
Version 0: 10KB (no complete blocks, 10KB pending)
Version 1: 6KB  (10KB + 6KB = 16KB → 1 complete block, 0KB pending)
Version 2: 20KB (20KB → 1 complete block, 4KB pending)
```

### Solution: Re-read from Verified Storage

**We do NOT store pending bytes in the outboard.** Instead:

1. Store `cumulative_size` in `SeriesOutboard`
2. Calculate `pending_size = cumulative_size % BLOCK_SIZE`
3. When resuming, read the tail of cumulative content through normal TinyFS read APIs
4. TinyFS read path validates the content (using version hashes or bao-tree)
5. Pass verified pending bytes to `append_version_*()`

This is safe because:
- We never trust bytes we haven't verified
- The read path always validates content
- No additional storage overhead for pending bytes

## API Usage

### Creating First Version

```rust
// For inline content (< threshold)
let outboard = SeriesOutboard::first_version_inline(content);

// For large files
let outboard = SeriesOutboard::first_version_large(content);
```

### Appending Subsequent Versions

```rust
// 1. Deserialize previous outboard
let prev = SeriesOutboard::from_bytes(&stored_bytes)?;

// 2. Calculate pending bytes needed
let pending_size = (prev.cumulative_size % BLOCK_SIZE as u64) as usize;

// 3. Read pending bytes from cumulative content (TinyFS validates)
let pending_bytes = read_tail_of_file(pending_size);

// 4. Append new version
let new_outboard = SeriesOutboard::append_version_inline(
    &prev,
    &pending_bytes,
    &new_content,
);

// 5. Serialize and store
let bytes = new_outboard.to_bytes();
```

### Resuming Incremental State Directly

```rust
// Resume from stored frontier
let state = IncrementalHashState::resume(
    &frontier,           // From SeriesOutboard.frontier
    cumulative_size,
    &verified_pending,   // Read from TinyFS
)?;

// Continue ingesting
state.ingest(&new_data);

// Get results
let hash = state.root_hash();
let new_frontier = state.to_frontier();
```

## Verified Streaming with BaoValidatingReader

For reading large files with block-by-block validation:

```rust
let reader = BaoValidatingReader::new(
    inner_reader,  // The raw data source
    root_hash,     // Expected root hash
    outboard,      // The stored outboard bytes
    size,          // Content size
)?;

// Read validates each 16KB block as accessed
let mut buf = vec![0u8; 1024];
reader.read(&mut buf)?;  // Validates block containing this range
```

For small files (≤ 16KB), validation uses simple `blake3::hash()` comparison.

## Serialization Format

### SeriesOutboard v2 Format

```
[1 byte]  Version marker: 0x02
[8 bytes] version_size (little-endian u64)
[8 bytes] cumulative_size (little-endian u64)
[4 bytes] version_outboard length (u32)
[N bytes] version_outboard data
[4 bytes] cumulative_outboard length (u32)
[M bytes] cumulative_outboard data
[4 bytes] frontier length (number of entries, u32)
[44 bytes per entry] frontier entries:
  - [4 bytes] level (u32)
  - [32 bytes] hash
  - [8 bytes] start_block (u64)
```

The format is backward compatible - v1 data (no version marker) is parsed as legacy format with empty frontier.

## Frontier Structure Examples

The frontier reflects the binary representation of the block count:

| Blocks | Binary | Frontier Levels | Description |
|--------|--------|-----------------|-------------|
| 1      | `001`  | [0]             | 1 block at level 0 |
| 2      | `010`  | [1]             | 2 blocks merged to level 1 |
| 3      | `011`  | [1, 0]          | 2 blocks at level 1, 1 at level 0 |
| 4      | `100`  | [2]             | 4 blocks merged to level 2 |
| 5      | `101`  | [2, 0]          | 4 blocks at level 2, 1 at level 0 |
| 7      | `111`  | [2, 1, 0]       | 4 + 2 + 1 blocks |
| 8      | `1000` | [3]             | 8 blocks merged to level 3 |

This is always O(log N) entries.

## Implementation Notes

### Why Post-Order Outboard?

The bao-tree library supports both pre-order and post-order outboard formats. We use **post-order** because it's better for append-only files - new hash pairs are appended to the end rather than requiring rewrites.

### Outboard vs Frontier

- **Outboard**: Stores hash pairs for all internal tree nodes (for validation)
- **Frontier**: Stores only the rightmost path hashes (for resumption)

Both are needed for `FilePhysicalSeries`:
- `cumulative_outboard` enables verified streaming of the concatenated content
- `frontier` enables efficient incremental checksumming when appending

### Hash Computation

Block hashes use BLAKE3's offset-aware hashing:

```rust
fn hash_block(data: &[u8], block_num: u64, is_root: bool) -> blake3::Hash {
    if is_root && data.len() <= BLOCK_SIZE {
        blake3::hash(data)
    } else {
        let mut hasher = blake3::Hasher::new();
        hasher.set_input_offset(block_num * BLOCK_SIZE as u64);
        hasher.update(data);
        blake3::Hash::from(hasher.finalize_non_root())
    }
}
```

This ensures the same content at different offsets produces different hashes, which is critical for verified streaming to work correctly.
## Persistence Layer Validation API

Validation is implemented in the persistence layer because:
1. Persistence layer knows storage format (inline vs chunked parquet)
2. Persistence layer stores the outboard data (OpLogEntry or parquet rows)  
3. TinyFS needs validated bytes, doesn't care about storage details

### New Trait Methods

```rust
#[async_trait]
pub trait PersistenceLayer: Send + Sync {
    // ... existing methods ...
    
    /// Read a file series as a validated stream
    /// 
    /// Returns a reader that validates content during read using bao-tree.
    /// For inline content: validates against SeriesOutboard in OpLogEntry
    /// For large files: validates chunks against SeriesOutboard in parquet rows
    /// 
    /// The cumulative_offset is the byte offset where this content starts
    /// in the overall file series (for resuming incremental checksumming).
    async fn read_file_series_validated(
        &self,
        id: FileID,
    ) -> Result<Pin<Box<dyn AsyncRead + Send>>>;
    
    /// Read a single file version as a validated stream
    async fn read_file_version_validated(
        &self,
        id: FileID,
        version: u64,
    ) -> Result<Pin<Box<dyn AsyncRead + Send>>>;
}
```

### Implementation by Persistence Type

**MemoryPersistence:**
```rust
async fn read_file_series_validated(&self, id: FileID) -> Result<...> {
    // Read all versions, concatenate
    let content = self.read_all_versions(id).await?;
    let metadata = self.metadata(id).await?;
    
    // Wrap in BaoValidatingReader using stored outboard
    let outboard = SeriesOutboard::from_bytes(&metadata.bao_outboard)?;
    Ok(BaoValidatingReader::new(content, outboard))
}
```

**TLogFS (inline content):**
```rust
async fn read_file_series_validated(&self, id: FileID) -> Result<...> {
    // Same as MemoryPersistence - content is in OpLogEntry
    // ... similar to above
}
```

**TLogFS (large file):**
```rust
async fn read_file_series_validated(&self, id: FileID) -> Result<...> {
    // Returns ChunkedBaoReader that:
    // 1. Loads chunks from parquet on demand
    // 2. Validates each chunk using SeriesOutboard at cumulative offset
    // 3. Tracks cumulative offset across chunks
    Ok(ChunkedBaoReader::new(parquet_path, cumulative_start_offset))
}
```

### ChunkedBaoReader for Large Files

```rust
pub struct ChunkedBaoReader {
    parquet_path: PathBuf,
    cumulative_offset: u64,  // Offset in overall file series
    current_chunk: Option<ValidatedChunk>,
    chunk_index: i64,
}

impl ChunkedBaoReader {
    async fn load_and_validate_chunk(&mut self, chunk_id: i64) -> Result<Vec<u8>> {
        // 1. Load chunk data and SeriesOutboard from parquet row
        let (chunk_data, series_outboard) = self.load_chunk(chunk_id).await?;
        
        // 2. Validate chunk at cumulative offset
        // The SeriesOutboard encodes:
        //   - cumulative_size: total bytes through this chunk
        //   - cumulative_blake3: bao root through this chunk
        //   - frontier: for resumption
        verify_chunk_at_offset(
            &chunk_data,
            &series_outboard,
            self.cumulative_offset,
        )?;
        
        // 3. Advance cumulative offset
        self.cumulative_offset += chunk_data.len() as u64;
        
        Ok(chunk_data)
    }
}
```

### Verification at Cumulative Offset

For chunk N of a large file that starts at cumulative offset O:

```rust
fn verify_chunk_at_offset(
    chunk_data: &[u8],
    series_outboard: &SeriesOutboard,
    start_offset: u64,
) -> Result<()> {
    // Chunk's cumulative_size should equal start_offset + chunk_data.len()
    let expected_end = start_offset + chunk_data.len() as u64;
    if series_outboard.cumulative_size != expected_end {
        return Err("Size mismatch");
    }
    
    // To verify cumulative_blake3, we need the previous chunk's frontier
    // This is stored in the previous chunk's SeriesOutboard
    // For chunk 0: frontier is empty (fresh start at offset=start_offset)
    // For chunk N: frontier comes from chunk N-1's SeriesOutboard
    
    // Reconstruct state and verify
    let mut state = IncrementalHashState::resume(
        &prev_frontier,
        start_offset,
        &pending_bytes,  // Read from previous chunk's tail
    )?;
    state.ingest(chunk_data);
    
    let computed = state.root_hash();
    let stored = blake3::Hash::from_bytes(series_outboard.cumulative_blake3);
    
    if computed != stored {
        return Err("Hash mismatch");
    }
    
    Ok(())
}
```