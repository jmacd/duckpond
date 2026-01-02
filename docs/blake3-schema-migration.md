# BLAKE3 Hash Tree Migration for Remote Backup Schema

**Date:** January 1, 2026  
**Status:** In Progress  
**Related:** `docs/remote-backup-chunked-parquet-design.md`, `bao-tree-rs/ENCODING_AND_VALIDATION.md`

## Progress

### ✅ Phase 1: Remove Unnecessary Fields (COMPLETED)
Removed from `ChunkedFileRecord`:
- `cli_args: String` - Transaction-level, not appropriate per-chunk
- `created_at: i64` - Transaction-level, not appropriate per-chunk

These fields remain in `TransactionMetadata` struct where they belong.

### Phase 2: Replace CRC32/SHA256 with BLAKE3 (PENDING)
Fields still to be addressed:
- `chunk_crc32: i32` → Replace with BLAKE3 tree
- `total_sha256: String` → Replace with BLAKE3 root hash (in bundle_id)
- `chunk_count: i64` → Remove (redundant, derivable)

## Motivation

Replace the current CRC32-based chunk validation with BLAKE3 hash trees (via bao-tree-rs) to enable:
1. **Verified streaming**: Validate data incrementally without loading entire files
2. **Rsync capability**: Tree-based synchronization for efficient remote updates
3. **Stronger integrity**: Cryptographically secure validation vs CRC32
4. **Faster validation**: BLAKE3 is faster than SHA256 while being more secure

## Space Overhead Analysis

### Current Design (CRC32)
- **Per chunk**: 4 bytes
- **For 1GB file** (50MB chunks): 80 bytes total
- **Overhead**: ~0.0000076% (negligible)

### Proposed Design (BLAKE3 with 16KB blocks)
- **Block size**: 16 KiB (configurable via `BlockSize::from_chunk_log(4)`)
- **Hash size**: 32 bytes per hash, 64 bytes per parent node
- **For 1GB file**: ~64MB outboard data
- **Overhead**: ~6.4% → **0.39%** with 16KB blocks

### Block Size Trade-offs

| Block Size | Overhead % | 1TB Overhead | Granularity | Use Case |
|------------|-----------|--------------|-------------|----------|
| 1 KB | 6.25% | 64 GB | Maximum | Fine-grained validation |
| 4 KB | 1.56% | 16 GB | High | Good for small files |
| **16 KB** ✨ | **0.39%** | **4 GB** | **Balanced** | **Recommended default** |
| 64 KB | 0.098% | 1 GB | Low | Large files |
| 256 KB | 0.024% | 256 MB | Minimal | Bandwidth-constrained |

**Recommendation:** Use 16KB block size for optimal balance between overhead and verification granularity.

## Proposed Schema Changes

### Fields to REMOVE:
1. ✅ `chunk_crc32: i32` - Replaced by blake3 tree
2. ✅ `total_sha256: String` - Replaced by blake3 root hash (in bundle_id)
3. ✅ `chunk_count: i64` - Redundant, can be derived from data
4. ✅ `cli_args: String` - Transaction-level, not appropriate per-chunk
5. ✅ `created_at: i64` - Transaction-level, not appropriate per-chunk

### Fields to ADD/MODIFY:
- Bundle naming should use blake3 root hash instead of SHA256
- Need to store blake3 outboard data (~0.39% overhead at 16KB blocks)
- Need to store block size parameter

### Minimal Schema Proposal:
```rust
pub struct ChunkedFileRecord {
    // === PARTITION COLUMN ===
    pub bundle_id: String,        // "POND-FILE-{blake3_root_hash}"
    
    // === File identity ===
    pub pond_txn_id: i64,
    pub original_path: String,
    pub file_type: String,
    
    // === Chunk information ===
    pub chunk_id: i64,            // 0, 1, 2, ... for data chunks
    pub chunk_data: Vec<u8>,      // Actual data (10-100MB)
    
    // === File-level metadata ===
    pub total_size: i64,          // Total file size
    
    // === BLAKE3 verification (NEW) ===
    pub bao_outboard: Vec<u8>,    // Tree hash data (~0.39% of file)
    pub bao_block_size: i32,      // 16384 (16KB recommended)
    pub bao_root_hash: String,    // BLAKE3 root for verification
}
```

## Open Questions (NEED ANSWERS)

### 1. Bundle ID Format
**Current:** `POND-FILE-{sha256}` (64 hex chars)  
**Proposed:** `POND-FILE-{blake3_root_hash}` (64 hex chars)

**Question:** Confirm we use BLAKE3 root hash (hex encoded) as bundle identifier?

### 2. Outboard Storage Strategy
For a 50MB file with 16KB blocks, we have:
- File data: 50MB (your existing chunk structure)
- Outboard data: ~200KB (0.39% overhead)

**Option A - Separate Outboard Record:**
```rust
// Data chunks: chunk_id = 0, 1, 2, ...
chunk_id: 0
chunk_data: Vec<u8>      // 50MB chunk
bao_outboard: vec![]     // Empty

// Outboard record: chunk_id = -1
chunk_id: -1
chunk_data: vec![]       // Empty
bao_outboard: Vec<u8>    // ~200KB
bao_block_size: 16384
bao_root_hash: String
```

**Option B - Store in First Chunk:**
```rust
// First chunk has both data and outboard
chunk_id: 0
chunk_data: Vec<u8>      // 50MB chunk
bao_outboard: Vec<u8>    // ~200KB (only in chunk 0)
bao_block_size: 16384
bao_root_hash: String

// Subsequent chunks
chunk_id: 1, 2, ...
chunk_data: Vec<u8>
bao_outboard: vec![]     // Empty for chunks 1+
```

**Option C - Separate Table/Partition:**
```rust
// Main data table: no outboard fields
ChunkedFileRecord { ... }

// Separate outboard table
BaoOutboardRecord {
    bundle_id: String,
    bao_outboard: Vec<u8>,
    bao_block_size: i32,
    bao_root_hash: String,
}
```

**Question:** Which storage strategy should we use? Option B (first chunk) is simplest but breaks uniformity.

### 3. Block Size Configuration
**Option A - Field (Flexible):**
- Store `bao_block_size` per file
- Allows optimization per file type
- More complex but future-proof

**Option B - Constant (Simple):**
```rust
pub const BAO_BLOCK_SIZE: usize = 16 * 1024; // 16KB
```
- Hardcoded for entire system
- Simpler, less storage
- Can't optimize later without migration

**Question:** Store block size as field or hardcode constant?

### 4. Transaction Metadata Location
The `TransactionMetadata` struct needs `cli_args` and `created_at`, but we're removing them from per-chunk records.

**Option A - Keep in Metadata FileType:**
```rust
// When file_type = "metadata", these fields are present
// When file_type = "pond_parquet" or "large_file", they're empty/default
```

**Option B - Separate Transaction Table:**
```rust
pub struct TransactionRecord {
    pub pond_id: String,
    pub pond_txn_id: i64,
    pub cli_args: Vec<String>,
    pub created_at: i64,
    pub file_count: usize,
}
```

**Option C - Delta Lake Transaction Metadata:**
- Store in Delta Lake commit metadata
- Not queryable as easily

**Question:** Where should transaction-level metadata live after removing from chunk records?

### 5. FileInfo Struct Update
The `FileInfo` struct currently has:
```rust
pub sha256: String,  // = bundle_id for this file
```

**Question:** Rename to `blake3_hash: String` or keep as generic `content_hash: String`?

### 6. Migration Strategy
**Question:** How to handle existing backups with SHA256/CRC32? Options:
- Support both formats (detect by bundle_id prefix)
- One-time migration script
- Version field in schema

### 7. Validation Points
**Question:** When/where should we validate:
- On write (before storing)?
- On read (when retrieving)?
- Both?
- Periodic background validation?

## Implementation Checklist (AFTER DECISIONS)

- [ ] Finalize schema with answered questions above
- [ ] Update `ChunkedFileRecord` struct
- [ ] Update `arrow_schema()` and `delta_schema()` methods
- [ ] Update bundle_id generation methods
- [ ] Update `FileInfo` struct
- [ ] Implement bao-tree integration in writer.rs
- [ ] Implement bao-tree validation in reader.rs (if created)
- [ ] Add constants for block size
- [ ] Update tests
- [ ] Update documentation
- [ ] Migration plan for existing data (if needed)

## References

- **bao-tree library**: `./bao-tree-rs/`
- **Encoding guide**: `./bao-tree-rs/ENCODING_AND_VALIDATION.md`
- **Current schema**: `crates/remote/src/schema.rs`
- **Original design**: `docs/remote-backup-chunked-parquet-design.md`

## Key Insights from bao-tree

1. **Tree structure**: Binary Merkle tree with configurable block sizes
2. **Outboard format**: Pre-order traversal, 64 bytes per parent node
3. **Root hash**: Single 32-byte BLAKE3 hash proves entire file
4. **Streaming validation**: Can verify chunks as they arrive
5. **Rsync support**: Tree structure enables efficient delta sync

## Next Steps

1. **Decision Meeting**: Answer all 7 open questions above
2. **Prototype**: Implement chosen design in a branch
3. **Benchmark**: Measure actual overhead and performance
4. **Review**: Validate against requirements
5. **Migrate**: Plan for existing data transition
