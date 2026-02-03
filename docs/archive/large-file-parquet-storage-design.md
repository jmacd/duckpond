# Large File Parquet Storage Design

**Date:** January 1, 2026  
**Status:** Planning  
**Goal:** Store large files locally in chunked parquet format for seamless remote backup/restore

## Overview

This design unifies local and remote large file storage by using the same chunked parquet format for both. This eliminates the need to chunk/reconstruct files during backup/restore operations - we can simply copy the parquet files directly.

## Current Architecture (Before)

```
Local Pond:
  _large_files/
    sha256=abc123...def.data         # Raw binary file (64KB+)
    sha256_16=ab/
      sha256=abc456...ghi.data       # Raw binary file

Remote Backup:
  remote_pond/ (Delta Lake)
    bundle_id=POND-FILE-{blake3}/    # Chunked parquet
      part-00000.parquet              # Contains chunks with BLAKE3 hashes
```

**Backup Process:**
1. Read raw file from `_large_files/`
2. Chunk into 16MB pieces
3. Compute BLAKE3 hashes per chunk
4. Write to remote as chunked parquet

**Restore Process:**
1. Read chunks from remote parquet
2. Verify BLAKE3 hashes
3. Reconstruct raw file
4. Write to `_large_files/`

## New Architecture (After)

```
Local Pond:
  _large_files/
    blake3={hash}/                   # Content-addressed by BLAKE3 root hash
      chunks.parquet                  # Chunked parquet (same schema as remote)

Remote Backup:
  remote_pond/ (Delta Lake)
    bundle_id=POND-FILE-{blake3}/    # Same parquet structure
      part-00000.parquet              # Copied directly from local
```

**Backup Process:**
1. Scan `_large_files/blake3={hash}/` for parquet files
2. **Copy parquet file directly to remote** with correct bundle_id
3. No chunking or hashing needed (already done at write time)

**Restore Process:**
1. Query remote for large file parquets
2. **Copy parquet files directly to `_large_files/blake3={hash}/`**
3. No reconstruction needed

## Schema (Shared Between Local and Remote)

Both local and remote use the **same** `ChunkedFileRecord` schema:

```rust
{
    bundle_id: String,           // "POND-FILE-{blake3_root_hash}"
    pond_txn_id: i64,           // Transaction when file was created
    path: String,               // "_large_files/blake3={hash}/chunks.parquet"
    chunk_id: i64,              // 0, 1, 2, ...
    chunk_hash: String,         // BLAKE3 subtree hash (hex)
    chunk_outboard: Binary,     // bao-tree Merkle outboard
    chunk_data: Binary,         // Actual data (16MB chunks)
    total_size: i64,            // Total file size
    root_hash: String,          // BLAKE3 root hash
}
```

## Directory Structure

### Local Pond

```
my_pond/
├── _delta_log/                      # Delta Lake transaction log
├── _large_files/                    # Large file storage
│   ├── blake3=abc123.../            # Content-addressed by BLAKE3
│   │   └── chunks.parquet           # Chunked parquet file
│   └── blake3=def456.../
│       └── chunks.parquet
└── part-00000-*.parquet             # Regular Delta Lake data files
```

**Key Points:**
- Directory name: `blake3={root_hash}` (first 64 hex chars of BLAKE3)
- File name: `chunks.parquet` (standard name)
- Content-addressed: Same file content → same BLAKE3 → same directory
- Automatic deduplication: Multiple references to same hash use same parquet

### Remote Backup

```
remote_pond/
├── _delta_log/                      # Delta Lake transaction log
└── bundle_id=POND-FILE-abc123.../   # Partition by BLAKE3 root hash
    └── part-00000.parquet           # Copied from local _large_files/
```

## OpLog Entry Schema

The OpLog entries remain the same, but the interpretation changes:

```rust
OplogEntry {
    node_id: String,           // "file_node_123"
    part_id: String,           // "part_001"
    content: Option<Vec<u8>>,  // None for large files
    sha256: Option<String>,    // BLAKE3 root hash (hex) for large files
    // ...
}
```

**Small files (< 64KB):** `content` contains data, `sha256` is None  
**Large files (≥ 64KB):** `content` is None, `sha256` contains BLAKE3 root hash

**Note:** We're keeping the field name as `sha256` for backward compatibility, but it now contains BLAKE3 hashes for large files.

## Implementation Changes

### 1. Large File Writing (tlogfs/large_files.rs)

**Current:**
```rust
// Writes raw binary to _large_files/sha256={hash}.data
pub async fn large_file_path(pond_path: &str, sha256: &str) -> PathBuf {
    pond_path.join("_large_files").join(format!("sha256={}", sha256))
}
```

**New:**
```rust
// Writes chunked parquet to _large_files/blake3={hash}/chunks.parquet
pub async fn large_file_parquet_path(pond_path: &str, blake3_hash: &str) -> PathBuf {
    pond_path.join("_large_files")
        .join(format!("blake3={}", blake3_hash))
        .join("chunks.parquet")
}

// Write large file as chunked parquet
pub async fn write_large_file_parquet(
    pond_path: &str,
    content: &[u8],
    pond_txn_id: i64,
) -> std::io::Result<String> {
    // Use ChunkedWriter from remote crate
    let reader = std::io::Cursor::new(content);
    let writer = remote::ChunkedWriter::new(
        pond_txn_id,
        "_large_files/file",
        reader,
    );
    
    // Write to in-memory parquet
    let parquet_bytes = writer.write_to_bytes().await?;
    let root_hash = writer.root_hash();
    
    // Save to disk
    let path = large_file_parquet_path(pond_path, &root_hash).await?;
    tokio::fs::create_dir_all(path.parent().unwrap()).await?;
    tokio::fs::write(&path, &parquet_bytes).await?;
    
    Ok(root_hash)
}

// Read large file from chunked parquet
pub async fn read_large_file_parquet(
    pond_path: &str,
    blake3_hash: &str,
) -> std::io::Result<Vec<u8>> {
    let path = large_file_parquet_path(pond_path, blake3_hash).await?;
    
    // Use ChunkedReader from remote crate
    let parquet_bytes = tokio::fs::read(&path).await?;
    let mut output = Vec::new();
    
    remote::ChunkedReader::from_bytes(&parquet_bytes)
        .read_to_end(&mut output)
        .await?;
    
    Ok(output)
}
```

### 2. Remote Backup Push (remote/factory.rs)

**Current:**
```rust
// Reads raw files and chunks them
for (abs_path, relative_path, size) in &large_files_to_backup {
    let file_data = tokio::fs::read(&abs_path).await?;
    let reader = std::io::Cursor::new(file_data);
    remote_table.write_file(current_version, relative_path, reader).await?;
}
```

**New:**
```rust
// Directly copies parquet files to remote
for (parquet_path, blake3_hash) in &large_files_to_backup {
    let parquet_bytes = tokio::fs::read(&parquet_path).await?;
    
    // The parquet already has the correct schema and bundle_id
    // Just need to register it with the remote Delta table
    let bundle_id = format!("POND-FILE-{}", blake3_hash);
    
    remote_table.add_existing_parquet_file(
        &bundle_id,
        &parquet_bytes,
    ).await?;
}
```

### 3. Remote Restore Pull (remote/factory.rs)

**Current:**
```rust
// Reconstructs file from chunks
let mut output = Vec::new();
remote_table.read_file(&bundle_id, &original_path, pond_txn_id, &mut output).await?;
tokio::fs::write(&large_file_fs_path, &output).await?;
```

**New:**
```rust
// Directly copies parquet file to local
let parquet_bytes = remote_table.read_parquet_file(&bundle_id).await?;

let local_path = pond_path
    .join("_large_files")
    .join(format!("blake3={}", extract_hash_from_bundle_id(&bundle_id)))
    .join("chunks.parquet");
    
tokio::fs::create_dir_all(local_path.parent().unwrap()).await?;
tokio::fs::write(&local_path, &parquet_bytes).await?;
```

## Migration Strategy

### Phase 1: Support Both Formats (Read)

```rust
pub async fn read_large_file(pond_path: &str, hash: &str) -> std::io::Result<Vec<u8>> {
    // Try new parquet format first
    let parquet_path = format!("_large_files/blake3={}/chunks.parquet", hash);
    if Path::new(&parquet_path).exists() {
        return read_large_file_parquet(pond_path, hash).await;
    }
    
    // Fall back to old raw format
    let old_path = find_large_file_path(pond_path, hash).await?;
    if let Some(path) = old_path {
        return tokio::fs::read(&path).await;
    }
    
    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("Large file not found: {}", hash),
    ))
}
```

### Phase 2: Write Only New Format

All new large files written as chunked parquet. Old files remain in raw format until GC or explicit migration.

### Phase 3: Optional Migration Tool

```bash
# Convert all raw large files to parquet
duckpond migrate large-files --pond-path /path/to/pond
```

## Benefits

1. **Simplified Backup/Restore:** Direct file copy instead of chunk/reconstruct
2. **Consistent Format:** Same schema locally and remotely
3. **Streaming Verification:** BLAKE3 validation works the same everywhere
4. **Deduplication:** Same BLAKE3 hash = same parquet file
5. **Future-Proof:** Can add compression, encryption to parquet without changing flow

## Testing Strategy

1. **Unit Tests:**
   - Write large file → verify parquet format
   - Read large file → verify content matches
   - BLAKE3 hash verification

2. **Integration Tests:**
   - Backup large file → verify remote parquet matches local
   - Restore large file → verify local parquet matches remote
   - Mixed format (old + new) → verify both read correctly

3. **Performance Tests:**
   - Backup speed: parquet copy vs chunking
   - Restore speed: parquet copy vs reconstruction
   - Disk space: parquet overhead vs raw files

## Risks and Mitigations

**Risk 1:** Parquet overhead for small "large" files (64KB-1MB)
- **Mitigation:** Adjust threshold to 1MB or keep hybrid approach for this range

**Risk 2:** Existing ponds have raw large files
- **Mitigation:** Support reading both formats during migration period

**Risk 3:** Parquet corruption
- **Mitigation:** BLAKE3 verification on read; remote backup remains intact

## Open Questions

1. **Directory structure:** `blake3={hash}/chunks.parquet` or `blake3={hash}.parquet`?
   - **Recommendation:** `blake3={hash}/chunks.parquet` (allows future metadata files)

2. **Field naming:** Keep `sha256` field name or rename to `blake3`?
   - **Recommendation:** Keep `sha256` for backward compatibility, document that it's BLAKE3

3. **Migration timeline:** Mandatory or optional?
   - **Recommendation:** Optional, support both formats indefinitely

4. **Threshold adjustment:** Still 64KB or increase to 1MB?
   - **Recommendation:** Keep 64KB, parquet overhead is acceptable

## Implementation Order

1. ✅ Design specification (this document)
2. ⏳ Update `large_files.rs` with parquet read/write functions
3. ⏳ Modify `HybridWriter` to output parquet
4. ⏳ Update `OpLogPersistence` load/store methods
5. ⏳ Simplify remote backup push/pull operations
6. ⏳ Add migration support for raw→parquet conversion
7. ⏳ Update tests and documentation

---

**Status:** Ready for implementation
**Estimated Effort:** 2-3 days
**Breaking Changes:** None (backward compatible with migration period)
